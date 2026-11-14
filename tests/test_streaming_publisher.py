"""Streaming input publication never needs a local video tree."""

import hashlib
import sqlite3
from dataclasses import replace

import pytest

from people_counter.adls import PublicationConflictError, RemoteObject
from people_counter.manifest import (
    ManifestPublisherError,
    VideoInspection,
    VideoValidationError,
    build_manifest_plan,
    load_camera_catalog,
)
from people_counter.manifest_publisher import (
    ManifestPackagePublishConfig,
    PublisherConfig,
    publish_manifest_package,
    run_publisher,
    validate_manifest_package,
)
from people_counter.video_input import InputStat, LocalVideoInput
from tests.test_manifest_publisher import FakeStorage, write_catalog
from tests.test_manifest_publisher import adls_storage as _adls_storage

adls_storage = _adls_storage


RELATIVE = "north/camera-17/clip-20260601T120000Z.mp4"
CONTENT = b"complete video bytes including the very last frame"


class RemoteInput:
    def __init__(self):
        self.content = CONTENT
        self.etag = '"original"'
        self.reads = 0
        self.change_on_read = None
        self.stat_change = None

    def paths(self):
        return [RELATIVE]

    def local_path(self, relative):
        assert relative == RELATIVE

    def stat(self, relative):
        assert relative == RELATIVE
        return InputStat(len(self.content), self.etag)

    def inspect(self, relative):
        return VideoInspection(
            len(self.content), hashlib.sha256(self.content).hexdigest(),
            1920, 1080, 30.0,
        )

    def chunks(self, relative):
        assert relative == RELATIVE
        self.reads += 1
        if self.reads == self.change_on_read:
            self.content = self.content[:-1] + b"!"
        content = self.content
        for offset in range(0, len(content), 7):
            yield content[offset:offset + 7]
        if self.stat_change:
            self.etag = self.stat_change


class StreamingStorage(FakeStorage):
    def upload_stream(self, chunks, remote_path, content_type, expected_size, expected_sha256):
        self.operations.append(("upload_stream", remote_path, content_type))
        content = b"".join(chunks())
        if len(content) != expected_size or hashlib.sha256(content).hexdigest() != expected_sha256:
            raise PublicationConflictError("Source video changed during upload")
        self.objects[remote_path] = (content, '"video-etag"')

    def upload_file(self, *args):
        raise AssertionError("remote input must not upload a local file")


@pytest.fixture
def prepared(tmp_path):
    source = RemoteInput()
    catalog = tmp_path / "catalog.csv"
    write_catalog(catalog)
    config = PublisherConfig(
        catalog_path=catalog, inventory_path=None,
        video_root=tmp_path / "nonexistent-videos", partition_prefix=None,
        storage_account="testaccount", filesystem="footage",
        staging_prefix="stage", incoming_prefix="incoming",
        checkpoint_path=tmp_path / "prepare.sqlite",
        rejection_report_path=tmp_path / "rejected.csv",
        generated_inventory_report_path=tmp_path / "inventory.csv",
        max_files=10, dry_run=True, output_dir=tmp_path / "package",
        summary_report_path=tmp_path / "summary.json",
    )
    summary = run_publisher(config, storage=None, input_source=source)
    assert summary.planned == 1 and summary.rejected == 0
    assert not config.video_root.exists()
    publish = ManifestPackagePublishConfig(
        config.output_dir, config.video_root, "testaccount", "footage",
        "stage", "incoming", tmp_path / "publish.sqlite",
        tmp_path / "publish-rejected.csv", tmp_path / "publish-summary.json", 7,
    )
    return source, publish


def test_metadata_only_prepare_validate_and_publish(prepared):
    source, config = prepared
    validated = validate_manifest_package(config, input_source=source)
    assert validated.plans[0].local_path is None
    assert source.reads == 1
    storage = StreamingStorage()
    summary = publish_manifest_package(config, storage, input_source=source)
    assert summary.published == 1 and summary.rejected == 0
    plan = validated.plans[0]
    assert storage.read_bytes(plan.incoming_video_path) == CONTENT
    assert storage.sha256(plan.incoming_video_path) == plan.inspection.sha256
    assert storage.operations[-1] == (
        "rename", plan.staging_manifest_path, plan.incoming_manifest_path,
    )
    before = list(storage.operations)
    assert publish_manifest_package(config, storage, input_source=source).already_published == 1
    assert storage.operations == before


@pytest.mark.parametrize("change", ["late-byte", "truncation", "etag", "size"])
def test_original_identity_is_checked_before_storage_mutation(prepared, change):
    source, config = prepared
    validated = validate_manifest_package(config, input_source=source)
    if change == "late-byte":
        source.content = CONTENT[:-1] + b"!"
    elif change == "truncation":
        source.content = CONTENT[:-1]
    elif change == "size":
        source.content += b"!"
    else:
        source.stat_change = '"changed"'
    storage = StreamingStorage()
    summary = publish_manifest_package(
        config, storage, validated_package=validated, input_source=source,
    )
    assert summary.rejected == 1 and summary.published == 0
    assert storage.operations == []


def test_change_during_upload_does_not_promote_video_or_manifest(prepared):
    source, config = prepared
    validated = validate_manifest_package(config, input_source=source)
    source.change_on_read = source.reads + 2
    storage = StreamingStorage()
    summary = publish_manifest_package(
        config, storage, validated_package=validated, input_source=source,
    )
    assert summary.rejected == 1
    assert not any(operation[0] == "rename" for operation in storage.operations)


def test_source_version_change_after_upload_is_checked_before_promotion(prepared, monkeypatch):
    source, config = prepared
    storage = StreamingStorage()
    upload = storage.upload_stream

    def changed_after_upload(*args):
        upload(*args)
        source.etag = '"new-version"'

    monkeypatch.setattr(storage, "upload_stream", changed_after_upload)
    summary = publish_manifest_package(config, storage, input_source=source)
    assert summary.rejected == 1 and summary.published == 0
    assert not any(operation[0] == "rename" for operation in storage.operations)


def test_source_version_change_before_manifest_keeps_manifest_unpublished(prepared, monkeypatch):
    source, config = prepared
    storage = StreamingStorage()
    rename = storage.rename

    def changed_after_promotion(source_path, destination):
        rename(source_path, destination)
        source.etag = '"new-version"'

    monkeypatch.setattr(storage, "rename", changed_after_promotion)
    summary = publish_manifest_package(config, storage, input_source=source)
    assert summary.rejected == 1 and summary.published == 0
    assert [operation[0] for operation in storage.operations] == ["upload_stream", "rename"]
    assert not any(path.endswith(".json") for path in storage.objects)


def test_remote_package_publishes_through_actual_stream_uploader(prepared, adls_storage):
    source, config = prepared
    storage, filesystem = adls_storage
    summary = publish_manifest_package(config, storage, input_source=source)
    assert summary.published == 1 and summary.rejected == 0
    videos = [content for path, (content, _) in filesystem.objects.items() if path.endswith(".mp4")]
    assert videos == [CONTENT]
    assert all(
        length <= 2 for path, offset, length in filesystem.appends
        if not path.endswith(".json")
    )
    with sqlite3.connect(config.checkpoint_path) as connection:
        row = connection.execute(
            "SELECT local_mtime_ns, input_version FROM publications"
        ).fetchone()
    assert row == (None, '"original"')


def test_validation_rejects_short_stream_even_when_stat_size_is_unchanged(prepared, monkeypatch):
    source, config = prepared
    monkeypatch.setattr(source, "chunks", lambda relative: iter((CONTENT[:-1],)))
    with pytest.raises(ManifestPublisherError, match="checksum changed"):
        validate_manifest_package(config, input_source=source)


def test_invalid_package_summary_metadata_cannot_mutate_storage(prepared):
    source, config = prepared
    validated = validate_manifest_package(config, input_source=source)
    validated = replace(validated, index={**validated.index, "inventory_sha256": 23})
    storage = StreamingStorage()
    with pytest.raises(ManifestPublisherError) as raised:
        publish_manifest_package(
            config, storage, validated_package=validated, input_source=source,
        )
    assert raised.value.field == "inventory_sha256"
    assert storage.operations == []


@pytest.mark.parametrize("field,value", [
    ("size_bytes", 0), ("duration_seconds", float("nan")),
    ("duration_seconds", -1), ("duration_seconds", 0),
    ("width", 0), ("height", 0), ("sha256", "not-a-digest"),
])
def test_pure_planner_validates_supplied_inspection(tmp_path, field, value):
    catalog = tmp_path / "catalog.csv"
    write_catalog(catalog)
    source = RemoteInput()
    with pytest.raises(VideoValidationError) as raised:
        build_manifest_plan(
            None, None, load_camera_catalog(catalog), {},
            staging_prefix="stage", incoming_prefix="incoming",
            relative_path=RELATIVE,
            inspection=replace(source.inspect(RELATIVE), **{field: value}),
        )
    if field == "size_bytes":
        assert raised.value.reason_code == "VIDEO_EMPTY"
        assert raised.value.field == "expected_size_bytes"
        assert raised.value.observed_value == "0"
    else:
        assert raised.value.reason_code == "VIDEO_METADATA_MISSING"


@pytest.mark.parametrize("width,height,line", [
    (1920, 1080, (0, 540, 1919, 540)),
    (1, 2, (0, 0, 0, 1)),
    (2, 1, (0, 0, 1, 0)),
])
def test_pure_planner_accepts_positive_dimensions_and_subsecond_video(tmp_path, width, height, line):
    catalog_path = tmp_path / "catalog.csv"
    write_catalog(catalog_path)
    catalog = replace(
        load_camera_catalog(catalog_path)[0],
        frame_width=width, frame_height=height, counting_line=line,
    )
    inspection = replace(
        RemoteInput().inspect(RELATIVE),
        width=width, height=height, duration_seconds=0.5,
    )
    plan = build_manifest_plan(
        None, None, [catalog], {}, staging_prefix="stage",
        incoming_prefix="incoming", relative_path=RELATIVE, inspection=inspection,
    )
    assert plan.inspection == inspection and plan.asset_version == inspection.sha256


@pytest.mark.parametrize("prefix", [b"", CONTENT[:11], b"wrong prefix", CONTENT])
def test_stream_upload_resumes_only_verified_prefix(adls_storage, prefix):
    storage, filesystem = adls_storage
    if prefix:
        filesystem.objects["stage/video"] = (prefix, '"etag"')
    storage.upload_stream(
        lambda: iter((CONTENT[:17], CONTENT[17:])), "stage/video",
        "video/mp4", len(CONTENT), hashlib.sha256(CONTENT).hexdigest(),
    )
    assert storage.read_bytes("stage/video") == CONTENT
    if not prefix or not CONTENT.startswith(prefix):
        assert filesystem.create_settings["stage/video"]["content_settings"].content_type == "video/mp4"
    assert all(length <= 2 for _, _, length in filesystem.appends)
    expected_offset = len(prefix) if CONTENT.startswith(prefix) else 0
    if len(prefix) != len(CONTENT):
        assert filesystem.appends[0][1] == expected_offset


@pytest.mark.parametrize("content", [CONTENT[:-1], CONTENT[:-1] + b"!", CONTENT + b"!"])
def test_stream_upload_checks_complete_size_and_digest(adls_storage, content):
    storage, filesystem = adls_storage
    with pytest.raises(PublicationConflictError, match="changed during upload"):
        storage.upload_stream(
            lambda: iter((content,)), "stage/video", "video/mp4",
            len(CONTENT), hashlib.sha256(CONTENT).hexdigest(),
        )
    assert filesystem.renames == []


def test_staged_resume_does_not_read_beyond_prefix_for_verification(adls_storage):
    storage, filesystem = adls_storage
    filesystem.objects["stage/video"] = (CONTENT[:11], '"etag"')
    delivered = []

    def chunks():
        count = [0]
        delivered.append(count)
        for offset in range(0, len(CONTENT), 7):
            count[0] += 1
            yield CONTENT[offset:offset + 7]

    storage.upload_stream(
        chunks, "stage/video", "video/mp4",
        len(CONTENT), hashlib.sha256(CONTENT).hexdigest(),
    )
    assert delivered[0][0] == 2
    assert storage.read_bytes("stage/video") == CONTENT


def test_staged_size_is_verified_even_when_checksum_matches(adls_storage, monkeypatch):
    storage, filesystem = adls_storage
    stat = storage.stat

    def wrong_uploaded_size(path):
        result = stat(path)
        if result is not None and filesystem.appends:
            return RemoteObject(result.size + 1, result.etag)
        return result

    monkeypatch.setattr(storage, "stat", wrong_uploaded_size)
    with pytest.raises(PublicationConflictError, match="does not match"):
        storage.upload_stream(
            lambda: iter((CONTENT,)), "stage/video", "video/mp4",
            len(CONTENT), hashlib.sha256(CONTENT).hexdigest(),
        )


def test_local_stream_reads_are_bounded_and_include_final_bytes(tmp_path):
    video = tmp_path / "large.mp4"
    content = b"x" * (8 * 1024 * 1024) + b"last frame"
    video.write_bytes(content)
    chunks = list(LocalVideoInput(tmp_path).chunks("large.mp4"))
    assert len(chunks) == 2
    assert all(len(chunk) <= 8 * 1024 * 1024 for chunk in chunks)
    assert hashlib.sha256(b"".join(chunks)).digest() == hashlib.sha256(content).digest()


def test_local_package_reuses_completed_checkpoint_and_real_mtime(prepared, adls_storage, monkeypatch):
    _source, config = prepared
    video = config.video_root / RELATIVE
    video.parent.mkdir(parents=True)
    video.write_bytes(CONTENT)
    storage, filesystem = adls_storage
    assert publish_manifest_package(config, storage).published == 1
    with sqlite3.connect(config.checkpoint_path) as connection:
        row = connection.execute(
            "SELECT local_mtime_ns, input_version FROM publications"
        ).fetchone()
    assert row == (video.stat().st_mtime_ns, str(video.stat().st_mtime_ns))
    appends = list(filesystem.appends)

    def cannot_republish(*args):
        raise AssertionError("An unchanged completed local checkpoint must be reused")

    monkeypatch.setattr("people_counter.manifest_publisher._verify_published", cannot_republish)
    assert publish_manifest_package(config, storage).already_published == 1
    assert filesystem.appends == appends


def test_rejected_local_package_preserves_real_source_version(prepared, adls_storage):
    _source, config = prepared
    video = config.video_root / RELATIVE
    video.parent.mkdir(parents=True)
    video.write_bytes(CONTENT)
    validated = validate_manifest_package(config)
    video.write_bytes(CONTENT + b"changed")
    storage, filesystem = adls_storage
    summary = publish_manifest_package(config, storage, validated_package=validated)
    assert summary.rejected == 1 and summary.published == 0
    assert filesystem.objects == {}
    with sqlite3.connect(config.checkpoint_path) as connection:
        row = connection.execute(
            "SELECT state, local_mtime_ns, input_version FROM publications"
        ).fetchone()
    assert row == ("REJECTED", video.stat().st_mtime_ns, str(video.stat().st_mtime_ns))


def test_local_input_paths_are_frozen_and_use_real_mtime(tmp_path):
    video = tmp_path / RELATIVE
    video.parent.mkdir(parents=True)
    video.write_bytes(CONTENT)
    source = LocalVideoInput(tmp_path)
    (video.parent / "later.mp4").write_bytes(b"later")
    paths = source.paths()
    paths.clear()
    assert source.paths() == [RELATIVE]
    assert source.local_path(RELATIVE) == video
    assert source.stat(RELATIVE) == InputStat(len(CONTENT), str(video.stat().st_mtime_ns))
    assert b"".join(source.chunks(RELATIVE)) == CONTENT

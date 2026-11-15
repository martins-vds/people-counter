"""Focused intake protocol tests; all storage operations are credential-free."""

import hashlib
import json
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from azure.core import MatchConditions
from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError

from people_counter.portal_intake import UPLOAD_CHUNK_BYTES, PortalIntake
from people_counter.portal_service import PortalStore
from people_counter.portal_sources import AdlsFootageSource


class File:
    def __init__(self, cloud, path):
        self.cloud, self.path = cloud, path

    def get_file_properties(self, **kwargs):
        if self.path not in self.cloud.files:
            raise ResourceNotFoundError("missing")
        value = self.cloud.files[self.path]
        return SimpleNamespace(**value)

    get_directory_properties = get_file_properties

    def download_file(self, **kwargs):
        from tests.test_portal_remote_media import RangedClient
        value = self.cloud.files[self.path]
        return RangedClient(value["data"], etag=value["etag"]).download_file(**kwargs)

    def create_file(self, metadata=None, **kwargs):
        assert kwargs["match_condition"] == MatchConditions.IfMissing
        self.cloud.files[self.path] = {"size": 0, "etag": "v1", "metadata": metadata or {}, "data": b"", "is_directory": False}

    def create_directory(self, **kwargs):
        self.create_file(**kwargs)
        self.cloud.files[self.path]["is_directory"] = True
        self.cloud.files[self.path]["metadata"]["hdi_isfolder"] = "true"

    def append_data(self, data, offset, length, **kwargs):
        entry = self.cloud.files[self.path]
        assert offset == len(entry["data"]) and length == len(data)
        entry["data"] += data

    def flush_data(self, position, **kwargs):
        entry = self.cloud.files[self.path]
        entry.update(size=position, etag="v2")

    def delete_file(self, **kwargs):
        del self.cloud.files[self.path]

    def rename_file(self, target, **kwargs):
        self.cloud.calls.append((self.path, target, kwargs))
        target = target.split("/", 1)[1]
        if self.cloud.files[self.path]["etag"] != kwargs["source_etag"]:
            raise ResourceModifiedError("source changed")
        if target in self.cloud.files and kwargs["match_condition"] == MatchConditions.IfMissing:
            raise ResourceModifiedError("collision")
        if kwargs["match_condition"] == MatchConditions.IfNotModified and self.cloud.files[target]["etag"] != kwargs["etag"]:
            raise ResourceModifiedError("destination changed")
        self.cloud.files[target] = self.cloud.files.pop(self.path)
        for path in list(self.cloud.files):
            if path.startswith(self.path + "/"):
                self.cloud.files[target + path[len(self.path):]] = self.cloud.files.pop(path)
        hook = kwargs.get("raw_response_hook")
        if hook:
            hook(SimpleNamespace(http_response=SimpleNamespace(
                status_code=201, headers={"ETag": self.cloud.files[target]["etag"]},
            )))
        return File(self.cloud, target)

    rename_directory = rename_file


class Cloud:
    def __init__(self):
        self.files, self.calls = {}, []

    def get_file_client(self, path):
        return File(self, path)

    get_directory_client = get_file_client

    def get_file_system_properties(self):
        return SimpleNamespace()

    def get_paths(self, path=None, recursive=True, **kwargs):
        class Paths(list):
            def by_page(self, continuation_token=None):
                return iter([iter(self)])
        prefix = path + "/" if path else ""
        return Paths([
            SimpleNamespace(name=name, content_length=data["size"], last_modified=datetime.now(timezone.utc), **data)
            for name, data in self.files.items()
            if name.startswith(prefix) and (recursive or "/" not in name[len(prefix):])
        ])


@pytest.fixture
def intake(tmp_path, monkeypatch):
    import threading

    azure = Mock()
    azure.require_identity.return_value = {"account": "operator", "tenant_id": "tenant", "user_id": "principal", "principal_type": "user"}
    source = AdlsFootageSource({"kind": "adls", "account": "testing", "filesystem": "videos", "root": "root"}, azure)
    cloud = Cloud()
    cloud.files["root"] = {"size": 0, "etag": "root-etag", "metadata": {"hdi_isfolder": "true"}, "data": b"", "is_directory": True}
    source._client = cloud
    store = PortalStore(tmp_path / "state.sqlite3")
    service = SimpleNamespace(source=source, store=store, operation_lock=threading.RLock(), max_upload_bytes=20)
    result = PortalIntake(service)
    monkeypatch.setattr("people_counter.portal_intake.complete_chunks", lambda client, *args, **kwargs: iter([cloud.files[client.path]["data"]]))
    return result


def put_video(intake, path, data=b"hello"):
    intake.source.client.files["root/" + path] = {"size": len(data), "etag": "reviewed", "metadata": {}, "data": data, "is_directory": False}


def test_upload_exact_offsets_hash_and_no_early_destination(intake):
    operation = intake.start_upload("", "new.mp4", 5)
    with pytest.raises(ValueError):
        intake.append_upload(operation["id"], 1, b"hello")
    assert intake.append_upload(operation["id"], 0, b"hello")["received"] == 5
    assert "root/new.mp4" not in intake.source.client.files
    assert intake.ready_upload(operation["id"])["state"] == "queued"
    completed = intake.complete(operation["id"])
    assert completed["state"] == "completed"
    assert completed["sha256"] == hashlib.sha256(b"hello").hexdigest()
    assert completed["verified"]["size"] == 5
    assert intake.source.client.calls[-1][2]["source_etag"] == "v2"
    assert intake.source.client.calls[-1][2]["match_condition"] == MatchConditions.IfMissing


def test_upload_staging_uses_the_chosen_folder_not_extra_root_permissions(intake):
    intake.create_folder("north")
    operation = intake.start_upload("north", "clip.mp4", 5)
    assert operation["stage"].startswith("north/")
    assert "root/" + operation["stage"] in intake.source.client.files
    intake.cancel(operation["id"])


def test_intake_operations_retain_reviewed_identity_before_a_restart_or_refresh(intake):
    operation = intake.start_upload("", "clip.mp4", 5)
    assert operation.get("execution_identity") == intake.source.azure.require_identity.return_value
    intake.source.azure.check_identity.side_effect = ValueError("Execution identity changed")
    with pytest.raises(ValueError, match="identity changed"):
        intake.append_upload(operation["id"], 0, b"hello")
    assert intake.record(operation["id"])["received"] == 0


def test_external_change_after_rename_cannot_be_adopted_as_the_moved_recording(intake, monkeypatch):
    put_video(intake, "old.mp4")
    intake.store.put("draft", "editable", {"source": intake.source.descriptor, "videos": ["old.mp4"],
                                         "observations": {"old.mp4": {"size": 5, "etag": "reviewed"}}, "folder": ""})
    operation = intake.start_move(["old.mp4"], "", rename="new.mp4")
    real = File.rename_file
    def rename(self, target, **kwargs):
        result = real(self, target, **kwargs)
        self.cloud.files[result.path]["etag"] = "changed-after-rename"
        return result
    monkeypatch.setattr(File, "rename_file", rename)
    result = intake.complete(operation["id"])
    assert result["state"] == "recovery"
    assert intake.store.entries("draft")[0]["videos"] == ["old.mp4"]


def test_explicit_reconciliation_can_verify_an_upload_after_finalization_was_interrupted(intake, monkeypatch):
    operation = intake.start_upload("", "clip.mp4", 5)
    intake.append_upload(operation["id"], 0, b"hello")
    intake.ready_upload(operation["id"])
    real = intake._hash
    def interrupted(record, path):
        if path == record["path"]:
            raise ValueError("Interrupted final verification")
        return real(record, path)
    monkeypatch.setattr(intake, "_hash", interrupted)
    assert intake.complete(operation["id"])["state"] == "recovery"
    monkeypatch.setattr(intake, "_hash", real)
    assert intake.reconcile(operation["id"])["state"] == "completed"
    assert intake.record(operation["id"])["sha256"] == hashlib.sha256(b"hello").hexdigest()


def test_prepared_footage_stays_readable_and_selectable_but_cannot_be_mutated(intake, monkeypatch):
    put_video(intake, "clip.mp4")
    intake.store.enqueue("prepare", {"source": intake.source.descriptor, "videos": ["clip.mp4"]})
    monkeypatch.setattr(intake.source, "page", lambda *args: ([{
        "path": "clip.mp4", "video": True, "directory": False, "blocked": False, "error": None,
    }], None))
    rows, _ = intake.page("")
    assert rows[0]["read_only"]
    assert not rows[0]["blocked"]
    assert rows[0]["error"] is None
    with pytest.raises(ValueError, match="protected"):
        intake.start_move(["clip.mp4"], "", rename="other.mp4")


def test_move_rejected_before_remote_request_releases_paths_after_reconciliation(intake):
    put_video(intake, "clip.mp4")
    operation = intake.start_move(["clip.mp4"], "", rename="renamed.mp4")
    intake.source.client.files["root/clip.mp4"]["etag"] = "changed-after-review"
    rejected = intake.complete(operation["id"])
    assert rejected["state"] == "recovery"
    assert rejected["results"][0]["state"] == "pending"
    assert intake.source.client.calls == []
    assert intake.reconcile(operation["id"])["state"] == "cancelled"
    intake.assert_available(["clip.mp4", "renamed.mp4"])
    refreshed = intake.start_move(["clip.mp4"], "", rename="renamed.mp4")
    assert intake.complete(refreshed["id"])["state"] == "completed"


def test_bulk_move_cancellation_keeps_completed_items_and_releases_unattempted_paths(intake):
    intake.create_folder("archive")
    put_video(intake, "first.mp4")
    put_video(intake, "second.mp4")
    operation = intake.start_move(["first.mp4", "second.mp4"], "archive")
    intake.source.client.files["root/second.mp4"]["etag"] = "changed-after-review"
    rejected = intake.complete(operation["id"])
    assert [item["state"] for item in rejected["results"]] == ["completed", "pending"]
    assert intake.cancel(operation["id"])["state"] == "cancelled"
    assert "root/archive/first.mp4" in intake.source.client.files
    assert "root/second.mp4" in intake.source.client.files
    assert "root/archive/second.mp4" not in intake.source.client.files
    intake.assert_available(["first.mp4", "second.mp4", "archive/first.mp4", "archive/second.mp4"])


def test_unknown_remote_move_outcome_keeps_reservations(intake, monkeypatch):
    put_video(intake, "clip.mp4")
    operation = intake.start_move(["clip.mp4"], "", rename="renamed.mp4")
    monkeypatch.setattr(File, "rename_file", Mock(side_effect=RuntimeError("Remote response lost")))
    assert intake.complete(operation["id"])["results"][0]["state"] == "mutating"
    with pytest.raises(ValueError, match="reconciliation"):
        intake.cancel(operation["id"])
    with pytest.raises(ValueError, match="reserved"):
        intake.assert_available(["clip.mp4"])


@pytest.mark.parametrize("after_first", [False, True])
def test_worker_honors_move_cancellation_between_items(intake, monkeypatch, after_first):
    intake.create_folder("archive")
    put_video(intake, "first.mp4")
    put_video(intake, "second.mp4")
    operation = intake.start_move(["first.mp4", "second.mp4"], "archive")
    if after_first:
        real_commit = intake._commit_item
        def commit(record, item):
            real_commit(record, item)
            if item["path"] == "first.mp4":
                intake.cancel(record["id"])
        monkeypatch.setattr(intake, "_commit_item", commit)
    else:
        real_move = intake._complete_move
        def move(record):
            intake.cancel(record["id"])
            real_move(record)
        monkeypatch.setattr(intake, "_complete_move", move)
    result = intake.complete(operation["id"])
    assert result["state"] == "cancelled"
    assert [row["state"] for row in result["results"]] == (
        ["completed", "pending"] if after_first else ["pending", "pending"]
    )
    assert len(intake.source.client.calls) == int(after_first)
    assert "root/second.mp4" in intake.source.client.files
    assert "root/archive/second.mp4" not in intake.source.client.files
    intake.assert_available(["first.mp4", "second.mp4", "archive/first.mp4", "archive/second.mp4"])


@pytest.mark.parametrize("name,size", [("x.txt", 1), ("x.mp4", 0), ("x.mp4", 21), ("../x.mp4", 1), ("clip?sv=2025-01-01&sig=example.mp4", 1)])
def test_upload_validation(intake, name, size):
    with pytest.raises(ValueError):
        intake.start_upload("", name, size)
    assert intake.service.store.entries("intake") == []


def test_native_rename_query_like_root_is_rejected_before_remote_access(intake):
    intake.source.remote_root = "root?sv=2025-01-01&sig=example/inner"
    with pytest.raises(ValueError, match="native rename"):
        intake._destination("x.mp4", False, None)


def test_upload_chunk_bounds_complete_and_cancel_ownership(intake):
    operation = intake.start_upload("", "x.mp4", 5)
    for data in (b"", b"x" * (UPLOAD_CHUNK_BYTES + 1), b"xxxxxx"):
        with pytest.raises(ValueError):
            intake.append_upload(operation["id"], 0, data)
    with pytest.raises(ValueError):
        intake.ready_upload(operation["id"])
    assert intake.cancel(operation["id"])["state"] == "cancelled"
    assert "root/" + operation["stage"] not in intake.source.client.files
    with pytest.raises(ValueError):
        intake.record("../foreign")


def test_upload_refuses_flush_or_finalize_after_staging_ownership_changes(intake):
    for after_flush in (False, True):
        operation = intake.start_upload("", f"{after_flush}.mp4", 5)
        intake.append_upload(operation["id"], 0, b"hello")
        if after_flush:
            intake.ready_upload(operation["id"])
        intake.source.client.files["root/" + operation["stage"]]["metadata"] = {"foreign": "yes"}
        if after_flush:
            assert intake.complete(operation["id"])["state"] == "recovery"
            assert "root/" + operation["path"] not in intake.source.client.files
        else:
            with pytest.raises(ValueError, match="ownership"):
                intake.ready_upload(operation["id"])
        assert "root/" + operation["stage"] in intake.source.client.files


def test_move_metadata_and_conditional_replacement(intake):
    put_video(intake, "old.mp4")
    put_video(intake, "new.mp4", b"old")
    store, source = intake.service.store, intake.source
    store.put("footage", source.assignment_prefix + "old.mp4", {"camera_id": "camera"})
    store.put("exception", source.logical_prefix + "new.mp4", {"override": True})
    store.put("draft", "editable", {"source": source.descriptor, "videos": ["old.mp4", "new.mp4"], "observations": {}, "folder": ""})
    operation = intake.start_move(["old.mp4"], "", rename="new.mp4", replacements={"new.mp4": "reviewed"})
    assert intake.complete(operation["id"])["state"] == "completed"
    assert store.entries("footage")[0]["id"] == source.assignment_prefix + "new.mp4"
    assert store.entries("exception") == []
    assert store.entries("source_review")
    assert store.entries("draft")[0]["videos"] == ["new.mp4"]
    conditions = source.client.calls[-1][2]
    assert conditions["source_match_condition"] == MatchConditions.IfNotModified
    assert conditions["etag"] == "reviewed"


def test_replacing_move_keeps_source_camera_ownership_regardless_of_key_order(intake):
    put_video(intake, "a.mp4")
    put_video(intake, "z.mp4")
    prefix = intake.source.assignment_prefix
    intake.store.put("footage", prefix + "a.mp4", {"camera_id": "source-camera"})
    intake.store.put("footage", prefix + "z.mp4", {"camera_id": "destination-camera"})
    operation = intake.start_move(["a.mp4"], "", rename="z.mp4", replacements={"z.mp4": "reviewed"})
    assert intake.complete(operation["id"])["state"] == "completed"
    assert intake.store.entries("footage") == [{"id": prefix + "z.mp4", "camera_id": "source-camera"}]


def test_reviewed_replacement_rejects_new_destination_version_without_metadata_changes(intake):
    put_video(intake, "source.mp4")
    put_video(intake, "target.mp4", b"original")
    intake.store.put("exception", intake.source.logical_prefix + "target.mp4", {"override": True})
    operation = intake.start_move(["source.mp4"], "", rename="target.mp4", replacements={"target.mp4": "reviewed"})
    intake.source.client.files["root/target.mp4"]["etag"] = "newer"
    result = intake.complete(operation["id"])
    assert result["state"] == "recovery"
    assert intake.source.client.files["root/target.mp4"]["data"] == b"original"
    assert "root/source.mp4" in intake.source.client.files
    assert intake.store.entries("exception")[0]["override"]


def test_definitively_rejected_upload_can_cancel_owned_stage_and_retry(intake):
    operation = intake.start_upload("", "x.mp4", 5)
    intake.append_upload(operation["id"], 0, b"hello")
    intake.ready_upload(operation["id"])
    put_video(intake, "x.mp4", b"other")
    failed = intake.complete(operation["id"])
    assert failed["state"] == "recovery"
    assert intake.cancel(operation["id"])["state"] == "cancelled"
    assert "root/" + operation["stage"] not in intake.source.client.files
    assert intake.source.client.files["root/x.mp4"]["data"] == b"other"
    retry = intake.start_upload("", "x.mp4", 5, replace=True, destination_etag="reviewed")
    assert retry["state"] == "uploading"


def test_protection_and_reservation_are_subtree_wide(intake):
    put_video(intake, "folder/x.mp4")
    store = intake.service.store
    with store.connection() as db:
        db.execute("INSERT INTO jobs(id,kind,payload,status) VALUES (?,?,?,?)", ("snapshot", "prepare", json.dumps({"source": intake.source.descriptor, "videos": ["folder/x.mp4"]}), "SUCCEEDED"))
    intake.assert_available(["folder"])
    with pytest.raises(ValueError, match="protected"):
        intake.start_move(["folder"], "", rename="renamed")
    operation = intake.start_upload("", "a.mp4", 5)
    with pytest.raises(ValueError, match="reserved"):
        intake.assert_available(["a.mp4"])
    intake.cancel(operation["id"])
    intake.assert_available(["a.mp4"])


def test_recovery_cleans_abandoned_upload_not_foreign_metadata(intake):
    operation = intake.start_upload("", "x.mp4", 5)
    intake.recover()
    assert intake.record(operation["id"])["state"] == "cancelled"
    second = intake.start_upload("", "y.mp4", 5)
    intake.source.client.files["root/" + second["stage"]]["metadata"] = {"foreign": "yes"}
    intake.recover()
    assert intake.record(second["id"])["state"] == "recovery"
    assert "root/" + second["stage"] in intake.source.client.files


def test_remote_unknown_outcome_never_reports_success(intake, monkeypatch):
    put_video(intake, "x.mp4")
    operation = intake.start_move(["x.mp4"], "", rename="y.mp4")
    monkeypatch.setattr(File, "rename_file", Mock(side_effect=TimeoutError("unknown")))
    assert intake.complete(operation["id"])["state"] == "recovery"
    with pytest.raises(ValueError, match="reserved"):
        intake.assert_available(["y.mp4"])


def test_received_bytes_are_verified_across_chunk_boundaries(intake, monkeypatch):
    operation = intake.start_upload("", "x.mp4", 5)
    intake.append_upload(operation["id"], 0, b"he")
    intake.append_upload(operation["id"], 2, b"llo")
    intake.ready_upload(operation["id"])
    monkeypatch.setattr("people_counter.portal_intake.complete_chunks", lambda client, *args, **kwargs: iter([b"h", b"ell", b"o"]))
    assert intake.complete(operation["id"])["state"] == "completed"
    operation = intake.start_upload("", "y.mp4", 5)
    intake.append_upload(operation["id"], 0, b"hello")
    intake.ready_upload(operation["id"])
    monkeypatch.setattr("people_counter.portal_intake.complete_chunks", lambda client, *args, **kwargs: iter([b"other"]))
    failed = intake.complete(operation["id"])
    assert failed["state"] == "recovery"
    assert "differ from received" in failed["message"]
    assert "root/y.mp4" not in intake.source.client.files


def test_tiny_upload_chunks_keep_durable_metadata_bounded(intake):
    operation = intake.start_upload("", "tiny.mp4", 20)
    first = intake.append_upload(operation["id"], 0, b"x")
    last = first
    for offset in range(1, 20):
        last = intake.append_upload(operation["id"], offset, b"x")
    assert len(json.dumps(last)) - len(json.dumps(first)) < 100
    intake.ready_upload(operation["id"])
    assert intake.complete(operation["id"])["sha256"] == hashlib.sha256(b"x" * 20).hexdigest()


def test_upload_hash_context_is_shared_by_scoped_intakes_but_not_resumed_after_restart(intake):
    operation = intake.start_upload("", "scoped.mp4", 5)
    intake.append_upload(operation["id"], 0, b"he")
    scoped = PortalIntake(intake.service)
    scoped.append_upload(operation["id"], 2, b"llo")
    scoped.ready_upload(operation["id"])
    assert scoped.complete(operation["id"])["sha256"] == hashlib.sha256(b"hello").hexdigest()
    pending = intake.start_upload("", "interrupted.mp4", 5)
    intake.append_upload(pending["id"], 0, b"he")
    restarted = PortalIntake(SimpleNamespace(
        source=intake.source, store=PortalStore(intake.store.path),
        operation_lock=intake.lock, max_upload_bytes=20,
    ))
    with pytest.raises(ValueError, match="interrupted"):
        restarted.append_upload(pending["id"], 2, b"llo")
    assert restarted.record(pending["id"])["received"] == 2
    restarted.recover()
    assert restarted.record(pending["id"])["state"] == "cancelled"


def test_cancel_during_each_verification_pass(intake, monkeypatch):
    for final_pass in (False, True):
        cancellations = []
        operation = intake.start_upload("", f"{final_pass}.mp4", 5)
        intake.append_upload(operation["id"], 0, b"hello")
        intake.ready_upload(operation["id"])

        def chunks(client, *args, operation=operation, final_pass=final_pass, cancellations=cancellations, **kwargs):
            if (client.path.endswith(operation["stage"])) != final_pass:
                cancelled = intake.cancel(operation["id"])
                cancellations.append(cancelled)
                assert cancelled["state"] in {"cancelled", "recovery"}
            yield b"hello"

        monkeypatch.setattr("people_counter.portal_intake.complete_chunks", chunks)
        result = intake.complete(operation["id"])
        assert result["state"] == ("recovery" if final_pass else "cancelled")
        assert "verified" not in result
        assert len(cancellations) == 1


def test_interrupted_active_job_protects_destination(intake):
    put_video(intake, "x.mp4")
    put_video(intake, "y.mp4")
    with intake.store.connection() as db:
        db.execute("INSERT INTO jobs(id,kind,payload,status) VALUES (?,?,?,?)", ("active", "publish", json.dumps({"snapshot": {"source": intake.source.descriptor, "videos": ["y.mp4"]}}), "INTERRUPTED"))
    with pytest.raises(ValueError, match="protected"):
        intake.start_move(["x.mp4"], "", rename="y.mp4", replacements={"y.mp4": "reviewed"})


@pytest.mark.parametrize("current_root,historical_root,video,expected", [
    ("root", "root/sub", "x.mp4", "sub/x.mp4"),
    ("", "root/sub", "x.mp4", "root/sub/x.mp4"),
    ("root/sub", "root", "sub/x.mp4", "x.mp4"),
    ("root", "elsewhere", "x.mp4", None),
])
def test_generated_snapshot_from_overlapping_historical_root_protects_same_physical_file(intake, current_root, historical_root, video, expected):
    old = intake.source
    intake.source = AdlsFootageSource({**old.descriptor, "root": current_root}, Mock())
    intake.source._client = old.client
    descriptor = {**old.descriptor, "root": historical_root}
    with intake.store.connection() as db:
        db.execute("INSERT INTO jobs(id,kind,payload,status) VALUES (?,?,?,?)", ("historical", "prepare", json.dumps({"source": descriptor, "videos": [video]}), "SUCCEEDED"))
    assert intake._protected() == ({expected} if expected else set())
    if expected:
        with pytest.raises(ValueError, match="protected"):
            intake.start_move([expected], "", rename="renamed.mp4")


def test_folder_move_ownership_drafts_and_catalog_prefix(intake):
    intake.create_folder("old")
    put_video(intake, "old/x.mp4")
    source, store = intake.source, intake.store
    store.put("footage", source.assignment_prefix + "old/x.mp4", {"camera_id": "camera"})
    store.put("draft", "draft", {"source": source.descriptor, "videos": ["old/x.mp4"], "folder": "old", "observations": {"old/x.mp4": {"size": 5, "etag": "reviewed"}}})
    with store.connection() as db:
        db.execute("INSERT INTO entries VALUES ('camera','calibration',?)", (json.dumps({"video": source.assignment_prefix + "old/x.mp4", "folder": "old", "catalog": {"source_path_prefix": source.logical_prefix + "old/", "camera_id": "camera"}}),))
    operation = intake.start_move(["old"], "", rename="new")
    assert intake.complete(operation["id"])["state"] == "completed"
    assert store.entries("draft")[0]["folder"] == "new"
    assert store.entries("camera")[0]["catalog"]["source_path_prefix"] == source.logical_prefix + "new/"
    assert store.entries("footage")[0]["id"] == source.assignment_prefix + "new/x.mp4"


def test_move_migrates_per_file_overrides_previews_and_preserves_other_sources(intake):
    put_video(intake, "old.mp4")
    store, source = intake.store, intake.source
    logical = source.logical_prefix
    store.put("exception", logical + "old.mp4", {"video_relative_path": logical + "old.mp4", "override": True})
    store.put("source_preview", "preview", {"video": logical + "old.mp4", "observations": {"size": 5, "etag": "reviewed"}})
    store.put("source_preview", "stale-preview", {"video": logical + "old.mp4", "observations": {"size": 1, "etag": "old"}})
    store.put("source_preview", "unrelated-preview", {"video": "other-source/video.mp4"})
    store.put("draft", "unrelated-draft", {"source": {"kind": "local", "root": "/unrelated"}, "videos": ["old.mp4"]})
    operation = intake.start_move(["old.mp4"], "", rename="new.mp4")
    assert intake.complete(operation["id"])["state"] == "completed"
    exception = store.entries("exception")[0]
    assert exception["id"] == exception["video_relative_path"] == logical + "new.mp4"
    preview = next(row for row in store.entries("source_preview") if row["id"] == "preview")
    assert preview["video"] == logical + "new.mp4"
    assert preview["observations"] == {"size": 5, "etag": "reviewed"}
    assert all(row["id"] != "stale-preview" for row in store.entries("source_preview"))
    assert store.entries("draft")[0]["videos"] == ["old.mp4"]
    assert next(row for row in store.entries("source_preview") if row["id"] == "unrelated-preview")["video"] == "other-source/video.mp4"


@pytest.mark.parametrize("paths,destination,rename", [([""], "", "new"), (["x"], "x", None), (["x", "x/child"], "", None), (["x", "z"], "", "new")])
def test_reject_unsafe_move_selections(intake, paths, destination, rename):
    with pytest.raises(ValueError):
        intake.start_move(paths, destination, rename=rename)
    assert intake.source.client.calls == []


def test_folder_collision_unsupported_descendants_and_bulk_collision(intake):
    intake.create_folder("a")
    intake.create_folder("b")
    put_video(intake, "a/x.mp4")
    put_video(intake, "b/x.mp4")
    with pytest.raises(ValueError, match="collid"):
        intake.start_move(["a/x.mp4", "b/x.mp4"], "")
    with pytest.raises(ValueError, match="Folder"):
        intake.start_move(["a"], "", rename="b", replacements={"b": "v1"})
    put_video(intake, "a/note.txt")
    with pytest.raises(ValueError, match="unsupported"):
        intake.start_move(["a"], "", rename="c")
    assert intake.source.client.calls == []


def test_folder_subtree_is_rechecked_before_native_move(intake):
    intake.create_folder("folder")
    put_video(intake, "folder/x.mp4")
    operation = intake.start_move(["folder"], "", rename="moved")
    put_video(intake, "folder/late.txt")
    assert intake.complete(operation["id"])["state"] == "recovery"
    assert "root/folder/late.txt" in intake.source.client.files
    assert "root/moved" not in intake.source.client.files
    assert intake.source.client.calls == []


def test_partial_bulk_preserves_completed_work_and_blocks_unknown(intake, monkeypatch):
    put_video(intake, "x.mp4")
    put_video(intake, "y.mp4")
    intake.create_folder("dest")
    operation = intake.start_move(["x.mp4", "y.mp4"], "dest")
    real = intake._rename

    def rename(item):
        if item["path"] == "y.mp4":
            raise TimeoutError("unknown second move")
        real(item)

    monkeypatch.setattr(intake, "_rename", rename)
    result = intake.complete(operation["id"])
    assert result["state"] == "recovery"
    assert [row["state"] for row in result["results"]] == ["completed", "mutating"]
    assert "root/dest/x.mp4" in intake.source.client.files
    assert "root/x.mp4" not in intake.source.client.files
    assert "root/y.mp4" in intake.source.client.files
    intake.recover()
    assert intake.record(operation["id"])["state"] == "recovery"


def test_native_move_success_response_with_source_remaining_is_ambiguous(intake, monkeypatch):
    put_video(intake, "x.mp4")
    operation = intake.start_move(["x.mp4"], "", rename="y.mp4")

    def partial(item):
        put_video(intake, "y.mp4")

    monkeypatch.setattr(intake, "_rename", partial)
    result = intake.complete(operation["id"])
    assert result["state"] == "recovery"
    assert "source" in result["message"].lower()


def test_remote_done_journal_recovery_commits_metadata_but_rejects_changed_target(intake, monkeypatch):
    put_video(intake, "old.mp4")
    intake.store.put("footage", intake.source.assignment_prefix + "old.mp4", {"camera_id": "camera"})
    operation = intake.start_move(["old.mp4"], "", rename="new.mp4")
    real = intake._metadata
    monkeypatch.setattr(intake, "_metadata", Mock(side_effect=RuntimeError("sqlite failed")))
    result = intake.complete(operation["id"])
    assert result["state"] == "recovery"
    assert result["results"][0]["state"] == "remote_done"
    assert intake.store.entries("footage")[0]["id"].endswith("old.mp4")
    monkeypatch.setattr(intake, "_metadata", real)
    intake.source.client.files["root/new.mp4"]["etag"] = "changed"
    intake.recover()
    assert intake.record(operation["id"])["state"] == "recovery"
    assert intake.store.entries("footage")[0]["id"].endswith("old.mp4")
    intake.source.client.files["root/new.mp4"]["etag"] = "reviewed"
    intake.recover()
    assert intake.record(operation["id"])["state"] == "completed"
    assert intake.store.entries("footage")[0]["id"].endswith("new.mp4")


def test_recover_known_upload_rename_reverifies_before_completing(intake, monkeypatch):
    operation = intake.start_upload("", "x.mp4", 5)
    intake.append_upload(operation["id"], 0, b"hello")
    intake.ready_upload(operation["id"])
    real = intake._hash

    def crash_after_rename(record, path):
        if path == record["path"]:
            raise SystemExit("process stopped")
        return real(record, path)

    monkeypatch.setattr(intake, "_hash", crash_after_rename)
    with pytest.raises(SystemExit):
        intake.complete(operation["id"])
    interrupted = intake.record(operation["id"])
    assert interrupted["state"] == "verifying"
    assert "verified" not in interrupted
    monkeypatch.setattr(intake, "_hash", real)
    intake.recover()
    completed = intake.record(operation["id"])
    assert completed["state"] == "completed"
    assert completed["verified"] == {"size": 5, "etag": "v2"}
    assert completed["sha256"] == hashlib.sha256(b"hello").hexdigest()


def test_recovery_transaction_failure_keeps_remote_done_journal(intake, monkeypatch):
    put_video(intake, "old.mp4")
    intake.store.put("footage", intake.source.assignment_prefix + "old.mp4", {"camera_id": "camera"})
    operation = intake.start_move(["old.mp4"], "", rename="new.mp4")
    real = intake._write

    def fail_completion(db, kind, identity, data):
        real(db, kind, identity, data)
        if kind == "intake" and data["state"] == "completed":
            raise RuntimeError("transaction rolled back")

    monkeypatch.setattr(intake, "_write", fail_completion)
    intake.complete(operation["id"])
    intake.recover()
    failed = intake.record(operation["id"])
    assert failed["state"] == "recovery"
    assert failed["results"][0]["state"] == "remote_done"
    assert intake.store.entries("footage")[0]["id"].endswith("old.mp4")
    monkeypatch.setattr(intake, "_write", real)
    intake.recover()
    assert intake.record(operation["id"])["state"] == "completed"
    assert intake.store.entries("footage")[0]["id"].endswith("new.mp4")


@pytest.mark.parametrize("timing", ["listing", "verification"])
def test_recovery_does_not_resurrect_cancelled_verification_from_stale_listing(intake, monkeypatch, timing):
    operation = intake.start_upload("", "x.mp4", 5)
    intake.append_upload(operation["id"], 0, b"hello")
    intake.ready_upload(operation["id"])
    real_hash = intake._hash

    def crash(record, path):
        if path == record["path"]:
            raise SystemExit()
        return real_hash(record, path)

    monkeypatch.setattr(intake, "_hash", crash)
    with pytest.raises(SystemExit):
        intake.complete(operation["id"])
    monkeypatch.setattr(intake, "_hash", real_hash)
    real_entries = intake.store.entries
    cancelled = False

    def entries(kind):
        nonlocal cancelled
        rows = real_entries(kind)
        if timing == "listing" and kind == "intake" and not cancelled:
            cancelled = True
            intake.cancel(operation["id"])
        return rows

    monkeypatch.setattr(intake.store, "entries", entries)
    if timing == "verification":
        real_recover = intake._recover_upload

        def cancel_before_verify(record):
            intake.cancel(record["id"])
            real_recover(record)

        monkeypatch.setattr(intake, "_recover_upload", cancel_before_verify)
    intake.recover()
    assert intake.record(operation["id"])["state"] == "recovery"
    assert "verified" not in intake.record(operation["id"])


def test_page_hides_only_metadata_owned_staging_and_tags_reserved(intake, monkeypatch):
    operation = intake.start_upload("", "x.mp4", 5)
    foreign = ".pc-intake-foreign.mp4"
    put_video(intake, foreign)
    monkeypatch.setattr(intake.source, "page", lambda folder, token: ([
        {"path": path, "directory": False, "video": path != "note.txt"}
        for path in (operation["stage"], foreign, "note.txt", "x.mp4")
    ], "next"))
    page, token = intake.page("")
    assert token == "next"
    assert [row["path"] for row in page] == [foreign, "note.txt", "x.mp4"]
    assert page[1]["read_only"] and not page[1]["protected"]
    assert page[2]["protected"] and page[2]["read_only"]


def test_native_sdk_properties_detect_directory_without_fake_attributes(intake):
    from azure.core.pipeline.transport import HttpResponse, HttpTransport
    from azure.storage.filedatalake import FileSystemClient

    class Response(HttpResponse):
        def __init__(self, request):
            super().__init__(request, None)
            self.status_code = 200
            self.headers = {"etag": '"directory"', "content-length": "0", "x-ms-meta-hdi_isfolder": "true"}
            self.reason = "OK"
            self.content_type = None

        def body(self):
            return b""

    class Transport(HttpTransport):
        def open(self):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send(self, request, **kwargs):
            return Response(request)

    intake.source._client = FileSystemClient("https://testing.dfs.core.windows.net", "videos", credential=None, transport=Transport())
    properties = intake._properties("folder")
    assert properties.is_directory
    with pytest.raises(ValueError, match="Folder"):
        intake._destination("folder", True, '"directory"')
    intake.source.remote_root = ""
    intake._parent("new.mp4")


def test_native_sdk_rename_sends_conditions():
    from azure.core.pipeline.transport import HttpTransport
    from azure.storage.filedatalake import DataLakeDirectoryClient, DataLakeFileClient

    class Capture(HttpTransport):
        def open(self):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send(self, request, **kwargs):
            self.headers = dict(request.headers)
            raise RuntimeError("captured without network")

    for cls, method in ((DataLakeFileClient, "rename_file"), (DataLakeDirectoryClient, "rename_directory")):
        transport = Capture()
        client = cls("https://testing.dfs.core.windows.net", "videos", "root/source", credential=None, transport=transport)
        with pytest.raises(RuntimeError, match="captured"):
            getattr(client, method)("videos/root/target", source_etag='"source"', source_match_condition=MatchConditions.IfNotModified, etag='"target"', match_condition=MatchConditions.IfNotModified)
        assert transport.headers["x-ms-source-if-match"] == '"source"'
        assert transport.headers["If-Match"] == '"target"'
        with pytest.raises(RuntimeError, match="captured"):
            getattr(client, method)("videos/root/target", source_etag='"source"', source_match_condition=MatchConditions.IfNotModified, etag="*", match_condition=MatchConditions.IfMissing)
        assert transport.headers["If-None-Match"] == "*"
        create = client.create_file if isinstance(client, DataLakeFileClient) else client.create_directory
        with pytest.raises(RuntimeError, match="captured"):
            create(etag="*", match_condition=MatchConditions.IfMissing)
        assert transport.headers["If-None-Match"] == "*"
        if isinstance(client, DataLakeFileClient):
            with pytest.raises(RuntimeError, match="captured"):
                client.delete_file(etag='"owned"', match_condition=MatchConditions.IfNotModified)
            assert transport.headers["If-Match"] == '"owned"'

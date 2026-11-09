import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from people_counter import backfill_operator as operator
from people_counter.manifest import ManifestPublisherError, manifest_bytes
from people_counter.manifest_publisher import (
    PublisherConfig,
    run_publisher,
    validate_manifest_package,
)
from tests.test_manifest_publisher import FakeStorage, inspection, write_catalog


@pytest.fixture
def setup(tmp_path):
    video = tmp_path / "north/camera-17/20260101T120000Z.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"video")
    catalog = tmp_path / "catalog.csv"
    write_catalog(catalog)
    package = tmp_path / "package"
    run_publisher(
        PublisherConfig(
            catalog, None, tmp_path, None, "account", "footage",
            "staging", "incoming", tmp_path / "prepare.sqlite3",
            tmp_path / "reject.csv", tmp_path / "inventory.csv",
            10, True, output_dir=package, summary_report_path=tmp_path / "summary.json",
        ),
        storage=None,
        inspector=inspection,
    )
    config = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "environment_id": "33333333-3333-3333-3333-333333333333",
        "control_sjd_id": "44444444-4444-4444-4444-444444444444",
        "intake_mode": "explicit",
        "storage_account": "account",
        "filesystem": "footage",
        "shortcut_path": "Files/videos",
        "config_sha256": "c" * 64,
        "profile": {
            "pipeline": "rtdetr-osnet",
            "detector_model": "r18",
            "model_format": "onnx",
            "device_variant": "cpu",
            "batch_size": 1,
            "sample_fps": 1.0,
            "profile_id": "reviewed-profile",
            "runtime_key": "r" * 64,
            "model_artifact_sha256": {"model.onnx": "a" * 64},
        },
    }
    config_path = tmp_path / "target.json"
    config_path.write_text(json.dumps(config))
    args = [
        "--manifest-package-dir", str(package),
        "--video-root", str(tmp_path),
        "--config", str(config_path),
        "--state-dir", str(tmp_path / "state"),
    ]
    return SimpleNamespace(
        root=tmp_path, package=package, config=config, config_path=config_path,
        args=args,
    )


@pytest.mark.parametrize("flags", [[], ["--wait"]])
def test_preview_is_offline_and_preserves_package(setup, capsys, flags):
    before = {p: p.read_bytes() for p in setup.package.rglob("*") if p.is_file()}
    with patch.object(operator, "AzureDataLakeStorage") as storage, patch.object(
        operator, "FabricAutomationClient"
    ) as fabric:
        assert operator.main(setup.args + flags) == 0
    storage.assert_not_called()
    fabric.assert_not_called()
    preview = json.loads(capsys.readouterr().out)
    assert preview["status"] == "preview"
    assert preview["item_count"] == 1
    assert preview["partition_count"] == 1
    assert type(preview["partition_count"]) is int
    assert preview["workspace_id"] == setup.config["workspace_id"]
    assert preview["lakehouse_id"] == setup.config["lakehouse_id"]
    assert preview["control_sjd_id"] == setup.config["control_sjd_id"]
    assert preview["destination"] == "abfss://footage@account.dfs.core.windows.net/incoming/"
    assert preview["profile_id"] == "reviewed-profile"
    assert preview["package_sha256"] == hashlib.sha256(
        (setup.package / "manifest-package.json").read_bytes()
    ).hexdigest()
    assert "--submit" in preview["next_step"]
    assert not (setup.root / "state").exists()
    assert before == {p: p.read_bytes() for p in setup.package.rglob("*") if p.is_file()}


@pytest.mark.parametrize("change", [
    {"intake_mode": "arrival"},
    {"workspace_id": "../other-workspace"},
    {"workspace_id": None},
    {"storage_account": "https://evil.example"},
    {"filesystem": "../other"},
    {"shortcut_path": "Files/../outside"},
    {"shortcut_path": "/lakehouse/default/Files/videos"},
    {"shortcut_path": None},
    {"config_sha256": "not-a-digest"},
    {"table_directory": "Tables/../outside"},
    {"table_directory": None},
    {"profile": {"source_video": "/unreviewed"}},
    {"unexpected_option": True},
])
def test_invalid_config_fails_before_network(setup, change):
    setup.config_path.write_text(json.dumps({**setup.config, **change}))
    with patch.object(operator, "AzureDataLakeStorage") as storage, pytest.raises(
        (ValueError, ManifestPublisherError)
    ):
        operator.main(setup.args + ["--submit"])
    storage.assert_not_called()


@pytest.mark.parametrize("change", [
    {"source_video": "/unreviewed"},
    {"runtime_key": ""},
    {"batch_size": 0},
    {"batch_size": True},
    {"sample_fps": 0},
    {"sample_fps": float("inf")},
    {"sample_fps": True},
    {"model_artifact_sha256": {}},
    {"model_artifact_sha256": {"model.onnx": "bad-hash"}},
    {"model_artifact_sha256": {"../model.onnx": "a" * 64}},
])
def test_invalid_profile_fails_before_network(setup, change):
    profile = {**setup.config["profile"], **change}
    setup.config_path.write_text(json.dumps({**setup.config, "profile": profile}))
    with patch.object(operator, "OneLakeFiles") as files, pytest.raises(
        (ValueError, ManifestPublisherError)
    ):
        operator.main(setup.args + ["--submit"])
    files.assert_not_called()


def test_state_directory_cannot_be_inside_package(setup):
    args = setup.args[:-2] + ["--state-dir", str(setup.package / "state")]
    with pytest.raises(ValueError, match="outside"):
        operator.main(args)


@pytest.mark.parametrize("config", [[], {"intake_mode": "explicit"}])
def test_missing_configuration_fields_fail_before_cloud(setup, config):
    setup.config_path.write_text(json.dumps(config))
    with patch.object(operator, "OneLakeFiles") as files, pytest.raises(ValueError):
        operator.main(setup.args + ["--submit", "--wait"])
    files.assert_not_called()


def test_records_preserve_metadata_and_have_stable_configuration_bound_ids(setup):
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(
        setup.package, setup.root, setup.root / "state", config
    )
    validated = validate_manifest_package(publication)
    records = operator.registration_records(validated, config, "d" * 64)
    assert records == operator.registration_records(validated, config, "d" * 64)
    plan = validated.plans[0]
    record = records[0]
    assert record["work_id"] == (
        "backfill-9809728950f9c58ce09c3d32417c684fe5376d9e643c107fee4fe3e51bf6a06c"
    )
    assert record["max_attempts"] == 3
    payload = record["payload"]
    assert payload["source_video"] == (
        f"/lakehouse/default/Files/videos/{plan.incoming_video_path}"
    )
    assert payload["source_sha256"] == plan.inspection.sha256
    assert payload["line"] == [0, 540, 1919, 540]
    assert payload["camera_id"] == "camera-17"
    assert payload["location_id"] == "north-entrance"
    assert payload["camera_timezone"] == "America/Denver"
    assert payload["captured_at_utc"] == "2026-01-01T12:00:00Z"
    assert record["duration_seconds"] == plan.inspection.duration_seconds
    assert record["config_sha256"] == "c" * 64
    assert record["release_digest"] == "d" * 64
    assert record["work_id"] != operator.registration_records(
        validated, config, "e" * 64
    )[0]["work_id"]
    changed = replace(config, profile={**config.profile, "sample_fps": 2.0})
    assert record["work_id"] != operator.registration_records(
        validated, changed, "d" * 64
    )[0]["work_id"]
    second = replace(
        plan, relative_path="z.mp4", asset_id="b" * 64,
        incoming_video_path="incoming/z.mp4",
    )
    ordered = operator.registration_records(
        replace(validated, plans=(second, plan)), config, "d" * 64
    )
    assert ordered[0] == record
    assert ordered[1]["payload"]["source_video"].endswith("/incoming/z.mp4")


def cloud_mocks(setup):
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(
        setup.package, setup.root, setup.root / "state", config
    )
    plan = validate_manifest_package(publication).plans[0]
    storage = FakeStorage()
    files = Mock()
    files.read_bytes.return_value = manifest_bytes(
        plan, storage_account="account", filesystem="footage",
        source_etag='"video-etag"',
    )
    release = SimpleNamespace(
        manifest_sha256="d" * 64,
        manifest=SimpleNamespace(environment_id=config.environment_id),
    )
    return storage, files, release


def test_submit_publishes_verifies_then_registers_and_resumes(setup, capsys):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (
        202, {"Location": "https://api.fabric.microsoft.com/v1/jobs/job-1"}, b""
    )
    fabric.request_json.return_value = {"id": "job-1", "status": "Completed"}
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        assert operator.main(setup.args + ["--submit"]) == 0
        assert operator.main(setup.args + ["--submit"]) == 0
    assert fabric.request_response.call_count == 1
    call = fabric.request_response.call_args
    assert "sparkJobDefinitions/44444444-4444-4444-4444-444444444444" in call.args[1]
    command = call.kwargs["value"]["executionData"]["commandLineArguments"]
    assert "--manual-artifacts" in command
    assert "bulk-register --partition-path Files/people-counter/sjd/v1/intake/backfill/" in command
    assert "--max-items 1" in command
    content = files.write_partition.call_args.args[1]
    assert hashlib.sha256(content).hexdigest() in command
    record = json.loads(content)["items"][0]
    assert record["payload"]["source_sha256"] == hashlib.sha256(b"video").hexdigest()
    report = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert report["status"] == "registered"
    assert report["item_count"] == 1
    assert report["processing_complete"] is False
    files.verify_video.assert_any_call("Files/models/model.onnx", None, "a" * 64)
    evidence = json.loads((setup.root / "state/publication-evidence.json").read_text())
    assert evidence[0]["work_id"] == record["work_id"]
    assert evidence[0]["source_manifest"]["source_etag"] == '"video-etag"'


@pytest.mark.parametrize("failure", ["publication", "shortcut", "release"])
def test_preflight_failure_never_submits_control(setup, failure):
    storage, files, release = cloud_mocks(setup)
    if failure == "shortcut":
        files.read_bytes.return_value = b"{}"
    if failure == "release":
        release.manifest.environment_id = "wrong-environment"
    publication = patch.object(
        operator, "publish_manifest_package",
        return_value=SimpleNamespace(rejected=1),
    ) if failure == "publication" else patch.object(
        operator, "publish_manifest_package", wraps=operator.publish_manifest_package
    )
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient") as fabric, publication, pytest.raises(
        (ValueError, RuntimeError), match="publication|manifest|Environment"
    ):
        operator.main(setup.args + ["--submit"])
    assert not fabric.return_value.request_response.called


@pytest.mark.parametrize("status", ["Failed", "Cancelled", "Deduped"])
def test_failed_job_is_not_success_and_retry_does_not_resubmit(setup, status):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job-1"}, b"")
    fabric.request_json.return_value = {"status": status}
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        for _ in range(2):
            with pytest.raises(RuntimeError, match=status):
                operator.main(setup.args + ["--submit"])
    assert fabric.request_response.call_count == 1


def test_notebook_only_prepares_packages():
    path = Path(__file__).parents[1] / "notebooks/manifest_package_operator_tutorial.ipynb"
    notebook = json.loads(path.read_text())
    code = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )
    assert "RUN_PUBLICATION" not in code
    assert '"publish-manifests"' not in code
    assert "bulk-register" not in code
    assert "prepare-manifests" in code
    text = "\n".join("".join(cell["source"]) for cell in notebook["cells"])
    assert "backfill_operator" in text
    assert "10.3 Translate" not in text


@pytest.mark.parametrize("bad_size,bad_digest", [(True, False), (False, True)])
def test_onelake_video_integrity_checks(bad_size, bad_digest):
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    file = Mock()
    file.download_file.return_value.chunks.return_value = [b"vi", b"deo"]
    files._file = Mock(return_value=file)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        files.verify_video(
            "Files/videos/video.mp4", 99 if bad_size else 5,
            "0" * 64 if bad_digest else hashlib.sha256(b"video").hexdigest(),
        )


def test_onelake_model_verification_does_not_require_video_size():
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    file = Mock()
    file.download_file.return_value.chunks.return_value = [b"model"]
    files._file = Mock(return_value=file)
    files.verify_video("Files/models/model.onnx", None, hashlib.sha256(b"model").hexdigest())


def test_onelake_video_verification_accumulates_all_chunks():
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    file = Mock()
    file.download_file.return_value.chunks.return_value = [b"vi", b"deo"]
    files._file = Mock(return_value=file)
    files.verify_video("Files/videos/video.mp4", 5, hashlib.sha256(b"video").hexdigest())
    files._file.assert_called_once_with("Files/videos/video.mp4")


@pytest.mark.parametrize("existing", [b"partition", b"changed"])
def test_onelake_partition_never_overwrites_existing_content(existing):
    from azure.core.exceptions import ResourceExistsError

    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    files._exists_error = ResourceExistsError
    files._lakehouse = "lakehouse"
    files._filesystem = Mock()
    files._filesystem.create_directory.side_effect = ResourceExistsError()
    file = Mock()
    file.upload_data.side_effect = ResourceExistsError()
    files._file = Mock(return_value=file)
    files.read_bytes = Mock(return_value=existing)
    if existing == b"partition":
        files.write_partition(f"{operator.INTAKE_ROOT}/run/partition.json", b"partition")
    else:
        with pytest.raises(ValueError, match="conflicts"):
            files.write_partition(f"{operator.INTAKE_ROOT}/run/partition.json", b"partition")
    file.upload_data.assert_called_once_with(b"partition", overwrite=False)


def test_partition_readback_failure_is_explicit():
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    files._lakehouse = "lakehouse"
    files._filesystem = Mock()
    files._file = Mock()
    files.read_bytes = Mock(return_value=b"changed")
    with pytest.raises(ValueError, match="readback mismatch"):
        files.write_partition(f"{operator.INTAKE_ROOT}/run/partition.json", b"partition")


def test_partition_bounds_are_exact(setup):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    records = [{"work_id": f"work-{index}"} for index in range(1001)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(
        operator, "registration_records", return_value=records
    ), patch.object(
        operator, "publish_manifest_package",
        return_value=SimpleNamespace(rejected=0, published=1001, already_published=0),
    ), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ), patch.object(
        storage, "stat", return_value=SimpleNamespace(etag='"video-etag"')
    ):
        assert operator.main(setup.args + ["--submit"]) == 0
    partitions = files.write_partition.call_args_list
    assert [len(json.loads(call.args[1])["items"]) for call in partitions] == [1000, 1]
    commands = [
        call.kwargs["value"]["executionData"]["commandLineArguments"]
        for call in fabric.request_response.call_args_list
    ]
    assert commands[0].endswith("--max-items 1000")
    assert commands[1].endswith("--max-items 1")
    for call, command in zip(partitions, commands):
        assert hashlib.sha256(call.args[1]).hexdigest() in command


def test_unknown_submission_outcome_requires_recovery(setup):
    config = operator.load_config(setup.config_path)
    state = setup.root / "job.json"
    state.write_text('{"status":"submitting"}')
    client = Mock()
    with pytest.raises(RuntimeError, match="outcome unknown"):
        operator._register_partition(client, config, "partition.json", b"{}", 1, state)
    client.request_response.assert_not_called()


def test_pending_job_timeout_preserves_location_for_retry(setup):
    config = operator.load_config(setup.config_path)
    state = setup.root / "job.json"
    state.write_text('{"status":"submitted","location":"/jobs/job"}')
    client = Mock()
    client.request_json.return_value = {"status": "InProgress"}
    with patch.object(operator.time, "monotonic", side_effect=[0, 1, 1801]), patch.object(
        operator.time, "sleep"
    ), pytest.raises(TimeoutError, match="resume polling"):
        operator._register_partition(client, config, "partition.json", b"{}", 1, state)
    client.request_response.assert_not_called()
    assert json.loads(state.read_text())["location"] == "/jobs/job"


def test_state_directory_cannot_change_release_or_target(setup):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        assert operator.main(setup.args + ["--submit"]) == 0
        release.manifest_sha256 = "e" * 64
        with pytest.raises(ValueError, match="different package"):
            operator.main(setup.args + ["--submit"])
    assert fabric.request_response.call_count == 1


@pytest.mark.parametrize("wait", [False, True])
def test_wait_cli_only_monitors_after_submission(setup, capsys, wait):
    events = []
    connection = Mock()

    @contextmanager
    def reader(config):
        events.append("reader")
        yield connection

    def submitted(*args):
        events.append("submit")
        return {"status": "registered", "processing_complete": False}

    def monitored(*args):
        events.append("wait")
        assert args[0] is connection
        return {"status": "processed", "processing_complete": True}

    with patch.object(operator, "processing_reader", side_effect=reader), patch.object(
        operator, "submit", side_effect=submitted
    ), patch.object(operator, "wait_for_processing", side_effect=monitored):
        assert operator.main(setup.args + ["--submit"] + (["--wait"] if wait else [])) == 0
    assert events == (["reader", "submit", "wait"] if wait else ["submit"])
    result = json.loads(capsys.readouterr().out)
    assert result["processing_complete"] is wait
    assert result["status"] == ("processed" if wait else "registered")


def test_processing_progress_requires_every_exact_work_id_and_committed_success():
    rows = [
        {"work_id": "unrelated", "status": "SUCCEEDED", "committed_attempt_id": "other"},
        {"work_id": "one", "status": "SUCCEEDED", "committed_attempt_id": "attempt-1"},
        {"work_id": "two", "status": "SUCCEEDED", "committed_attempt_id": None},
        {"work_id": "three", "status": "LEASED", "committed_attempt_id": None},
    ]
    result = operator.processing_progress(["one", "two", "three", "four"], rows)
    assert result["item_count"] == 4
    assert result["completed"] == 1
    assert result["processing_complete"] is False
    assert result["counts"] == {
        "SUCCEEDED": 1, "WAITING_COMMIT": 1, "LEASED": 1, "WAITING_REGISTRATION": 1,
    }


@pytest.mark.parametrize("status", ["DEAD", "DEAD_LETTER", "FAILED", "QUARANTINED", "CANCELLED"])
def test_processing_progress_reports_terminal_failures(status):
    result = operator.processing_progress(["work"], [{
        "work_id": "work", "status": status, "committed_attempt_id": None,
        "last_error": "source could not be decoded",
    }])
    assert result["processing_complete"] is False
    assert result["failures"] == [{
        "work_id": "work", "status": status, "last_error": "source could not be decoded",
    }]


def test_processing_progress_rejects_duplicate_rows():
    row = {"work_id": "work", "status": "SUCCEEDED", "committed_attempt_id": "attempt"}
    with pytest.raises(RuntimeError, match="Duplicate"):
        operator.processing_progress(["work"], [row, row])


def processing_state(setup, ids=("one", "two")):
    state = setup.root / "state"
    state.mkdir(exist_ok=True)
    (state / "registration-plan.json").write_text(json.dumps({
        "items": [{"work_id": work_id} for work_id in ids],
    }))
    return state


def test_wait_polls_exact_ids_persists_progress_and_then_finishes(setup, capsys):
    config = operator.load_config(setup.config_path)
    state = processing_state(setup)
    connection = Mock()
    initial = [
        {"work_id": "one", "status": "READY", "committed_attempt_id": None},
        {"work_id": "two", "status": "LEASED", "committed_attempt_id": None},
    ]
    complete = [
        {"work_id": "one", "status": "SUCCEEDED", "committed_attempt_id": "attempt-1"},
        {"work_id": "two", "status": "SUCCEEDED", "committed_attempt_id": "attempt-2"},
    ]
    with patch.object(
        operator, "read_demo_delta", side_effect=[initial, complete]
    ) as read, patch.object(operator.time, "sleep") as sleep:
        report = operator.wait_for_processing(connection, config, state)
    assert report["status"] == "processed"
    assert report["processing_complete"] is True
    assert report["completed"] == 2
    assert report["committed_attempts"] == {"one": "attempt-1", "two": "attempt-2"}
    assert json.loads((state / "processing-summary.json").read_text()) == report
    assert read.call_count == 2
    for call in read.call_args_list:
        assert call.args == (
            connection,
            f"abfss://{config.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{config.lakehouse_id}/Tables/dbo/people_counter_sjd_",
            "work",
            ["work_id", "status", "committed_attempt_id", "last_error"],
            [("work_id", "in", ["one", "two"])],
        )
    sleep.assert_called_once_with(15)
    output = capsys.readouterr()
    assert output.out == ""
    assert "0/2" in output.err
    assert "2/2" in output.err
    assert "READY=1" in output.err
    assert "LEASED=1" in output.err


def test_wait_stops_on_dead_work_with_saved_diagnostics(setup):
    config = operator.load_config(setup.config_path)
    state = processing_state(setup, ("dead",))
    with patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "dead", "status": "DEAD", "committed_attempt_id": None,
        "last_error": "model unavailable",
    }]), patch.object(
        operator.time, "sleep", side_effect=AssertionError("Terminal work must stop the wait")
    ) as sleep, pytest.raises(
        RuntimeError, match="dead.*DEAD.*model unavailable"
    ):
        operator.wait_for_processing(Mock(), config, state)
    sleep.assert_not_called()
    report = json.loads((state / "processing-summary.json").read_text())
    assert report["status"] == "failed"
    assert report["failures"][0]["last_error"] == "model unavailable"


def test_wait_read_failure_is_not_replaced_with_success(setup):
    state = processing_state(setup)
    with patch.object(
        operator, "read_demo_delta", side_effect=PermissionError("OneLake access denied")
    ), pytest.raises(PermissionError, match="denied"):
        operator.wait_for_processing(Mock(), operator.load_config(setup.config_path), state)
    assert not (state / "processing-summary.json").exists()


def test_processing_reader_preflights_table_and_closes_connection(setup):
    connection = Mock()
    duckdb = Mock()
    duckdb.connect.return_value = connection
    config = operator.load_config(setup.config_path)
    with patch.dict("sys.modules", {"duckdb": duckdb}), patch.object(
        operator, "read_demo_delta", return_value=[]
    ) as read:
        with operator.processing_reader(config) as actual:
            assert actual is connection
    connection.close.assert_called_once_with()
    assert read.call_args.args[2] == "work"
    assert read.call_args.args[4] == [("work_id", "in", ["backfill-monitor-preflight"])]
    setup_sql = " ".join(call.args[0] for call in connection.execute.call_args_list)
    assert "LOAD delta" in setup_sql
    assert "CHAIN 'cli'" in setup_sql
    assert "onelake.blob.fabric.microsoft.com" in setup_sql


def test_wait_dependency_failure_prevents_submission(setup):
    with patch.dict("sys.modules", {"duckdb": None}), patch.object(
        operator, "submit"
    ) as submit, pytest.raises(RuntimeError, match="--extra demo"):
        operator.main(setup.args + ["--submit", "--wait"])
    submit.assert_not_called()


def test_ctrl_c_stops_monitoring_without_cancelling_and_can_resume(setup, capsys):
    connection = Mock()
    state = processing_state(setup, ("work",))
    fabric = Mock()

    @contextmanager
    def reader(config):
        yield connection

    with patch.object(operator, "processing_reader", side_effect=reader), patch.object(
        operator, "submit", return_value={"status": "registered", "processing_complete": False}
    ) as submit, patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "work", "status": "LEASED", "committed_attempt_id": None,
    }]), patch.object(operator.time, "sleep", side_effect=KeyboardInterrupt), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ):
        assert operator.main(setup.args + ["--submit", "--wait"]) == 130
        assert submit.call_count == 1
    assert not fabric.mock_calls
    assert json.loads((state / "processing-summary.json").read_text())["completed"] == 0
    output = capsys.readouterr()
    assert "Fabric" in output.err
    assert "continue" in output.err
    assert "--submit --wait" in output.err
    assert json.loads(output.out) == {
        "status": "monitoring_stopped", "processing_complete": False,
    }
    with patch.object(operator, "processing_reader", side_effect=reader), patch.object(
        operator, "submit", return_value={"status": "registered", "processing_complete": False}
    ), patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "work", "status": "SUCCEEDED", "committed_attempt_id": "attempt",
    }]), patch.object(
        operator.time, "sleep", side_effect=AssertionError("Resumed work is already complete")
    ):
        assert operator.main(setup.args + ["--submit", "--wait"]) == 0
    assert json.loads(capsys.readouterr().out)["processing_complete"] is True


def test_publication_and_registration_print_progress_to_stderr(setup, capsys):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        assert operator.main(setup.args + ["--submit"]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "registered"
    assert "Publishing 1" in output.err
    assert "Publication complete" in output.err
    assert "partition-00001" in output.err
    assert "Completed" in output.err


@pytest.mark.parametrize("directory", ["Tables", "Tables/dbo", "Tables/custom_schema"])
def test_monitor_uses_configured_lakehouse_table_directory(setup, directory):
    setup.config_path.write_text(json.dumps({**setup.config, "table_directory": directory}))
    config = operator.load_config(setup.config_path)
    assert operator._table_prefix(config) == (
        f"abfss://{config.workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{config.lakehouse_id}/{directory}/people_counter_sjd_"
    )


@pytest.mark.parametrize("ids", [[], ["work", "work"]])
def test_monitor_rejects_empty_or_repeated_expected_ids(ids):
    with pytest.raises(ValueError, match="nonempty, unique"):
        operator.processing_progress(ids, [])


@pytest.mark.parametrize("status", [None, ""])
def test_monitor_rejects_invalid_status(status):
    with pytest.raises(ValueError, match="Invalid work status"):
        operator.processing_progress(["work"], [{
            "work_id": "work", "status": status, "committed_attempt_id": None,
        }])


def test_processing_reader_permission_failure_closes_and_never_submits(setup):
    duckdb = Mock()
    connection = duckdb.connect.return_value
    with patch.dict("sys.modules", {"duckdb": duckdb}), patch.object(
        operator, "read_demo_delta", side_effect=PermissionError("work table access denied")
    ), patch.object(operator, "submit") as submit, pytest.raises(
        PermissionError, match="denied"
    ):
        operator.main(setup.args + ["--submit", "--wait"])
    connection.close.assert_called_once_with()
    submit.assert_not_called()


def test_wait_queries_bounded_batches_of_work_ids(setup):
    ids = tuple(f"work-{index}" for index in range(1001))
    state = processing_state(setup, ids)
    config = operator.load_config(setup.config_path)

    def rows(connection, prefix, suffix, columns, filters):
        return [
            {"work_id": work_id, "status": "SUCCEEDED", "committed_attempt_id": "attempt"}
            for work_id in filters[0][2]
        ]

    with patch.object(operator, "read_demo_delta", side_effect=rows) as read, patch.object(
        operator.time, "sleep", side_effect=AssertionError("All queried work must be complete")
    ):
        report = operator.wait_for_processing(Mock(), config, state)
    assert report["completed"] == 1001
    assert [len(call.args[4][0][2]) for call in read.call_args_list] == [1000, 1]


def test_wait_observes_new_delta_commits_and_ignores_unrelated_success(setup):
    import duckdb
    import pyarrow as pa
    from deltalake import write_deltalake

    table = setup.root / "tables/people_counter_sjd_work"
    table.parent.mkdir()
    state = processing_state(setup, ("selected",))
    config = operator.load_config(setup.config_path)

    def write(status, attempt, mode):
        write_deltalake(table, pa.table({
            "work_id": ["selected", "unrelated"],
            "status": [status, "SUCCEEDED"],
            "committed_attempt_id": pa.array([attempt, "other-attempt"], type=pa.string()),
            "last_error": pa.array([None, None], type=pa.string()),
        }), mode=mode)

    write("READY", None, "error")
    finished = False

    def finish_processing(_):
        nonlocal finished
        if finished:
            raise AssertionError("The next query must observe the committed Delta update")
        finished = True
        write("SUCCEEDED", "selected-attempt", "overwrite")

    with duckdb.connect() as connection:
        connection.execute("INSTALL delta; LOAD delta")
        with patch.object(
            operator, "_table_prefix", return_value=str(table.parent / "people_counter_sjd_")
        ), patch.object(
            operator.time, "sleep",
            side_effect=finish_processing,
        ) as sleep:
            result = operator.wait_for_processing(connection, config, state)
    sleep.assert_called_once_with(15)
    assert result["completed"] == 1
    assert result["committed_attempts"] == {"selected": "selected-attempt"}


@pytest.mark.parametrize("flag", [
    "--manifest-package-dir", "--video-root", "--config", "--state-dir",
])
def test_required_cli_paths_are_enforced_by_parser(setup, flag):
    args = list(setup.args)
    position = args.index(flag)
    del args[position:position + 2]
    with pytest.raises(SystemExit) as error:
        operator.main(args)
    assert error.value.code == 2


@pytest.mark.parametrize("count,partitions", [(1000, 1), (1001, 2)])
def test_preview_partition_count_respects_exact_boundaries(setup, capsys, count, partitions):
    with patch.object(operator, "validate_manifest_package", return_value=SimpleNamespace(
        plans=[None] * count, index_bytes=b"index",
    )):
        assert operator.main(setup.args) == 0
    assert json.loads(capsys.readouterr().out)["partition_count"] == partitions


def test_submit_wait_integrates_publication_registration_processing_and_resume(setup, capsys):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"id": "job", "status": "Completed"}
    connection = Mock()
    state = setup.root / "state"

    @contextmanager
    def reader(config):
        assert config.workspace_id == setup.config["workspace_id"]
        yield connection

    def completed_rows(*args):
        plan = json.loads((state / "registration-plan.json").read_text())
        ids = [row["work_id"] for row in plan["items"]]
        assert args[4] == [("work_id", "in", ids)]
        return [
            {"work_id": work_id, "status": "SUCCEEDED", "committed_attempt_id": "attempt"}
            for work_id in ids
        ]

    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "resolve_published_release", return_value=release
    ), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ), patch.object(
        operator, "processing_reader", side_effect=reader
    ), patch.object(
        operator, "read_demo_delta", side_effect=completed_rows
    ), patch.object(
        operator.time, "sleep", side_effect=AssertionError("Processing is already complete")
    ):
        for _ in range(2):
            assert operator.main(setup.args + ["--submit", "--wait"]) == 0
            report = json.loads(capsys.readouterr().out)
            assert report["status"] == "processed"
            assert report["processing_complete"] is True
            assert report["completed"] == report["item_count"] == 1
            assert report["jobs"][0]["job"]["id"] == "job"
    assert fabric.request_response.call_count == 1
    assert json.loads((state / "processing-summary.json").read_text())["completed"] == 1

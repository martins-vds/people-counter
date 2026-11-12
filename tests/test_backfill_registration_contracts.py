import hashlib
import json
import shlex
from unittest.mock import Mock, call, patch

import pytest

from people_counter import backfill_operator as operator
from tests.test_backfill_operator import cloud_mocks, forbid_live_cloud_clients, setup
from tests.test_manifest_publisher import inspection


@pytest.mark.parametrize("reply,headers,job_id", [
    ({}, {"LoCaTiOn": "/jobs/from-header"}, None),
    ({"id": "primary", "jobInstanceId": "secondary"}, {}, "primary"),
    ({"jobInstanceId": "secondary"}, {}, "secondary"),
    ({"id": "", "jobInstanceId": "secondary"}, {}, "secondary"),
])
def test_registration_handoff_is_persisted_before_submission_and_before_polling(
    setup, tmp_path, reply, headers, job_id,
):
    config = operator.load_config(setup.config_path)
    state_path = tmp_path / "job.json"
    content = b'{"items":[]}'
    digest = hashlib.sha256(content).hexdigest()
    path = config.runtime_config().file_path("intake/backfill/run/partition.json")
    pending = {
        "status": "submitting", "partition_path": path,
        "partition_sha256": digest, "item_count": 7,
    }
    location = (
        f"/workspaces/{config.workspace_id}/items/{config.control_sjd_id}/jobs/instances/{job_id}"
        if job_id is not None else "/jobs/from-header"
    )
    client = Mock()

    def submit(method, url, *, value, expected):
        assert json.loads(state_path.read_bytes()) == pending
        assert state_path.read_bytes() == (
            json.dumps(pending, sort_keys=True, allow_nan=False, indent=2) + "\n"
        ).encode()
        assert (method, url, expected) == (
            "POST",
            f"/workspaces/{config.workspace_id}/sparkJobDefinitions/{config.control_sjd_id}/jobs/sparkjob/instances",
            (202,),
        )
        assert value == {"executionData": {"commandLineArguments": shlex.join([
            *config.runtime_config().command_line_arguments(),
            "bulk-register", "--partition-path", path,
            "--partition-sha256", digest, "--max-items", "7",
        ])}}
        return 202, headers, json.dumps(reply).encode()

    completed = {"status": "Completed", "id": job_id or "header-job"}

    def poll(method, url):
        assert (method, url) == ("GET", location)
        assert json.loads(state_path.read_bytes()) == {**pending, "status": "submitted", "location": location}
        return completed

    client.request_response.side_effect = submit
    client.request_json.side_effect = poll
    messages = []
    with operator.progress_reporter(messages.append):
        result = operator._register_partition(client, config, path, content, 7, state_path)
    assert result == {**pending, "status": "Completed", "location": location, "job": completed}
    assert json.loads(state_path.read_bytes()) == result
    assert messages == [
        "Submitting registration partition.json: 7 items",
        f"Registration job: {location}",
        "Registration partition.json: Completed",
    ]
    client.request_response.assert_called_once()
    client.request_json.assert_called_once()


@pytest.mark.parametrize("reply", [{}, {"id": None}, {"id": 1}, {"jobInstanceId": ""}])
def test_missing_job_identity_keeps_the_ambiguous_submission_state(setup, tmp_path, reply):
    config = operator.load_config(setup.config_path)
    path = tmp_path / "job.json"
    client = Mock()
    client.request_response.return_value = (202, {}, json.dumps(reply).encode())
    with pytest.raises(RuntimeError) as error:
        operator._register_partition(client, config, "partition.json", b"{}", 1, path)
    assert str(error.value) == "Fabric submission omitted the job ID and Location; inspect job history"
    assert json.loads(path.read_bytes())["status"] == "submitting"
    client.request_json.assert_not_called()


def test_empty_submission_response_is_an_explicit_missing_job_error(setup, tmp_path):
    config = operator.load_config(setup.config_path)
    client = Mock()
    client.request_response.return_value = (202, {}, b"")
    with pytest.raises(RuntimeError) as error:
        operator._register_partition(client, config, "partition.json", b"{}", 1, tmp_path / "job.json")
    assert str(error.value) == "Fabric submission omitted the job ID and Location; inspect job history"
    client.request_json.assert_not_called()


def test_registration_deadline_is_exact_and_exclusive(setup, tmp_path):
    config = operator.load_config(setup.config_path)
    path = tmp_path / "job.json"
    previous = {"status": "submitted", "location": "/jobs/accepted"}
    path.write_text(json.dumps(previous))
    client = Mock()
    client.request_json.return_value = {"status": "Completed"}
    with patch.object(operator.time, "monotonic", side_effect=[0, 1800]):
        with pytest.raises(TimeoutError) as error:
            operator._register_partition(client, config, "partition.json", b"{}", 1, path)
    assert str(error.value) == "Control registration timed out; rerun to resume polling /jobs/accepted"
    assert json.loads(path.read_bytes()) == previous
    client.request_json.assert_not_called()
    client.request_response.assert_not_called()


@pytest.mark.parametrize("existing", [False, True])
def test_partition_creation_uses_the_exact_remote_path_and_ignores_only_existing_directories(existing):
    class AlreadyExists(Exception):
        pass

    files = object.__new__(operator.OneLakeFiles)
    files._filesystem = Mock()
    files._lakehouse = "selected-lakehouse"
    files._exists_error = AlreadyExists
    path = "Files/chosen/intake/partition.json"
    file = Mock()
    if existing:
        files._filesystem.create_directory.side_effect = AlreadyExists
        file.upload_data.side_effect = AlreadyExists
    files._file = Mock(return_value=file)
    files.read_bytes = Mock(return_value=b"partition")
    files.write_partition(path, b"partition")
    assert files._filesystem.create_directory.call_args_list == [
        call(f"selected-lakehouse/{directory}")
        for directory in ("Files", "Files/chosen", "Files/chosen/intake")
    ]
    files._file.assert_called_once_with(path)
    file.upload_data.assert_called_once_with(b"partition", overwrite=False)
    assert files.read_bytes.call_args_list == [call(path)] * (2 if existing else 1)


def test_a_completed_registration_is_reused_without_any_cloud_calls(setup, tmp_path):
    config = operator.load_config(setup.config_path)
    path = tmp_path / "completed.json"
    previous = {"status": "Completed", "location": "/jobs/accepted", "job": {"id": "keep"}}
    path.write_text(json.dumps(previous))
    client = Mock()
    messages = []
    with operator.progress_reporter(messages.append):
        assert operator._register_partition(client, config, "partition.json", b"{}", 1, path) == previous
    assert messages == ["Registration partition.json: Completed (previous submission)"]
    assert json.loads(path.read_bytes()) == previous
    client.request_json.assert_not_called()
    client.request_response.assert_not_called()


def test_an_unknown_submission_is_not_resent_or_discarded(setup, tmp_path):
    config = operator.load_config(setup.config_path)
    path = tmp_path / "ambiguous.json"
    previous = {"status": "submitting", "partition_sha256": "a" * 64}
    path.write_text(json.dumps(previous))
    client = Mock()
    with pytest.raises(RuntimeError) as error:
        operator._register_partition(client, config, "partition.json", b"{}", 1, path)
    assert str(error.value) == f"submission outcome unknown; inspect Fabric job history before recovering {path}"
    assert json.loads(path.read_bytes()) == previous
    client.request_json.assert_not_called()
    client.request_response.assert_not_called()


@pytest.mark.parametrize("status", ["Failed", "Cancelled", "Canceled", "Deduped"])
def test_terminal_registration_failure_keeps_the_full_remote_job_and_diagnostic(setup, tmp_path, status):
    config = operator.load_config(setup.config_path)
    path = tmp_path / "failed.json"
    path.write_text(json.dumps({"status": "submitted", "location": "/jobs/accepted"}))
    client = Mock()
    job = {"status": status, "id": "accepted", "reason": "observed failure"}
    client.request_json.return_value = job
    with pytest.raises(RuntimeError) as error:
        operator._register_partition(client, config, "partition.json", b"{}", 1, path)
    assert str(error.value) == f"Control registration job {status}: {job}"
    assert json.loads(path.read_bytes()) == {"status": status, "location": "/jobs/accepted", "job": job}
    client.request_json.assert_called_once_with("GET", "/jobs/accepted")
    client.request_response.assert_not_called()


def test_submission_summary_and_progress_do_not_claim_processing_or_gold_completion(setup):
    storage, files, release = cloud_mocks(setup)
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(setup.package, setup.root, setup.root / "state", config)
    package = operator.validate_manifest_package(publication)
    client = Mock()
    client.request_response.return_value = (202, {"Location": "/jobs/accepted"}, b"")
    client.request_json.return_value = {"id": "accepted", "status": "Completed"}
    progress = []
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "AzureDataLakeStorage", return_value=storage,
    ), patch.object(
        operator, "installed_release", return_value=release,
    ), patch.object(
        operator, "FabricAutomationClient", return_value=client,
    ), operator.progress_reporter(progress.append):
        report = operator.submit(package, publication, config, setup.root / "state")
    assert report == {
        "status": "registered", "item_count": 1, "partition_count": 1,
        "processing_complete": False,
        "jobs": [json.loads((setup.root / "state/partition-00001-job.json").read_bytes())],
        "next_step": "Monitor Dispatcher, committed work, Reconciliation, Gold, and Refresh.",
    }
    assert json.loads((setup.root / "state/registration-summary.json").read_bytes()) == report
    assert progress[0:2] == [
        "Using SDK 1.2.3", "Verifying deployed model artifacts...",
    ]
    expected_models = [
        f"Verifying model: Files/models/{name}" for name in config.profile["model_artifact_sha256"]
    ]
    assert progress[2:2 + len(expected_models)] == expected_models
    assert progress[2 + len(expected_models)] == "Publishing 1 videos and manifests (or verifying existing publications)..."


def test_submission_keeps_source_order_and_reuses_the_validated_multi_video_package(setup):
    from people_counter.manifest import manifest_bytes
    from people_counter.manifest_publisher import PublisherConfig, run_publisher

    video = setup.root / "north/camera-17/20260101T120001Z.mp4"
    video.write_bytes(b"video")
    setup.package = setup.root / "multi-package"
    run_publisher(
        PublisherConfig(
            setup.root / "catalog.csv", None, setup.root, None, "account", "footage",
            "staging", "incoming", setup.root / "multi.sqlite3",
            setup.root / "multi-reject.csv", setup.root / "multi-inventory.csv",
            10, True, output_dir=setup.package,
            summary_report_path=setup.root / "multi-summary.json",
        ),
        storage=None, inspector=inspection,
    )
    storage, files, release = cloud_mocks(setup)
    config = operator.load_config(setup.config_path)
    state = setup.root / "missing" / "nested" / "state"
    publication = operator.publication_config(setup.package, setup.root, state, config)
    package = operator.validate_manifest_package(publication)
    assert len(package.plans) == 2
    by_manifest = {
        f"{config.shortcut_path}/{plan.incoming_manifest_path}": plan
        for plan in package.plans
    }

    def read(path):
        plan = by_manifest[path]
        remote = storage.stat(plan.incoming_video_path)
        return manifest_bytes(
            plan, storage_account=config.storage_account,
            filesystem=config.filesystem, source_etag=remote.etag,
        )

    files.read_bytes.side_effect = read
    client = Mock()
    client.request_response.return_value = (202, {"Location": "/jobs/accepted"}, b"")
    client.request_json.return_value = {"id": "accepted", "status": "Completed"}
    progress = []
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "AzureDataLakeStorage", return_value=storage,
    ), patch.object(
        operator, "installed_release", return_value=release,
    ), patch.object(
        operator, "FabricAutomationClient", return_value=client,
    ), patch.object(
        operator, "publish_manifest_package", wraps=operator.publish_manifest_package,
    ) as publish, operator.progress_reporter(progress.append):
        report = operator.submit(package, publication, config, state)
    publish.assert_called_once_with(publication, storage, validated_package=package)
    assert report["item_count"] == 2
    assert [message for message in progress if message.startswith("Verified Lakehouse source")] == [
        f"Verified Lakehouse source {index}/2: {plan.relative_path}"
        for index, plan in enumerate(sorted(package.plans, key=lambda value: value.relative_path), start=1)
    ]


@pytest.mark.parametrize("stage", ["model", "state", "publication", "missing_video"])
def test_submission_failures_keep_the_correct_operator_recovery_instruction(setup, stage):
    from types import SimpleNamespace

    storage, files, release = cloud_mocks(setup)
    config = operator.load_config(setup.config_path)
    state = setup.root / "state"
    publication = operator.publication_config(setup.package, setup.root, state, config)
    package = operator.validate_manifest_package(publication)
    if stage == "model":
        files.verify_video.side_effect = ValueError("model hash mismatch")
        message = "model hash mismatch"
    elif stage == "state":
        state.mkdir()
        (state / "registration-plan.json").write_text("{}")
        message = "state directory belongs to a different package, target, profile, or release"
    elif stage == "publication":
        message = "publication incomplete; inspect publication-rejections.csv before retrying"
    else:
        message = f"published video missing: {package.plans[0].incoming_video_path}"
    publisher = Mock(return_value=SimpleNamespace(
        rejected=1 if stage == "publication" else 0,
        published=0 if stage == "publication" else 1, already_published=0,
    ))
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "AzureDataLakeStorage", return_value=storage,
    ), patch.object(
        operator, "installed_release", return_value=release,
    ), patch.object(operator, "publish_manifest_package", publisher):
        with pytest.raises((ValueError, RuntimeError)) as error:
            operator.submit(package, publication, config, state)
    assert str(error.value) == message
    if stage in {"model", "state"}:
        publisher.assert_not_called()
    files.write_partition.assert_not_called()

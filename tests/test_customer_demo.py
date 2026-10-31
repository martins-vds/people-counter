import hashlib
import ast
import json
import sys
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import Mock
from pathlib import Path
from types import SimpleNamespace

import pytest

from people_counter.adls import AzureDataLakeStorage, PublicationConflictError, RemoteObject
from people_counter.customer_demo import (
    asset_progress,
    azure_cli,
    ensure_capacity,
    ensure_storage_role,
    promote_staged,
    read_demo_delta,
    verify_arrival_runtime,
    verify_release_alignment,
    raise_processing_failure,
    verify_compute_policy,
    stage_videos,
)
from people_counter.manifest import CameraCatalogEntry, ManifestPlan, VideoInspection
from people_counter.manifest_publisher import CheckpointStore, PublisherConfig


@pytest.fixture
def plan(tmp_path):
    source = tmp_path / "clip.mp4"
    source.write_bytes(b"video")
    digest = hashlib.sha256(b"video").hexdigest()
    now = datetime(2023, 10, 1, tzinfo=timezone.utc)
    camera = CameraCatalogEntry(
        "demo/", "camera", "location", "UTC", 1920, 1080,
        (0, 540, 1919, 540), "embedded", None, None, now, None,
    )
    return ManifestPlan(
        source, "demo/clip.mp4", camera, now, "embedded",
        VideoInspection(5, digest, 1920, 1080, 7.0),
        "a" * 64, digest, "staging/video.mp4", "incoming/video.mp4",
        "staging/video.json", "incoming/video.json", "video/mp4",
    )


class Storage:
    def __init__(self):
        self.files = {}
        self.operations = []

    def stat(self, path):
        return RemoteObject(len(self.files[path]), '"etag"') if path in self.files else None

    def sha256(self, path):
        return hashlib.sha256(self.files[path]).hexdigest()

    def read_bytes(self, path):
        return self.files[path]

    def upload_file(self, local_path, remote_path, *args):
        self.operations.append(("upload", remote_path))
        self.files[remote_path] = local_path.read_bytes()

    def upload_bytes(self, content, remote_path, content_type):
        self.operations.append(("manifest", remote_path))
        self.files[remote_path] = content

    def rename(self, source, destination):
        self.operations.append(("rename", source, destination))
        self.files[destination] = self.files.pop(source)


def test_staging_uploads_only_video_and_is_retry_safe(plan):
    storage = Storage()
    stage_videos(storage, [plan])
    stage_videos(storage, [plan])
    assert storage.files == {plan.staging_video_path: b"video"}
    assert storage.operations == [("upload", plan.staging_video_path)]


@pytest.mark.parametrize("path", ["staging/video.mp4", "incoming/video.mp4", "incoming/video.json"])
def test_stage_refuses_conflicts_or_previously_published_input(plan, path):
    storage = Storage()
    storage.files[path] = b"wrong"
    with pytest.raises(PublicationConflictError):
        stage_videos(storage, [plan])
    assert storage.files[path] == b"wrong"
    assert not storage.operations


def test_stage_detects_corrupt_upload(plan):
    storage = Storage()
    storage.upload_file = lambda *args: storage.files.update({plan.staging_video_path: b"wrong"})
    with pytest.raises(PublicationConflictError, match="checksum"):
        stage_videos(storage, [plan])


def publisher_config(tmp_path):
    return PublisherConfig(
        tmp_path / "catalog.csv", None, tmp_path, None, "account", "videos",
        "staging", "incoming", tmp_path / "state.db", tmp_path / "reject.csv",
        tmp_path / "inventory.csv", 10, False,
    )


def test_promotion_requires_staged_integrity_and_publishes_manifest_last(plan, tmp_path):
    storage = Storage()
    config = publisher_config(tmp_path)
    checkpoint = CheckpointStore(config.checkpoint_path)
    try:
        with pytest.raises(PublicationConflictError, match="staged"):
            promote_staged(storage, checkpoint, config, [plan])
        assert not storage.operations
        stage_videos(storage, [plan])
        assert promote_staged(storage, checkpoint, config, [plan]) == 1
        assert storage.operations[-1] == ("rename", plan.staging_manifest_path, plan.incoming_manifest_path)
        manifest = json.loads(storage.files[plan.incoming_manifest_path])
        assert manifest["source_etag"] == '"etag"'
        assert manifest["expected_sha256"] == plan.asset_version
        assert promote_staged(storage, checkpoint, config, [plan]) == 0
    finally:
        checkpoint.close()


def test_promotion_validates_all_staged_files_before_any_rename(plan, tmp_path):
    storage = Storage()
    stage_videos(storage, [plan])
    other = replace(plan, staging_video_path="staging/missing.mp4")
    checkpoint = CheckpointStore(tmp_path / "checkpoint.db")
    try:
        with pytest.raises(PublicationConflictError):
            promote_staged(storage, checkpoint, publisher_config(tmp_path), [plan, other])
        assert not any(operation[0] == "rename" for operation in storage.operations)
    finally:
        checkpoint.close()


def test_capacity_active_needs_no_resume():
    cli = Mock(return_value={"properties": {"state": "Active"}})
    assert ensure_capacity("/capacity", cli=cli) == "Active"
    assert cli.call_count == 1


def test_azure_cli_surfaces_errors_and_parses_output(monkeypatch):
    run = Mock(return_value=SimpleNamespace(returncode=0, stdout='{"state":"Active"}', stderr=""))
    monkeypatch.setattr("people_counter.customer_demo.subprocess.run", run)
    assert azure_cli(["account", "show"]) == {"state": "Active"}
    assert run.call_args.args[0] == ["az", "account", "show", "--output", "json", "--only-show-errors"]
    assert run.call_args.kwargs["timeout"] == 120
    run.return_value = SimpleNamespace(returncode=1, stdout="", stderr="AuthorizationFailed")
    with pytest.raises(RuntimeError, match="AuthorizationFailed"):
        azure_cli(["account", "show"])
    run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")
    assert azure_cli(["account", "show"]) == {}


@pytest.mark.parametrize("explicit", [False, True])
def test_storage_uses_explicit_cli_credential_without_changing_default(monkeypatch, explicit):
    default = Mock()
    service = Mock()
    monkeypatch.setitem(sys.modules, "azure.identity", SimpleNamespace(DefaultAzureCredential=default))
    monkeypatch.setitem(sys.modules, "azure.storage.filedatalake", SimpleNamespace(DataLakeServiceClient=service))
    provided = object()
    kwargs = {"credential": provided} if explicit else {}
    AzureDataLakeStorage("account", "videos", chunk_size=1024, **kwargs)
    assert service.call_args.kwargs["credential"] is (provided if explicit else default.return_value)
    assert default.call_count == (0 if explicit else 1)


def test_paused_capacity_requires_approval_then_waits_for_active():
    cli = Mock(side_effect=[
        {"properties": {"state": "Paused"}}, {},
        {"properties": {"state": "Resuming"}}, {"properties": {"state": "Active"}},
    ])
    with pytest.raises(RuntimeError, match="resume"):
        ensure_capacity("/capacity", cli=cli)
    cli.reset_mock(side_effect=True)
    cli.side_effect = [
        {"properties": {"state": "Paused"}}, {},
        {"properties": {"state": "Resuming"}}, {"properties": {"state": "Active"}},
    ]
    assert ensure_capacity("/capacity", allow_resume=True, cli=cli, sleep=lambda _: None) == "Active"
    assert cli.call_args_list[1].args[0][:4] == ["rest", "--method", "post", "--url"]
    assert "/capacity/resume?api-version=2023-11-01" in cli.call_args_list[1].args[0][4]


def test_capacity_timeout_and_unexpected_state_are_errors():
    with pytest.raises(RuntimeError, match="Failed"):
        ensure_capacity("/capacity", cli=Mock(return_value={"properties": {"state": "Failed"}}))
    clock = Mock(side_effect=[0, 2])
    with pytest.raises(TimeoutError):
        ensure_capacity("/capacity", cli=Mock(return_value={"properties": {"state": "Resuming"}}),
                        timeout=1, monotonic=clock)


def test_capacity_waits_through_paused_readback_after_resume():
    cli = Mock(side_effect=[
        {"properties": {"state": "Paused"}}, {},
        {"properties": {"state": "Paused"}}, {"properties": {"state": "Active"}},
    ])
    assert ensure_capacity("/capacity", allow_resume=True, cli=cli, sleep=lambda _: None) == "Active"
    assert sum(call.args[0][0] == "rest" for call in cli.call_args_list) == 1


def test_storage_role_is_container_scoped_and_never_granted_without_approval():
    cli = Mock(return_value=[])
    with pytest.raises(RuntimeError, match="Storage Blob Data Contributor"):
        ensure_storage_role("/storage", "videos", "principal", "User", cli=cli)
    assert cli.call_count == 1
    cli.reset_mock()
    cli.side_effect = [[], {}]
    ensure_storage_role("/storage", "videos", "principal", "User", allow_grant=True, cli=cli)
    command = cli.call_args.args[0]
    assert command[:3] == ["role", "assignment", "create"]
    assert command[command.index("--scope") + 1] == "/storage/blobServices/default/containers/videos"
    assert command[command.index("--assignee-object-id") + 1] == "principal"
    assert command[command.index("--role") + 1] == "Storage Blob Data Contributor"


@pytest.mark.parametrize("role", ["Storage Blob Data Contributor", "Storage Blob Data Owner"])
def test_existing_storage_data_role_does_not_grant_again(role):
    cli = Mock(return_value=[{"roleDefinitionName": role}])
    ensure_storage_role("/storage", "videos", "principal", "User", cli=cli)
    assert cli.call_count == 1


def work(plan, **updates):
    return {
        "work_id": "work-1", "status": "SUCCEEDED", "created_at": 101.0,
        "committed_attempt_id": "attempt-1",
        "payload_json": json.dumps({
            "source_video": "/lakehouse/default/Files/videos/" + plan.incoming_video_path,
            "source_sha256": plan.asset_version,
        }), **updates,
    }


def test_progress_requires_exact_source_hash_fresh_work_and_committed_result(plan):
    row = work(plan)
    result = {"work_id": "work-1", "attempt_id": "attempt-1", "distinct_people": 3}
    assert asset_progress([plan], [], [], started_at=100)[0]["state"] == "WAITING_REGISTRATION"
    assert asset_progress([plan], [work(plan, created_at=99)], [result], started_at=100)[0]["state"] == "PREVIOUS_RUN"
    assert asset_progress([plan], [row], [], started_at=100)[0]["state"] == "WAITING_RESULTS"
    observed = asset_progress([plan], [row], [result], started_at=100)
    assert observed[0]["state"] == "COMPLETED"
    assert observed[0]["distinct_people"] == 3
    assert observed[0]["work_id"] == "work-1"
    wrong = work(plan, payload_json=json.dumps({"source_video": "elsewhere", "source_sha256": plan.asset_version}))
    assert asset_progress([plan], [wrong], [result], started_at=100)[0]["state"] == "WAITING_REGISTRATION"
    wrong_hash = work(plan, payload_json=json.dumps({"source_video": "/lakehouse/default/Files/videos/" + plan.incoming_video_path, "source_sha256": "bad"}))
    assert asset_progress([plan], [wrong_hash], [result], started_at=100)[0]["state"] == "WAITING_REGISTRATION"
    assert asset_progress([plan], [row], [{**result, "attempt_id": "other"}], started_at=100)[0]["state"] == "WAITING_RESULTS"


def test_progress_reports_terminal_failures_and_processing(plan):
    for status in ("DEAD", "DEAD_LETTER", "FAILED", "QUARANTINED"):
        with pytest.raises(RuntimeError, match=status):
            asset_progress([plan], [work(plan, status=status)], [], started_at=100)
    assert asset_progress([plan], [work(plan, status="RUNNING", committed_attempt_id=None)], [], started_at=100)[0]["state"] == "RUNNING"


def test_ambiguous_registrations_fail_instead_of_choosing_one(plan):
    with pytest.raises(RuntimeError, match="Multiple"):
        asset_progress([plan], [work(plan), work(plan, work_id="other")], [], started_at=100)


NOTEBOOK = Path(__file__).parents[1] / "notebooks/customer_end_to_end_demo.ipynb"


def notebook_cells():
    return {
        cell["id"]: "".join(cell["source"])
        for cell in json.loads(NOTEBOOK.read_text())["cells"]
        if cell["cell_type"] == "code"
    }


def test_notebook_is_syntactically_valid_and_gates_are_explicit_booleans():
    cells = notebook_cells()
    for cell_id, source in cells.items():
        compile(source, f"{NOTEBOOK}:{cell_id}", "exec")
    assignments = {
        node.targets[0].id: node.value.value
        for node in ast.walk(ast.parse(cells["configuration"]))
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
    }
    for gate in (
        "RUN_PREFLIGHT", "ALLOW_CAPACITY_RESUME", "ALLOW_ROLE_GRANT",
        "ENABLE_EXISTING_SCHEDULES", "UPLOAD_TO_STAGING", "PROMOTE_TO_INCOMING",
        "MONITOR_FABRIC",
    ):
        assert type(assignments[gate]) is bool
    for cell in ("live-preflight", "upload-staging", "monitor-results"):
        assert isinstance(ast.parse(cells[cell]).body[0], ast.If)


def test_fabric_demo_derives_release_metadata_from_its_installed_sdk():
    path = NOTEBOOK.parent / "fabric/18_sjd_getting_started_demo.ipynb"
    notebook = json.loads(path.read_text())
    source = "".join(next(cell["source"] for cell in notebook["cells"] if cell["id"] == "reviewed-inputs"))
    assert "resolve_published_release(" in source
    assignments = {
        node.targets[0].id: node.value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
    }
    for field in ("RELEASE_MANIFEST_PATH", "RELEASE_MANIFEST_SHA256",
                  "RELEASE_RECEIPT_PATH", "RELEASE_RECEIPT_SHA256"):
        assert not isinstance(assignments[field], ast.Constant)


def test_notebook_uses_schema_directory_and_deletion_vector_reader():
    cells = notebook_cells()
    assignments = {
        node.targets[0].id: node.value.value
        for node in ast.walk(ast.parse(cells["configuration"]))
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
    }
    assert assignments["TABLE_DIRECTORY"] == "Tables/dbo"
    preflight = cells["live-preflight"]
    assert "read_demo_delta(" in preflight
    assert "DeltaTable(" not in preflight
    assert "azure_transport_option_type='curl'" in preflight
    assert "https://onelake.blob.fabric.microsoft.com" in preflight
    assert "CHAIN 'cli'" in preflight
    assert ast.parse(preflight).body[0].body[0].targets[0].id == "PREFLIGHT_OK"
    assert ast.parse(preflight).body[0].body[0].value.value is False


@pytest.fixture
def arrival_client(monkeypatch):
    source = {
        "name": "storage-events", "type": "AzureBlobStorageEvents", "status": "Running",
        "properties": {"azureBlobStorageEvents": [{"azureResourceId": "/storage"}]},
    }
    destination = {
        "name": "to-arrival-activator", "type": "Activator", "status": "Running",
        "properties": {"workspaceId": "workspace", "itemId": "activator"},
    }
    topology = {
        "sources": [source], "destinations": [destination],
        "streams": [{"name": "videos", "status": "Running"}],
    }
    client = Mock()
    client.request_json.return_value = topology
    client.run_operation.return_value = SimpleNamespace(status="succeeded", resource={"definition": {}}, error=None)
    evidence = SimpleNamespace(
        rules=[SimpleNamespace(rule_id="rule", name="run_pc_event_intake_on_manifest_renamed", enabled=True)],
        actions=[SimpleNamespace(rule_id="rule", kind="FabricItemInvocation",
                                 target_workspace_id="workspace", target_item_id="intake",
                                 target_item_type="Pipeline")],
    )
    validator = Mock(return_value=evidence)
    monkeypatch.setattr("people_counter.fabric_activator.validate_reflex_definition", validator)
    return client, topology, evidence, validator


def verify_arrival(client):
    return verify_arrival_runtime(
        client, workspace_id="workspace", eventstream_id="eventstream",
        activator_id="activator", intake_id="intake", storage_resource_id="/storage",
    )


def test_arrival_runtime_requires_live_delivery_and_enabled_rule(arrival_client):
    client, topology, evidence, validator = arrival_client
    result = verify_arrival(client)
    assert result == {
        "source_status": "Running", "stream_status": "Running",
        "destination_status": "Running", "rule_enabled": True,
    }
    client.request_json.assert_called_once_with("GET", "/workspaces/workspace/eventstreams/eventstream/topology")
    client.run_operation.assert_called_once_with(
        "POST", "/workspaces/workspace/items/activator/getDefinition",
        action="verify_arrival_rule",
    )
    validator.assert_called_once_with({"definition": {}})


@pytest.mark.parametrize("group,status", [
    ("sources", "Paused"), ("sources", "Failed"),
    ("destinations", "Paused"), ("destinations", "Warning"),
    ("streams", "Updating"),
])
def test_arrival_runtime_rejects_nonrunning_delivery_before_reading_rule(arrival_client, group, status):
    client, topology, _, _ = arrival_client
    topology[group][0]["status"] = status
    with pytest.raises(RuntimeError, match=status):
        verify_arrival(client)
    client.run_operation.assert_not_called()


@pytest.mark.parametrize("group", ["sources", "destinations", "streams"])
def test_arrival_runtime_requires_unambiguous_nodes_and_no_reported_error(arrival_client, group):
    client, topology, _, _ = arrival_client
    original = list(topology[group])
    topology[group] = []
    with pytest.raises(RuntimeError):
        verify_arrival(client)
    topology[group] = original + original
    with pytest.raises(RuntimeError):
        verify_arrival(client)
    topology[group] = original
    topology[group][0]["error"] = {"message": "Delivery authorization failed"}
    with pytest.raises(RuntimeError, match="Delivery authorization failed"):
        verify_arrival(client)


@pytest.mark.parametrize("group,label", [
    ("sources", "arrival storage source"), ("destinations", "arrival Activator destination"),
    ("streams", "arrival stream"),
])
def test_arrival_runtime_reports_missing_topology_groups(arrival_client, group, label):
    client, topology, _, _ = arrival_client
    del topology[group]
    with pytest.raises(RuntimeError, match=f"Expected exactly one {label}; found 0"):
        verify_arrival(client)


@pytest.mark.parametrize("group,field", [
    ("sources", "properties"), ("sources", "azureBlobStorageEvents"),
    ("sources", "azureResourceId"), ("destinations", "properties"),
])
def test_arrival_runtime_rejects_missing_binding_properties(arrival_client, group, field):
    client, topology, _, _ = arrival_client
    node = topology[group][0]
    if field == "properties":
        del node["properties"]
    elif field == "azureBlobStorageEvents":
        del node["properties"]["azureBlobStorageEvents"]
    else:
        del node["properties"]["azureBlobStorageEvents"][0]["azureResourceId"]
    with pytest.raises(RuntimeError, match="Expected exactly one"):
        verify_arrival(client)


@pytest.mark.parametrize("field,value", [
    ("storage", "/different-storage"),
    ("workspaceId", "other-workspace"),
    ("itemId", "other-activator"),
])
def test_arrival_runtime_rejects_wrong_storage_or_destination_binding(arrival_client, field, value):
    client, topology, _, _ = arrival_client
    if field == "storage":
        topology["sources"][0]["properties"]["azureBlobStorageEvents"][0]["azureResourceId"] = value
    else:
        topology["destinations"][0]["properties"][field] = value
    with pytest.raises(RuntimeError):
        verify_arrival(client)


def test_arrival_rule_read_failure_disabled_missing_and_wrong_target_fail_closed(arrival_client):
    client, _, evidence, _ = arrival_client
    client.run_operation.return_value.status = "failed"
    client.run_operation.return_value.error = "Definition read forbidden"
    with pytest.raises(RuntimeError, match="Definition read forbidden"):
        verify_arrival(client)
    client.run_operation.return_value.status = "succeeded"
    evidence.rules[0].enabled = False
    with pytest.raises(RuntimeError, match="disabled"):
        verify_arrival(client)
    evidence.rules[0].enabled = True
    for field in ("target_workspace_id", "target_item_id", "target_item_type"):
        action = evidence.actions[0]
        original = getattr(action, field)
        setattr(action, field, "other")
        with pytest.raises(RuntimeError, match="intake"):
            verify_arrival(client)
        setattr(action, field, original)
    evidence.rules = []
    with pytest.raises(RuntimeError, match="exactly one"):
        verify_arrival(client)


def test_notebook_checks_arrival_before_each_side_effect_and_monitor_poll():
    cells = notebook_cells()
    assert "'eventstream': 'azure_storage_events_eventstream'" in cells["live-preflight"]
    assert "verify_arrival_runtime(" in cells["live-preflight"]
    for cell_id, operation in [
        ("upload-staging", "stage_videos("),
        ("promote-incoming", "promote_staged("),
        ("monitor-results", "work_rows = read_delta("),
    ]:
        assert cells[cell_id].index("check_arrival_runtime()") < cells[cell_id].index(operation)


def test_release_alignment_detects_pipeline_and_dispatcher_drift():
    import base64

    def definition(path, content):
        return SimpleNamespace(status="succeeded", error=None, resource={"definition": {"parts": [{
            "path": path, "payloadType": "InlineBase64",
            "payload": base64.b64encode(json.dumps(content).encode()).decode(),
        }]}})

    pipeline = {"properties": {"activities": [{
        "name": "ValidateManifest", "typeProperties": {"parameters": {
            "RELEASE_MANIFEST_SHA256": {"type": "string", "value": "a" * 64},
        }},
    }]}}
    schedule = {"schedules": [{"executionData": {
        "commandLineArguments": "--release-manifest-sha256 " + "b" * 64,
    }}]}
    client = Mock()
    client.run_operation.side_effect = [
        definition("pipeline-content.json", pipeline),
        definition(".schedules", schedule),
    ]
    with pytest.raises(RuntimeError, match="release mismatch"):
        verify_release_alignment(client, "workspace", "intake", "dispatcher")
    schedule["schedules"][0]["executionData"]["commandLineArguments"] = "--release-manifest-sha256 " + "a" * 64
    client.run_operation.side_effect = [
        definition("pipeline-content.json", pipeline),
        definition(".schedules", schedule),
    ]
    assert verify_release_alignment(client, "workspace", "intake", "dispatcher") == "a" * 64


def test_preflight_validates_published_not_staged_compute():
    from people_counter.fabric_sjd_definition import reviewed_compute_settings

    client = Mock()
    client.request_json.return_value = reviewed_compute_settings({})
    verify_compute_policy(client, "workspace", "environment")
    client.request_json.assert_called_once_with(
        "GET", "/workspaces/workspace/environments/environment/sparkcompute?beta=false",
    )
    client.request_json.return_value["dynamicExecutorAllocation"]["enabled"] = True
    with pytest.raises(RuntimeError, match="fixed allocation"):
        verify_compute_policy(client, "workspace", "environment")


@pytest.mark.parametrize(("customize", "nodes", "workers", "accepted"), [
    (True, 3, 2, True), (False, 3, 2, False),
    (True, 10, 2, False), (True, 3, 9, False),
])
def test_notebook_checks_workspace_pool_before_arrival(customize, nodes, workers, accepted):
    from people_counter import fabric_sjd_definition

    check = next(
        node for node in ast.walk(ast.parse(notebook_cells()["live-preflight"]))
        if isinstance(node, ast.FunctionDef) and node.name == "check_arrival_runtime"
    )
    fabric, helpers = Mock(), Mock()
    fabric.request_json.return_value = {"pool": {
        "customizeComputeEnabled": customize,
        "starterPool": {"maxNodeCount": nodes, "maxExecutors": workers},
    }}
    namespace = {
        "demo_helpers": helpers, "definition_helpers": fabric_sjd_definition, "fabric": fabric,
        "WORKSPACE_ID": "workspace", "ENVIRONMENT_ID": "environment",
        "STORAGE_RESOURCE_ID": "storage",
        "item_ids": {"intake": "intake", "dispatcher": "dispatcher", "eventstream": "eventstream", "arrival": "arrival"},
    }
    exec(compile(ast.Module(body=[check], type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    if accepted:
        namespace["check_arrival_runtime"]()
        helpers.verify_arrival_runtime.assert_called_once()
    else:
        with pytest.raises(RuntimeError, match="Starter Pool"):
            namespace["check_arrival_runtime"]()
        helpers.verify_arrival_runtime.assert_not_called()
    fabric.request_json.assert_called_once_with("GET", "/workspaces/workspace/spark/settings")


def test_processing_diagnostic_is_authoritative_even_when_scheduled_job_completed():
    diagnostic = {
        "captured_at": 200.0, "error_type": "ProcessValidationError",
        "error_message": "claim release digest differs for ['work-a']",
    }
    with pytest.raises(RuntimeError, match="ProcessValidationError"):
        raise_processing_failure([diagnostic], ["work-a"], started_at=100)
    raise_processing_failure([diagnostic], ["other-work"], started_at=100)
    raise_processing_failure([diagnostic], ["work-a"], started_at=300)


def test_shared_runtime_failure_stops_monitoring_before_any_claim_exists():
    diagnostic = {
        "captured_at": 200.0, "error_type": "RuntimeError",
        "error_message": "Fabric SJD requires the reviewed fixed live pool",
    }
    with pytest.raises(RuntimeError, match="fixed live pool"):
        raise_processing_failure([diagnostic], ["registered-but-unclaimed-work"], started_at=100)


def test_preflight_refreshes_cached_demo_module_before_reading_delta(monkeypatch):
    import people_counter.customer_demo as demo

    monkeypatch.delattr(demo, "read_demo_delta")
    imports = []
    for node in ast.parse(notebook_cells()["live-preflight"]).body[0].body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "capacity":
            break
        imports.append(node)
    else:
        pytest.fail("Preflight capacity boundary was not found")
    namespace = {}
    exec(compile(ast.Module(body=imports, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    assert namespace["PREFLIGHT_OK"] is False
    assert callable(namespace["read_demo_delta"])
    assert namespace["read_demo_delta"] is demo.read_demo_delta


@pytest.mark.parametrize("continuation,allowed", [
    ("https://wabi-west-us3-a-primary-redirect.analysis.windows.net/next", True),
    ("https://analysis.windows.net.attacker.example/next", False),
    ("http://wabi-west-us3-a-primary-redirect.analysis.windows.net/next", False),
])
def test_notebook_client_follows_only_trusted_regional_history_pages(continuation, allowed):
    from people_counter.fabric_automation import FabricApiError, FabricAutomationClient

    client_assignment = next(
        node for node in ast.parse(notebook_cells()["live-preflight"]).body[0].body
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "fabric"
    )
    provider = Mock()
    provider.get_token.return_value = "demo-token"
    transport = Mock()
    transport.request.side_effect = [
        SimpleNamespace(status=200, headers={}, body=json.dumps({
            "value": [{"id": "first"}], "continuationUri": continuation,
        }).encode()),
        SimpleNamespace(status=200, headers={}, body=b'{"value":[{"id":"second"}]}'),
    ]
    namespace = {
        "FabricAutomationClient": FabricAutomationClient,
        "AzureCliResourceTokenProvider": lambda _: provider,
        "UrllibTransport": lambda: transport,
    }
    exec(compile(ast.Module(body=[client_assignment], type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    if allowed:
        assert namespace["fabric"].list_pages("/items/item/jobs/instances") == [
            {"id": "first"}, {"id": "second"},
        ]
        assert transport.request.call_args.args[1] == continuation
        assert all(call.args[0] == "GET" for call in transport.request.call_args_list)
        assert provider.get_token.call_count == 2
    else:
        with pytest.raises(FabricApiError):
            namespace["fabric"].list_pages("/items/item/jobs/instances")
        assert transport.request.call_count == provider.get_token.call_count == 1


def test_delta_reader_projects_and_filters_real_delta_rows(tmp_path):
    import duckdb
    import pyarrow as pa
    from deltalake import write_deltalake

    table_path = tmp_path / "people_counter_sjd_work"
    write_deltalake(table_path, pa.table({
        "work_id": ["old", "new", "quote'work"],
        "created_at": [1.0, 3.0, 4.0],
        "status": ["READY", "RUNNING", "SUCCEEDED"],
        "completed_at": pa.array(
            [datetime(2026, 10, 30, tzinfo=timezone.utc)] * 3,
            type=pa.timestamp("us", tz="UTC"),
        ),
    }))
    with duckdb.connect() as connection:
        connection.execute("INSTALL delta; LOAD delta")
        base = str(tmp_path / "people_counter_sjd_")
        assert read_demo_delta(
            connection, base, "work", ["work_id", "status"], [("created_at", ">=", 2.0)],
        ) == [
            {"work_id": "new", "status": "RUNNING"},
            {"work_id": "quote'work", "status": "SUCCEEDED"},
        ]
        assert read_demo_delta(
            connection, base, "work", ["work_id"],
            [("work_id", "in", ["quote'work"]), ("created_at", ">=", 2.0)],
        ) == [{"work_id": "quote'work"}]
        assert read_demo_delta(
            connection, base, "work", ["work_id"], [("work_id", "in", [])],
        ) == []
        assert len(read_demo_delta(connection, base, "work", ["work_id"])) == 3
        assert read_demo_delta(
            connection, base, "work", ["completed_at"], [("work_id", "in", ["new"])],
        )[0]["completed_at"] == datetime(2026, 10, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize("suffix,columns,filters", [
    ("other", ["work_id"], None),
    ("work", [], None),
    ("work", ["work_id; DROP TABLE work"], None),
    ("work", ["work_id"], [("created_at", "=", 1)]),
    ("work", ["work_id"], [("status", "in", ["READY"])]),
])
def test_delta_reader_rejects_unsupported_identifiers_and_filters(suffix, columns, filters):
    connection = Mock()
    with pytest.raises(ValueError):
        read_demo_delta(connection, "base", suffix, columns, filters)
    connection.execute.assert_not_called()


def test_delta_reader_propagates_missing_table_and_permission_errors():
    connection = Mock()
    connection.execute.side_effect = RuntimeError("Storage authorization failed")
    with pytest.raises(RuntimeError, match="authorization failed"):
        read_demo_delta(connection, "base", "work", ["work_id"])


def test_notebook_reuses_complete_package_and_rejects_changed_run_metadata(plan, tmp_path, monkeypatch):
    package_dir = tmp_path / "package"
    package_dir.mkdir()
    catalog = tmp_path / "camera.csv"
    catalog.write_text("reviewed-camera")
    index = {
        "complete": True, "catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest(),
        "inventory_sha256": None, "entries": [{"prepared_manifest_path": "prepared.json"}],
    }
    package_bytes = json.dumps(index).encode()
    (package_dir / "manifest-package.json").write_bytes(package_bytes)
    (package_dir / "prepared.json").write_text("{}")
    prepare = Mock(return_value=0)
    monkeypatch.setattr("people_counter.manifest_publisher.prepare_main", prepare)
    monkeypatch.setattr(
        "people_counter.manifest_publisher.validate_manifest_package",
        lambda _: SimpleNamespace(plans=[plan], index_bytes=package_bytes, index=index),
    )
    namespace = {
        "CATALOG_PATH": catalog, "DEMO_ROOT": tmp_path, "snapshot_prefix": "demo/",
        "PACKAGE_DIR": package_dir, "MAX_FILES": 1, "overrides": [],
        "INVENTORY_PATH": tmp_path / "inventory.csv", "STORAGE_ACCOUNT": "account",
        "FILESYSTEM": "videos", "RUN_DIR": tmp_path, "source_files": [plan.local_path],
        "json": json, "hashlib": hashlib, "display": lambda _: None,
        "pd": SimpleNamespace(read_csv=lambda _: [], DataFrame=lambda rows: rows),
    }
    source = notebook_cells()["prepare-package"]
    exec(compile(source, str(NOTEBOOK), "exec"), namespace)
    prepare.assert_not_called()
    assert (package_dir / "manifest-package.json").read_bytes() == package_bytes
    catalog.write_text("different-camera")
    with pytest.raises(RuntimeError, match="metadata changed"):
        exec(compile(source, str(NOTEBOOK), "exec"), namespace)
    prepare.assert_not_called()


@pytest.mark.parametrize("missing", [None, "intake", "dispatcher", "results", "old-jobs", "failed-jobs", "arrival-paused"])
def test_notebook_monitor_requires_real_input_results_and_new_intake_and_sjd(plan, tmp_path, missing):
    cells = notebook_cells()
    package_bytes = b"reviewed-package"
    binding = {
        "workspace": "workspace", "lakehouse": "lakehouse", "account": "account",
        "filesystem": "videos", "package_sha256": hashlib.sha256(package_bytes).hexdigest(),
    }
    state = tmp_path / "arrival-state.json"
    state.write_text(json.dumps({
        "binding": binding, "started_at": 100.0,
        "baseline_jobs": {"intake": ["old"], "dispatcher": ["old"]},
    }))
    gold = {"work_id": "work-1", "distinct_people": 3, "processing_seconds": 1.5}

    def read_delta(suffix, columns, filters):
        assert filters
        if suffix == "work":
            return [work(plan)]
        if suffix == "gold_work_operations":
            return [{"work_id": "work-1", "committed_attempt_id": "attempt-1"}]
        assert suffix == "gold_video"
        return [] if missing == "results" else [gold]

    def list_pages(path):
        role = path.split("/")[4]
        if role == missing:
            return []
        return [{"id": "old" if missing == "old-jobs" else role + "-new",
                 "status": "Failed" if missing == "failed-jobs" else "Completed"}]

    namespace = {
        "MONITOR_FABRIC": True, "PREFLIGHT_OK": True, "STATE_PATH": state,
        "json": json, "hashlib": hashlib, "datetime": datetime, "timezone": timezone,
        "WORKSPACE_ID": "workspace", "LAKEHOUSE_ID": "lakehouse",
        "STORAGE_ACCOUNT": "account", "FILESYSTEM": "videos",
        "SHORTCUT_NAME": "videos", "RUN_LABEL": "demo", "RUN_DIR": tmp_path,
        "MONITOR_TIMEOUT_SECONDS": 1, "POLL_SECONDS": 0,
        "package_config": object(),
        "validate_manifest_package": lambda _: SimpleNamespace(plans=[plan], index_bytes=package_bytes),
        "asset_progress": asset_progress, "read_delta": read_delta,
        "check_arrival_runtime": Mock(return_value={}),
        "check_processing_failure": Mock(),
        "fabric": SimpleNamespace(list_pages=list_pages),
        "item_ids": {"intake": "intake", "dispatcher": "dispatcher"},
        "pd": SimpleNamespace(DataFrame=lambda rows: rows),
        "display": lambda _: None, "clear_output": lambda **kwargs: None,
        "time": SimpleNamespace(monotonic=Mock(side_effect=[0, 2]), sleep=Mock()),
    }
    if missing == "arrival-paused":
        namespace["check_arrival_runtime"].side_effect = RuntimeError("Arrival destination is Paused")
        namespace["read_delta"] = Mock()
        with pytest.raises(RuntimeError, match="Paused"):
            exec(compile(cells["monitor-results"], str(NOTEBOOK), "exec"), namespace)
        namespace["read_delta"].assert_not_called()
        assert not (tmp_path / "execution-evidence.json").exists()
        return
    if missing is None:
        exec(compile(cells["monitor-results"], str(NOTEBOOK), "exec"), namespace)
    else:
        with pytest.raises(TimeoutError, match="No complete automatic"):
            exec(compile(cells["monitor-results"], str(NOTEBOOK), "exec"), namespace)
    evidence = json.loads((tmp_path / "execution-evidence.json").read_text())
    assert evidence["assets"][0]["state"] == ("WAITING_RESULTS" if missing == "results" else "COMPLETED")
    assert evidence["assets"][0]["work_id"] == "work-1"

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from people_counter import fabric_reconcilers as reconcilers
from people_counter.fabric_automation import DeploymentEvidence
from people_counter.fabric_deployment_manifest import DeploymentEnvironment, parse_deployment_manifest

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
_BASE = "/workspaces/ws/items/item-1/jobs/sparkjob/schedules"


def _uid(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))


@dataclass
class _FakeClient:
    list_pages_results: dict[str, list[Mapping[str, Any]]] = field(default_factory=dict)
    get_results: dict[str, list[Mapping[str, Any]]] = field(default_factory=dict)
    calls: list[tuple] = field(default_factory=list)
    response_headers: Mapping[str, str] = field(
        default_factory=lambda: {"Location": "/job-instances/job-1"}
    )

    def list_pages(self, path, *, value_key="value", empty_error_codes=()):
        self.calls.append(("LIST", path, value_key, empty_error_codes))
        return self.list_pages_results.get(path, [])

    def request_json(self, method, path, *, value=None, expected=(200,), content_type="application/json"):
        self.calls.append((method, path, value, content_type))
        if method == "GET":
            return self.get_results[path].pop(0)
        if method in {"POST", "PATCH", "PUT", "DELETE"}:
            return {}
        raise AssertionError(f"unexpected HTTP method: {method!r}")

    def run_operation(self, method, path, *, action, value=None, accepted=(200, 201, 202), content_type="json"):
        self.calls.append((method, path, value, action))
        return DeploymentEvidence(action=action, status="succeeded", resource={"id": "sched-1"})

    def request_response(
        self,
        method,
        path,
        *,
        value=None,
        expected=(200,),
        content_type="application/json",
    ):
        self.calls.append((method, path, value, content_type))
        return expected[0], self.response_headers, b""


def test_reconcile_environment_library_uploads_removes_stale_and_rejects_bad_names(tmp_path: Path) -> None:
    bogus = tmp_path / "notawheel.txt"
    bogus.write_text("x")
    with pytest.raises(reconcilers.ReconcileError, match="not a .whl file"):
        reconcilers.reconcile_environment_library(_FakeClient(), ws="ws", env_id="env", whl_path=bogus)

    whl = tmp_path / "people_counter-0.9.57-py3-none-any.whl"
    whl.write_bytes(b"wheel-bytes")
    client = _FakeClient(
        list_pages_results={
            "/workspaces/ws/environments/env/staging/libraries?beta=false": [
                {"name": "people_counter-0.9.56-py3-none-any.whl", "libraryType": "Custom"},
                {"name": "fuzzywuzzy", "libraryType": "External", "version": "0.18.0"},
            ]
        }
    )
    evidence = reconcilers.reconcile_environment_library(client, ws="ws", env_id="env", whl_path=whl)
    assert evidence.resource["removed"] == ["people_counter-0.9.56-py3-none-any.whl"]
    assert client.calls[0][3] == ("EnvironmentLibrariesNotFound",)
    deletes = [c for c in client.calls if c[0] == "DELETE"]
    uploads = [c for c in client.calls if c[0] == "POST"]
    assert deletes[0][1].endswith("0.9.56-py3-none-any.whl")
    assert uploads[0][1].endswith("0.9.57-py3-none-any.whl")
    assert uploads[0][2] == b"wheel-bytes" and uploads[0][3] == "application/octet-stream"


def test_reconcile_environment_publish_polls_then_raises_on_terminal_states() -> None:
    client = _FakeClient(
        get_results={
            "/workspaces/ws/environments/env": [
                {"properties": {"publishDetails": {"state": "Running"}}},
                {"properties": {"publishDetails": {"state": "Success"}}},
            ]
        }
    )
    sleeps: list[float] = []
    evidence = reconcilers.reconcile_environment_publish(
        client, ws="ws", env_id="env", sleep=sleeps.append, monotonic=lambda: 0.0
    )
    assert evidence.status == "succeeded" and sleeps == [15.0]

    failing = _FakeClient(get_results={"/workspaces/ws/environments/env": [{"publishDetails": {"state": "Failed"}}]})
    with pytest.raises(reconcilers.ReconcileError, match="publish failed"):
        reconcilers.reconcile_environment_publish(failing, ws="ws", env_id="env", sleep=lambda _s: None, monotonic=lambda: 0.0)

    rejected = _FakeClient()
    rejected.run_operation = lambda *_args, **_kwargs: DeploymentEvidence(
        action="publish_environment",
        status="failed",
        error="submission rejected",
    )
    with pytest.raises(
        reconcilers.ReconcileError,
        match="publish failed to submit: submission rejected",
    ):
        reconcilers.reconcile_environment_publish(
            rejected,
            ws="ws",
            env_id="env",
            sleep=lambda _s: None,
            monotonic=lambda: 0.0,
        )


@pytest.mark.parametrize(
    ("role", "existing", "interval", "expected_method", "expected_path"),
    [
        ("control", [], 5, "POST", _BASE),
        ("control", [{"id": "sched-9"}], 5, "PATCH", f"{_BASE}/sched-9"),
        ("process", [{"id": "sched-9"}], None, "DELETE", f"{_BASE}/sched-9"),
        ("process", [], None, None, None),
    ],
)
def test_reconcile_role_schedule_create_update_delete_or_noop(role, existing, interval, expected_method, expected_path) -> None:
    client = _FakeClient(list_pages_results={_BASE: existing})
    evidence = reconcilers.reconcile_role_schedule(
        client, ws="ws", role=role, item_id="item-1", enabled=False, interval_minutes=interval, now=_NOW
    )
    assert evidence.status == "succeeded"
    if expected_method is None:
        assert evidence.resource == {"state": "absent"}
        assert not any(c[0] != "LIST" for c in client.calls)
        return
    calls = [c for c in client.calls if c[0] == expected_method]
    assert calls[0][1] == expected_path
    if expected_method != "DELETE":
        assert calls[0][2]["enabled"] is False


def test_reconcile_role_schedule_rejects_multiple_schedules_and_class_mismatch() -> None:
    client = _FakeClient(list_pages_results={_BASE: [{"id": "a"}, {"id": "b"}]})
    with pytest.raises(reconcilers.ReconcileError, match="multiple schedules"):
        reconcilers.reconcile_role_schedule(
            client, ws="ws", role="control", item_id="item-1", enabled=False, interval_minutes=5
        )


def test_run_spark_job_polls_job_instance_and_surfaces_failure() -> None:
    location = "/job-instances/job-1"
    client = _FakeClient(
        get_results={
        location: [
            {"id": "job-1", "status": "InProgress"},
            {"id": "job-1", "status": "Completed"},
        ]
        }
    )
    sleeps: list[float] = []

    evidence = reconcilers.run_spark_job(
        client,
        ws="ws",
        item_id="control",
        command_line_arguments="bootstrap",
        sleep=sleeps.append,
        monotonic=lambda: 0.0,
        poll_seconds=0.0,
    )

    assert evidence.status == "succeeded"
    assert evidence.operation_id == "job-1"
    assert sleeps == [0.0]
    assert client.calls[0][1] == (
        "/workspaces/ws/sparkJobDefinitions/control/jobs/sparkjob/instances"
    )

    failed = _FakeClient(
        get_results={
        location: [{"id": "job-1", "status": "Failed", "failureReason": "boom"}]
        }
    )
    assert reconcilers.run_spark_job(
        failed,
        ws="ws",
        item_id="control",
        command_line_arguments="bootstrap",
    ).status == "failed"


def test_reconcile_reporting_refresh_validates_items_and_refreshes_models() -> None:
    path = Path(__file__).resolve().parents[1] / "src/people_counter/data/deploy/dev.json"
    document = json.loads(path.read_text())
    document["workspace_id"] = _uid("ws")
    for name, key in (
        ("analytics", "analytics_reporting"),
        ("operational", "operational_reporting"),
    ):
        document[key]["semantic_model"]["target_item_id"] = _uid(f"{name}-model")
        document[key]["report"]["target_item_id"] = _uid(f"{name}-report")
    manifest = parse_deployment_manifest(document, DeploymentEnvironment.DEV)
    location = "/job-instances/job-1"
    get_results = {location: []}
    for name, bundle in (
        ("analytics", manifest.analytics_reporting),
        ("operational", manifest.operational_reporting),
    ):
        get_results[
            f"/workspaces/{manifest.workspace_id}/items/"
            f"{bundle.semantic_model.target_item_id}"
        ] = [{"type": "SemanticModel"}]
        get_results[
            f"/workspaces/{manifest.workspace_id}/items/{bundle.report.target_item_id}"
        ] = [{"type": "Report"}]
        get_results[location].append({"id": f"{name}-refresh", "status": "Completed"})
    client = _FakeClient(get_results=get_results)

    evidence = reconcilers.reconcile_reporting_refresh(client, manifest)

    assert set(evidence) == {"analytics", "operational"}
    assert all(item.status == "succeeded" for item in evidence.values())
    assert any(
        call[1].endswith("/jobs/Refresh/instances")
        for call in client.calls
        if call[0] == "POST"
    )

    document["analytics_reporting"]["report"]["target_item_id"] = None
    unresolved = parse_deployment_manifest(document, DeploymentEnvironment.DEV)
    with pytest.raises(reconcilers.ReconcileError, match="unresolved"):
        reconcilers.reconcile_reporting_refresh(_FakeClient(), unresolved)
    for role, interval in (("control", None), ("process", 5)):
        with pytest.raises(reconcilers.ReconcileError, match="scheduled/on-demand"):
            reconcilers.reconcile_role_schedule(
                _FakeClient(), ws="ws", role=role, item_id="item-1", enabled=False, interval_minutes=interval
            )


def test_merge_terraform_output_and_full_runtime_reconciliation(tmp_path: Path) -> None:
    path = Path(__file__).resolve().parents[1] / "src/people_counter/data/deploy/dev.json"
    document = json.loads(path.read_text())
    document["roles"]["control"]["item_id"] = _uid("pinned-control")
    tf_output = {
        "workspace_id": {"value": _uid("ws")},
        "environment_id": {"value": _uid("env")},
        "lakehouse_id": {"value": _uid("lakehouse")},
        "spark_job_definition_ids": {"value": {role: _uid(role) for role in document["roles"]}},
        "semantic_model_ids": {
            "value": {
                "analytics": _uid("analytics-model"),
                "operational": _uid("operational-model"),
            }
        },
        "report_ids": {
            "value": {
                "analytics": _uid("analytics-report"),
                "operational": _uid("operational-report"),
            }
        },
        "eventhouse_id": {"value": _uid("eventhouse")},
        "kql_database_id": {"value": _uid("kql-database")},
        "kql_queryset_id": {"value": _uid("queryset")},
        "kql_dashboard_id": {"value": _uid("dashboard")},
        "activator_ids": {"value": {"operations": _uid("activator")}},
    }
    merged = reconcilers.merge_terraform_output(document, tf_output)
    assert merged["workspace_id"] == _uid("ws")
    assert merged["roles"]["control"]["item_id"] == _uid("pinned-control")
    assert merged["roles"]["dispatcher"]["item_id"] == _uid("dispatcher")
    assert (
        merged["analytics_reporting"]["semantic_model"]["target_item_id"]
        == _uid("analytics-model")
    )
    assert merged["operational_reporting"]["report"]["target_item_id"] == _uid(
        "operational-report"
    )
    assert merged["real_time_intelligence"]["dashboard_id"] == _uid("dashboard")
    assert merged["activator"]["reflex_id"] == _uid("activator")

    manifest = parse_deployment_manifest(merged, DeploymentEnvironment.DEV)
    assert manifest.analytics_reporting.semantic_model.target_item_id == _uid(
        "analytics-model"
    )
    assert manifest.real_time_intelligence.dashboard_id == _uid("dashboard")
    whl = tmp_path / "people_counter-0.9.57-py3-none-any.whl"
    whl.write_bytes(b"wheel-bytes")
    client = _FakeClient(
        get_results={
            f"/workspaces/{manifest.workspace_id}/environments/{manifest.fabric_environment_id}": [
                {"properties": {"publishDetails": {"state": "Success"}}}
            ],
            "/job-instances/job-1": [
                {"id": "job-1", "status": "Completed"}
            ],
        }
    )

    result = reconcilers.reconcile_runtime(
        client,
        manifest,
        whl_path=whl,
        enable_schedules=False,
        bootstrap_lakehouse=True,
        now=_NOW,
        sleep=lambda _s: None,
        monotonic=lambda: 0.0,
    )
    assert set(result["schedules"]) == set(document["roles"])
    assert result["schedules"]["process"]["resource"] == {"state": "absent"}
    assert result["lakehouse_bootstrap"]["status"] == "succeeded"
    created = [c for c in client.calls if c[0] == "POST" and c[1].endswith("/schedules")]
    assert created and all(c[2]["enabled"] is False for c in created)


def test_activator_enablement_fails_closed_without_a_definition_bundle(
    tmp_path: Path,
) -> None:
    path = Path("src/people_counter/data/deploy/dev.json")
    document = json.loads(path.read_text())
    document["workspace_id"] = _uid("ws")
    document["fabric_environment_id"] = _uid("env")
    document["activator"] = {
        "reflex_id": _uid("activator"),
        "trigger_name": "Crowding detected",
    }
    for role, binding in document["roles"].items():
        binding["item_id"] = _uid(role)
    manifest = parse_deployment_manifest(document, DeploymentEnvironment.DEV)
    whl = tmp_path / "people_counter-0.9.57-py3-none-any.whl"
    whl.write_bytes(b"wheel")
    client = _FakeClient(
        get_results={
            f"/workspaces/{manifest.workspace_id}/environments/"
            f"{manifest.fabric_environment_id}": [
                {"properties": {"publishDetails": {"state": "Success"}}}
            ]
        }
    )

    with pytest.raises(
        reconcilers.ReconcileError,
        match="source-controlled definition bundle",
    ):
        reconcilers.reconcile_runtime(
            client,
            manifest,
            whl_path=whl,
            enable_schedules=False,
            enable_activator=True,
            now=_NOW,
            sleep=lambda _s: None,
            monotonic=lambda: 0.0,
        )


def _resolved_manifest() -> object:
    document = json.loads(Path("src/people_counter/data/deploy/dev.json").read_text())
    document["workspace_id"] = _uid("ws")
    document["fabric_environment_id"] = _uid("env")
    document["lakehouse"]["lakehouse_id"] = _uid("lakehouse")
    for role, binding in document["roles"].items():
        binding["item_id"] = _uid(role)
    for bundle_name, document_key in (
        ("analytics", "analytics_reporting"),
        ("operational", "operational_reporting"),
    ):
        document[document_key]["semantic_model"]["target_item_id"] = _uid(
            f"{bundle_name}-model"
        )
        document[document_key]["report"]["target_item_id"] = _uid(
            f"{bundle_name}-report"
        )
    return parse_deployment_manifest(document, DeploymentEnvironment.DEV)


def test_runtime_contract_is_strict_and_defaults_optional_sections() -> None:
    contract = reconcilers.validate_runtime_contract(
        {"schema": reconcilers.RUNTIME_CONTRACT_SCHEMA}
    )
    assert contract["connections"] == []
    assert contract["powerBi"] == {}

    with pytest.raises(reconcilers.ReconcileError, match="extra"):
        reconcilers.validate_runtime_contract(
            {
                "schema": reconcilers.RUNTIME_CONTRACT_SCHEMA,
                "undocumentedEndpoint": "/internal",
            }
        )
    with pytest.raises(reconcilers.ReconcileError, match="unsupported"):
        reconcilers.validate_runtime_contract({"schema": "future"})


@dataclass
class _DataPlaneClient:
    responses: dict[tuple[str, str], list[tuple[int, Mapping[str, str], bytes]]]
    calls: list[tuple[str, str, object]] = field(default_factory=list)

    def request_response(
        self,
        method,
        path,
        *,
        value=None,
        expected=(200,),
        content_type="application/json",
    ):
        self.calls.append((method, path, value))
        response = self.responses[(method, path)].pop(0)
        assert response[0] in expected
        return response


def test_onelake_folder_and_immutable_release_manifest_reconcile() -> None:
    manifest = _resolved_manifest()
    folder_path = (
        f"/{manifest.workspace_id}/{manifest.lakehouse.lakehouse_id}/"
        "Files/people-counter/dev/v1/releases"
    )
    release_path = f"{folder_path}/abcdef1.json"
    document = {"schema": reconcilers.RELEASE_MANIFEST_SCHEMA, "commitSha": "abcdef1"}
    payload = reconcilers._canonical_json_bytes(document)
    client = _DataPlaneClient(
        responses={
            ("HEAD", folder_path): [(404, {}, b"")],
            ("PUT", f"{folder_path}?resource=directory"): [(201, {}, b"")],
            ("GET", release_path): [(404, {}, b""), (200, {}, payload)],
            ("PUT", f"{release_path}?resource=file"): [(201, {}, b"")],
            ("PATCH", f"{release_path}?action=append&position=0"): [
                (202, {}, b"")
            ],
            ("PATCH", f"{release_path}?action=flush&position={len(payload)}"): [
                (200, {}, b"")
            ],
        }
    )

    folder = reconcilers.ensure_onelake_directory(client, manifest, "releases")
    created = reconcilers.publish_immutable_onelake_json(
        client, manifest, "releases/abcdef1.json", document
    )
    present = reconcilers.publish_immutable_onelake_json(
        client, manifest, "releases/abcdef1.json", document
    )

    assert folder.resource["state"] == "created"
    assert created.action == "publish_release_manifest"
    assert created.resource["state"] == "created"
    assert present.resource["state"] == "present"
    assert not any("credential" in repr(call).lower() for call in client.calls)
    append_call = next(
        call
        for call in client.calls
        if call[0] == "PATCH" and "action=append" in call[1]
    )
    assert append_call[2] == payload


def test_release_manifest_is_stable_across_repeat_deployments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _resolved_manifest()
    published: list[dict[str, Any]] = []

    def capture(_client, _manifest, _path, value):
        published.append(dict(value))
        return DeploymentEvidence(
            action="publish_release_manifest",
            status="succeeded",
        )

    monkeypatch.setattr(reconcilers, "publish_immutable_onelake_json", capture)
    context = {
        "commitSha": "a" * 40,
        "definitionHashes": {"treeSha256": "b" * 64},
        "terraformOutputSha256": "c" * 64,
    }
    base = {
        "status": "succeeded",
        "resources": {"workspaceId": manifest.workspace_id},
    }

    reconcilers._publish_release_manifest(
        object(),
        manifest,
        {"publishReleaseManifest": True},
        context,
        {**base, "generatedAt": "2026-01-01", "library": {"state": "created"}},
    )
    reconcilers._publish_release_manifest(
        object(),
        manifest,
        {"publishReleaseManifest": True},
        context,
        {**base, "generatedAt": "2026-01-02", "library": {"state": "present"}},
    )

    assert published[0] == published[1]
    assert "generatedAt" not in published[0]
    assert "operationSummarySha256" not in published[0]


def test_onelake_release_manifest_conflict_fails_closed() -> None:
    manifest = _resolved_manifest()
    path = (
        f"/{manifest.workspace_id}/{manifest.lakehouse.lakehouse_id}/"
        "Files/people-counter/dev/v1/releases/abcdef1.json"
    )
    client = _DataPlaneClient(
        responses={("GET", path): [(200, {}, b"different")]}
    )

    with pytest.raises(reconcilers.ReconcileError, match="conflicts"):
        reconcilers.publish_immutable_onelake_json(
            client,
            manifest,
            "releases/abcdef1.json",
            {"commitSha": "abcdef1"},
        )


def test_table_readback_uses_schema_aware_control_job() -> None:
    manifest = _resolved_manifest()
    client = _FakeClient(
        get_results={
            "/job-instances/job-1": [
                {"id": "job-1", "status": "Completed"},
            ]
        }
    )
    evidence = reconcilers.verify_lakehouse_tables(
        client,
        manifest,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )
    assert evidence.status == "succeeded"
    assert evidence.action == "verify_lakehouse_tables"
    assert evidence.operation_id == "job-1"
    assert evidence.attempts == 1
    assert evidence.resource["method"] == "control-sjd-bootstrap"
    assert evidence.resource["expected"] == sorted(
        manifest.lakehouse.table(suffix)
        for suffix in reconcilers.TABLE_SUFFIXES
    )
    submission = next(call for call in client.calls if call[0] == "POST")
    assert submission[2] == {"executionData": {"commandLineArguments": "bootstrap"}}

    failing = _FakeClient(
        get_results={
            "/job-instances/job-1": [
                {"id": "job-1", "status": "Failed"},
            ]
        }
    )
    with pytest.raises(
        reconcilers.ReconcileError,
        match="Schema-aware Lakehouse table verification failed",
    ):
        reconcilers.verify_lakehouse_tables(
            failing,
            manifest,
            sleep=lambda _seconds: None,
            monotonic=lambda: 0.0,
        )


def test_connection_authentication_resolves_environment_without_evidence_leak() -> None:
    connection_id = _uid("connection")
    client = _FakeClient()
    evidence, checkpoints = reconcilers.reconcile_connection_authentication(
        client,
        connection_ids={"warehouse": connection_id},
        configurations=[
            {
                "key": "warehouse",
                "connectivityType": "ShareableCloud",
                "authentication": {
                    "type": "Basic",
                    "valuesFromEnvironment": {
                        "username": "PC_USER",
                        "password": "PC_PASSWORD",
                    },
                },
            }
        ],
        env={"PC_USER": "operator", "PC_PASSWORD": "super-secret"},
    )

    assert checkpoints == []
    request = next(call for call in client.calls if call[0] == "PATCH")
    assert request[2]["credentialDetails"]["credentials"]["password"] == "super-secret"
    serialized = json.dumps(evidence["warehouse"].to_dict())
    assert "super-secret" not in serialized
    assert "operator" not in serialized


def test_oauth_connection_requires_an_explicit_operator_checkpoint() -> None:
    client = _FakeClient()
    evidence, checkpoints = reconcilers.reconcile_connection_authentication(
        client,
        connection_ids={"oauth": _uid("oauth-connection")},
        configurations=[
            {
                "key": "oauth",
                "connectivityType": "ShareableCloud",
                "authentication": {"type": "OAuth2"},
            }
        ],
        env={},
    )

    assert evidence == {}
    assert checkpoints[0]["status"] == "operator_checkpoint_required"
    assert checkpoints[0]["connectionKey"] == "oauth"
    assert client.calls == []


def test_on_premises_connection_requires_encrypted_operator_rotation() -> None:
    client = _FakeClient()
    evidence, checkpoints = reconcilers.reconcile_connection_authentication(
        client,
        connection_ids={"gateway": _uid("gateway-connection")},
        configurations=[
            {
                "key": "gateway",
                "connectivityType": "OnPremisesGateway",
                "authentication": {
                    "type": "Basic",
                    "valuesFromEnvironment": {
                        "username": "IGNORED_USER",
                        "password": "IGNORED_PASSWORD",
                    },
                },
            }
        ],
        env={},
    )

    assert evidence == {}
    assert checkpoints[0]["id"] == "connection-on-premises-gateway"
    assert client.calls == []


def test_deprecated_workspace_and_connection_access_are_removed() -> None:
    workspace_id = _uid("ws")
    connection_id = _uid("connection")
    old_workspace = _uid("old-workspace")
    old_connection = _uid("old-connection")
    workspace_path = f"/workspaces/{workspace_id}/roleAssignments"
    connection_path = f"/connections/{connection_id}/roleAssignments"
    client = _FakeClient(
        list_pages_results={
            workspace_path: [
                {
                    "id": _uid("workspace-assignment"),
                    "principal": {"id": old_workspace},
                    "role": "Viewer",
                },
                {
                    "id": _uid("admin-assignment"),
                    "principal": {"id": _uid("admin")},
                    "role": "Admin",
                },
            ],
            connection_path: [
                {
                    "id": _uid("connection-assignment"),
                    "principal": {"id": old_connection},
                    "role": "User",
                }
            ],
        }
    )

    evidence, checkpoints = reconcilers.reconcile_deprecated_access(
        client,
        workspace_id=workspace_id,
        connection_ids={"warehouse": connection_id},
        configuration={
            "deprecatedWorkspacePrincipals": [old_workspace],
            "deprecatedConnectionPrincipals": {
                "warehouse": [old_connection]
            },
        },
    )

    assert set(evidence) == {"workspace", "connection:warehouse"}
    assert checkpoints == []
    assert len([call for call in client.calls if call[0] == "DELETE"]) == 2


def test_deprecated_item_access_and_last_admin_do_not_report_success() -> None:
    workspace_id = _uid("ws")
    admin = _uid("admin")
    path = f"/workspaces/{workspace_id}/roleAssignments"
    client = _FakeClient(
        list_pages_results={
            path: [
                {
                    "id": _uid("admin-assignment"),
                    "principal": {"id": admin},
                    "role": "Admin",
                }
            ]
        }
    )
    with pytest.raises(reconcilers.ReconcileError, match="last Admin"):
        reconcilers.reconcile_deprecated_access(
            client,
            workspace_id=workspace_id,
            connection_ids={},
            configuration={"deprecatedWorkspacePrincipals": [admin]},
        )

    _, checkpoints = reconcilers.reconcile_deprecated_access(
        _FakeClient(),
        workspace_id=workspace_id,
        connection_ids={},
        configuration={"deprecatedItemAccess": [{"principalId": admin}]},
    )
    assert checkpoints[0]["status"] == "operator_checkpoint_required"


def test_deprecated_access_can_remove_one_of_multiple_admins() -> None:
    workspace_id = _uid("ws")
    removed_admin = _uid("removed-admin")
    path = f"/workspaces/{workspace_id}/roleAssignments"
    client = _FakeClient(
        list_pages_results={
            path: [
                {
                    "id": _uid("removed-admin-assignment"),
                    "principal": {"id": removed_admin},
                    "role": "Admin",
                },
                {
                    "id": _uid("remaining-admin-assignment"),
                    "principal": {"id": _uid("remaining-admin")},
                    "role": "Admin",
                },
            ]
        }
    )

    evidence, _ = reconcilers.reconcile_deprecated_access(
        client,
        workspace_id=workspace_id,
        connection_ids={},
        configuration={"deprecatedWorkspacePrincipals": [removed_admin]},
    )

    assert evidence["workspace"].resource["removedRoleAssignmentIds"] == [
        _uid("removed-admin-assignment")
    ]


def test_power_bi_supported_runtime_calls_and_direct_lake_checkpoint() -> None:
    manifest = _resolved_manifest()
    client = _FakeClient()
    evidence, checkpoints = reconcilers.reconcile_power_bi_runtime(
        client,
        manifest,
        {
            "analytics": {
                "takeOver": True,
                "datasourceUpdates": [
                    {
                        "datasourceType": "Sql",
                        "selector": {"server": "old", "database": "pc"},
                        "target": {"server": "new", "database": "pc"},
                    }
                ],
                "gatewayBinding": {
                    "gatewayId": _uid("gateway"),
                    "datasourceIds": [_uid("datasource")],
                },
                "refreshSchedule": {
                    "enabled": True,
                    "days": ["Monday"],
                    "times": ["07:00"],
                    "localTimeZoneId": "UTC",
                    "notifyOption": "NoNotification",
                },
                "directLakeBinding": {
                    "expectedLakehouseId": manifest.lakehouse.lakehouse_id
                },
            }
        },
    )

    assert len(evidence["analytics"]) == 4
    assert checkpoints[0]["id"] == "direct-lake-binding-analytics"
    assert all(checkpoint["status"] == "operator_checkpoint_required" for checkpoint in checkpoints)
    paths = [call[1] for call in client.calls]
    assert any(path.endswith("/Default.TakeOver") for path in paths)
    assert any(path.endswith("/Default.UpdateDatasources") for path in paths)
    assert any(path.endswith("/Default.BindToGateway") for path in paths)
    assert any(path.endswith("/refreshSchedule") for path in paths)


def test_runtime_checkpoint_disables_schedules_and_emits_machine_evidence(
    tmp_path: Path,
) -> None:
    manifest = _resolved_manifest()
    whl = tmp_path / "people_counter-0.9.57-py3-none-any.whl"
    whl.write_bytes(b"wheel")
    client = _FakeClient(
        get_results={
            f"/workspaces/{manifest.workspace_id}/environments/"
            f"{manifest.fabric_environment_id}": [
                {"properties": {"publishDetails": {"state": "Success"}}}
            ]
        }
    )

    result = reconcilers.reconcile_runtime(
        client,
        manifest,
        whl_path=whl,
        enable_schedules=True,
        runtime_contract={
            "schema": reconcilers.RUNTIME_CONTRACT_SCHEMA,
            "connections": [
                {
                    "key": "oauth",
                    "connectivityType": "ShareableCloud",
                    "authentication": {"type": "OAuth2"},
                }
            ],
        },
        terraform_output={
            "connection_ids": {"value": {"oauth": _uid("oauth-connection")}}
        },
        now=_NOW,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )

    assert result["schema"] == reconcilers.EVIDENCE_SCHEMA
    assert result["status"] == "operator_checkpoint_required"
    assert result["schedulesEnabled"] is False
    assert result["operatorCheckpoints"][0]["connectionKey"] == "oauth"
    created = [
        call
        for call in client.calls
        if call[0] == "POST" and call[1].endswith("/schedules")
    ]
    assert created and all(call[2]["enabled"] is False for call in created)


def test_runtime_evidence_and_cli_fail_closed_on_failed_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _resolved_manifest()
    succeeded = DeploymentEvidence(action="ok", status="succeeded")
    failed = DeploymentEvidence(
        action="verify_activator",
        status="failed",
        error="target mismatch",
    )

    evidence = reconcilers._runtime_evidence(
        manifest,
        requested_schedules=True,
        enabled_schedules=False,
        generated_at=_NOW.isoformat(),
        context={},
        connection_ids={},
        library=succeeded,
        publish=succeeded,
        bootstrap=None,
        table_verification=None,
        directories={},
        connections={},
        power_bi={},
        access={},
        reporting={},
        activator=failed,
        schedules={},
        checkpoints=[
            {
                "status": "operator_checkpoint_required",
                "reason": "review failure",
            }
        ],
    )

    assert evidence["status"] == "failed"
    checkpoint_without_contract = reconcilers._runtime_evidence(
        manifest,
        requested_schedules=False,
        enabled_schedules=False,
        generated_at=_NOW.isoformat(),
        context={},
        connection_ids={},
        library=succeeded,
        publish=succeeded,
        bootstrap=None,
        table_verification=None,
        directories={},
        connections={},
        power_bi={},
        access={},
        reporting={},
        activator=DeploymentEvidence(
            action="verify_activator",
            status="operator_checkpoint_required",
        ),
        schedules={},
        checkpoints=[],
    )
    assert checkpoint_without_contract["status"] == "failed"
    monkeypatch.setattr(
        reconcilers,
        "_execute",
        lambda _arguments: (DeploymentEnvironment.DEV, evidence),
    )
    assert (
        reconcilers.main(
            [
                "--manifest-path",
                "unused.json",
                "--whl-path",
                "unused.whl",
                "--stage",
                "dev",
            ]
        )
        == 1
    )


def test_runtime_disables_schedules_before_a_postconfiguration_failure(
    tmp_path: Path,
) -> None:
    manifest = _resolved_manifest()
    whl = tmp_path / "people_counter-0.9.57-py3-none-any.whl"
    whl.write_bytes(b"wheel")
    client = _FakeClient(
        get_results={
            f"/workspaces/{manifest.workspace_id}/environments/"
            f"{manifest.fabric_environment_id}": [
                {"properties": {"publishDetails": {"state": "Failed"}}}
            ]
        }
    )

    with pytest.raises(reconcilers.ReconcileError, match="publish failed"):
        reconcilers.reconcile_runtime(
            client,
            manifest,
            whl_path=whl,
            enable_schedules=True,
            now=_NOW,
            sleep=lambda _seconds: None,
            monotonic=lambda: 0.0,
        )

    scheduled_mutations = [
        call
        for call in client.calls
        if call[0] in {"POST", "PATCH"} and call[1].endswith("/schedules")
    ]
    assert scheduled_mutations
    assert all(call[2]["enabled"] is False for call in scheduled_mutations)
    first_environment_mutation = next(
        index
        for index, call in enumerate(client.calls)
        if "/environments/" in call[1] and call[0] != "LIST"
    )
    last_schedule_mutation = max(
        index
        for index, call in enumerate(client.calls)
        if call in scheduled_mutations
    )
    assert last_schedule_mutation < first_environment_mutation

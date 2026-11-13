from __future__ import annotations

import base64
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from people_counter import fabric_acceptance as acceptance
from people_counter.fabric_automation import DeploymentEvidence
from people_counter.fabric_deployment_manifest import (
    DeploymentEnvironment,
    SCHEDULED_ROLES,
    SJD_ROLES,
    parse_deployment_manifest,
)

_COMMIT = "a" * 40


def _uid(value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, value))


def _manifest():
    document = json.loads(
        Path("src/people_counter/data/deploy/dev.json").read_text()
    )
    document["workspace_id"] = _uid("workspace")
    document["fabric_environment_id"] = _uid("environment")
    document["lakehouse"]["lakehouse_id"] = _uid("lakehouse")
    for role in document["roles"]:
        document["roles"][role]["item_id"] = _uid(role)
    for bundle, key in (
        ("analytics", "analytics_reporting"),
        ("operational", "operational_reporting"),
    ):
        document[key]["semantic_model"]["target_item_id"] = _uid(f"{bundle}-model")
        document[key]["report"]["target_item_id"] = _uid(f"{bundle}-report")
    return parse_deployment_manifest(document, DeploymentEnvironment.DEV)


def _outputs() -> dict[str, Any]:
    return {
        "workspace_id": {"value": _uid("workspace")},
        "lakehouse_id": {"value": _uid("lakehouse")},
        "environment_id": {"value": _uid("environment")},
        "eventhouse_id": {"value": None},
        "kql_database_id": {"value": None},
        "kql_queryset_id": {"value": None},
        "kql_dashboard_id": {"value": None},
        "managed_shortcuts": {"value": {}},
        "spark_job_definition_ids": {
            "value": {role: _uid(role) for role in SJD_ROLES}
        },
        "semantic_model_ids": {
            "value": {
                bundle: _uid(f"{bundle}-model")
                for bundle in ("analytics", "operational")
            }
        },
        "report_ids": {
            "value": {
                bundle: _uid(f"{bundle}-report")
                for bundle in ("analytics", "operational")
            }
        },
        "activator_ids": {"value": {}},
    }


def _definition(*values: str) -> dict[str, Any]:
    text = "\n".join(values)
    return {
        "definition": {
            "parts": [
                {
                    "path": "definition.json",
                    "payload": base64.b64encode(text.encode()).decode(),
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }


def _sjd_definition(role: str, manifest: Any) -> dict[str, Any]:
    root = Path("fabric/spark-job-definitions")
    parts = {
        "Main/main.py": (root / role / "Main/main.py").read_bytes(),
        "SparkJobDefinitionV1.json": json.dumps(
            {
                "defaultLakehouseArtifactId": manifest.lakehouse.lakehouse_id,
                "environmentArtifactId": manifest.fabric_environment_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode(),
    }
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(content).decode(),
                    "payloadType": "InlineBase64",
                }
                for path, content in parts.items()
            ]
        }
    }


@dataclass
class _Client:
    schedules: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    items: list[dict[str, Any]] = field(default_factory=list)
    definitions: dict[str, dict[str, Any]] = field(default_factory=dict)
    lists: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    definition_calls: list[tuple[str, str]] = field(default_factory=list)

    def list_pages(self, path: str):
        if path in self.lists:
            return self.lists[path]
        if path.endswith("/items"):
            return self.items
        if path.endswith("/schedules"):
            return self.schedules[path.rsplit("/", 4)[-4]]
        raise AssertionError(path)

    def request_json(self, method: str, path: str, **_: object):
        item_id = path.rsplit("/", 1)[-1]
        return next(item for item in self.items if item["id"] == item_id)

    def run_operation(self, method: str, path: str, **_: object):
        self.definition_calls.append((method, path))
        item_id = path.split("/")[-2]
        return DeploymentEvidence(
            action="read", status="succeeded", resource=self.definitions[item_id]
        )


@dataclass
class _OneLakeClient:
    release: dict[str, Any]
    missing_folder: bool = False

    def request_response(self, method: str, path: str, **_: object):
        if method == "HEAD":
            return (404 if self.missing_folder else 200), {}, b""
        if method == "GET" and "/releases/" in path:
            return 200, {}, json.dumps(self.release).encode()
        raise AssertionError((method, path))


@dataclass
class _OneLakeTableClient:
    pages: dict[str, dict[str, Any]]
    calls: list[tuple[str, str]] = field(default_factory=list)

    def request_json(self, method: str, path: str, **_: object):
        self.calls.append((method, path))
        return self.pages[path]


def test_onelake_table_listing_paginates_and_validates_responses() -> None:
    manifest = _manifest()
    workspace_id = manifest.workspace_id
    lakehouse_id = manifest.lakehouse.lakehouse_id
    assert workspace_id is not None
    assert lakehouse_id is not None
    base_path = (
        f"/{workspace_id}/{lakehouse_id}/api/2.1/unity-catalog/tables"
        f"?catalog_name={lakehouse_id}&schema_name=dbo"
    )
    next_path = f"{base_path}&page_token=page%2F2%2B"
    client = _OneLakeTableClient(
        pages={
            base_path: {
                "tables": [{"name": "first"}],
                "next_page_token": "page/2+",
            },
            next_path: {
                "tables": [{"name": "second"}],
                "next_page_token": None,
            },
        }
    )

    result = acceptance._list_lakehouse_tables(
        client,
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id,
    )

    assert [table["name"] for table in result] == ["first", "second"]
    assert client.calls == [("GET", base_path), ("GET", next_path)]

    for tables in (None, {}, [None]):
        malformed = _OneLakeTableClient(
            pages={
                base_path: {
                    "tables": tables,
                    "next_page_token": None,
                }
            }
        )
        with pytest.raises(
            acceptance.CleanroomAcceptanceError,
            match="^OneLake Delta table response must contain a tables array$",
        ):
            acceptance._list_lakehouse_tables(
                malformed,
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
            )

    for token in (1, ""):
        malformed = _OneLakeTableClient(
            pages={
                base_path: {
                    "tables": [],
                    "next_page_token": token,
                }
            }
        )
        with pytest.raises(
            acceptance.CleanroomAcceptanceError,
            match="^OneLake Delta table pagination token is invalid$",
        ):
            acceptance._list_lakehouse_tables(
                malformed,
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
            )

    repeated_token_path = f"{base_path}&page_token=repeat"
    repeated = _OneLakeTableClient(
        pages={
            base_path: {"tables": [], "next_page_token": "repeat"},
            repeated_token_path: {
                "tables": [],
                "next_page_token": "repeat",
            },
        }
    )
    with pytest.raises(
        acceptance.CleanroomAcceptanceError,
        match="^OneLake Delta table pagination token is invalid$",
    ):
        acceptance._list_lakehouse_tables(
            repeated,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
        )


def _postconfigure(manifest, outputs):
    return {
        "schema": "people-counter-fabric-postconfigure-evidence-v1",
        "status": "succeeded",
        "schedulesEnabled": False,
        "operatorCheckpoints": [],
        "context": {
            "commitSha": _COMMIT,
            "terraformOutputSha256": acceptance._sha256(outputs),
            "definitionHashes": {},
        },
        "resources": {
            "workspaceId": manifest.workspace_id,
            "lakehouseId": manifest.lakehouse.lakehouse_id,
        },
        "reporting_refresh": {
            bundle: {"status": "succeeded"}
            for bundle in ("analytics", "operational")
        },
    }


def test_definition_decoder_rejects_duplicate_and_invalid_payloads() -> None:
    valid = _definition("alpha", "beta")
    assert "alpha\nbeta" in acceptance._definition_text(valid, "item")
    valid["definition"]["parts"].append(valid["definition"]["parts"][0])
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="path"):
        acceptance._definition_text(valid, "item")

    invalid = _definition("alpha")
    invalid["definition"]["parts"][0]["payload"] = "not-base64"
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="base64"):
        acceptance._definition_text(invalid, "item")


def test_repeat_evidence_requires_two_stable_disabled_runs() -> None:
    manifest = _manifest()
    outputs = _outputs()
    first = _postconfigure(manifest, outputs)
    second = json.loads(json.dumps(first))

    assert acceptance._verify_postconfigure(
        [first, second], commit_sha=_COMMIT
    )["resourceIdentityStable"] is True

    second["schedulesEnabled"] = True
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="safe-state"):
        acceptance._verify_postconfigure([first, second], commit_sha=_COMMIT)

    second = json.loads(json.dumps(first))
    second["context"]["terraformOutputSha256"] = "b" * 64
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="provenance"):
        acceptance._verify_postconfigure([first, second], commit_sha=_COMMIT)


def test_schedule_acceptance_requires_one_disabled_recurring_schedule() -> None:
    manifest = _manifest()
    outputs = _outputs()
    client = _Client(
        schedules={
            str(outputs["spark_job_definition_ids"]["value"][role]): (
                [{"id": _uid(f"{role}-schedule"), "enabled": False}]
                if role in SCHEDULED_ROLES
                else []
            )
            for role in SJD_ROLES
        }
    )

    result = acceptance._verify_schedules(client, manifest, outputs)

    assert result["control"] == {"count": 1, "enabled": False}
    assert result["process"] == {"count": 0, "enabled": None}
    client.schedules[str(outputs["spark_job_definition_ids"]["value"]["control"])][
        0
    ]["enabled"] = True
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="disabled"):
        acceptance._verify_schedules(client, manifest, outputs)


def test_sjd_acceptance_verifies_all_roles_and_runtime_references() -> None:
    manifest = _manifest()
    outputs = _outputs()
    items = [
        {
            "id": item_id,
            "type": "SparkJobDefinition",
            "displayName": role,
        }
        for role, item_id in outputs["spark_job_definition_ids"]["value"].items()
    ]
    definitions = {
        item_id: _sjd_definition(role, manifest)
        for role, item_id in outputs["spark_job_definition_ids"]["value"].items()
    }

    client = _Client(items=items, definitions=definitions)
    result = acceptance._verify_sjds(client, manifest, outputs)

    assert set(result) == SJD_ROLES
    assert all(len(item["sourceSha256"]) == 64 for item in result.values())
    assert set(client.definition_calls) == {
        (
            "POST",
            f"/workspaces/{manifest.workspace_id}/sparkJobDefinitions/"
            f"{item_id}/getDefinition?format=SparkJobDefinitionV2",
        )
        for item_id in outputs["spark_job_definition_ids"]["value"].values()
    }
    definitions[_uid("control")]["definition"]["parts"][0]["payload"] = (
        base64.b64encode(b"from wrong import main\n").decode()
    )
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="canonical source hash"):
        acceptance._verify_sjds(
            _Client(items=items, definitions=definitions), manifest, outputs
        )


def test_reporting_acceptance_checks_direct_lake_report_binding_and_both_refreshes() -> None:
    manifest = _manifest()
    outputs = _outputs()
    items = []
    definitions = {}
    for bundle in ("analytics", "operational"):
        model_id = outputs["semantic_model_ids"]["value"][bundle]
        report_id = outputs["report_ids"]["value"][bundle]
        items.extend(
            [
                {
                    "id": model_id,
                    "type": "SemanticModel",
                    "displayName": f"{bundle}-model",
                },
                {
                    "id": report_id,
                    "type": "Report",
                    "displayName": f"{bundle}-report",
                },
            ]
        )
        definitions[model_id] = _definition(
            str(manifest.workspace_id),
            str(manifest.lakehouse.lakehouse_id),
            "mode: DirectLake",
        )
        definitions[report_id] = _definition(model_id)
    evidence = _postconfigure(manifest, outputs)

    result = acceptance._verify_reporting(
        _Client(items=items, definitions=definitions),
        manifest,
        outputs,
        [evidence, evidence],
    )

    assert result["analytics"]["refreshRuns"] == 2
    definitions[outputs["report_ids"]["value"]["analytics"]] = _definition("wrong")
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="references"):
        acceptance._verify_reporting(
            _Client(items=items, definitions=definitions),
            manifest,
            outputs,
            [evidence, evidence],
        )


def test_duplicate_item_acceptance_rejects_same_managed_type_and_name() -> None:
    manifest = _manifest()
    outputs = _outputs()
    managed_ids = {
        outputs[key]["value"]
        for key in ("lakehouse_id", "environment_id")
    }
    for key in (
        "spark_job_definition_ids",
        "semantic_model_ids",
        "report_ids",
    ):
        managed_ids.update(outputs[key]["value"].values())
    items = [
        {
            "id": item_id,
            "type": "Managed",
            "displayName": f"name-{index}",
        }
        for index, item_id in enumerate(managed_ids)
    ]
    client = _Client(items=items)

    result = acceptance._verify_no_duplicate_items(client, manifest, outputs)

    assert result["duplicates"] == 0
    items.append(
        {
            "id": _uid("duplicate"),
            "type": items[0]["type"],
            "displayName": items[0]["displayName"],
        }
    )
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="duplicated"):
        acceptance._verify_no_duplicate_items(client, manifest, outputs)


def test_lakehouse_acceptance_verifies_tables_folders_shortcuts_and_release() -> None:
    manifest = _manifest()
    outputs = _outputs()
    outputs["managed_shortcuts"]["value"] = {
        "adls": {
            "id": _uid("archive-shortcut"),
            "name": "archive",
            "path": "Files/archive",
            "target_type": "AdlsGen2",
            "location": "https://peoplecounterdev.dfs.core.windows.net",
            "subpath": "/archive/source",
            "connection_id": _uid("adls-connection"),
        }
    }
    table_path = (
        f"/{manifest.workspace_id}/{manifest.lakehouse.lakehouse_id}"
        "/api/2.1/unity-catalog/tables"
        f"?catalog_name={manifest.lakehouse.lakehouse_id}&schema_name=dbo"
    )
    shortcut_path = (
        f"/workspaces/{manifest.workspace_id}/items/"
        f"{manifest.lakehouse.lakehouse_id}/shortcuts"
    )
    items = [
        {
            "id": manifest.lakehouse.lakehouse_id,
            "type": "Lakehouse",
            "displayName": "lakehouse",
        }
    ]
    client = _Client(
        items=items,
        lists={
            shortcut_path: [
                {
                    "name": "archive",
                    "path": "/Files/archive",
                    "target": {
                        "adlsGen2": {
                            "location": (
                                "https://peoplecounterdev.dfs.core.windows.net"
                            ),
                            "subpath": "/archive/source",
                            "connectionId": _uid("adls-connection"),
                        }
                    },
                },
            ],
        },
    )
    table_client = _OneLakeTableClient(
        pages={
            table_path: {
                "tables": [
                    {"name": manifest.lakehouse.table(suffix)}
                    for suffix in acceptance.BOOTSTRAP_TABLE_SUFFIXES
                ],
                "next_page_token": None,
            }
        }
    )
    terraform_sha = acceptance._sha256(outputs)
    release = {
        "schema": acceptance.RELEASE_MANIFEST_SCHEMA,
        "commitSha": _COMMIT,
        "terraformOutputSha256": terraform_sha,
        "status": "succeeded",
    }

    result = acceptance._verify_lakehouse(
        client,
        _OneLakeClient(release),
        table_client,
        manifest,
        outputs,
        {"lakehouse": {"folders": ["deployment-evidence", "releases"]}},
        commit_sha=_COMMIT,
        terraform_sha=terraform_sha,
    )

    assert result["folderCount"] == 2
    assert result["shortcutCount"] == 1
    assert result["tableCount"] == len(acceptance.BOOTSTRAP_TABLE_SUFFIXES)
    assert table_client.calls == [("GET", table_path)]
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="folder"):
        acceptance._verify_lakehouse(
            client,
            _OneLakeClient(release, missing_folder=True),
            table_client,
            manifest,
            outputs,
            {"lakehouse": {"folders": ["releases"]}},
            commit_sha=_COMMIT,
            terraform_sha=terraform_sha,
        )


@pytest.mark.parametrize(
    ("function", "arguments", "message"),
    [
        (
            acceptance._live_shortcut_identity,
            ({"target": []},),
            "live shortcut target must be an object",
        ),
        (
            acceptance._live_shortcut_identity,
            ({"target": {"adlsGen2": []}},),
            "live shortcut ADLS Gen2 target must be an object",
        ),
        (
            acceptance._live_shortcut_identity,
            ({"target": {}},),
            "live shortcut ADLS Gen2 target must be an object",
        ),
        (
            acceptance._expected_shortcut_identity,
            ("source", []),
            "managed_shortcuts.source must be an object",
        ),
        (
            acceptance._expected_shortcut_identity,
            ("source", {"target_type": "S3"}),
            "managed_shortcuts.source.target_type must be AdlsGen2",
        ),
    ],
)
def test_shortcut_identity_rejects_invalid_targets(
    function: Any,
    arguments: tuple[object, ...],
    message: str,
) -> None:
    with pytest.raises(acceptance.CleanroomAcceptanceError) as captured:
        function(*arguments)

    assert str(captured.value) == message


def test_rti_and_activator_acceptance_require_live_assets_and_action_evidence() -> None:
    manifest = _manifest()
    outputs = _outputs()
    rti = {
        "eventhouse_id": "Eventhouse",
        "kql_database_id": "KQLDatabase",
        "kql_queryset_id": "KQLQueryset",
        "kql_dashboard_id": "KQLDashboard",
    }
    items = []
    definitions = {}
    for output_name, item_type in rti.items():
        item_id = _uid(output_name)
        outputs[output_name]["value"] = item_id
        items.append(
            {"id": item_id, "type": item_type, "displayName": output_name}
        )
    for output_name in ("kql_queryset_id", "kql_dashboard_id"):
        definitions[outputs[output_name]["value"]] = _definition(
            outputs["kql_database_id"]["value"]
        )
    activator_ids = {"operations": _uid("operations-activator")}
    outputs["activator_ids"]["value"] = activator_ids
    items.extend(
        {
            "id": activator_id,
            "type": "Reflex",
            "displayName": name,
        }
        for name, activator_id in activator_ids.items()
    )
    evidence = _postconfigure(manifest, outputs)
    evidence["activator"] = {
        "status": "succeeded",
        "resource": {"actionExecutionVerified": True},
    }
    client = _Client(items=items, definitions=definitions)

    assert set(acceptance._verify_rti(client, manifest, outputs)) == set(rti)
    for output_name, item_type in (
        ("kql_queryset_id", "KQLQueryset"),
        ("kql_dashboard_id", "KQLDashboard"),
    ):
        item_id = outputs[output_name]["value"]
        valid_definition = definitions[item_id]
        definitions[item_id] = _definition("unrelated")
        with pytest.raises(
            acceptance.CleanroomAcceptanceError,
            match=f"{item_type} definition is missing references",
        ):
            acceptance._verify_rti(client, manifest, outputs)
        definitions[item_id] = valid_definition
    assert acceptance._verify_activator(
        client, manifest, outputs, [evidence, evidence]
    ) == {
        "actionRuns": 2,
        "id": activator_ids["operations"],
        "ids": activator_ids,
    }

    evidence["activator"]["resource"]["actionExecutionVerified"] = False
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="absent"):
        acceptance._verify_activator(
            client, manifest, outputs, [evidence, evidence]
        )

    evidence["activator"]["resource"]["actionExecutionRequired"] = False
    assert acceptance._verify_activator(
        client,
        manifest,
        outputs,
        [evidence, evidence],
        require_action_checkpoint=False,
    )["actionRuns"] == 2

    evidence["activator"]["resource"]["actionExecutionVerified"] = True
    del evidence["activator"]["resource"]["actionExecutionRequired"]
    assert acceptance._verify_activator(
        client,
        manifest,
        outputs,
        [evidence, evidence],
        require_action_checkpoint=False,
    )["actionRuns"] == 2

    evidence["activator"]["status"] = "failed"
    with pytest.raises(
        acceptance.CleanroomAcceptanceError,
        match="definition/action verification failed",
    ):
        acceptance._verify_activator(
            client, manifest, outputs, [evidence, evidence]
        )

    evidence["activator"]["status"] = "succeeded"
    outputs["activator_ids"]["value"]["operations"] = True
    with pytest.raises(
        acceptance.CleanroomAcceptanceError,
        match="operations Activator output is unresolved",
    ):
        acceptance._verify_activator(
            client, manifest, outputs, [evidence, evidence]
        )

    del outputs["activator_ids"]["value"]["operations"]
    with pytest.raises(
        acceptance.CleanroomAcceptanceError,
        match="operations Activator output is unresolved",
    ):
        acceptance._verify_activator(
            client, manifest, outputs, [evidence, evidence]
        )


def test_cleanroom_orchestrator_requires_zero_drift_and_matching_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest()
    outputs = _outputs()
    definition_evidence = {"treeSha256": "b" * 64}
    postconfigure = _postconfigure(manifest, outputs)
    postconfigure["context"]["definitionHashes"] = {
        "treeSha256": "b" * 64
    }
    monkeypatch.setattr(
        acceptance,
        "_verify_no_duplicate_items",
        lambda *_args: {"duplicates": 0},
    )
    monkeypatch.setattr(
        acceptance, "_verify_sjds", lambda *_args: {"roles": 7}
    )
    monkeypatch.setattr(
        acceptance, "_verify_schedules", lambda *_args: {"disabled": True}
    )
    monkeypatch.setattr(
        acceptance,
        "_verify_lakehouse",
        lambda *_args, **_kwargs: {"release": True},
    )

    result = acceptance.validate_cleanroom(
        object(),
        object(),
        object(),
        manifest,
        terraform_output=outputs,
        runtime_contract={"schema": acceptance.RUNTIME_CONTRACT_SCHEMA},
        postconfigure_evidence=[postconfigure, postconfigure],
        definition_evidence=definition_evidence,
        commit_sha=_COMMIT,
        terraform_drift_exit_code=0,
        require_reporting=False,
        require_rti=False,
        require_activator=False,
    )


    assert result["status"] == "succeeded"
    assert result["checks"]["terraform"]["driftExitCode"] == 0
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="zero drift"):
        acceptance.validate_cleanroom(
            object(), object(), object(), manifest,
            terraform_output=outputs,
            runtime_contract={"schema": acceptance.RUNTIME_CONTRACT_SCHEMA},
            postconfigure_evidence=[postconfigure, postconfigure],
            definition_evidence=definition_evidence,
            commit_sha=_COMMIT, terraform_drift_exit_code=2,
            require_reporting=False, require_rti=False, require_activator=False,
        )


@pytest.fixture
def cleanroom_case():
    manifest = _manifest()
    outputs = _outputs()
    types = {
    "lakehouse_id": "Lakehouse", "environment_id": "Environment",
    "eventhouse_id": "Eventhouse", "kql_database_id": "KQLDatabase",
    "kql_queryset_id": "KQLQueryset", "kql_dashboard_id": "KQLDashboard",
    }
    for key in ("eventhouse_id", "kql_database_id", "kql_queryset_id", "kql_dashboard_id"):
        outputs[key]["value"] = _uid(key)
    outputs["activator_ids"]["value"] = {"operations": _uid("activator")}
    items = [
    {"id": outputs[key]["value"], "type": item_type, "displayName": key}
    for key, item_type in types.items()
    ]
    for key, item_type in (
    ("spark_job_definition_ids", "SparkJobDefinition"),
    ("semantic_model_ids", "SemanticModel"), ("report_ids", "Report"),
    ("activator_ids", "Reflex"),
    ):
        items.extend(
            {"id": value, "type": item_type, "displayName": f"{key}-{role}"}
            for role, value in outputs[key]["value"].items()
        )
    definitions = {
    item_id: _sjd_definition(role, manifest)
    for role, item_id in outputs["spark_job_definition_ids"]["value"].items()
    }
    for bundle in ("analytics", "operational"):
        model_id = outputs["semantic_model_ids"]["value"][bundle]
        definitions[model_id] = _definition(
            manifest.workspace_id, manifest.lakehouse.lakehouse_id, "mode: DirectLake",
        )
        definitions[outputs["report_ids"]["value"][bundle]] = _definition(model_id)
    for key in ("kql_queryset_id", "kql_dashboard_id"):
        definitions[outputs[key]["value"]] = _definition(outputs["kql_database_id"]["value"])
    shortcut_path = f"/workspaces/{manifest.workspace_id}/items/{manifest.lakehouse.lakehouse_id}/shortcuts"
    client = _Client(
    items=items, definitions=definitions, lists={shortcut_path: []},
    schedules={
        item_id: [{"enabled": False}] if role in SCHEDULED_ROLES else []
        for role, item_id in outputs["spark_job_definition_ids"]["value"].items()
    },
    )
    table_path = (
    f"/{manifest.workspace_id}/{manifest.lakehouse.lakehouse_id}"
    f"/api/2.1/unity-catalog/tables?catalog_name={manifest.lakehouse.lakehouse_id}&schema_name=dbo"
    )
    evidence = _postconfigure(manifest, outputs)
    evidence["activator"] = {
    "status": "succeeded", "resource": {"actionExecutionVerified": True},
    }
    return {
    "client": client,
    "onelake_client": _OneLakeClient({
        "schema": acceptance.RELEASE_MANIFEST_SCHEMA,
        "commitSha": _COMMIT,
        "terraformOutputSha256": acceptance._sha256(outputs),
        "status": "succeeded",
    }),
    "onelake_table_client": _OneLakeTableClient({
        table_path: {"tables": [
            {"name": manifest.lakehouse.table(suffix)}
            for suffix in acceptance.BOOTSTRAP_TABLE_SUFFIXES
        ]},
    }),
    "manifest": manifest, "terraform_output": outputs,
    "runtime_contract": {
        "schema": acceptance.RUNTIME_CONTRACT_SCHEMA,
        "lakehouse": {"folders": ["releases"]},
    },
    "postconfigure_evidence": [evidence, json.loads(json.dumps(evidence))],
    "definition_evidence": {}, "commit_sha": _COMMIT, "terraform_drift_exit_code": 0,
    "require_reporting": True, "require_rti": True, "require_activator": True,
    }


def test_cleanroom_verifies_all_assets_through_external_adapters(cleanroom_case):
    result = acceptance.validate_cleanroom(**cleanroom_case)
    assert result["status"] == "succeeded"
    assert result["checks"]["reporting"]["analytics"]["refreshRuns"] == 2
    assert result["checks"]["activator"]["actionRuns"] == 2
    assert result["checks"]["realTimeIntelligence"]["eventhouse_id"] == _uid("eventhouse_id")
    assert result["checks"]["lakehouse"]["tableCount"] == len(acceptance.BOOTSTRAP_TABLE_SUFFIXES)
    assert result["checks"]["items"]["managedItemCount"] == len(cleanroom_case["client"].items)


def test_cleanroom_requires_action_execution_unless_explicitly_deferred(cleanroom_case):
    for evidence in cleanroom_case["postconfigure_evidence"]:
        evidence["activator"]["resource"] = {
            "actionExecutionVerified": False, "actionExecutionRequired": False,
        }
    with pytest.raises(acceptance.CleanroomAcceptanceError, match="action evidence is absent"):
        acceptance.validate_cleanroom(**cleanroom_case)
    cleanroom_case["require_activator_action_checkpoint"] = False
    result = acceptance.validate_cleanroom(**cleanroom_case)
    assert result["status"] == "succeeded"
    assert result["checks"]["activator"]["actionRuns"] == 2


@pytest.mark.parametrize("section,field,value,message", [
    (None, "commit_sha", "short", "full lowercase commit SHA"),
    ("runtime_contract", "schema", "old", "runtime contract schema"),
    ("context", "terraformOutputSha256", "0" * 64, "Terraform output provenance"),
    ("context", "definitionHashes", {"treeSha256": "0" * 64}, "definition provenance"),
    ("context", "commitSha", "b" * 40, "commit does not match"),
    ("evidence", "operatorCheckpoints", ["pending"], "unresolved checkpoints"),
])
def test_cleanroom_rejects_invalid_provenance(cleanroom_case, section, field, value, message):
    if section == "context":
        for evidence in cleanroom_case["postconfigure_evidence"]:
            evidence["context"][field] = value
    elif section == "evidence":
        cleanroom_case["postconfigure_evidence"][0][field] = value
    else:
        target = cleanroom_case if section is None else cleanroom_case[section]
        target[field] = value
    with pytest.raises(acceptance.CleanroomAcceptanceError, match=message):
        acceptance.validate_cleanroom(**cleanroom_case)
    assert cleanroom_case["client"].definition_calls == []


@pytest.mark.parametrize("field,value,message", [
    ("parts", [], "no parts"),
    ("payload", None, "no payload"),
    ("payloadType", "Reference", "not InlineBase64"),
    ("path", "", "path is invalid"),
])
def test_cleanroom_rejects_unreadable_live_definition(cleanroom_case, field, value, message):
    definition = cleanroom_case["client"].definitions[_uid("control")]["definition"]
    if field == "parts":
        definition[field] = value
    else:
        definition["parts"][0][field] = value
    with pytest.raises(acceptance.CleanroomAcceptanceError, match=message):
        acceptance.validate_cleanroom(**cleanroom_case)


@pytest.mark.parametrize("failure,message", [
    ("missing-item", "managed items are missing"),
    ("wrong-type", "item readback does not match"),
    ("wrong-schedule-count", "schedules; expected"),
    ("missing-table", "tables are missing"),
    ("invalid-folders", "folders must be an array"),
    ("shortcut-mismatch", "shortcuts do not exactly match"),
    ("wrong-release", "release manifest provenance"),
    ("non-directlake", "not bound for Direct Lake"),
    ("failed-refresh", "refresh did not succeed"),
    ("missing-action-evidence", "action evidence is absent"),
])
def test_cleanroom_fails_closed_for_incomplete_deployments(cleanroom_case, failure, message):
    client = cleanroom_case["client"]
    if failure == "missing-item":
        client.items.pop()
    elif failure == "wrong-type":
        next(item for item in client.items if item["id"] == _uid("control"))["type"] = "Report"
    elif failure == "wrong-schedule-count":
        client.schedules[_uid("control")] = []
    elif failure == "missing-table":
        next(iter(cleanroom_case["onelake_table_client"].pages.values()))["tables"] = []
    elif failure == "invalid-folders":
        cleanroom_case["runtime_contract"]["lakehouse"]["folders"] = {}
    elif failure == "shortcut-mismatch":
        next(iter(client.lists.values())).append({
            "path": "Files", "name": "unexpected", "target": {"adlsGen2": {}},
        })
    elif failure == "wrong-release":
        cleanroom_case["onelake_client"].release["commitSha"] = "b" * 40
    elif failure == "non-directlake":
        manifest = cleanroom_case["manifest"]
        client.definitions[_uid("analytics-model")] = _definition(
            manifest.workspace_id, manifest.lakehouse.lakehouse_id,
        )
    elif failure == "failed-refresh":
        cleanroom_case["postconfigure_evidence"][0]["reporting_refresh"]["analytics"]["status"] = "failed"
    elif failure == "missing-action-evidence":
        cleanroom_case["postconfigure_evidence"][0]["activator"]["resource"] = None
    with pytest.raises(acceptance.CleanroomAcceptanceError, match=message):
        acceptance.validate_cleanroom(**cleanroom_case)


def test_acceptance_cli_reads_documents_and_uses_separate_external_clients(
    cleanroom_case, tmp_path, monkeypatch, capsys,
):
    from people_counter import fabric_automation, fabric_rest

    clients = iter(cleanroom_case[key] for key in (
    "client", "onelake_client", "onelake_table_client",
    ))
    roots = []

    def make_client(token_provider, **kwargs):
        roots.append(kwargs["api_root"])
        return next(clients)

    monkeypatch.setattr(fabric_automation, "FabricAutomationClient", make_client)
    monkeypatch.setattr(acceptance, "make_resource_token_provider", lambda resource: lambda: "test-token")
    monkeypatch.setattr(fabric_rest, "UrllibTransport", lambda: None)
    args = []
    for option, document in (
    ("manifest", json.loads(Path("src/people_counter/data/deploy/dev.json").read_text())),
    ("terraform-output", cleanroom_case["terraform_output"]),
    ("runtime-contract", cleanroom_case["runtime_contract"]),
    ("definition-evidence", cleanroom_case["definition_evidence"]),
    ):
        path = tmp_path / f"{option}.json"
        path.write_text(json.dumps(document))
        args.extend([f"--{option}-path", str(path)])
    for index, document in enumerate(cleanroom_case["postconfigure_evidence"]):
        path = tmp_path / f"postconfigure-{index}.json"
        path.write_text(json.dumps(document))
        args.extend(["--postconfigure-evidence-path", str(path)])
    args.extend([
    "--stage", "dev", "--commit-sha", _COMMIT, "--terraform-drift-exit-code", "0",
    "--require-reporting", "--require-rti", "--require-activator",
    ])
    assert acceptance.main(args) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "succeeded"
    assert roots == [
    "https://api.fabric.microsoft.com/v1",
    "https://onelake.dfs.fabric.microsoft.com",
    "https://onelake.table.fabric.microsoft.com/delta",
    ]
    (tmp_path / "terraform-output.json").write_text("[]")
    assert acceptance.main(args) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["status"] == "failed"
    assert "must be an object" in captured.err
    (tmp_path / "terraform-output.json").write_text("{")
    assert acceptance.main(args) == 1
    assert "could not be read" in capsys.readouterr().err

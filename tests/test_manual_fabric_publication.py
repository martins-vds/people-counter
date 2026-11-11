import base64
import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from people_counter.manual_fabric_publication import (
    PUBLICATION_DESCRIPTION,
    ensure_item,
    stopped_activator_definition,
    render_definition,
)
from scripts.prepare_manual_fabric_assets import prepare_assets


def definition(content, path="definition.txt"):
    return {
        "parts": [
            {
                "path": path,
                "payload": base64.b64encode(content.encode()).decode(),
                "payloadType": "InlineBase64",
            }
        ],
    }


def test_render_binds_tokens_and_report_workspace_without_mutating_source():
    source = definition(
        json.dumps({
            "datasetReference": {"byConnection": {
                "connectionString": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
                    "people-counter-dev;initial catalog={{ .TargetSemanticModelName }};"
                    "semanticmodelid={{ .TargetSemanticModelId }}"
                )
            }}
        }),
        path="definition.pbir",
    )
    original = copy.deepcopy(source)
    rendered = render_definition(
        source,
        {"TargetSemanticModelName": "model", "TargetSemanticModelId": "model-id"},
        workspace_name="manual-workspace",
    )
    text = base64.b64decode(rendered["parts"][0]["payload"]).decode()
    assert "myorg/manual-workspace;" in text
    assert "initial catalog=model;semanticmodelid=model-id" in text
    assert "people-counter-dev" not in text
    assert source == original


def test_render_preserves_binary_resources_and_omits_platform_metadata():
    source = definition("{{ .WorkspaceId }}", path="model.tmdl")
    source["format"] = "PBIR"
    binary = {"path": "image.png", "payload": "/wAB", "payloadType": "InlineBase64"}
    source["parts"].extend([binary, {**binary, "path": ".platform"}])
    rendered = render_definition(source, {"WorkspaceId": "target"})
    assert rendered["format"] == "PBIR"
    assert rendered["parts"] == [
        {**source["parts"][0], "payload": base64.b64encode(b"target").decode()},
        binary,
    ]


def test_render_rejects_unresolved_tokens():
    with pytest.raises(ValueError, match="Unresolved"):
        render_definition(definition("{{ .Missing }}"), {})


def test_render_binds_the_workspace_token_without_a_reference_workspace_literal():
    source = definition("myorg/{{ .TargetWorkspaceName }};", path="definition.pbir")
    rendered = render_definition(source, {}, workspace_name="selected-workspace")
    assert base64.b64decode(rendered["parts"][0]["payload"]).decode() == "myorg/selected-workspace;"


def test_render_does_not_rebind_other_parts_or_a_missing_workspace():
    connection = "powerbi://api.powerbi.com/v1.0/myorg/people-counter-dev;"
    unchanged = render_definition(definition(connection, path="definition.pbir"), {})
    assert base64.b64decode(unchanged["parts"][0]["payload"]).decode() == connection
    other_part = render_definition(
        definition(connection, path="resource.txt"), {}, workspace_name="manual"
    )
    assert base64.b64decode(other_part["parts"][0]["payload"]).decode() == connection


def test_render_rejects_invalid_base64():
    source = definition("expected")
    source["parts"][0]["payload"] += "!"
    with pytest.raises(ValueError):
        render_definition(source, {})


def client_with(items):
    client = Mock()
    client.workspace_id = "guided-workspace"
    client.list_items.return_value = items
    return client


def test_existing_unowned_item_is_not_overwritten():
    client = client_with([{"id": "other", "displayName": "report", "type": "Report"}])
    with pytest.raises(ValueError, match="not owned"):
        ensure_item(client, "report", "Report", definition("new"))
    client._lro_json.assert_not_called()


@pytest.mark.parametrize("items", [
    [{"id": "one", "displayName": "report", "type": "SemanticModel"}],
    [
        {"id": "one", "displayName": "report", "type": "Report"},
        {"id": "two", "displayName": "report", "type": "Report"},
    ],
])
def test_ambiguous_or_wrong_type_items_fail_closed(items):
    client = client_with(items)
    with pytest.raises(ValueError):
        ensure_item(client, "report", "Report")
    client._lro_json.assert_not_called()


def test_create_definition_then_verify_readback():
    expected = definition("report")
    client = client_with([])
    client.list_items.side_effect = [
        [],
        [{"id": "created", "displayName": "report", "type": "Report",
          "description": PUBLICATION_DESCRIPTION}],
    ]
    client._lro_json.side_effect = [{}, {"definition": expected}]
    assert ensure_item(client, "report", "Report", expected) == "created"
    assert client._lro_json.call_args_list[0].args == (
        "POST", "/workspaces/guided-workspace/items",
    )
    assert client._lro_json.call_args_list[0].kwargs["value"] == {
        "displayName": "report", "type": "Report",
        "description": PUBLICATION_DESCRIPTION, "definition": expected,
    }
    assert client._lro_json.call_args_list[1].args == (
        "POST", "/workspaces/guided-workspace/items/created/getDefinition",
    )


@pytest.mark.parametrize("created", [
    [],
    [{"id": "wrong-type", "displayName": "report", "type": "SemanticModel"}],
    [{"id": "wrong-name", "displayName": "another", "type": "Report"}],
    [
        {"id": "one", "displayName": "report", "type": "Report"},
        {"id": "two", "displayName": "report", "type": "Report"},
    ],
])
def test_create_requires_unique_name_and_type_readback(created):
    client = client_with([])
    client.list_items.side_effect = [[], created]
    with pytest.raises(ValueError, match="uniquely read back"):
        ensure_item(client, "report", "Report", definition("new"))


def test_readback_rejects_invalid_base64():
    client = client_with([{
        "id": "owned", "displayName": "report", "type": "Report",
        "description": PUBLICATION_DESCRIPTION,
    }])
    invalid = definition("expected")
    invalid["parts"][0]["payload"] += "!"
    client._lro_json.side_effect = [{}, {"definition": invalid}]
    with pytest.raises(ValueError):
        ensure_item(client, "report", "Report", definition("expected"))


def test_readback_ignores_service_generated_platform_metadata():
    expected = definition("expected")
    actual = copy.deepcopy(expected)
    actual["parts"].append({
        "path": ".platform", "payloadType": "InlineBase64",
        "payload": base64.b64encode(b'{"logicalId":"service-assigned"}').decode(),
    })
    client = client_with([{
        "id": "owned", "displayName": "report", "type": "Report",
        "description": PUBLICATION_DESCRIPTION,
    }])
    client._lro_json.side_effect = [{}, {"definition": actual}]
    assert ensure_item(client, "report", "Report", expected) == "owned"


def test_approved_existing_item_is_updated_and_read_back():
    expected = definition("updated")
    client = client_with([{"id": "approved", "displayName": "queries", "type": "KQLQueryset"}])
    client._lro_json.side_effect = [{}, {"definition": expected}]
    assert ensure_item(
        client, "queries", "KQLQueryset", expected, approved_existing_id="approved"
    ) == "approved"
    assert client._lro_json.call_args_list[0].args == (
        "POST", "/workspaces/guided-workspace/items/approved/updateDefinition",
    )
    assert client._lro_json.call_args_list[0].kwargs == {"value": {"definition": expected}}


def test_definition_mismatch_blocks_success():
    client = client_with([{
        "id": "owned", "displayName": "report", "type": "Report",
        "description": PUBLICATION_DESCRIPTION,
    }])
    client._lro_json.side_effect = [{}, {"definition": definition("wrong")}]
    with pytest.raises(ValueError, match="readback"):
        ensure_item(client, "report", "Report", definition("expected"))


def test_create_payload_cannot_be_combined_with_definition():
    client = client_with([])
    with pytest.raises(ValueError, match="both"):
        ensure_item(client, "db", "KQLDatabase", definition("x"), creation_payload={})
    client.list_items.assert_not_called()


def test_database_creation_payload_is_preserved():
    client = client_with([])
    client.list_items.side_effect = [
        [], [{"id": "db", "displayName": "operations", "type": "KQLDatabase"}],
    ]
    payload = {"databaseType": "ReadWrite", "parentEventhouseItemId": "house"}
    assert ensure_item(client, "operations", "KQLDatabase", creation_payload=payload) == "db"
    assert client._lro_json.call_args.kwargs["value"]["creationPayload"] == payload


def test_default_format_is_omitted_from_api_payload():
    source = definition("query")
    source["format"] = "Default"
    expected = copy.deepcopy(source)
    expected.pop("format")
    client = client_with([{
        "id": "owned", "displayName": "query", "type": "KQLQueryset",
        "description": PUBLICATION_DESCRIPTION,
    }])
    client._lro_json.side_effect = [{}, {"definition": expected}]
    assert ensure_item(client, "query", "KQLQueryset", source) == "owned"
    assert client._lro_json.call_args_list[0].kwargs["value"] == {"definition": expected}
    assert source["format"] == "Default"


@pytest.mark.parametrize("path", ["settings.json", "definition.pbir", "definition.pbism"])
def test_json_readback_ignores_whitespace_but_not_changed_values(path):
    expected = definition('{"b":2,"a":1}', path=path)
    actual = definition('{\n "a": 1, "b": 2\n}', path=path)
    client = client_with([{
        "id": "owned", "displayName": "variables", "type": "VariableLibrary",
        "description": PUBLICATION_DESCRIPTION,
    }])
    client._lro_json.side_effect = [{}, {"definition": actual}]
    assert ensure_item(client, "variables", "VariableLibrary", expected) == "owned"
    changed = definition('{"a":1,"b":3}', path=path)
    client._lro_json.side_effect = [{}, {"definition": changed}]
    with pytest.raises(ValueError, match="readback"):
        ensure_item(client, "variables", "VariableLibrary", expected)


def test_readback_requests_the_submitted_tmdl_format():
    expected = definition("model text", path="definition/model.tmdl")
    expected["format"] = "TMDL"
    client = client_with([{
        "id": "owned", "displayName": "model", "type": "SemanticModel",
        "description": PUBLICATION_DESCRIPTION,
    }])
    client._lro_json.side_effect = [{}, {"definition": expected}]
    assert ensure_item(client, "model", "SemanticModel", expected) == "owned"
    assert client._lro_json.call_args.args == (
        "POST", "/workspaces/guided-workspace/items/owned/getDefinition?format=TMDL",
    )


def test_eventhouse_and_automatic_database_may_share_the_same_name():
    client = client_with([
        {"id": "house", "displayName": "operations", "type": "Eventhouse",
         "description": PUBLICATION_DESCRIPTION},
        {"id": "default-db", "displayName": "operations", "type": "KQLDatabase"},
    ])
    assert ensure_item(client, "operations", "Eventhouse") == "house"
    client._lro_json.assert_not_called()


def test_operations_activator_stops_rules_and_uses_only_portal_draft_actions():
    root = Path(__file__).resolve().parents[1]
    from people_counter.fabric_definition_bundle import read_definition_bundle
    source = read_definition_bundle(root / "fabric/activators/operations")["definition"]
    original = copy.deepcopy(source)
    tenant = "11111111-1111-4111-8111-111111111111"
    portal_actions = [{
        "name": "EmailBinding", "kind": "Email",
        "arguments": [{"name": "recipients", "type": "array", "values": [
            {"type": "string", "value": "operator@example.invalid"},
        ]}],
    }]
    rendered = stopped_activator_definition(
        source,
        {
            "TargetWorkspaceId": "workspace",
            "TargetSparkJobDispatcherId": "dispatcher",
            "TargetKqlDatabaseId": "monitoring-db",
            "TargetKqlDashboardId": "dashboard",
            "TargetKqlQuerysetId": "queryset",
            "TargetEventhouseId": "eventhouse",
        },
        tenant_id=tenant,
        dispatcher_name="pc_sjd_dev_dispatcher",
        action_rows=portal_actions,
    )
    entities = json.loads(base64.b64decode(next(
        p for p in rendered["parts"] if p["path"] == "ReflexEntities.json"
    )["payload"]))
    rules = []
    sources = []
    for entity in entities:
        payload = entity["payload"]
        if payload.get("definition", {}).get("type") == "Rule":
            rules.append(payload)
            assert payload["definition"]["settings"] == {
                "shouldRun": False, "shouldApplyRuleOnUpdate": False,
            }
            instance = json.loads(payload["definition"]["instance"])
            assert all(step["rows"] == portal_actions for step in instance["steps"] if step["name"] == "ActStep")
        if "connection" in payload:
            sources.append(payload)
            assert payload["connection"]["tenantId"] == tenant
            assert payload["connection"]["workspaceId"] == "workspace"
            assert payload["connection"]["artifactId"] == "dispatcher"
        if "runSettings" in payload:
            assert payload["runSettings"]["isStopped"] is True
            assert payload["runSettings"]["executionIntervalInSeconds"] == 300
    assert len(rules) == 2 and len(sources) == 1
    assert {rule["name"] for rule in rules} == {
        "alert_pc_sjd_dev_dispatcher_failed", "alert_pc_event_intake_duration_20m",
    }
    assert any(entity["payload"].get("name") == "pc_sjd_dev_dispatcher" for entity in entities)
    assert not any(entity["payload"].get("name") == "pc-dispatcher-00" for entity in entities)
    assert portal_actions[0]["arguments"][0]["values"][0]["value"] == "operator@example.invalid"
    assert source == original


@pytest.mark.parametrize("tenant", [
    "not-a-uuid", "00000000-0000-0000-0000-000000000000",
    "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
])
def test_stopped_activator_rejects_invalid_tenants(tenant):
    with pytest.raises(ValueError):
        stopped_activator_definition(
            definition("[]", path="ReflexEntities.json"), {},
            tenant_id=tenant, dispatcher_name="dispatcher",
            action_rows=[{"name": "portal-action"}],
        )


def test_stopped_activator_requires_the_entities_part():
    with pytest.raises(ValueError, match="exactly one"):
        stopped_activator_definition(
            {"parts": []}, {},
            tenant_id="11111111-1111-4111-8111-111111111111",
            dispatcher_name="dispatcher",
            action_rows=[{"name": "portal-action"}],
        )


def test_stopped_activator_refuses_an_invalid_empty_action_step():
    with pytest.raises(ValueError, match="portal-authored"):
        stopped_activator_definition(
            {"parts": []}, {},
            tenant_id="11111111-1111-4111-8111-111111111111",
            dispatcher_name="dispatcher", action_rows=[],
        )


def test_prepare_assets_validates_sources_and_never_overwrites_different_inputs(tmp_path):
    root = Path(__file__).resolve().parents[1]
    output = tmp_path / "inputs.json"
    digest = prepare_assets(root, output)
    import hashlib
    assert digest == hashlib.sha256(output.read_bytes()).hexdigest()
    payload = json.loads(output.read_text())
    expected_bundles = {
        "semantic-models/analytics", "semantic-models/operational",
        "reports/analytics", "reports/operational", "rti/queryset", "rti/dashboard",
        "activators/operations",
    }
    assert set(payload["definitions"]) == expected_bundles
    from people_counter.fabric_definition_bundle import read_definition_bundle
    assert payload["definitions"] == {
        relative: read_definition_bundle(root / "fabric" / relative)["definition"]
        for relative in expected_bundles
    }
    assert payload["publicationSource"] == (
        root / "src/people_counter/manual_fabric_publication.py"
    ).read_text()
    assert payload["variableSettings"] == json.loads(
        (root / "infra/modules/fabric-foundation/definitions/variable-library/settings.json").read_text()
    )
    assert payload["variableTemplate"] == (
        root / "infra/modules/fabric-foundation/definitions/variable-library/variables.json.tmpl"
    ).read_text()
    assert payload["schema"] == "people-counter-manual-publication-inputs-v1"
    assert payload["clientSource"] == (root / "src/people_counter/fabric_rest.py").read_text()
    assert "workspace_id: str," in payload["clientSource"]
    assert "from people_counter.fabric_sjd" not in payload["clientSource"]
    assert prepare_assets(root, output) == digest
    output.write_text("unrelated inputs")
    with pytest.raises(ValueError, match="overwrite"):
        prepare_assets(root, output)
    assert output.read_text() == "unrelated inputs"

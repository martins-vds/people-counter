import base64
import dataclasses
import json
from unittest.mock import Mock
from uuid import UUID

import pytest

from scripts import deploy_sjd_analytics as analytics
from scripts import deploy_sjd_operational_report as operational
from tests.fabric_fixtures import ROLE_IDS
from tests.test_fabric_operations_portable import manifest


@pytest.fixture(autouse=True)
def forbid_unexpected_live_reporting_client(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Injected reporting client must not be replaced with a live client")

    monkeypatch.setattr(analytics, "FabricRESTController", forbidden)
    monkeypatch.setattr(analytics, "make_token_provider", forbidden)


@pytest.mark.parametrize("module,bundle", [
    (analytics, "analytics_reporting"), (operational, "operational_reporting"),
])
@pytest.mark.parametrize("offset", [0, 100])
def test_reporting_publishes_checked_repository_definitions_without_source_item_ids(
    monkeypatch, tmp_path, module, bundle, offset,
):
    selected = manifest(offset=offset)
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [
        {"displayName": f"workspace-{offset}"},
        {"displayName": f"lakehouse_{offset}"},
    ]
    published = []

    def publish(client_arg, name, item_type, definition, *, approved_existing_id=None):
        assert client_arg is client
        published.append((name, item_type, definition, approved_existing_id))
        return ROLE_IDS["control"] if item_type == "SemanticModel" else ROLE_IDS["gold"]

    monkeypatch.setattr(analytics, "ensure_item", publish)
    output = tmp_path / "reporting.json"
    result = module.deploy(output, manifest=selected, client=client)
    assert json.loads(output.read_text()) == result
    assert result["workspace_id"] == selected.workspace_id
    assert result["model"]["id"] == ROLE_IDS["control"]
    assert result["report"]["id"] == ROLE_IDS["gold"]
    resolved = result["deployment"][bundle]
    assert resolved["semantic_model"]["target_item_id"] == ROLE_IDS["control"]
    assert resolved["report"]["target_item_id"] == ROLE_IDS["gold"]
    assert [call.args for call in client._json.call_args_list] == [
        ("GET", f"/workspaces/{selected.workspace_id}"),
        ("GET", f"/workspaces/{selected.workspace_id}/lakehouses/{selected.lakehouse.lakehouse_id}"),
    ]
    assert len(published) == 2
    expected = getattr(selected, bundle)
    assert published[0][:2] == (expected.semantic_model.target_name, "SemanticModel")
    assert published[1][:2] == (expected.report.target_name, "Report")
    model_text = "\n".join(
        base64.b64decode(part["payload"]).decode(errors="replace")
        for part in published[0][2]["parts"]
    )
    assert selected.workspace_id in model_text
    assert selected.lakehouse.lakehouse_id in model_text
    assert "{{ ." not in model_text
    report_part = next(
        part for part in published[1][2]["parts"] if part["path"] == "definition.pbir"
    )
    connection = json.loads(base64.b64decode(report_part["payload"]))[
        "datasetReference"
    ]["byConnection"]["connectionString"]
    assert f"myorg/workspace-{offset};" in connection
    assert f"semanticmodelid={ROLE_IDS['control']}" in connection
    assert f"initial catalog={expected.semantic_model.target_name};" in connection
    assert not client._lro_json.called


@pytest.mark.parametrize("module", [analytics, operational])
def test_reporting_rejects_a_client_for_another_workspace_before_any_reads(
    tmp_path, module,
):
    client = Mock(workspace_id=manifest(offset=100).workspace_id)
    with pytest.raises(analytics.ReportingDeploymentError, match="workspace"):
        module.deploy(tmp_path / "output.json", manifest=manifest(), client=client)
    client._json.assert_not_called()
    assert not (tmp_path / "output.json").exists()


@pytest.mark.parametrize("readback", [{}, {"displayName": None}, {"displayName": ""}])
def test_reporting_missing_live_names_stops_before_publication(monkeypatch, tmp_path, readback):
    selected = manifest()
    client = Mock(workspace_id=selected.workspace_id)
    client._json.return_value = readback
    publisher = Mock()
    monkeypatch.setattr(analytics, "ensure_item", publisher)
    with pytest.raises(analytics.ReportingDeploymentError, match="name"):
        analytics.deploy(tmp_path / "output.json", manifest=selected, client=client)
    publisher.assert_not_called()
    assert not (tmp_path / "output.json").exists()


def test_reporting_retains_explicit_existing_ownership_approvals(monkeypatch, tmp_path):
    selected = manifest()
    reporting = selected.analytics_reporting
    selected = dataclasses.replace(
        selected,
        analytics_reporting=dataclasses.replace(
            reporting,
            semantic_model=dataclasses.replace(reporting.semantic_model, target_item_id=ROLE_IDS["control"]),
            report=dataclasses.replace(reporting.report, target_item_id=ROLE_IDS["gold"]),
        ),
    )
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [{"displayName": "workspace"}, {"displayName": "lakehouse"}]
    publisher = Mock(side_effect=[ROLE_IDS["control"], ROLE_IDS["gold"]])
    monkeypatch.setattr(analytics, "ensure_item", publisher)
    analytics.deploy(tmp_path / "output.json", manifest=selected, client=client)
    assert [call.kwargs["approved_existing_id"] for call in publisher.call_args_list] == [
        ROLE_IDS["control"], ROLE_IDS["gold"],
    ]


def test_reporting_evidence_can_be_used_as_the_next_deployment_input(monkeypatch, tmp_path):
    from people_counter.fabric_deployment_manifest import load_deployment_manifest

    selected = manifest()
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [{"displayName": "workspace"}, {"displayName": "lakehouse"}]
    monkeypatch.setattr(analytics, "ensure_item", Mock(side_effect=[
        ROLE_IDS["control"], ROLE_IDS["gold"],
    ]))
    output = tmp_path / "reporting.json"
    analytics.deploy(output, manifest=selected, client=client)
    loaded = load_deployment_manifest(env={"PEOPLE_COUNTER_DEPLOY_CONFIG_PATH": str(output)})
    assert loaded.target() == selected.target()
    assert loaded.analytics_reporting.semantic_model.target_item_id == ROLE_IDS["control"]
    assert loaded.analytics_reporting.report.target_item_id == ROLE_IDS["gold"]


@pytest.mark.parametrize("returned_ids", [
    [None], [""], [str(UUID(int=0))],
    [ROLE_IDS["control"], None],
])
def test_invalid_returned_reporting_ids_never_become_deployment_evidence(
    monkeypatch, tmp_path, returned_ids,
):
    selected = manifest()
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [{"displayName": "workspace"}, {"displayName": "lakehouse"}]
    monkeypatch.setattr(analytics, "ensure_item", Mock(side_effect=returned_ids))
    output = tmp_path / "reporting.json"
    with pytest.raises(ValueError, match="UUID"):
        analytics.deploy(output, manifest=selected, client=client)
    assert not output.exists()

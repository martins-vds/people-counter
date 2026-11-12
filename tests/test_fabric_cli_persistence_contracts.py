import dataclasses
import json
from unittest.mock import Mock

import pytest

from scripts import deploy_sjd_analytics as analytics
from scripts import deploy_sjd_operations as operations
from tests.fabric_fixtures import ROLE_IDS
from tests.test_fabric_operations_portable import API, manifest


def test_operations_cli_writes_and_prints_stable_sorted_json(monkeypatch, tmp_path, capsys):
    selected = manifest()
    source = tmp_path / "input.json"
    source.write_text(json.dumps(selected.to_document()))
    output = tmp_path / "missing" / "nested" / "resolved.json"
    monkeypatch.setattr(operations, "_token", lambda: "test-token")
    client = API(selected)
    monkeypatch.setattr(operations, "FabricApi", lambda target, *, token: client)
    assert operations.main([
        "--config", str(source), "--output", str(output), "--provision-only",
    ]) == 0
    printed = capsys.readouterr().out
    result = json.loads(printed)
    assert printed == json.dumps(result, allow_nan=False, indent=2, sort_keys=True) + "\n"
    assert output.read_bytes() == (
        json.dumps(result["deployment"], allow_nan=False, indent=2, sort_keys=True) + "\n"
    ).encode()


@pytest.mark.parametrize("kind,message", [
    ("partial_hashes", "legacy release hashes must be supplied together"),
    ("model", "configuration requires --semantic-model-id or a resolved reporting target ID"),
])
def test_configuration_usage_errors_include_the_complete_next_action(
    monkeypatch, tmp_path, capsys, kind, message,
):
    source = tmp_path / "input.json"
    source.write_text(json.dumps(manifest().to_document()))
    monkeypatch.setattr(
        operations, "_token", Mock(side_effect=AssertionError("invalid input requested credentials")),
    )
    arguments = [
        "--config", str(source), "--output", str(tmp_path / "output.json"),
        "--release-manifest-sha256", "a" * 64,
    ]
    if kind == "model":
        arguments.extend(["--release-receipt-sha256", "b" * 64])
    with pytest.raises(SystemExit) as error:
        operations.main(arguments)
    assert error.value.code == 2
    assert capsys.readouterr().err.rstrip().split(": error: ")[-1] == message
    operations._token.assert_not_called()


@pytest.mark.parametrize("phase", ["provision", "configure"])
@pytest.mark.parametrize("mismatch", ["target", "namespace"])
def test_mixed_target_error_stops_before_compute_and_definition_reads(phase, mismatch):
    selected = manifest()
    client = API(selected if mismatch == "namespace" else manifest(offset=100))
    if mismatch == "namespace":
        client.config = dataclasses.replace(client.config, files_root="Files/other/")
    call = operations.provision_operations if phase == "provision" else operations.deploy_operations
    arguments = {"manifest": selected}
    if phase == "configure":
        arguments.update(
            release_manifest_sha256="a" * 64, release_receipt_sha256="b" * 64,
            semantic_model_id=ROLE_IDS["gold"], enable_schedules=False,
        )
    with pytest.raises(operations.OperationsDeploymentError) as error:
        call(client, **arguments)
    assert str(error.value) == (
        "API target differs from deployment target" if mismatch == "target"
        else "API target namespace differs from deployment configuration"
    )
    assert client.compute_verified is False
    assert client.calls == []


@pytest.mark.parametrize("kind,message", [
    ("model", "published semantic model ID must be a canonical UUID"),
    ("report", "published report ID must be a canonical UUID"),
    ("workspace_name", "Published workspace name is missing"),
    ("lakehouse_name", "Published Lakehouse name is missing"),
    ("workspace", "Reporting client workspace differs from deployment"),
])
def test_reporting_errors_distinguish_bad_remote_ids_names_and_client_target(
    monkeypatch, tmp_path, kind, message,
):
    selected = manifest()
    client = Mock(workspace_id=selected.workspace_id)
    names = [{"displayName": "workspace"}, {"displayName": "lakehouse"}]
    ids = [ROLE_IDS["control"], ROLE_IDS["gold"]]
    if kind == "model":
        ids[0] = None
    elif kind == "report":
        ids[1] = None
    elif kind == "workspace_name":
        names[0] = {}
    elif kind == "lakehouse_name":
        names[1] = {}
    else:
        client.workspace_id = "another"
    client._json.side_effect = names
    monkeypatch.setattr(analytics, "ensure_item", Mock(side_effect=ids))
    with pytest.raises((ValueError, analytics.ReportingDeploymentError)) as error:
        analytics.deploy(tmp_path / "evidence.json", manifest=selected, client=client)
    assert str(error.value) == message
    assert not (tmp_path / "evidence.json").exists()


@pytest.mark.parametrize("location", ["deployment", "summary"])
def test_cli_serialization_rejects_nonfinite_values_from_a_deployment_dependency(
    monkeypatch, tmp_path, location,
):
    selected = manifest()
    source = tmp_path / "input.json"
    source.write_text(json.dumps(selected.to_document()))
    result = {
        "status": "provisioned", "enabled": False, "items": ROLE_IDS,
        "deployment": selected.to_document(),
    }
    if location == "deployment":
        result["deployment"]["externalEvidence"] = {"duration": float("nan")}
    else:
        result["externalEvidence"] = {"duration": float("nan")}
    monkeypatch.setattr(operations, "_token", lambda: "test-token")
    monkeypatch.setattr(operations, "FabricApi", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(operations, "provision_operations", Mock(return_value=result))
    with pytest.raises(ValueError) as error:
        operations.main([
            "--config", str(source), "--output", str(tmp_path / "output.json"), "--provision-only",
        ])
    assert "Out of range float values are not JSON compliant" in str(error.value)


def test_invalid_cli_token_error_is_not_a_success_shaped_empty_credential(monkeypatch):
    monkeypatch.setattr(operations.subprocess, "check_output", Mock(return_value='{"accessToken": null}'))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations._token()
    assert str(error.value) == "Azure CLI returned an invalid or empty Fabric token"


def test_configuration_without_additional_models_uses_an_empty_sequence(monkeypatch, tmp_path, capsys):
    source = tmp_path / "input.json"
    selected = manifest()
    source.write_text(json.dumps(selected.to_document()))
    client = API(selected)
    monkeypatch.setattr(operations, "_token", lambda: "test-token")
    monkeypatch.setattr(operations, "FabricApi", lambda *args, **kwargs: client)
    assert operations.main([
        "--config", str(source), "--output", str(tmp_path / "output.json"),
        "--release-manifest-sha256", "a" * 64, "--release-receipt-sha256", "b" * 64,
        "--semantic-model-id", ROLE_IDS["gold"],
    ]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "configured"


def test_provision_only_cannot_be_combined_with_activation(monkeypatch, tmp_path, capsys):
    source = tmp_path / "input.json"
    source.write_text(json.dumps(manifest().to_document()))
    token = Mock(side_effect=AssertionError("invalid input requested credentials"))
    monkeypatch.setattr(operations, "_token", token)
    with pytest.raises(SystemExit) as error:
        operations.main([
            "--config", str(source), "--output", str(tmp_path / "output.json"),
            "--provision-only", "--enable-schedules",
        ])
    assert error.value.code == 2
    assert capsys.readouterr().err.rstrip().split(": error: ")[-1] == "--provision-only cannot enable schedules"
    token.assert_not_called()


def test_provisioning_validates_the_actual_returned_id_and_duplicate_role_mapping():
    selected = manifest()
    client = API(selected)
    client.upsert_sjd = Mock(return_value=None)
    with pytest.raises(ValueError) as error:
        operations.provision_operations(client, manifest=selected)
    assert str(error.value) == "control SJD ID must be a canonical UUID"
    client.upsert_sjd = Mock(return_value=ROLE_IDS["control"])
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.provision_operations(client, manifest=selected)
    assert str(error.value) == "Fabric returned the same SJD ID for multiple roles"


def test_operational_model_is_included_without_an_explicit_additional_model_flag():
    import shlex
    from tests.test_fabric_reconcilers import _NOW

    selected = manifest()
    selected = dataclasses.replace(
        selected,
        operational_reporting=dataclasses.replace(
            selected.operational_reporting,
            semantic_model=dataclasses.replace(
                selected.operational_reporting.semantic_model, target_item_id=ROLE_IDS["process"],
            ),
        ),
    )
    client = API(selected)
    report = operations.deploy_operations(
        client, manifest=selected, release_manifest_sha256="a" * 64,
        release_receipt_sha256="b" * 64, semantic_model_id=ROLE_IDS["gold"],
        enable_schedules=False, now=_NOW,
    )
    args = shlex.split(report["schedules"]["refresh"]["executionData"]["commandLineArguments"])
    assert [args[index + 1] for index, value in enumerate(args) if value == "--semantic-model-id"] == [
        ROLE_IDS["gold"], ROLE_IDS["process"],
    ]
    assert all(value["configuration"]["startDateTime"] == "2026-01-01T00:05:00Z"
               for value in report["schedules"].values())


def test_disabled_schedule_readback_failure_names_the_role_before_activation():
    selected = manifest()
    client = API(selected)
    client.reconcile_schedule = Mock(side_effect=lambda item_id, *, payload: {**payload, "enabled": True})
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.deploy_operations(
            client, manifest=selected, release_manifest_sha256="a" * 64,
            release_receipt_sha256="b" * 64, semantic_model_id=ROLE_IDS["gold"],
            enable_schedules=True,
        )
    assert str(error.value) == "dispatcher schedule readback differs"

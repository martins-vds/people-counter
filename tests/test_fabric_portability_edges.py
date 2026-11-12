from __future__ import annotations

import ast
import base64
import builtins
import copy
import dataclasses
import json
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call
from uuid import UUID

import pytest

from people_counter import fabric_deployment_manifest as deployment
from people_counter import fabric_sjd_definition as definitions
from people_counter import fabric_sjd_jobs as jobs
from people_counter.fabric_namespace import (
    FabricNamespaceConfig,
    FabricNamespaceMode,
    build_sjd_v2_definition,
    validate_environment_library_policy,
)
from people_counter.fabric_sjd import FabricSjdConfig
from people_counter.fabric_target import FabricTarget, validate_artifact_id
from scripts import deploy_sjd_analytics as analytics
from scripts import deploy_sjd_operations as operations
from scripts import deploy_sjd_operational_report as operational
from tests.fabric_fixtures import ROLE_IDS, RUNTIME_CONTEXT, TARGET_IDS, TEST_TARGET
from tests.test_fabric_operations_portable import API, manifest


pytestmark = pytest.mark.usefixtures("fabric_runtime_context")


@pytest.mark.parametrize("role", list(definitions.ENTRY_POINTS))
def test_generated_main_executes_only_the_requested_installed_entrypoint(monkeypatch, role):
    module_name, function = definitions.ENTRY_POINTS[role].split(":")
    module = ModuleType(module_name)
    run = Mock(return_value=37)
    setattr(module, function, run)
    monkeypatch.setitem(sys.modules, module_name, module)
    value = definitions.build_sjd_definition(
        role, config=TEST_TARGET, command_line_arguments="--files-root Files/chosen/",
    )
    assert set(value) == {"definition"}
    assert value["definition"]["format"] == "SparkJobDefinitionV2"
    parts = value["definition"]["parts"]
    assert [part["path"] for part in parts] == ["Main/main.py", "SparkJobDefinitionV1.json"]
    assert all(part["payloadType"] == "InlineBase64" for part in parts)
    metadata = json.loads(base64.b64decode(parts[1]["payload"]))
    assert metadata == {
        "additionalLakehouseIds": [], "additionalLibraryUris": [],
        "commandLineArguments": "--files-root Files/chosen/",
        "defaultLakehouseArtifactId": TEST_TARGET.lakehouse_id,
        "environmentArtifactId": TEST_TARGET.environment_id,
        "executableFile": "main.py", "language": "Python", "mainClass": "",
        "retryPolicy": None,
    }
    source = base64.b64decode(parts[0]["payload"]).decode()
    ast.parse(source)
    with pytest.raises(SystemExit) as error:
        exec(compile(source, "generated-main", "exec"), {"__name__": "__main__"})
    assert error.value.code == 37
    run.assert_called_once_with()
    run.reset_mock()
    exec(compile(source, "imported-main", "exec"), {"__name__": "not_main"})
    run.assert_not_called()


def test_generated_sjd_rejects_embedded_nul_before_publication():
    with pytest.raises(ValueError) as error:
        definitions.build_sjd_definition(
            "control", config=TEST_TARGET, command_line_arguments="a\x00b",
        )
    assert str(error.value) == "command_line_arguments contains NUL"


@pytest.mark.parametrize("mode", list(FabricNamespaceMode))
def test_candidate_namespace_targets_and_one_lake_paths_are_not_reference_bound(mode):
    config = FabricNamespaceConfig.for_mode(mode, target=TEST_TARGET)
    path = config.file_path("attempts/batch=one")
    assert config.abfss_path(path) == (
        f"abfss://{TEST_TARGET.workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{TEST_TARGET.lakehouse_id}/{path}"
    )
    with pytest.raises(ValueError, match="fixed Fabric SJD root"):
        config.abfss_path("Files/outside/work")


def test_candidate_definition_and_environment_policy_use_the_selected_artifacts():
    target = FabricTarget(
        workspace_id=str(UUID(int=100)), lakehouse_id=str(UUID(int=101)),
        environment_id=str(UUID(int=102)),
    )
    definition = build_sjd_v2_definition("control", config=target)
    metadata = json.loads(base64.b64decode(definition["definition"]["parts"][1]["payload"]))
    assert metadata["defaultLakehouseArtifactId"] == target.lakehouse_id
    assert metadata["environmentArtifactId"] == target.environment_id
    policy = {
        "environmentArtifactId": target.environment_id, "runtimeVersion": "2.0",
        "libraryMode": "Full", "published": True, "additionalLibraryUris": [],
    }
    validate_environment_library_policy(policy, config=target)
    with pytest.raises(ValueError, match="configured Fabric Environment"):
        validate_environment_library_policy(
            {**policy, "environmentArtifactId": TEST_TARGET.environment_id}, config=target,
        )


def test_all_manifest_fields_round_trip_without_losing_assigned_ids():
    document = manifest(ids=True).to_document()
    document["release"].update(manifest_sha256="a" * 64, receipt_sha256="b" * 64)
    for index, key in enumerate(
        ("eventhouse_id", "kql_database_id", "queryset_id", "dashboard_id"), start=200,
    ):
        document["real_time_intelligence"][key] = str(UUID(int=index))
    document["activator"] = {"reflex_id": str(UUID(int=210)), "trigger_name": "chosen_trigger"}
    for index, bundle in enumerate(("analytics_reporting", "operational_reporting"), start=220):
        for offset, artifact in enumerate(("semantic_model", "report")):
            row = document[bundle][artifact]
            row["source_item_id"] = str(UUID(int=index * 10 + offset))
            row["target_item_id"] = str(UUID(int=index * 10 + offset + 100))
    parsed = deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert parsed.to_document() == document
    assert dataclasses.asdict(parsed) == {
        key: (parsed.stage if key == "stage" else value)
        for key, value in document.items() if key != "schema"
    }


@pytest.mark.parametrize("field", [
    "schema", "stage", "workspace_id", "fabric_environment_id", "fabric_runtime",
    "lakehouse", "roles", "analytics_reporting", "operational_reporting",
    "real_time_intelligence", "activator", "release",
])
def test_missing_required_manifest_sections_do_not_produce_a_ready_target(field):
    document = manifest(ids=True).to_document()
    del document[field]
    if field in {"workspace_id", "fabric_environment_id"}:
        parsed = deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
        with pytest.raises(ValueError):
            parsed.target()
    else:
        with pytest.raises(ValueError):
            deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)


@pytest.mark.parametrize("field", ["workspace_id", "lakehouse_id", "environment_id"])
@pytest.mark.parametrize("value", [None, "", "wrong", str(UUID(int=0)), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"])
def test_invalid_artifact_diagnostics_identify_the_actual_field(field, value):
    with pytest.raises(ValueError) as error:
        validate_artifact_id(value, field)
    assert str(error.value) == f"{field} must be a canonical UUID"


def test_current_target_rethrows_missing_dependency_inside_notebookutils(monkeypatch):
    original = builtins.__import__

    def importing(name, *args, **kwargs):
        if name == "notebookutils":
            raise ModuleNotFoundError("missing Fabric helper", name="fabric_helper")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", importing)
    with pytest.raises(ModuleNotFoundError) as error:
        FabricTarget.current()
    assert error.value.name == "fabric_helper"


@pytest.mark.parametrize("notebookutils", [None, SimpleNamespace(), SimpleNamespace(runtime=None)])
def test_current_target_never_swallows_missing_runtime_context(monkeypatch, notebookutils):
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    expected = RuntimeError if notebookutils is None else ValueError
    with pytest.raises(expected):
        FabricTarget.current()


def test_attached_runtime_can_select_files_root_without_repeating_target_ids(monkeypatch):
    monkeypatch.setitem(
        sys.modules, "notebookutils",
        SimpleNamespace(runtime=SimpleNamespace(context=RUNTIME_CONTEXT)),
    )
    config, remaining = jobs._runtime_binding(["--files-root", "Files/chosen/", "bootstrap"])
    assert config == FabricSjdConfig(**TARGET_IDS, files_root="Files/chosen/")
    assert remaining == ["bootstrap"]


def resolved():
    value = manifest(ids=True)
    return dataclasses.replace(
        value,
        release=dataclasses.replace(value.release, manifest_sha256="a" * 64, receipt_sha256="b" * 64),
        analytics_reporting=dataclasses.replace(
            value.analytics_reporting,
            semantic_model=dataclasses.replace(value.analytics_reporting.semantic_model, target_item_id=str(UUID(int=80))),
        ),
        operational_reporting=dataclasses.replace(
            value.operational_reporting,
            semantic_model=dataclasses.replace(value.operational_reporting.semantic_model, target_item_id=str(UUID(int=81))),
        ),
    )


@pytest.mark.parametrize("role,tail", [
    ("control", ["recover"]), ("reconciliation", ["reconcile"]),
    ("gold", ["run", "--lookback-hours", "48"]),
    ("dispatcher", [
        "--max-items", "64", "--maximum-active-batches", "1", "--lease-seconds", "14400",
        "--minimum-speed-x", "1", "--safety-factor", "1.25", "--margin-seconds", "60",
    ]),
    ("refresh", [
        "--workspace-id", TEST_TARGET.workspace_id, "--semantic-model-id", str(UUID(int=80)),
        "--semantic-model-id", str(UUID(int=81)),
    ]),
])
def test_schedule_arguments_are_executable_role_contracts_not_just_target_prefixes(role, tail):
    value = resolved()
    assert shlex.split(value.schedule_arguments(role, enabled=True)) == [
        *TEST_TARGET.command_line_arguments(), *tail,
    ]


@pytest.mark.parametrize("field", ["manifest_sha256", "receipt_sha256"])
def test_dispatcher_schedule_does_not_depend_on_legacy_release_hashes(field):
    value = resolved()
    value = dataclasses.replace(value, release=dataclasses.replace(value.release, **{field: None}))
    arguments = shlex.split(value.schedule_arguments("dispatcher", enabled=True))
    assert "--max-items" in arguments
    assert "--release-manifest-sha256" not in arguments
    assert "--release-receipt-sha256" not in arguments


def test_process_is_not_a_scheduled_role_even_with_complete_admission():
    with pytest.raises(ValueError) as error:
        resolved().schedule_arguments("process", enabled=True)
    assert str(error.value) == "process is not a scheduled role"


def test_compute_property_readback_is_order_independent():
    settings = definitions.reviewed_compute_settings({})
    settings["sparkProperties"].reverse()
    definitions.validate_reviewed_compute_settings(settings)


@pytest.mark.parametrize("connection", [
    None, "", "initial catalog=model;semanticmodelid=selected;semanticmodelid=other",
    "invalid;initial catalog=model;semanticmodelid=selected",
    "initial catalog=model;semanticmodelid=selected;invalid",
    "initial catalog=other;semanticmodelid=selected",
    "initial catalog=model;semanticmodelid=selected-extra",
])
def test_report_binding_rejects_ambiguous_malformed_and_prefix_only_matches(connection):
    with pytest.raises(analytics.ReportingDeploymentError) as error:
        analytics.validate_report_binding(connection, target_model_id="selected", target_model_name="model")
    assert str(error.value) == "report target model binding is invalid"


def test_report_binding_allows_whitespace_empty_parameters_and_equals_in_values():
    analytics.validate_report_binding(
        "; Data Source = powerbi://example/a=b; ; initial catalog = model ; semanticmodelid = selected ;",
        target_model_id="selected", target_model_name="model",
    )


@pytest.mark.parametrize("module", [analytics, operational])
def test_reporting_default_client_uses_only_the_configured_workspace(monkeypatch, tmp_path, module):
    selected = resolved()
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [{"displayName": "chosen-workspace"}, {"displayName": "chosen-lakehouse"}]
    token = object()
    token_provider = Mock(return_value=token)
    factory = Mock(return_value=client)
    monkeypatch.setattr(analytics, "make_token_provider", token_provider)
    monkeypatch.setattr(analytics, "FabricRESTController", factory)
    monkeypatch.setattr(analytics, "ensure_item", Mock(side_effect=[ROLE_IDS["control"], ROLE_IDS["gold"]]))
    result = module.deploy(tmp_path / "output.json", manifest=selected)
    token_provider.assert_called_once_with("azure-cli")
    factory.assert_called_once_with(token, workspace_id=selected.workspace_id)
    assert set(result) == {
        "deployed_at", "bundle", "model", "report", "schema", "workspace_id",
        "lakehouse_id", "deployment",
    }
    assert result["schema"] == "people-counter-sjd-reporting-deployment-v2"
    assert set(result["model"]) == {"id", "name", "definition_sha256"}
    assert set(result["report"]) == {"id", "name", "definition_sha256"}
    for artifact in ("model", "report"):
        assert len(result[artifact]["definition_sha256"]) == 64
    assert datetime.fromisoformat(result["deployed_at"]).utcoffset().total_seconds() == 0


@pytest.mark.parametrize("module", [analytics, operational])
def test_reporting_main_passes_selected_config_and_output(monkeypatch, tmp_path, capsys, module):
    selected = resolved()
    output = tmp_path / "evidence.json"
    arguments = ["--config", "chosen.json", "--output", str(output)]
    parser = Mock(return_value=(output, selected))
    publish = Mock(return_value={"status": "verified"})
    monkeypatch.setattr(module, "reporting_arguments", parser)
    monkeypatch.setattr(module, "deploy", publish)
    assert module.main(arguments) == 0
    parser.assert_called_once_with(arguments)
    publish.assert_called_once_with(output, manifest=selected)
    assert json.loads(capsys.readouterr().out) == {"status": "verified"}


@pytest.mark.parametrize("stage", list(deployment.DeploymentEnvironment))
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("override_model", [False, True])
def test_operations_cli_configures_with_real_arguments_and_returns_resolved_state(
    monkeypatch, tmp_path, capsys, stage, enabled, override_model,
):
    value = resolved()
    value = dataclasses.replace(value, stage=stage)
    path = tmp_path / "input.json"
    path.write_text(json.dumps(value.to_document()))
    output = tmp_path / "nested" / "resolved.json"
    api = API(value)
    factory = Mock(return_value=api)
    monkeypatch.setattr(operations, "FabricApi", factory)
    monkeypatch.setattr(operations, "_token", lambda: "test-token")
    monkeypatch.setenv("PEOPLE_COUNTER_DEPLOY_CONFIG_JSON", '{"workspace_id":"wrong"}')
    arguments = [
        "--stage", stage.value, "--config", str(path), "--output", str(output),
        "--release-manifest-sha256", "a" * 64, "--release-receipt-sha256", "b" * 64,
        "--additional-semantic-model-id", str(UUID(int=81)),
        "--additional-semantic-model-id", str(UUID(int=82)),
        "--intake-pipeline-id", str(UUID(int=90)),
        "--registration-notebook-id", str(UUID(int=91)),
    ]
    if enabled:
        arguments.append("--enable-schedules")
    if override_model:
        arguments.extend(["--semantic-model-id", str(UUID(int=83))])
    assert operations.main(arguments) == 0
    factory.assert_called_once_with(value.runtime_config(), token="test-token")
    result = json.loads(capsys.readouterr().out)
    assert set(result) == {
        "status", "enabled", "items", "deployment", "schedules", "intake_pipeline_id",
        "registration_notebook_id", "release_manifest_sha256",
    }
    assert result["status"] == "configured"
    assert result["enabled"] is enabled
    assert result["items"] == ROLE_IDS
    assert result["intake_pipeline_id"] == str(UUID(int=90))
    assert result["registration_notebook_id"] == str(UUID(int=91))
    assert result["release_manifest_sha256"] == "a" * 64
    assert json.loads(output.read_text()) == result["deployment"]
    assert result["deployment"]["stage"] == stage.value
    expected_model = str(UUID(int=83 if override_model else 80))
    assert result["deployment"]["analytics_reporting"]["semantic_model"]["target_item_id"] == expected_model
    expected_command = value.runtime_config().command_line_arguments() + [
        "--workspace-id", value.workspace_id, "--semantic-model-id", expected_model,
        "--semantic-model-id", str(UUID(int=81)), "--semantic-model-id", str(UUID(int=82)),
    ]
    assert shlex.split(result["schedules"]["refresh"]["executionData"]["commandLineArguments"]) == expected_command
    assert all(schedule["enabled"] is enabled for schedule in result["schedules"].values())


@pytest.mark.parametrize("arguments,expected_message", [
    ([], "the following arguments are required: --output"),
    (["--stage", "unknown", "--output", "ignored"], "invalid choice"),
    (["--provision-only", "--enable-schedules", "--output", "ignored"],
     "--provision-only cannot enable schedules"),
    (["--release-manifest-sha256", "a" * 64, "--output", "ignored"],
     "legacy release hashes must be supplied together"),
    (["--release-receipt-sha256", "b" * 64, "--output", "ignored"],
     "legacy release hashes must be supplied together"),
])
def test_operations_cli_usage_errors_are_explicit_and_never_request_credentials(
    monkeypatch, tmp_path, capsys, arguments, expected_message,
):
    value = resolved()
    path = tmp_path / "input.json"
    path.write_text(json.dumps(value.to_document()))
    monkeypatch.setenv("PEOPLE_COUNTER_DEPLOY_CONFIG_PATH", str(path))
    token = Mock(side_effect=AssertionError("invalid input requested credentials"))
    monkeypatch.setattr(operations, "_token", token)
    with pytest.raises(SystemExit) as error:
        operations.main(arguments)
    assert error.value.code == 2
    assert expected_message in capsys.readouterr().err
    token.assert_not_called()


@pytest.mark.parametrize("field,value,message", [
    ("release_manifest_sha256", None, "legacy release hashes must be supplied together"),
    ("release_manifest_sha256", "z" * 64, "release manifest hash must be a SHA-256 digest"),
    ("release_receipt_sha256", None, "legacy release hashes must be supplied together"),
    ("semantic_model_id", "", "semantic model ID must be a canonical UUID"),
    ("additional_semantic_model_ids", [""], "additional semantic model ID must be a canonical UUID"),
    ("enable_schedules", 1, "enable_schedules must be a boolean"),
    ("intake_pipeline_id", "", "intake pipeline ID must be a canonical UUID"),
    ("registration_notebook_id", "", "registration notebook ID must be a canonical UUID"),
    ("intake_pipeline_id", None, "intake pipeline and registration notebook IDs must be supplied together"),
    ("registration_notebook_id", None, "intake pipeline and registration notebook IDs must be supplied together"),
])
def test_configuration_errors_identify_the_invalid_deployment_input(field, value, message):
    values = {
        "release_manifest_sha256": "a" * 64, "release_receipt_sha256": "b" * 64,
        "semantic_model_id": str(UUID(int=80)), "additional_semantic_model_ids": [],
        "enable_schedules": False, "intake_pipeline_id": str(UUID(int=90)),
        "registration_notebook_id": str(UUID(int=91)), field: value,
    }
    with pytest.raises((ValueError, operations.OperationsDeploymentError)) as error:
        operations._validate_configuration(**values)
    assert str(error.value) == message


@pytest.mark.parametrize("custom", [False, True])
def test_asset_preparation_cli_uses_only_local_output_and_prints_its_digest(
    monkeypatch, tmp_path, capsys, custom,
):
    from scripts import prepare_manual_fabric_assets as prepare

    output = tmp_path / "assets.json"
    monkeypatch.setattr(sys, "argv", ["prepare", *(["--output", str(output)] if custom else [])])
    build = Mock(return_value="a" * 64)
    monkeypatch.setattr(prepare, "prepare_assets", build)
    prepare.main()
    root, destination = build.call_args.args
    assert root == Path(prepare.__file__).resolve().parents[1]
    assert destination == (output if custom else Path("outputs/manual-fabric/manual-publication.json"))
    text = capsys.readouterr().out
    assert "SHA256 " + "a" * 64 in text
    assert "makes no cloud requests" in text


@pytest.mark.parametrize("stage", list(deployment.DeploymentEnvironment))
@pytest.mark.parametrize("explicit_config", [False, True])
def test_reporting_argument_parser_carries_the_exact_selected_stage_and_config(
    monkeypatch, tmp_path, stage, explicit_config,
):
    value = dataclasses.replace(resolved(), stage=stage)
    path = tmp_path / "input.json"
    path.write_text(json.dumps(value.to_document()))
    output = tmp_path / "reporting.json"
    arguments = ["--stage", stage.value, "--output", str(output)]
    if explicit_config:
        arguments.extend(["--config", str(path)])
        monkeypatch.setenv("PEOPLE_COUNTER_DEPLOY_CONFIG_JSON", '{"workspace_id":"wrong"}')
    else:
        monkeypatch.setenv("PEOPLE_COUNTER_DEPLOY_CONFIG_PATH", str(path))
    selected_output, selected = analytics.reporting_arguments(arguments)
    assert selected_output == output
    assert selected.to_document() == value.to_document()


@pytest.mark.parametrize("arguments", [
    [], ["--stage", "unknown", "--output", "ignored"],
])
def test_reporting_argument_parser_rejects_missing_output_and_unknown_stage(arguments):
    with pytest.raises(SystemExit) as error:
        analytics.reporting_arguments(arguments)
    assert error.value.code == 2


def test_stable_target_validates_all_known_table_names_and_rejects_foreign_tables():
    from people_counter.fabric_sjd import TABLE_SUFFIXES

    config = FabricSjdConfig(**TARGET_IDS)
    for suffix in TABLE_SUFFIXES:
        assert config.validate_table_name(config.table(suffix)) == "people_counter_sjd_" + suffix
    with pytest.raises(ValueError) as error:
        config.validate_table_name("another_work")
    assert str(error.value) == "table is outside the stable production namespace: 'another_work'"


def set_field(document, field, value):
    keys = field.split(".")
    target = document
    for key in keys[:-1]:
        target = target[key]
    target[keys[-1]] = value


@pytest.mark.parametrize("field", [
    "workspace_id", "fabric_environment_id", "lakehouse.lakehouse_id",
    "roles.control.item_id",
    "analytics_reporting.semantic_model.source_item_id",
    "analytics_reporting.semantic_model.target_item_id",
    "analytics_reporting.report.source_item_id",
    "analytics_reporting.report.target_item_id",
    "operational_reporting.semantic_model.source_item_id",
    "operational_reporting.semantic_model.target_item_id",
    "operational_reporting.report.source_item_id",
    "operational_reporting.report.target_item_id",
    "real_time_intelligence.eventhouse_id", "real_time_intelligence.kql_database_id",
    "real_time_intelligence.queryset_id", "real_time_intelligence.dashboard_id",
    "activator.reflex_id",
])
def test_manifest_invalid_ids_are_rejected_with_the_exact_binding_location(field):
    document = resolved().to_document()
    set_field(document, field, "not-a-uuid")
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == field + " must be a canonical UUID"


@pytest.mark.parametrize("field", [
    "schema", "stage", "fabric_runtime", "lakehouse.table_prefix", "lakehouse.files_root",
    "roles.control.display_name", "release.identity_schema",
    "analytics_reporting.semantic_model.source_name",
    "analytics_reporting.semantic_model.target_name",
    "analytics_reporting.report.source_name", "analytics_reporting.report.target_name",
    "operational_reporting.semantic_model.source_name",
    "operational_reporting.semantic_model.target_name",
    "operational_reporting.report.source_name", "operational_reporting.report.target_name",
    "activator.trigger_name",
])
def test_manifest_empty_names_and_contract_fields_identify_the_bad_field(field):
    document = resolved().to_document()
    set_field(document, field, "")
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == field + " must be non-empty"


@pytest.mark.parametrize("field", [
    "lakehouse", "roles", "roles.control", "roles.control.schedule", "release",
    "analytics_reporting", "analytics_reporting.semantic_model", "analytics_reporting.report",
    "operational_reporting", "operational_reporting.semantic_model", "operational_reporting.report",
    "real_time_intelligence", "activator",
])
def test_manifest_object_shape_errors_keep_the_exact_field_location(field):
    document = resolved().to_document()
    set_field(document, field, [])
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == field + " must be an object"


@pytest.mark.parametrize("field", ["release.require_identity", "roles.control.schedule.enabled"])
def test_manifest_boolean_fields_do_not_accept_integer_or_string_substitutes(field):
    document = resolved().to_document()
    set_field(document, field, "false")
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == field + " must be a boolean"


@pytest.mark.parametrize("interval", [None, 0, True, 1.0, "5", 10081])
def test_manifest_schedule_interval_type_and_boundary_errors_are_explicit(interval):
    document = resolved().to_document()
    set_field(document, "roles.control.schedule.interval_minutes", interval)
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    suffix = "must be an integer" if type(interval) is not int else "must be between 1 and 10080"
    assert str(error.value) == "roles.control.schedule.interval_minutes " + suffix


@pytest.mark.parametrize("interval", [1, 10080])
def test_operations_schedule_interval_boundaries_are_inclusive(interval):
    value = operations.schedule_payload(interval_minutes=interval, command_line_arguments="", enabled=False)
    assert value["configuration"]["interval"] == interval
    for invalid in (0, 10081, True, None, "5"):
        with pytest.raises(ValueError) as error:
            operations.schedule_payload(interval_minutes=invalid, command_line_arguments="", enabled=False)
        assert str(error.value) == "interval_minutes must be between 1 and 10080"


@pytest.mark.parametrize("field", [
    "workspace_id", "lakehouse_id", "environment_id",
])
def test_target_constructor_itself_preserves_the_invalid_field_diagnostic(field):
    with pytest.raises(ValueError) as error:
        FabricTarget(**{**TARGET_IDS, field: ""})
    assert str(error.value) == field + " must be a canonical UUID"


@pytest.mark.parametrize("field,label", [
    ("defaultLakehouseWorkspaceId", "workspace_id"),
    ("defaultLakehouseId", "lakehouse_id"),
    ("environmentId", "environment_id"),
])
def test_runtime_context_bad_ids_name_the_exact_runtime_binding(field, label):
    with pytest.raises(ValueError) as error:
        FabricTarget.from_runtime_context({**RUNTIME_CONTEXT, field: "not-an-id"})
    assert str(error.value) == f"runtime {label} must be a canonical UUID"

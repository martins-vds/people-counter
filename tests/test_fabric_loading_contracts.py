import copy
import dataclasses
import json
from pathlib import Path

import pytest

from people_counter import fabric_deployment_manifest as deployment
from people_counter import fabric_reconcilers as reconcile
from people_counter.fabric_benchmark import FabricBenchmarkConfig
from people_counter.fabric_namespace import FabricNamespaceConfig, FabricNamespaceMode
from people_counter.fabric_sjd import FabricSjdConfig
from tests.fabric_fixtures import TEST_TARGET
from tests.test_fabric_operations_portable import manifest
from tests.test_fabric_reconcilers import _resolved_manifest, _uid


@pytest.mark.parametrize("value", [[], "text", 7, None])
def test_nonobject_json_configuration_identifies_the_actual_file(tmp_path, value):
    path = tmp_path / "input.json"
    path.write_text(json.dumps(value))
    with pytest.raises(deployment.ManifestLoadError) as error:
        deployment._load_json_file(path, "selected configuration")
    assert str(error.value) == f"selected configuration at {path} must be a JSON object"


def test_missing_and_invalid_json_configurations_have_context_and_a_cause(tmp_path):
    path = tmp_path / "input.json"
    with pytest.raises(deployment.ManifestLoadError) as error:
        deployment._load_json_file(path, "selected configuration")
    assert str(error.value) == f"selected configuration not found at {path}"
    assert isinstance(error.value.__cause__, OSError)
    path.write_text("{bad")
    with pytest.raises(deployment.ManifestLoadError) as error:
        deployment._load_json_file(path, "selected configuration")
    assert isinstance(error.value.__cause__, json.JSONDecodeError)
    assert str(error.value) == f"selected configuration at {path} is not valid JSON: {error.value.__cause__}"


@pytest.mark.parametrize("kind,message", [
    ("missing", "reporting evidence omitted its deployment manifest"),
    ("workspace", "reporting evidence contradicts its deployment target"),
    ("lakehouse", "reporting evidence contradicts its deployment target"),
    ("lakehouse_shape", "reporting evidence contradicts its deployment target"),
])
def test_reporting_evidence_wrapper_is_validated_before_its_manifest_is_used(tmp_path, kind, message):
    inner = manifest().to_document()
    value = {
        "schema": "people-counter-sjd-reporting-deployment-v2",
        "workspace_id": inner["workspace_id"], "lakehouse_id": inner["lakehouse"]["lakehouse_id"],
        "deployment": inner,
    }
    if kind == "missing":
        del value["deployment"]
    elif kind == "workspace":
        value["workspace_id"] = "different"
    elif kind == "lakehouse":
        value["lakehouse_id"] = "different"
    else:
        inner["lakehouse"] = []
    path = tmp_path / "reporting.json"
    path.write_text(json.dumps(value))
    with pytest.raises(deployment.ManifestLoadError) as error:
        deployment._load_json_file(path, "chosen reporting")
    assert str(error.value) == "chosen reporting " + message


@pytest.mark.parametrize("value", ["not-json", "[]", "null", "7"])
def test_inline_configuration_error_is_explicit_and_never_uses_bundled_ids(value):
    with pytest.raises(deployment.ManifestLoadError) as error:
        deployment.load_manifest_document(deployment.DeploymentEnvironment.PROD, env={
            "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON": value,
        })
    if value == "not-json":
        assert isinstance(error.value.__cause__, json.JSONDecodeError)
        assert str(error.value) == f"PEOPLE_COUNTER_DEPLOY_CONFIG_JSON is not valid JSON: {error.value.__cause__}"
    else:
        assert str(error.value) == "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON must be a JSON object"


@pytest.mark.parametrize("document,location", [
    ({"client_secret": "never-store"}, "client_secret"),
    ({"outer": {"credential": "never-store"}}, "outer.credential"),
    ({"outer": {"nested": {"password": "never-store"}}}, "outer.nested.password"),
])
def test_secret_field_diagnostics_include_the_full_nested_location(document, location):
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment._reject_secret_like_keys(document)
    assert str(error.value) == f"deployment manifest field {location!r} looks like a secret"
    assert "never-store" not in str(error.value)


@pytest.mark.parametrize("name", ["workspace_id", "fabric_environment_id", "lakehouse_id"])
def test_deployment_target_errors_name_the_missing_foundation_binding(name):
    value = manifest()
    if name == "lakehouse_id":
        value = dataclasses.replace(value, lakehouse=dataclasses.replace(value.lakehouse, lakehouse_id=None))
        label = "lakehouse.lakehouse_id"
    else:
        value = dataclasses.replace(value, **{name: None})
        label = name
    with pytest.raises(ValueError) as error:
        value.target()
    assert str(error.value) == f"{label} must be non-empty"


@pytest.mark.parametrize("field,output", [
    ("workspace_id", "workspace_id"), ("fabric_environment_id", "environment_id"),
    ("lakehouse_id", "lakehouse_id"),
])
def test_foundation_id_conflicts_are_reported_using_the_output_name(field, output):
    document = _resolved_manifest().to_document()
    before = copy.deepcopy(document)
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output(document, {output: {"value": _uid("another")}})
    assert str(error.value) == f"{output} conflicts with the configured deployment"
    assert document == before


def test_invalid_foundation_and_role_output_shape_errors_are_explicit():
    document = _resolved_manifest().to_document()
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output({**document, "lakehouse": []}, {})
    assert str(error.value) == "deployment lakehouse must be an object"
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output(document, {
            "spark_job_definition_ids": {"value": "invalid"},
        })
    assert str(error.value) == "terraform output spark_job_definition_ids must be an object"
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output(document, {"workspace_id": {"value": "invalid"}})
    assert str(error.value) == "workspace_id must be a canonical UUID"


@pytest.mark.parametrize("suffix", [
    "a/../outside", "a\\outside", "a://outside",
])
def test_benchmark_absolute_files_guard_rejects_all_unsafe_syntax(suffix):
    config = FabricBenchmarkConfig(
        workspace_id=TEST_TARGET.workspace_id,
        lakehouse_id=TEST_TARGET.lakehouse_id,
        environment_id=TEST_TARGET.environment_id,
    )
    path = config.files_root + suffix
    with pytest.raises(ValueError) as error:
        config.abfss_path(path)
    assert str(error.value) == f"path must remain under the fixed benchmark root: {path!r}"


def test_benchmark_table_error_identifies_the_invalid_suffix():
    config = FabricBenchmarkConfig(
        workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
        environment_id=TEST_TARGET.environment_id,
    )
    with pytest.raises(ValueError) as error:
        config.table("bad/name")
    assert str(error.value) == "invalid benchmark table suffix 'bad/name'"


@pytest.mark.parametrize("factory", [
    lambda: FabricSjdConfig(
        workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
        environment_id=TEST_TARGET.environment_id,
    ),
    lambda: FabricNamespaceConfig.for_mode(FabricNamespaceMode.CANARY, target=TEST_TARGET),
    lambda: manifest().lakehouse,
])
@pytest.mark.parametrize("path", [None, "", "/escape", "x/../y", "x\\y", "x//y"])
def test_file_path_guard_errors_include_the_rejected_input(factory, path):
    config = factory()
    try:
        config.file_path(path)
    except ValueError as error:
        assert repr(path) in str(error)
    except AttributeError:
        assert path is None
    else:
        pytest.fail("unsafe file path was accepted")


@pytest.mark.parametrize("kind,message", [
    ("schema", "unsupported deployment manifest schema"),
    ("stage", "manifest stage does not match requested stage"),
    ("prefix", "lakehouse.table_prefix has an unsafe value"),
    ("root", "lakehouse.files_root must end with '/'"),
    ("runtime", "deployment must retain the reviewed Fabric runtime and table prefix"),
    ("duplicate", "deployment roles contain duplicate names or item IDs"),
    ("hash_pair", "release manifest and receipt hashes must be supplied together"),
])
def test_manifest_contract_errors_are_exact_and_do_not_describe_another_failure(kind, message):
    document = manifest().to_document()
    if kind == "schema":
        document["schema"] = "unknown"
    elif kind == "stage":
        document["stage"] = "dev"
    elif kind == "prefix":
        document["lakehouse"]["table_prefix"] = "bad/prefix"
    elif kind == "root":
        document["lakehouse"]["files_root"] = "Files/no-trailing-slash"
    elif kind == "runtime":
        document["fabric_runtime"] = "1.3"
    elif kind == "duplicate":
        document["roles"]["process"]["display_name"] = document["roles"]["control"]["display_name"]
    else:
        document["release"]["manifest_sha256"] = "a" * 64
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == message


def test_unknown_role_names_are_sorted_in_manifest_errors():
    document = manifest().to_document()
    document["roles"]["zebra"] = document["roles"].pop("control")
    document["roles"]["another"] = document["roles"].pop("process")
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == (
        "roles must declare exactly the six SJD roles; "
        "missing=['control', 'process'] extra=['another', 'zebra']"
    )


def test_minimal_release_section_uses_the_portable_schema_without_disabling_boolean_validation():
    document = manifest().to_document()
    document["release"] = {}
    parsed = deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert parsed.release == deployment.ReleaseProvenanceRef(
        require_identity=False, identity_schema="people-counter-sjd-release-identity-v2",
    )


@pytest.mark.parametrize("field,value,message", [
    ("manifest_sha256", "", "release.manifest_sha256 must be non-empty"),
    ("receipt_sha256", "", "release.receipt_sha256 must be non-empty"),
    ("manifest_sha256", "invalid", "release.manifest_sha256 must be a SHA-256 digest"),
    ("receipt_sha256", "invalid", "release.receipt_sha256 must be a SHA-256 digest"),
])
def test_bad_release_digest_error_always_names_the_correct_field(field, value, message):
    document = manifest().to_document()
    document["release"].update(manifest_sha256="a" * 64, receipt_sha256="b" * 64)
    document["release"][field] = value
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.parse_deployment_manifest(document, deployment.DeploymentEnvironment.PROD)
    assert str(error.value) == message


@pytest.mark.parametrize("field,value,message", [
    ("environmentArtifactId", "another", "Fabric SJD requires the configured Fabric Environment"),
    ("runtimeVersion", "1.3", "Fabric SJD requires Fabric Runtime 2.0"),
    ("libraryMode", "Session", "Fabric SJD requires Full environment library mode"),
    ("published", False, "Fabric SJD requires a published Environment"),
    ("additionalLibraryUris", ["inline"], "Fabric SJD forbids inline or session libraries"),
])
def test_environment_policy_diagnostics_distinguish_qualification_failures(field, value, message):
    from people_counter.fabric_namespace import validate_environment_library_policy

    metadata = {
        "environmentArtifactId": TEST_TARGET.environment_id, "runtimeVersion": "2.0",
        "libraryMode": "Full", "published": True, "additionalLibraryUris": [],
        field: value,
    }
    with pytest.raises(ValueError) as error:
        validate_environment_library_policy(metadata, config=TEST_TARGET)
    assert str(error.value) == message


def test_candidate_direct_production_enablement_error_keeps_the_routing_requirement():
    with pytest.raises(ValueError) as error:
        FabricNamespaceConfig(
            workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
            environment_id=TEST_TARGET.environment_id, production_enabled=True,
        )
    assert str(error.value) == (
        "direct PRODUCTION adapter execution is disabled; use the "
        "hash-bound production router after migration verification"
    )


def test_model_resolution_distinguishes_a_missing_artifact_map_from_a_missing_model(monkeypatch, tmp_path):
    from people_counter.fabric_input_resolver import InputBackend
    from people_counter.fabric_source_cache import LocalizedSourceCache
    from people_counter import fabric_sjd_runtime as runtime

    values = {
        "backend": InputBackend.FABRIC_FALLBACK, "mount_root": None,
        "cache": LocalizedSourceCache(tmp_path / "cache", max_entries=1, max_total_bytes=1000),
        "staged_sources": {}, "broadcast_digests": set(),
        "workspace_id": TEST_TARGET.workspace_id, "lakehouse_id": TEST_TARGET.lakehouse_id,
        "validation_error": ValueError,
    }
    with pytest.raises(ValueError) as error:
        runtime._resolve_localized_models(object(), {}, ("model.onnx",), **values)
    assert str(error.value) == "model_artifact_sha256 is missing 'model.onnx'"
    with pytest.raises(ValueError) as error:
        runtime._resolve_localized_models(
            object(), {"model_artifact_sha256": None}, ("model.onnx",), **values,
        )
    assert str(error.value) == "model_artifact_sha256 must be an object"


def test_process_environment_is_used_when_no_environment_map_is_supplied(monkeypatch):
    monkeypatch.setenv("PEOPLE_COUNTER_DEPLOY_STAGE", "test")
    monkeypatch.setenv("PEOPLE_COUNTER_DEPLOY_CONFIG_JSON", '{"workspace_id":"' + TEST_TARGET.workspace_id + '"}')
    monkeypatch.delenv("PEOPLE_COUNTER_DEPLOY_CONFIG_PATH", raising=False)
    assert deployment.resolve_stage() is deployment.DeploymentEnvironment.TEST
    document = deployment.load_manifest_document(deployment.DeploymentEnvironment.TEST)
    assert document["workspace_id"] == TEST_TARGET.workspace_id
    assert document["stage"] == "test"


def test_missing_bundled_or_overlay_configuration_has_the_correct_source_label(monkeypatch, tmp_path):
    with monkeypatch.context() as context:
        context.setattr(deployment, "_DATA_DIR", tmp_path)
        with pytest.raises(deployment.ManifestLoadError) as error:
            deployment.load_manifest_document(deployment.DeploymentEnvironment.PROD, env={})
        assert str(error.value) == f"bundled deployment manifest not found at {tmp_path / 'prod.json'}"
    missing = tmp_path / "overlay.json"
    with pytest.raises(deployment.ManifestLoadError) as error:
        deployment.load_manifest_document(deployment.DeploymentEnvironment.PROD, env={
            "PEOPLE_COUNTER_DEPLOY_CONFIG_PATH": str(missing),
        })
    assert str(error.value) == f"deployment manifest overlay file not found at {missing}"


def test_json_loading_is_utf8_even_on_a_non_utf8_host(monkeypatch, tmp_path):
    path = tmp_path / "config.json"
    path.write_bytes('{"name":"opérateur"}'.encode("utf-8"))
    original = Path.read_text

    def locale_read(self, encoding=None, errors=None):
        return original(self, encoding="ascii" if encoding is None else encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", locale_read)
    assert deployment._load_json_file(path, "configuration") == {"name": "opérateur"}


def test_manifest_overlay_replaces_scalar_and_object_values_in_both_directions():
    base = {"first": {"nested": "before"}, "second": "before"}
    result = deployment._deep_merge(base, {"first": "after", "second": {"nested": "after"}})
    assert result == {"first": "after", "second": {"nested": "after"}}
    assert base == {"first": {"nested": "before"}, "second": "before"}


@pytest.mark.parametrize("name", ["workspace_id", "lakehouse_id"])
def test_half_provisioned_target_cannot_build_a_qualified_one_lake_uri(name):
    value = manifest()
    if name == "workspace_id":
        value = dataclasses.replace(value, workspace_id=None)
    else:
        value = dataclasses.replace(value, lakehouse=dataclasses.replace(value.lakehouse, lakehouse_id=None))
    with pytest.raises(deployment.ManifestValidationError) as error:
        value.abfss_path(value.lakehouse.file_path("control"))
    assert str(error.value) == "prod is not provisioned with a workspace/lakehouse id"


def test_unknown_deployment_role_names_the_requested_role():
    with pytest.raises(deployment.ManifestValidationError) as error:
        manifest().role("unknown")
    assert str(error.value) == "unknown SJD role 'unknown'"


@pytest.mark.parametrize("field,value", [("runtime", "1.3"), ("table_prefix", "another_")])
def test_runtime_binding_does_not_discard_invalid_typed_manifest_contract_fields(field, value):
    selected = manifest()
    if field == "runtime":
        selected = dataclasses.replace(selected, fabric_runtime=value)
    else:
        selected = dataclasses.replace(selected, lakehouse=dataclasses.replace(selected.lakehouse, table_prefix=value))
    with pytest.raises(ValueError) as error:
        selected.runtime_config()
    assert str(error.value).startswith("stable production configuration is fixed:")


def test_release_verification_error_keeps_the_stage_context_for_an_invalid_schema():
    selected = manifest()
    selected = dataclasses.replace(
        selected, release=dataclasses.replace(selected.release, identity_schema="unknown"),
    )
    with pytest.raises(deployment.ManifestValidationError) as error:
        deployment.verify_release_provenance(selected)
    assert str(error.value) == "prod release identity schema does not match the manifest"


def test_schedule_default_enabled_value_is_false_not_an_implicit_activation():
    value = manifest().to_document()
    del value["roles"]["control"]["schedule"]["enabled"]
    parsed = deployment.parse_deployment_manifest(value, deployment.DeploymentEnvironment.PROD)
    assert parsed.role("control").schedule.enabled is False


def test_production_write_guards_detect_faulted_in_memory_configuration():
    config = FabricSjdConfig(
        workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
        environment_id=TEST_TARGET.environment_id,
    )
    object.__setattr__(config, "stable_production", False)
    with pytest.raises(ValueError) as error:
        config.require_write_enabled()
    assert str(error.value) == "stable production writes are disabled"
    candidate = FabricNamespaceConfig.production(target=TEST_TARGET)
    with pytest.raises(ValueError) as error:
        candidate.require_write_enabled()
    assert str(error.value) == "direct PRODUCTION writes are disabled; use the production router"


def test_invalid_candidate_mode_is_named_in_the_configuration_error():
    with pytest.raises(ValueError) as error:
        FabricNamespaceConfig(
            workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
            environment_id=TEST_TARGET.environment_id, mode="unknown",
        )
    assert str(error.value) == "invalid Fabric SJD namespace mode 'unknown'"


@pytest.mark.parametrize("factory", [
    lambda: FabricSjdConfig(
        workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
        environment_id=TEST_TARGET.environment_id,
    ),
    lambda: manifest().lakehouse,
])
def test_file_guards_reject_nonpath_syntax_not_just_traversal_segments(factory):
    for path in ("folder?query", "folder%escape", "folder#fragment", "folder/with space"):
        with pytest.raises(ValueError) as error:
            factory().file_path(path)
        assert repr(path) in str(error.value)


def test_model_resolver_keeps_only_the_final_name_of_deeply_nested_artifacts(monkeypatch, tmp_path):
    from unittest.mock import Mock
    from people_counter import fabric_sjd_runtime as runtime
    from people_counter.fabric_input_resolver import InputBackend
    from people_counter.fabric_source_cache import LocalizedSourceCache

    fallback = Mock(return_value=("localized", "a" * 64))
    monkeypatch.setattr(runtime, "_fallback_localize", fallback)
    runtime._resolve_localized_models(
        object(), {"model_artifact_sha256": {"pipeline/detector/model.onnx": "a" * 64}},
        ("pipeline/detector/model.onnx",),
        backend=InputBackend.FABRIC_FALLBACK, mount_root=None,
        cache=LocalizedSourceCache(tmp_path / "cache", max_entries=1, max_total_bytes=1000),
        staged_sources={}, broadcast_digests=set(),
        workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
        validation_error=ValueError,
    )
    assert fallback.call_args.kwargs["original_basename"] == "model.onnx"


def test_video_resolver_passes_an_empty_missing_digest_to_the_verifier_not_a_placeholder(monkeypatch, tmp_path):
    from unittest.mock import Mock
    from people_counter import fabric_sjd_runtime as runtime
    from people_counter.fabric_input_resolver import InputBackend
    from people_counter.fabric_source_cache import LocalizedSourceCache

    fallback = Mock(side_effect=ValueError("digest missing"))
    monkeypatch.setattr(runtime, "_fallback_localize", fallback)
    with pytest.raises(ValueError, match="digest missing"):
        runtime._resolve_localized_video(
            object(), {"source_video": "/lakehouse/default/Files/videos/clip.mp4"},
            backend=InputBackend.FABRIC_FALLBACK, mount_root=None,
            cache=LocalizedSourceCache(tmp_path / "cache", max_entries=1, max_total_bytes=1000),
            staged_sources={}, broadcast_digests=set(),
            workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
            validation_error=ValueError,
        )
    assert fallback.call_args.kwargs["expected_sha256"] == ""


def test_compute_policy_missing_properties_has_the_fixed_allocation_diagnostic():
    from people_counter.fabric_sjd_definition import reviewed_compute_settings, validate_reviewed_compute_settings

    settings = reviewed_compute_settings({})
    del settings["sparkProperties"]
    with pytest.raises(RuntimeError) as error:
        validate_reviewed_compute_settings(settings)
    assert str(error.value) == "Environment Spark properties differ from reviewed fixed allocation"


def test_invalid_role_output_identifies_the_exact_role_in_the_merge_error():
    value = _resolved_manifest().to_document()
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output(value, {
            "spark_job_definition_ids": {"value": {"control": "invalid"}},
        })
    assert str(error.value) == "roles.control.item_id must be a canonical UUID"

from __future__ import annotations

import json
from pathlib import Path

import pytest

from people_counter import fabric_deployment_manifest as deploy
from people_counter import fabric_release_provenance as provenance
from people_counter.fabric_sjd import (
    ENVIRONMENT_ID,
    FABRIC_RUNTIME,
    FILES_ROOT,
    LAKEHOUSE_ID,
    TABLE_PREFIX,
    WORKSPACE_ID,
    FabricSjdConfig,
)
from people_counter.fabric_sjd_definition import ENTRY_POINTS


def test_roles_cover_exactly_the_stable_entry_points() -> None:
    assert deploy.SJD_ROLES == frozenset(ENTRY_POINTS)
    assert deploy.SCHEDULED_ROLES | deploy.ON_DEMAND_ROLES == deploy.SJD_ROLES
    assert not deploy.SCHEDULED_ROLES & deploy.ON_DEMAND_ROLES


def test_default_manifest_preserves_production_bindings() -> None:
    manifest = deploy.load_deployment_manifest(env={})

    assert manifest.stage is deploy.DeploymentEnvironment.PROD
    assert manifest.workspace_id == WORKSPACE_ID
    assert manifest.fabric_environment_id == ENVIRONMENT_ID
    assert manifest.fabric_runtime == FABRIC_RUNTIME
    assert manifest.lakehouse.lakehouse_id == LAKEHOUSE_ID
    assert manifest.lakehouse.table_prefix == TABLE_PREFIX
    assert manifest.lakehouse.files_root == FILES_ROOT


def test_production_role_ids_come_from_release_identity() -> None:
    manifest = deploy.load_deployment_manifest("prod", env={})

    for role in deploy.SJD_ROLES:
        assert (
            manifest.role(role).item_id
            == provenance.RELEASE_IDENTITY.sjd_ids[f"stable-{role}"]
        )


def test_production_release_provenance_is_verified() -> None:
    manifest = deploy.load_deployment_manifest("prod", env={})
    assert deploy.verify_release_provenance(manifest) == (
        provenance.RELEASE_IDENTITY_SHA256
    )


@pytest.mark.parametrize("stage", ["dev", "test"])
def test_nonproduction_stages_have_no_fixed_resource_ids(stage: str) -> None:
    manifest = deploy.load_deployment_manifest(stage, env={})

    assert manifest.workspace_id is None
    assert manifest.fabric_environment_id is None
    assert manifest.lakehouse.lakehouse_id is None
    assert manifest.release.require_identity is False
    assert deploy.verify_release_provenance(manifest) is None


def test_nonproduction_namespaces_are_isolated() -> None:
    dev = deploy.load_deployment_manifest("dev", env={})
    test = deploy.load_deployment_manifest("test", env={})

    assert dev.lakehouse.table_prefix != test.lakehouse.table_prefix
    assert dev.lakehouse.files_root != test.lakehouse.files_root
    assert dev.lakehouse.table_prefix != TABLE_PREFIX
    assert test.lakehouse.files_root != FILES_ROOT


def test_production_abfss_path_matches_existing_configuration() -> None:
    manifest = deploy.load_deployment_manifest("prod", env={})
    legacy = FabricSjdConfig()
    path = legacy.file_path("attempts/batch=a")

    assert manifest.abfss_path(path) == legacy.abfss_path(path)


def test_unprovisioned_environment_has_no_abfss_path() -> None:
    manifest = deploy.load_deployment_manifest("dev", env={})
    path = manifest.lakehouse.file_path("attempts/batch=a")

    with pytest.raises(deploy.ManifestValidationError, match="not provisioned"):
        manifest.abfss_path(path)


def test_lakehouse_namespace_helpers_reject_unsafe_values() -> None:
    manifest = deploy.load_deployment_manifest("prod", env={})

    assert manifest.lakehouse.table("work") == "people_counter_sjd_work"
    assert (
        manifest.lakehouse.validate_table_name("people_counter_sjd_work")
        == "people_counter_sjd_work"
    )
    with pytest.raises(deploy.ManifestValidationError, match="invalid deployment"):
        manifest.lakehouse.table("unknown")
    with pytest.raises(deploy.ManifestValidationError, match="outside"):
        manifest.lakehouse.validate_table_name("another_namespace_work")
    for relative in ("../escape", "/absolute", "a\\b", "a//b"):
        with pytest.raises(deploy.ManifestValidationError, match="unsafe"):
            manifest.lakehouse.file_path(relative)
    with pytest.raises(deploy.ManifestValidationError, match="outside"):
        manifest.lakehouse.validate_files_path("Files/not-people-counter/file")


def test_manifest_entry_points_use_canonical_mapping() -> None:
    manifest = deploy.load_deployment_manifest("prod", env={})

    for role, entry_point in ENTRY_POINTS.items():
        assert manifest.entry_point(role) == entry_point


def test_stage_environment_variable_selects_manifest() -> None:
    manifest = deploy.load_deployment_manifest(
        env={"PEOPLE_COUNTER_DEPLOY_STAGE": "test"}
    )
    assert manifest.stage is deploy.DeploymentEnvironment.TEST


def test_invalid_stage_is_rejected() -> None:
    with pytest.raises(deploy.ManifestValidationError, match="must be dev, test, or prod"):
        deploy.resolve_stage(None, env={"PEOPLE_COUNTER_DEPLOY_STAGE": "invalid"})


def test_file_overlay_deep_merges_defaults(tmp_path: Path) -> None:
    overlay_path = tmp_path / "dev.json"
    overlay_path.write_text(
        json.dumps(
            {
                "workspace_id": "11111111-1111-1111-1111-111111111111",
                "lakehouse": {
                    "lakehouse_id": "22222222-2222-2222-2222-222222222222"
                },
            }
        ),
        encoding="utf-8",
    )

    manifest = deploy.load_deployment_manifest(
        env={
            "PEOPLE_COUNTER_DEPLOY_STAGE": "dev",
            "PEOPLE_COUNTER_DEPLOY_CONFIG_PATH": str(overlay_path),
        }
    )

    assert manifest.workspace_id == "11111111-1111-1111-1111-111111111111"
    assert manifest.lakehouse.lakehouse_id == (
        "22222222-2222-2222-2222-222222222222"
    )
    assert manifest.lakehouse.table_prefix == "people_counter_dev_"


def test_inline_overlay_wins_over_file_overlay(tmp_path: Path) -> None:
    overlay_path = tmp_path / "dev.json"
    overlay_path.write_text(
        json.dumps(
            {"workspace_id": "11111111-1111-1111-1111-111111111111"}
        ),
        encoding="utf-8",
    )

    manifest = deploy.load_deployment_manifest(
        env={
            "PEOPLE_COUNTER_DEPLOY_STAGE": "dev",
            "PEOPLE_COUNTER_DEPLOY_CONFIG_PATH": str(overlay_path),
            "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON": json.dumps(
                {"workspace_id": "33333333-3333-3333-3333-333333333333"}
            ),
        }
    )

    assert manifest.workspace_id == "33333333-3333-3333-3333-333333333333"


def test_secret_like_fields_are_rejected() -> None:
    with pytest.raises(deploy.ManifestValidationError, match="looks like a secret"):
        deploy.load_deployment_manifest(
            "dev",
            env={
                "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON": json.dumps(
                    {"client_secret": "not-allowed"}
                )
            },
        )


def test_scheduled_and_on_demand_roles_are_enforced() -> None:
    document = deploy.load_manifest_document(deploy.DeploymentEnvironment.DEV, env={})
    document["roles"]["control"]["schedule"] = None
    with pytest.raises(deploy.ManifestValidationError, match="requires a schedule"):
        deploy.parse_deployment_manifest(document, deploy.DeploymentEnvironment.DEV)

    document = deploy.load_manifest_document(deploy.DeploymentEnvironment.DEV, env={})
    document["roles"]["process"]["schedule"] = {
        "interval_minutes": 5,
        "enabled": False,
    }
    with pytest.raises(deploy.ManifestValidationError, match="must not declare"):
        deploy.parse_deployment_manifest(document, deploy.DeploymentEnvironment.DEV)


@pytest.mark.parametrize(
    ("interval", "valid"),
    [(0, False), (1, True), (10_080, True), (10_081, False)],
)
def test_schedule_interval_boundaries(interval: int, valid: bool) -> None:
    document = deploy.load_manifest_document(deploy.DeploymentEnvironment.DEV, env={})
    document["roles"]["control"]["schedule"]["interval_minutes"] = interval

    if valid:
        manifest = deploy.parse_deployment_manifest(
            document, deploy.DeploymentEnvironment.DEV
        )
        schedule = manifest.role("control").schedule
        assert schedule == deploy.ScheduleSpec(interval_minutes=interval, enabled=False)
    else:
        with pytest.raises(deploy.ManifestValidationError, match="between 1 and 10080"):
            deploy.parse_deployment_manifest(
                document, deploy.DeploymentEnvironment.DEV
            )


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda doc: doc.__setitem__("schema", "unknown"), "unsupported"),
        (lambda doc: doc.__setitem__("stage", "test"), "does not match"),
        (
            lambda doc: doc["lakehouse"].__setitem__("table_prefix", "unsafe"),
            "unsafe value",
        ),
        (
            lambda doc: doc["lakehouse"].__setitem__(
                "files_root", "Files/people-counter/dev/v1"
            ),
            "must end",
        ),
        (
            lambda doc: doc["lakehouse"].__setitem__("table_prefix", TABLE_PREFIX),
            "must not reuse",
        ),
    ],
)
def test_manifest_shape_validation_branches(mutation, match: str) -> None:
    document = deploy.load_manifest_document(deploy.DeploymentEnvironment.DEV, env={})
    mutation(document)

    with pytest.raises(deploy.ManifestValidationError, match=match):
        deploy.parse_deployment_manifest(document, deploy.DeploymentEnvironment.DEV)


def test_manifest_requires_exactly_seven_roles() -> None:
    document = deploy.load_manifest_document(deploy.DeploymentEnvironment.DEV, env={})
    del document["roles"]["cutover"]

    with pytest.raises(deploy.ManifestValidationError, match=r"missing=\['cutover'\]"):
        deploy.parse_deployment_manifest(document, deploy.DeploymentEnvironment.DEV)


def test_reporting_artifacts_are_loaded_from_manifest() -> None:
    manifest = deploy.load_deployment_manifest("prod", env={})

    assert manifest.analytics_reporting.semantic_model == deploy.ArtifactRef(
        source_item_id="5dbaf3d1-9f86-4bcd-a590-f32c7fbea08c",
        source_name="pc_analytics_model",
        target_name="pc_sjd_analytics_model",
    )
    assert manifest.operational_reporting.report == deploy.ArtifactRef(
        source_item_id="8b81cc28-cf18-4707-a245-afb98a8acbcc",
        source_name="pc_operations_report",
        target_name="pc_sjd_operations_report",
    )


def test_production_identity_drift_requires_explicit_double_override() -> None:
    changed_workspace = "44444444-4444-4444-4444-444444444444"
    with pytest.raises(deploy.ManifestValidationError, match="identity is fixed"):
        deploy.load_deployment_manifest(
            "prod",
            env={
                "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON": json.dumps(
                    {"workspace_id": changed_workspace}
                )
            },
        )

    with pytest.raises(deploy.ManifestValidationError, match="disagrees"):
        deploy.load_deployment_manifest(
            "prod",
            env={
                "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON": json.dumps(
                    {
                        "workspace_id": changed_workspace,
                        "allow_legacy_mismatch": True,
                    }
                )
            },
        )

    manifest = deploy.load_deployment_manifest(
        "prod",
        env={
            "PEOPLE_COUNTER_DEPLOY_CONFIG_JSON": json.dumps(
                {
                    "workspace_id": changed_workspace,
                    "allow_legacy_mismatch": True,
                    "release": {"require_identity": False},
                }
            )
        },
    )
    assert manifest.workspace_id == changed_workspace

from dataclasses import FrozenInstanceError

import pytest

from people_counter.fabric_namespace import (
    BENCHMARK_FILES_ROOT,
    BENCHMARK_TABLE_PREFIX,
    CANARY_FILES_ROOT,
    CANARY_TABLE_PREFIX,
    ENVIRONMENT_ID,
    LEGACY_TABLE_PREFIX,
    LAKEHOUSE_ID,
    PRODUCTION_FILES_ROOT,
    PRODUCTION_SHADOW_FILES_ROOT,
    PRODUCTION_SHADOW_TABLE_PREFIX,
    PRODUCTION_TABLE_PREFIX,
    WORKSPACE_ID,
    FabricNamespaceConfig,
    FabricNamespaceMode,
)


@pytest.mark.parametrize(
    ("mode", "prefix", "root"),
    [
        (
            FabricNamespaceMode.CANARY,
            CANARY_TABLE_PREFIX,
            CANARY_FILES_ROOT,
        ),
        (
            FabricNamespaceMode.BENCHMARK,
            BENCHMARK_TABLE_PREFIX,
            BENCHMARK_FILES_ROOT,
        ),
        (
            FabricNamespaceMode.PRODUCTION_SHADOW,
            PRODUCTION_SHADOW_TABLE_PREFIX,
            PRODUCTION_SHADOW_FILES_ROOT,
        ),
        (
            FabricNamespaceMode.PRODUCTION,
            PRODUCTION_TABLE_PREFIX,
            PRODUCTION_FILES_ROOT,
        ),
    ],
)
def test_mode_selects_one_immutable_exact_namespace(mode, prefix, root) -> None:
    config = FabricNamespaceConfig.for_mode(mode)

    assert config.mode is mode
    assert config.table_prefix == prefix
    assert config.files_root == root
    assert config.table("work") == prefix + "work"
    assert config.file_path("attempts/run=one") == root + "attempts/run=one"
    with pytest.raises(FrozenInstanceError):
        config.mode = FabricNamespaceMode.PRODUCTION  # type: ignore[misc]


def test_namespace_literals_are_the_reviewed_contract() -> None:
    assert (CANARY_TABLE_PREFIX, CANARY_FILES_ROOT) == (
        "pc_ca_canary_v1_",
        "Files/_canary/people-counter/sjd/v1/",
    )
    assert (BENCHMARK_TABLE_PREFIX, BENCHMARK_FILES_ROOT) == (
        "pc_ca_benchmark_v1_",
        "Files/_benchmark/people-counter/sjd/v1/",
    )
    assert (PRODUCTION_SHADOW_TABLE_PREFIX, PRODUCTION_SHADOW_FILES_ROOT) == (
        "pc_ca_prod_shadow_v1_",
        "Files/_shadow/people-counter/sjd/v1/",
    )
    assert PRODUCTION_TABLE_PREFIX == "people_counter_ca_"


def test_default_and_legacy_constructor_preserve_canary_semantics() -> None:
    default = FabricNamespaceConfig()
    legacy = FabricNamespaceConfig(
        WORKSPACE_ID,
        LAKEHOUSE_ID,
        ENVIRONMENT_ID,
        CANARY_TABLE_PREFIX,
        CANARY_FILES_ROOT,
        "2.0",
    )

    assert default == legacy
    assert default.mode is FabricNamespaceMode.CANARY


def test_named_constructors_select_their_mode() -> None:
    assert FabricNamespaceConfig.canary().mode is FabricNamespaceMode.CANARY
    assert (
        FabricNamespaceConfig.benchmark().mode
        is FabricNamespaceMode.BENCHMARK
    )
    assert (
        FabricNamespaceConfig.production_shadow().mode
        is FabricNamespaceMode.PRODUCTION_SHADOW
    )
    assert (
        FabricNamespaceConfig.production().mode
        is FabricNamespaceMode.PRODUCTION
    )
    with pytest.raises(ValueError, match="direct PRODUCTION writes"):
        FabricNamespaceConfig.production().require_write_enabled()


@pytest.mark.parametrize(
    "override",
    [
        {"workspace_id": "00000000-0000-0000-0000-000000000000"},
        {"lakehouse_id": "00000000-0000-0000-0000-000000000000"},
        {"environment_id": "00000000-0000-0000-0000-000000000000"},
    ],
)
def test_artifact_identity_mismatches_fail_closed(override) -> None:
    with pytest.raises(ValueError, match="configuration is fixed"):
        FabricNamespaceConfig(**override)


def test_mode_namespace_mismatches_and_canary_reuse_fail_closed() -> None:
    with pytest.raises(ValueError, match="BENCHMARK configuration is fixed"):
        FabricNamespaceConfig(
            mode=FabricNamespaceMode.BENCHMARK,
            table_prefix=CANARY_TABLE_PREFIX,
        )
    with pytest.raises(ValueError, match="PRODUCTION_SHADOW configuration is fixed"):
        FabricNamespaceConfig(
            mode=FabricNamespaceMode.PRODUCTION_SHADOW,
            files_root=CANARY_FILES_ROOT,
        )
    with pytest.raises(ValueError, match="binding mismatch"):
        FabricNamespaceConfig().require_mode(FabricNamespaceMode.BENCHMARK)


def test_runtime_binding_checks_mode_and_all_artifact_ids() -> None:
    config = FabricNamespaceConfig.for_mode(FabricNamespaceMode.BENCHMARK)
    config.require_mode(FabricNamespaceMode.BENCHMARK)
    config.validate_binding(
        workspace_id=WORKSPACE_ID,
        lakehouse_id=LAKEHOUSE_ID,
        environment_id=ENVIRONMENT_ID,
        mode=FabricNamespaceMode.BENCHMARK,
    )

    for field in ("workspace_id", "lakehouse_id", "environment_id"):
        values = {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
            "mode": FabricNamespaceMode.BENCHMARK,
        }
        values[field] = "00000000-0000-0000-0000-000000000000"
        with pytest.raises(ValueError, match="binding mismatch"):
            config.validate_binding(**values)


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("workspace_id", "00000000-0000-0000-0000-000000000000"),
        ("lakehouse_id", "00000000-0000-0000-0000-000000000000"),
        ("environment_id", "00000000-0000-0000-0000-000000000000"),
        ("runtime", "3.0"),
    ],
)
def test_for_mode_does_not_discard_explicit_artifact_bindings(
    argument: str, value: str
) -> None:
    with pytest.raises(ValueError, match="configuration is fixed"):
        FabricNamespaceConfig.for_mode(
            FabricNamespaceMode.BENCHMARK,
            **{argument: value},
        )


def test_binding_rejects_invalid_mode_with_specific_evidence() -> None:
    with pytest.raises(
        ValueError, match="invalid Fabric SJD namespace mode 'OTHER'"
    ):
        FabricNamespaceConfig().validate_binding(
            workspace_id=WORKSPACE_ID,
            lakehouse_id=LAKEHOUSE_ID,
            environment_id=ENVIRONMENT_ID,
            mode="OTHER",
        )


@pytest.mark.parametrize(
    "suffix",
    [
        "",
        "anything",
        "work;drop_table",
        "people_counter_work",
        "../work",
    ],
)
def test_only_reviewed_table_suffixes_are_resolved(suffix) -> None:
    with pytest.raises(ValueError, match="invalid Fabric SJD table suffix"):
        FabricNamespaceConfig().table(suffix)


@pytest.mark.parametrize(
    "relative",
    [
        "",
        "/attempts",
        "../attempts",
        "attempts/../work",
        "attempts//work",
        r"attempts\work",
        "attempts/%2e%2e/work",
        "attempts/work?x=1",
        "attempts/work#fragment",
        "attempts/\x00work",
    ],
)
def test_relative_paths_reject_traversal_and_non_path_syntax(relative) -> None:
    with pytest.raises(ValueError, match="unsafe Fabric SJD relative path"):
        FabricNamespaceConfig().file_path(relative)


@pytest.mark.parametrize(
    "mode",
    [
        FabricNamespaceMode.BENCHMARK,
        FabricNamespaceMode.PRODUCTION_SHADOW,
    ],
)
def test_benchmark_and_shadow_cannot_mutate_legacy_namespace(mode) -> None:
    config = FabricNamespaceConfig.for_mode(mode)

    with pytest.raises(ValueError, match="cannot mutate legacy production tables"):
        config.validate_table_name(LEGACY_TABLE_PREFIX + "work")
    with pytest.raises(ValueError, match="cannot mutate legacy production files"):
        config.validate_files_path(PRODUCTION_FILES_ROOT + "attempts/run=old")


def test_exact_namespace_validation_rejects_other_modes_and_prefix_collisions() -> None:
    benchmark = FabricNamespaceConfig.for_mode(
        FabricNamespaceMode.BENCHMARK
    )

    assert benchmark.validate_table_name(
        BENCHMARK_TABLE_PREFIX + "work"
    ) == BENCHMARK_TABLE_PREFIX + "work"
    assert benchmark.validate_files_path(
        BENCHMARK_FILES_ROOT + "attempts/run=one"
    ) == BENCHMARK_FILES_ROOT + "attempts/run=one"
    with pytest.raises(ValueError, match="outside"):
        benchmark.validate_table_name(CANARY_TABLE_PREFIX + "work")
    with pytest.raises(ValueError, match="outside"):
        benchmark.validate_files_path(
            BENCHMARK_FILES_ROOT.rstrip("/") + "-other/attempts"
        )

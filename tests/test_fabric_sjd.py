from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from people_counter.fabric_sjd import (
    FILES_ROOT,
    TABLE_PREFIX,
    TABLE_SUFFIXES,
    FabricSjdConfig,
)
from people_counter.fabric_sjd_jobs import (
    control_main,
    gold_main,
    process_main,
    reconciliation_main,
)
from people_counter.fabric_sjd_definition import (
    ENTRY_POINTS,
    build_sjd_definition,
)


def test_stable_config_is_fixed_and_contains_no_candidate_or_shadow_tables() -> None:
    config = FabricSjdConfig()
    assert config.table_prefix == "people_counter_sjd_"
    assert config.files_root == "Files/people-counter/sjd/v1/"
    assert "routing_allowlist" not in TABLE_SUFFIXES
    assert "shadow_audit" not in TABLE_SUFFIXES
    assert "candidate" not in TABLE_PREFIX
    assert "candidate" not in FILES_ROOT
    assert config.table("work") == "people_counter_sjd_work"
    assert config.file_path("attempts/batch=a") == (
        "Files/people-counter/sjd/v1/attempts/batch=a"
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("table_prefix", "people_counter_ca_"),
        ("files_root", "Files/people-counter/candidate-a/v1/"),
        ("stable_production", False),
    ],
)
def test_stable_config_rejects_mutable_bindings(field: str, value: object) -> None:
    with pytest.raises(ValueError, match="configuration is fixed"):
        FabricSjdConfig(**{field: value})


def test_stable_config_rejects_unknown_tables_and_escaped_paths() -> None:
    config = FabricSjdConfig()
    with pytest.raises(ValueError, match="table suffix"):
        config.table("shadow_audit")
    with pytest.raises(ValueError, match="unsafe"):
        config.file_path("../candidate")
    with pytest.raises(ValueError, match="outside"):
        config.validate_files_path("Files/people-counter/candidate-a/v1/work")


def test_stable_entry_points_bind_exact_production_config() -> None:
    with patch(
        "people_counter.fabric_sjd_runtime.control_main",
        return_value=7,
    ) as run:
        assert control_main(["reconcile"]) == 7
    config = run.call_args.kwargs["config"]
    assert isinstance(config, FabricSjdConfig)
    assert run.call_args.args == (["reconcile"],)

    with patch(
        "people_counter.fabric_sjd_runtime.process_main",
        return_value=8,
    ) as run:
        assert process_main(["batch"]) == 8
    assert isinstance(run.call_args.kwargs["config"], FabricSjdConfig)
    assert run.call_args.kwargs["route_mode"] == "PRODUCTION"

    with patch(
        "people_counter.fabric_sjd_runtime.gold_main",
        return_value=9,
    ) as run:
        assert gold_main(["validate"]) == 9
    assert isinstance(run.call_args.kwargs["config"], FabricSjdConfig)


def test_stable_reconciliation_rejects_any_other_command() -> None:
    with patch(
        "people_counter.fabric_sjd_jobs.control_main",
        return_value=0,
    ) as run:
        assert reconciliation_main() == 0
        run.assert_called_once_with(["reconcile"])
    with pytest.raises(ValueError, match="only the reconcile"):
        reconciliation_main(["recover"])


def test_stable_config_enables_production_attempt_adapter() -> None:
    from people_counter.sjd_process import OneLakeDeltaAttemptAdapter

    config = FabricSjdConfig()
    adapter = OneLakeDeltaAttemptAdapter(
        config.file_path("attempts"),
        SimpleNamespace(),
        files=SimpleNamespace(),
        config=config,
        route_mode="PRODUCTION",
    )
    assert adapter.route_mode.value == "PRODUCTION"


@pytest.mark.parametrize("kind", sorted(ENTRY_POINTS))
def test_stable_sjd_definition_has_only_installed_entrypoint_and_fixed_bindings(
    kind: str,
) -> None:
    import base64
    import json

    definition = build_sjd_definition(kind)["definition"]
    assert definition["format"] == "SparkJobDefinitionV2"
    parts = {part["path"]: part for part in definition["parts"]}
    assert set(parts) == {"Main/main.py", "SparkJobDefinitionV1.json"}
    source = base64.b64decode(parts["Main/main.py"]["payload"]).decode("utf-8")
    module, function = ENTRY_POINTS[kind].split(":")
    assert f"from {module} import {function} as main" in source
    assert "candidate" not in source.lower()
    metadata = json.loads(
        base64.b64decode(parts["SparkJobDefinitionV1.json"]["payload"])
    )
    assert metadata["additionalLibraryUris"] == []
    assert metadata["commandLineArguments"] == ""
    assert metadata["environmentArtifactId"] == (
        "3e580f48-9ff7-4bc6-af2e-a59158029ada"
    )
    assert metadata["defaultLakehouseArtifactId"] == (
        "883cff91-eaa8-40be-870f-6e9716303cb2"
    )


def test_stable_sjd_definition_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        build_sjd_definition("shadow")

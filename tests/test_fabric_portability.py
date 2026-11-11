from __future__ import annotations

import base64
import dataclasses
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from people_counter.fabric_sjd import FabricSjdConfig
from people_counter.fabric_sjd_definition import ENTRY_POINTS, build_sjd_definition
from tests.fabric_fixtures import RUNTIME_CONTEXT, TARGET_IDS


def target_ids(offset: int) -> dict[str, str]:
    return {
        key: str(UUID(int=offset + index))
        for index, key in enumerate(TARGET_IDS, start=1)
    }


@pytest.mark.parametrize("offset", [0, 100])
def test_same_sdk_builds_definitions_for_independent_targets(offset: int) -> None:
    config = FabricSjdConfig(**target_ids(offset))
    assert set(ENTRY_POINTS) == {
        "control", "dispatcher", "process", "refresh", "reconciliation", "gold",
    }
    for role in ENTRY_POINTS:
        definition = build_sjd_definition(role, config=config)["definition"]
        parts = {
            part["path"]: base64.b64decode(part["payload"])
            for part in definition["parts"]
        }
        metadata = json.loads(parts["SparkJobDefinitionV1.json"])
        assert metadata["defaultLakehouseArtifactId"] == config.lakehouse_id
        assert metadata["environmentArtifactId"] == config.environment_id
        assert metadata["additionalLibraryUris"] == []
        assert metadata["commandLineArguments"] == ""
        assert ENTRY_POINTS[role].split(":")[0] in parts["Main/main.py"].decode()
        assert config.workspace_id not in parts["Main/main.py"].decode()


def test_runtime_target_has_no_implicit_reference_deployment() -> None:
    with pytest.raises(TypeError):
        FabricSjdConfig()


@pytest.mark.parametrize("field", list(TARGET_IDS))
@pytest.mark.parametrize(
    "invalid",
    [None, "", "workspace", str(UUID(int=0)), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"],
)
def test_target_rejects_noncanonical_missing_and_nil_ids(field: str, invalid) -> None:
    with pytest.raises(ValueError, match="canonical UUID"):
        FabricSjdConfig(**{**TARGET_IDS, field: invalid})


def test_runtime_context_selects_and_checks_the_actual_deployment(monkeypatch) -> None:
    from people_counter import fabric_sjd_jobs as jobs
    from people_counter import fabric_sjd_runtime as runtime

    monkeypatch.setitem(
        sys.modules, "notebookutils",
        SimpleNamespace(runtime=SimpleNamespace(context=RUNTIME_CONTEXT)),
    )
    observed = {}

    def run(arguments, *, config):
        observed.update(arguments=arguments, config=config)
        return 17

    monkeypatch.setattr(runtime, "control_main", run)
    assert jobs.control_main(["bootstrap"]) == 17
    assert dataclasses.asdict(observed["config"])["workspace_id"] == TARGET_IDS["workspace_id"]
    assert observed["config"] == FabricSjdConfig(**TARGET_IDS)
    assert observed["arguments"] == ["bootstrap"]


def test_runtime_binding_preserves_an_explicit_deployment_files_root(monkeypatch) -> None:
    from people_counter import fabric_sjd_jobs as jobs
    from people_counter import fabric_sjd_runtime as runtime

    monkeypatch.setitem(
        sys.modules, "notebookutils",
        SimpleNamespace(runtime=SimpleNamespace(context=RUNTIME_CONTEXT)),
    )
    observed = {}

    def run(arguments, *, config):
        observed.update(arguments=arguments, config=config)
        return 0

    monkeypatch.setattr(runtime, "control_main", run)
    assert jobs.control_main([
        "--artifacts", *TARGET_IDS.values(), "--files-root", "Files/people-counter/dev/v1/",
        "bootstrap",
    ]) == 0
    assert observed["arguments"] == ["bootstrap"]
    config = observed["config"]
    assert config.files_root == "Files/people-counter/dev/v1/"
    assert config.file_path("control") == "Files/people-counter/dev/v1/control"
    with pytest.raises(ValueError, match="outside"):
        config.validate_files_path("Files/people-counter/sjd/v1/control")


@pytest.mark.parametrize("root", [
    "", "Files/", "/Files/deployment/", "Tables/deployment/",
    "Files/deployment", "Files/../deployment/", "Files/deployment//v1/",
    "Files/deployment\\v1/", "Files/deployment/%2e%2e/",
])
def test_production_target_requires_a_safe_nonbroad_files_namespace(root: str) -> None:
    with pytest.raises(ValueError, match="files root"):
        FabricSjdConfig(**TARGET_IDS, files_root=root)


@pytest.mark.parametrize("key", list(RUNTIME_CONTEXT))
def test_runtime_context_mismatch_never_reaches_a_job(monkeypatch, key: str) -> None:
    from people_counter import fabric_sjd_jobs as jobs
    from people_counter import fabric_sjd_runtime as runtime

    context = {**RUNTIME_CONTEXT, key: str(UUID(int=99))}
    monkeypatch.setitem(
        sys.modules, "notebookutils",
        SimpleNamespace(runtime=SimpleNamespace(context=context)),
    )
    monkeypatch.setattr(
        runtime, "control_main",
        lambda *args, **kwargs: pytest.fail("unvalidated target reached job"),
    )
    with pytest.raises(ValueError, match="runtime context"):
        jobs.control_main(["--artifacts", *TARGET_IDS.values(), "bootstrap"])


def test_sdk_and_deployment_defaults_contain_no_literal_target_ids() -> None:
    import re

    root = Path(__file__).resolve().parents[1]
    guid = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", re.I)
    for base in (root / "src/people_counter", root / "scripts"):
        for path in base.rglob("*.py"):
            assert not guid.search(path.read_text(encoding="utf-8")), path.relative_to(root)
    for path in (root / "src/people_counter/data/deploy").glob("*.json"):
        document = json.loads(path.read_text())
        assert document["workspace_id"] is None
        assert document["fabric_environment_id"] is None
        assert document["lakehouse"]["lakehouse_id"] is None
        assert set(document["roles"]) == set(ENTRY_POINTS)
        assert all(binding["item_id"] is None for binding in document["roles"].values())
        for bundle in ("analytics_reporting", "operational_reporting"):
            for artifact in document[bundle].values():
                assert artifact["source_item_id"] is None


def test_removed_cutover_is_not_packaged_or_exported() -> None:
    root = Path(__file__).resolve().parents[1]
    assert not (root / "src/people_counter/fabric_sjd_cutover.py").exists()
    assert not (root / "src/people_counter/fabric_sjd_cutover_jobs.py").exists()
    assert not (root / "fabric/spark-job-definitions/cutover/Main/main.py").exists()
    assert "pc-production-schema-cutover" not in (root / "pyproject.toml").read_text()


def test_getting_started_notebook_reads_assigned_ids_from_deployment_state(monkeypatch):
    from people_counter import fabric_deployment_manifest as deployment
    from tests.test_fabric_operations_portable import manifest
    from tests.fabric_fixtures import ROLE_IDS

    selected = manifest(ids=True)
    selected = dataclasses.replace(
        selected,
        analytics_reporting=dataclasses.replace(
            selected.analytics_reporting,
            semantic_model=dataclasses.replace(
                selected.analytics_reporting.semantic_model, target_item_id=str(UUID(int=80)),
            ),
            report=dataclasses.replace(
                selected.analytics_reporting.report, target_item_id=str(UUID(int=81)),
            ),
        ),
    )
    monkeypatch.setattr(deployment, "load_deployment_manifest", lambda **kwargs: selected)
    monkeypatch.setitem(
        sys.modules, "notebookutils",
        SimpleNamespace(runtime=SimpleNamespace(context=RUNTIME_CONTEXT)),
    )
    root = Path(__file__).resolve().parents[1]
    notebook = json.loads((root / "notebooks/fabric/18_sjd_getting_started_demo.ipynb").read_text())
    source = "".join(next(cell["source"] for cell in notebook["cells"] if cell.get("id") == "parameters"))
    namespace = {}
    exec(compile(source, "demo-parameters", "exec"), namespace)
    assert namespace["WORKSPACE_ID"] == selected.workspace_id
    assert namespace["LAKEHOUSE_ID"] == selected.lakehouse.lakehouse_id
    assert namespace["CONTROL_SJD_ID"] == ROLE_IDS["control"]
    assert namespace["PROCESS_SJD_ID"] == ROLE_IDS["process"]
    assert namespace["GOLD_SJD_ID"] == ROLE_IDS["gold"]
    assert namespace["SEMANTIC_MODEL_ID"] == str(UUID(int=80))
    assert namespace["REPORT_ID"] == str(UUID(int=81))
    assert namespace["REGISTER_INPUTS"] is False
    assert namespace["PROCESS_BATCH"] is False


@pytest.mark.parametrize("role", ["control", "dispatcher", "process", "gold", "refresh", "reconciliation"])
def test_job_help_does_not_require_fabric_context_or_start_spark(monkeypatch, role, capsys):
    from people_counter import fabric_sjd_jobs as jobs
    from people_counter import fabric_sjd_runtime as runtime

    monkeypatch.setitem(sys.modules, "notebookutils", None)
    monkeypatch.setattr(runtime, "_spark", lambda: pytest.fail("help started Spark"))
    with pytest.raises(SystemExit) as error:
        getattr(jobs, role + "_main")(["--help"])
    assert error.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_missing_runtime_context_never_selects_another_deployment(monkeypatch):
    from people_counter import fabric_sjd_jobs as jobs
    from people_counter import fabric_sjd_runtime as runtime

    monkeypatch.setitem(sys.modules, "notebookutils", None)
    monkeypatch.setattr(runtime, "_spark", lambda: pytest.fail("missing target started Spark"))
    with pytest.raises(RuntimeError, match="explicit Fabric target"):
        jobs.control_main(["bootstrap"])

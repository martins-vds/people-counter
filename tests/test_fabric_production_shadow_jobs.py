from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    PRODUCTION_SHADOW_FILES_ROOT,
    PRODUCTION_SHADOW_TABLE_PREFIX,
    WORKSPACE_ID,
    CandidateANamespaceMode,
)
from people_counter.fabric_production_shadow_jobs import (
    DIAGNOSTICS_ROOT,
    PACKAGE_DISTRIBUTION,
    PACKAGE_VERSION,
    SJD_NAMES,
    ShadowDiagnostics,
    build_sjd_v2_definition,
    export_sjd_definitions,
    fixed_provenance,
    run_shadow_entry,
    sjd_definition_bytes,
    thin_main_source,
    validate_observed_provenance,
)


EXPORT_ROOT = Path("fabric/candidate_a/production_shadow")
EXPORT_HASHES = {
    "control": "fe114799101c7914b679452d5f641a01c61a1b33120f97504bb6c8b0717f8431",
    "process": "94789042faced36c6e4cf557e1e179fe8304f2c9dae6bfb3a4ec1f1235ebe05f",
    "reconcile": "0e6bada6871f389c5495a501076241e7f7f10c86b4207885e9405ee637250f15",
}


def test_observed_provenance_fails_before_runner_on_wrong_wheel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_production_shadow_jobs.observed_provenance",
        lambda: {
            "package_version": "9.9.9",
            "fabric": None,
            "spark": None,
            "python": "3.12",
            "java": None,
        },
    )
    called: list[bool] = []
    with pytest.raises(RuntimeError, match="package version differs"):
        run_shadow_entry(
            "control",
            lambda argv: called.append(True) or 0,
            diagnostics=ShadowDiagnostics(
                "control", writer=MemoryCreateOnlyWriter(), invocation_id="wrong-wheel"
            ),
        )
    assert called == []


def test_runtime_provenance_validation_checks_present_fabric_stack() -> None:
    validate_observed_provenance(
        {
            "package_version": PACKAGE_VERSION,
            "fabric": "2.0",
            "spark": "4.1.1",
            "python": "3.13.7",
            "java": "21.0.8",
        }
    )
    with pytest.raises(RuntimeError, match="Spark runtime"):
        validate_observed_provenance(
            {
                "package_version": PACKAGE_VERSION,
                "fabric": "2.0",
                "spark": "3.5.0",
                "python": "3.13.7",
                "java": "21",
            }
        )


class MemoryCreateOnlyWriter:
    def __init__(self) -> None:
        self.content: dict[str, str] = {}

    def create_text(self, path: str, content: str) -> None:
        if path in self.content:
            raise FileExistsError(path)
        self.content[path] = content


def _parts(definition: dict[str, object]) -> dict[str, bytes]:
    body = definition["definition"]
    assert isinstance(body, dict)
    return {
        str(part["path"]): base64.b64decode(str(part["payload"]))
        for part in body["parts"]
    }


def _diagnostics(job: str) -> tuple[ShadowDiagnostics, MemoryCreateOnlyWriter]:
    writer = MemoryCreateOnlyWriter()
    return (
        ShadowDiagnostics(
            job,
            writer=writer,
            invocation_id=f"{job}-invocation-001",
        ),
        writer,
    )


@pytest.mark.parametrize(
    ("module_name", "job"),
    [
        (
            "people_counter.fabric_production_shadow_control",
            "control",
        ),
        (
            "people_counter.fabric_production_shadow_process",
            "process",
        ),
    ],
)
def test_live_entries_dispatch_only_to_fixed_signed_driver(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    job: str,
) -> None:
    module = __import__(module_name, fromlist=["main"])
    calls: list[tuple[object, object]] = []

    def live(arguments, **keywords):
        calls.append((arguments, keywords))
        return 0

    monkeypatch.setattr(module, "live_main", live)
    marker_files, marker_spark = object(), object()

    assert module.main(
        ["snapshot"], files=marker_files, spark=marker_spark
    ) == 0
    assert calls == [
        (
            ["snapshot"],
            {"job": job, "files": marker_files, "spark": marker_spark},
        )
    ]


def test_reconcile_entry_dispatches_only_to_fixed_signed_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import people_counter.fabric_production_shadow_reconcile as reconcile
    from people_counter.fabric_production_shadow_reconcile import main

    calls: list[tuple[object, object]] = []

    def live(arguments, **keywords):
        calls.append((arguments, keywords))
        return 0

    monkeypatch.setattr(reconcile, "live_main", live)
    marker_files, marker_spark = object(), object()

    assert main(
        ["compare"], files=marker_files, spark=marker_spark
    ) == 0
    assert calls == [
        (
            ["compare"],
            {
                "job": "reconcile",
                "files": marker_files,
                "spark": marker_spark,
            },
        )
    ]


def test_diagnostics_are_fixed_provenance_create_only_stages() -> None:
    diagnostics, writer = _diagnostics("control")
    path, digest = diagnostics.mark("started", "RUNNING", {"operation": "claim"})

    assert path == (
        f"{DIAGNOSTICS_ROOT}/job=control/"
        "invocation=control-invocation-001/stages/00-started.json"
    )
    content = writer.content[path]
    assert hashlib.sha256(content.encode()).hexdigest() == digest
    payload = json.loads(content)
    assert payload["artifact_binding"] == {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }
    assert payload["fixed_provenance"] == fixed_provenance()
    assert payload["fixed_provenance"]["package"] == {
        "distribution": PACKAGE_DISTRIBUTION,
        "version": PACKAGE_VERSION,
    }
    assert payload["namespace"] == "PRODUCTION_SHADOW"
    with pytest.raises(FileExistsError):
        ShadowDiagnostics(
            "control",
            writer=writer,
            invocation_id="control-invocation-001",
        ).mark("started", "RUNNING")


@pytest.mark.parametrize("job", SJD_NAMES)
def test_sjd_policy_is_fixed_installed_wheel_only_and_retry_free(job: str) -> None:
    definition = build_sjd_v2_definition(job)
    assert definition["definition"]["format"] == "SparkJobDefinitionV2"
    parts = _parts(definition)
    assert set(parts) == {"Main/main.py", "SparkJobDefinitionV1.json"}
    assert not any(path.startswith("Libs/") for path in parts)
    assert parts["Main/main.py"] == thin_main_source(job)
    source = parts["Main/main.py"].decode("utf-8")
    assert f"people_counter.fabric_production_shadow_{job} import main" in source
    assert "sys.path" not in source

    metadata = json.loads(parts["SparkJobDefinitionV1.json"])
    assert metadata == {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": "",
        "defaultLakehouseArtifactId": LAKEHOUSE_ID,
        "environmentArtifactId": ENVIRONMENT_ID,
        "executableFile": "main.py",
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    }
    if job == "process":
        assert metadata["retryPolicy"] is None


@pytest.mark.parametrize("job", SJD_NAMES)
def test_export_bytes_and_sha256_are_deterministic(job: str) -> None:
    content = sjd_definition_bytes(job)
    export = EXPORT_ROOT / f"{job}.SparkJobDefinitionV2.json"

    assert content == sjd_definition_bytes(job)
    assert export.read_bytes() == content
    assert hashlib.sha256(content).hexdigest() == EXPORT_HASHES[job]
    assert (
        EXPORT_ROOT / job / "Main" / "main.py"
    ).read_bytes() == thin_main_source(job)


def test_export_api_is_idempotent_for_reviewed_definition_bytes() -> None:
    exported = export_sjd_definitions(EXPORT_ROOT)

    assert set(exported) == set(SJD_NAMES)
    assert {
        job: values["sha256"] for job, values in exported.items()
    } == EXPORT_HASHES
    assert {
        job: values["path"] for job, values in exported.items()
    } == {
        job: str(EXPORT_ROOT / f"{job}.SparkJobDefinitionV2.json")
        for job in SJD_NAMES
    }


@pytest.mark.parametrize("job", ["", "gold", "Control", "../control"])
def test_only_fixed_sjd_names_are_accepted(job: str) -> None:
    with pytest.raises(ValueError, match="unsupported production-shadow job"):
        build_sjd_v2_definition(job)


def test_nonzero_job_result_records_failed_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_production_shadow_jobs.observed_provenance",
        lambda: {
            "package_version": PACKAGE_VERSION,
            "fabric": None,
            "spark": None,
            "python": "3.12",
            "java": None,
        },
    )
    diagnostics, writer = _diagnostics("reconcile")
    assert run_shadow_entry(
        "reconcile", lambda argv: 1, diagnostics=diagnostics
    ) == 1
    stages = [json.loads(value) for value in writer.content.values()]
    assert [stage["status"] for stage in stages] == ["RUNNING", "FAILED"]
    assert stages[-1]["detail"] == {"exit_code": 1}

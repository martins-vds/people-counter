from __future__ import annotations

import base64
import hashlib
import json
import shlex
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import pytest

import people_counter.fabric_production_shadow_backend as backend
from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    WORKSPACE_ID,
    FabricCandidateAConfig,
)
from people_counter.fabric_production_migration_tool import PROJECT_WHEEL
from people_counter.fabric_production_routing import AllowlistRow
from people_counter.fabric_production_shadow import (
    AuthorizationResult,
    AuthorizationRow,
    LegacySourceRows,
    MigrationJournalProof,
    ProductionShadowPlan,
    ReviewReceipt,
)
from people_counter.fabric_production_shadow_jobs import build_sjd_v2_definition
from people_counter.fabric_production_shadow_live import (
    RESULT_SCHEMA,
    request_path,
    result_path,
)
from people_counter.fabric_reflex_definition import REFLEX_ID


NOW = datetime(2026, 10, 5, 4, 0, tzinfo=timezone.utc)
WORK_ID = "work-001"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


class MemoryFiles:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.values

    def read_bytes(self, path: str) -> bytes:
        return self.values[path]

    def create_bytes(self, path: str, content: bytes) -> None:
        if path in self.values:
            raise FileExistsError(path)
        self.values[path] = content


def environment_state(wheel: str = PROJECT_WHEEL) -> dict[str, Any]:
    compute = {"runtimeVersion": "2.0"}
    return {
        "item": {
            "id": ENVIRONMENT_ID,
            "properties": {"publishDetails": {"state": "Success"}},
        },
        "published_compute": compute,
        "published_libraries": {
            "libraries": [
                {"libraryType": "Custom", "name": wheel},
                {"libraryType": "External", "name": "numpy", "version": "2.4.6"},
            ]
        },
        "staged_compute": deepcopy(compute),
        "staged_libraries": {
            "customLibraries": {
                "jarFiles": [],
                "pyFiles": [],
                "rTarFiles": [],
                "wheelFiles": [wheel],
            },
            "environmentYml": "dependencies:\n  - pip:\n      - numpy==2.4.6\n",
        },
    }


class FakeAPI:
    def __init__(self, files: MemoryFiles, *, include_sjds: bool = True) -> None:
        self.files = files
        self.items = [
            {"id": ENVIRONMENT_ID, "displayName": "people-counter-dev", "type": "Environment"},
            {"id": LAKEHOUSE_ID, "displayName": "people_counter_dev", "type": "Lakehouse"},
            {"id": REFLEX_ID, "displayName": "pc_manifest_arrival_activator", "type": "Reflex"},
            *[
                {"id": item_id, "displayName": name, "type": "DataPipeline"}
                for name, item_id in backend.WRITER_ITEMS
            ],
        ]
        self.definitions: dict[tuple[str, str], Mapping[str, Any]] = {
            ("Reflex", REFLEX_ID): {"definition": {"parts": []}},
            **{
                ("DataPipeline", item_id): {"definition": {"parts": []}}
                for _, item_id in backend.WRITER_ITEMS
            },
        }
        self.environment = environment_state()
        self.uploads: list[tuple[str, bytes]] = []
        self.deleted: list[str] = []
        self.published = 0
        self.run_arguments = ""
        self.result: Mapping[str, Any] = {"ok": True}
        if include_sjds:
            for index, (job, display_name) in enumerate(
                backend.SJD_DISPLAY_NAMES.items(), start=1
            ):
                item_id = f"00000000-0000-4000-8000-{index:012d}"
                self.items.append(
                    {
                        "id": item_id,
                        "displayName": display_name,
                        "type": "SparkJobDefinition",
                    }
                )
                self.definitions[("SparkJobDefinition", item_id)] = (
                    build_sjd_v2_definition(job)
                )

    def list_items(self):
        return deepcopy(self.items)

    def get_definition(self, item_type: str, item_id: str):
        return deepcopy(self.definitions[(item_type, item_id)])

    def list_schedules(self, item_id: str, job_type: str):
        assert job_type in {"Pipeline", "sparkjob"}
        return []

    def list_job_instances(self, item_id: str):
        return []

    def environment_state(self):
        return deepcopy(self.environment)

    def delete_staged_environment_library(self, name: str):
        self.deleted.append(name)
        self.environment["staged_libraries"]["customLibraries"]["wheelFiles"].remove(name)

    def upload_environment_wheel(self, name: str, content: bytes):
        self.uploads.append((name, content))
        self.environment["staged_libraries"]["customLibraries"]["wheelFiles"] = [name]

    def publish_environment(self):
        self.published += 1
        wheels = self.environment["staged_libraries"]["customLibraries"]["wheelFiles"]
        external = [
            item
            for item in self.environment["published_libraries"]["libraries"]
            if item["libraryType"] != "Custom"
        ]
        self.environment["published_libraries"]["libraries"] = [
            {"libraryType": "Custom", "name": name} for name in wheels
        ] + external
        return {"status": "Succeeded"}

    def create_shadow_sjd(self, display_name: str, definition: Mapping[str, Any]):
        job = next(
            key for key, value in backend.SJD_DISPLAY_NAMES.items()
            if value == display_name
        )
        item_id = f"10000000-0000-4000-8000-{len(self.items):012d}"
        self.items.append(
            {
                "id": item_id,
                "displayName": display_name,
                "type": "SparkJobDefinition",
            }
        )
        self.definitions[("SparkJobDefinition", item_id)] = deepcopy(definition)
        assert definition == build_sjd_v2_definition(job)
        return {"id": item_id}

    def update_shadow_sjd(
        self,
        item_id: str,
        display_name: str,
        definition: Mapping[str, Any],
    ):
        self.definitions[("SparkJobDefinition", item_id)] = deepcopy(definition)
        return {"id": item_id, "displayName": display_name}

    def run_sjd(self, item_id: str, arguments: str):
        self.run_arguments = arguments
        values = shlex.split(arguments)
        invocation = values[values.index("--invocation-id") + 1]
        command = values[0]
        evidence = {
            "schema": RESULT_SCHEMA,
            "command": command,
            "invocation_id": invocation,
            "exit_code": 0,
            "runtime": {},
            "result": dict(self.result),
        }
        self.files.create_bytes(
            result_path(invocation),
            json.dumps(
                evidence, allow_nan=False, separators=(",", ":"), sort_keys=True
            ).encode()
            + b"\n",
        )
        return "job-001"

    def get_job_instance(self, item_id: str, instance_id: str):
        assert instance_id == "job-001"
        return {"status": "Completed"}


def identity() -> AllowlistRow:
    return AllowlistRow(
        work_id=WORK_ID,
        camera_sha256=HASH_A,
        location_sha256=HASH_B,
        model_sha256=HASH_C,
        source_sha256=HASH_D,
        config_sha256=HASH_E,
    )


def legacy_rows() -> LegacySourceRows:
    shared = {
        **identity().as_dict(),
        "attempt_id": "attempt-001",
        "output_path": "Files/people-counter/candidate-a/v1/out.json",
        "output_sha256": HASH_F,
    }
    return LegacySourceRows(
        work={
            **shared,
            "status": "SUCCEEDED",
            "committed_attempt_id": "attempt-001",
            "payload": {"duration_seconds": 10.0, "runtime_key": "cpu"},
        },
        attempt={**shared, "status": "SUCCEEDED"},
        publication={**shared, "publication_sequence": 7},
        committed_view={**shared, "publication_sequence": 7},
    )


def plan_and_receipt() -> tuple[ProductionShadowPlan, ReviewReceipt]:
    plan = ProductionShadowPlan(
        plan_id="shadow-001",
        work=identity(),
        migration_plan_sha256="9" * 64,
        created_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=10),
        legacy_source_rows_sha256="8" * 64,
        legacy_output_sha256=HASH_F,
    )
    receipt = ReviewReceipt(
        plan_sha256=plan.sha256,
        work_id=WORK_ID,
        work_identity_sha256=plan.identity_sha256,
        expires_at=plan.expires_at,
        reviewed_at=NOW,
        reviewer="release-control",
    )
    return plan, receipt


def test_rest_snapshot_reads_exact_fixed_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files, api = MemoryFiles(), FakeAPI(MemoryFiles())
    selected = backend.FabricShadowControllerBackend(api, files)
    monkeypatch.setattr(
        backend,
        "_definition_parts",
        lambda value: {"ReflexEntities.json": b"{}"},
    )
    monkeypatch.setattr(
        backend,
        "parse_reflex_rule_definition",
        lambda raw: SimpleRule(False),
    )

    value = selected._rest_snapshot()

    assert value["reflex"]["id"] == REFLEX_ID
    assert value["reflex"]["active"] is False
    assert [item["id"] for item in value["pipelines"]] == [
        item_id for _, item_id in backend.WRITER_ITEMS
    ]
    assert [item["display_name"] for item in value["sjds"]] == list(
        backend.SJD_DISPLAY_NAMES.values()
    )


def test_job_inventory_rejects_unknown_statuses() -> None:
    completed = {"id": "done", "status": "Completed"}
    running = {"id": "active", "status": "Running"}
    assert backend._active_jobs_exact([completed, running]) == [running]
    for invalid in (
        [{"id": "missing"}],
        [{"id": "unknown", "status": "Paused"}],
        [None],
    ):
        with pytest.raises(
            backend.LiveShadowBackendError,
            match="job inventory",
        ):
            backend._active_jobs_exact(invalid)


class SimpleRule:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def to_dict(self) -> dict[str, bool]:
        return {"enabled": self.enabled}


def test_signed_invocation_polls_and_requires_zero_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    selected = backend.FabricShadowControllerBackend(
        api,
        files,
        clock=lambda: NOW.timestamp(),
        monotonic=lambda: 0.0,
        sleep=lambda value: None,
    )
    monkeypatch.setattr(
        selected,
        "_rest_snapshot",
        lambda: {"reflex": {"id": REFLEX_ID, "active": False}},
    )

    result = selected._invoke("control", "status", {"selected_work_id": WORK_ID})

    assert result == {"ok": True}
    request = next(
        value for path, value in files.values.items() if path.endswith("request.json")
    )
    envelope = json.loads(request)
    assert set(envelope) == {
        "algorithm",
        "payload",
        "payload_sha256",
        "signature",
    }
    assert envelope["payload"]["command"] == "status"
    assert envelope["payload"]["created_at"] == NOW.isoformat()
    assert envelope["payload"]["rest_snapshot"] == {
        "reflex": {"id": REFLEX_ID, "active": False}
    }
    assert envelope["payload"]["selected_work_id"] == WORK_ID
    assert envelope["payload"]["artifact_binding"] == {
        "workspace_id": WORKSPACE_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "environment_id": ENVIRONMENT_ID,
        "migration_id": backend.MIGRATION_ID,
    }
    arguments = shlex.split(api.run_arguments)
    assert arguments[0] == "status"
    assert arguments[1] == "--invocation-id"
    assert arguments[3] == "--request-hmac-key"
    assert len(arguments[2]) == 32
    assert len(arguments[4]) == 64
    assert "work-001" not in api.run_arguments


@pytest.mark.parametrize(
    ("installed_wheel", "deleted"),
    (("old.whl", ["old.whl"]), (PROJECT_WHEEL, [PROJECT_WHEEL])),
)
def test_deploy_publishes_only_project_wheel_and_three_readbacks(
    installed_wheel: str,
    deleted: list[str],
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files, include_sjds=False)
    api.environment = environment_state(installed_wheel)
    wheel = b"PK fixed wheel"
    selected = backend.FabricShadowControllerBackend(
        api,
        files,
        wheel_builder=lambda: (wheel, hashlib.sha256(wheel).hexdigest()),
    )

    deployed = selected._deploy_all()

    assert set(deployed) == set(backend.SJD_DISPLAY_NAMES.values())
    assert api.deleted == deleted
    assert api.uploads == [(PROJECT_WHEEL, wheel)]
    assert api.published == 1
    assert [
        item["name"]
        for item in api.environment["published_libraries"]["libraries"]
        if item["libraryType"] == "External"
    ] == ["numpy"]
    assert selected._deploy_all() is deployed


def test_shadow_deployment_state_requires_three_exact_definitions() -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    selected = backend.FabricShadowControllerBackend(api, files)

    value = selected.shadow_deployment_state()

    assert value["ready"] is True
    assert set(value["jobs"]) == {"control", "process", "reconcile"}
    for job, display_name in backend.SJD_DISPLAY_NAMES.items():
        state = value["jobs"][job]
        item_id = next(
            str(item["id"])
            for item in api.items
            if item.get("displayName") == display_name
        )
        definition = api.definitions[("SparkJobDefinition", item_id)]
        assert state == {
            "definition_sha256": backend._definition_sha256(definition),
            "display_name": display_name,
            "item_id": item_id,
            "state": "READY",
        }


def test_sjd_definition_hash_accepts_only_fabric_lossless_normalization() -> None:
    expected = build_sjd_v2_definition("control")
    parts = backend._definition_parts(expected)
    metadata = json.loads(parts["SparkJobDefinitionV1.json"])
    observed = {
        "definition": {
            "parts": [
                {
                    "path": "SparkJobDefinitionV1.json",
                    "payload": base64.b64encode(
                        json.dumps(metadata, indent=2).encode()
                    ).decode(),
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "Main/main.py",
                    "payload": base64.b64encode(parts["Main/main.py"]).decode(),
                    "payloadType": "InlineBase64",
                },
                {
                    "path": ".platform",
                    "payload": base64.b64encode(b'{"generated":true}').decode(),
                    "payloadType": "InlineBase64",
                },
            ]
        }
    }

    assert backend._definition_sha256(observed) == backend._definition_sha256(
        expected
    )
    changed = deepcopy(observed)
    changed["definition"]["parts"][1]["payload"] = base64.b64encode(
        parts["Main/main.py"] + b"\n"
    ).decode()
    assert backend._definition_sha256(changed) != backend._definition_sha256(
        expected
    )


def test_backend_plan_pins_registration_claim_process_and_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    wheel = b"PK fixed wheel"
    selected = backend.FabricShadowControllerBackend(
        api,
        files,
        selected_work_id=WORK_ID,
        wheel_builder=lambda: (wheel, hashlib.sha256(wheel).hexdigest()),
    )
    plan, receipt = plan_and_receipt()
    from people_counter.fabric_production_shadow import ingest_legacy_source

    source = ingest_legacy_source(legacy_rows())
    selected.prepare_plan_context(
        plan=plan,
        receipt=receipt,
        safety_token=plan.safety_token,
        source=source,
    )
    authorization = AuthorizationRow(
        authorization_id="1" * 64,
        plan_sha256=plan.sha256,
        work=plan.work,
        work_identity_sha256=plan.identity_sha256,
        expires_at=plan.expires_at,
        authorized_at=receipt.reviewed_at,
        safety_token_sha256="2" * 64,
        migration_journal_sha256="3" * 64,
        reviewer=receipt.reviewer,
    )
    selected._authorization = AuthorizationResult(authorization, False, False)
    calls: list[tuple[str, str, Mapping[str, Any]]] = []

    def invoke(job: str, command: str, payload=None):
        value = dict(payload or {})
        calls.append((job, command, value))
        if command == "register":
            return {"work_id": WORK_ID}
        if command == "claim":
            return {
                "batch_id": "batch-001",
                "items": [{"work_id": WORK_ID}],
            }
        if command == "process":
            return {"processed": True, "batch_id": "batch-001"}
        if command == "route":
            if value["route_kind"] == "shadow" and not selected._processed:
                return {"route": None}
            return {
                "work_id": WORK_ID,
                "attempt_id": "attempt-001",
                "logical_identity_sha256": "4" * 64,
                "fence": 1,
                "pointer_fence": 1,
                "output_path": (
                    "Files/_shadow/people-counter/candidate-a/v1/out.json"
                    if value["route_kind"] == "shadow"
                    else "Files/people-counter/candidate-a/v1/out.json"
                ),
                "output_sha256": HASH_F,
                "sealed": True,
                "committed": True,
                "pointer_attempt_id": "attempt-001",
                "publication_sequence": 1,
                "publication_count": 1,
                "authorization_id": authorization.authorization_id,
                "plan_sha256": plan.sha256,
                "provenance": {
                    "package_version": "0.9.11",
                    "package_sha256": hashlib.sha256(wheel).hexdigest(),
                    "python_version": "3.13",
                    "spark_version": "4.1.1",
                    "java_version": "21",
                    "fabric_runtime": "2.0",
                },
                "identity": identity().as_dict(),
                "records": [{"count": 2.0}],
                "logical_total": 2.0,
                "frame_count": 1,
                "timestamp": NOW.isoformat(),
            }
        raise AssertionError(command)

    monkeypatch.setattr(selected, "_invoke", invoke)
    config = FabricCandidateAConfig.production_shadow()
    assert selected.committed_route(WORK_ID, config=config) is None
    registration = source.registration_payload()
    selected.register(registration, config=config)
    claim = selected.claim(WORK_ID, config=config)
    output = selected.process(
        claim, config=config, provenance=selected.runtime_provenance()
    )
    selected.seal(claim, output, config=config)
    route = selected.publish(
        claim,
        authorization=authorization,
        config=config,
        provenance=selected.runtime_provenance(),
    )

    assert route.work_id == WORK_ID
    assert route.output_path.startswith(
        "Files/_shadow/people-counter/candidate-a/v1/"
    )
    assert [(job, command) for job, command, _ in calls] == [
        ("control", "route"),
        ("control", "register"),
        ("control", "claim"),
        ("process", "process"),
        ("control", "route"),
    ]
    assert all(
        value.get("selected_work_id", WORK_ID) == WORK_ID
        for _, _, value in calls
    )


def test_backend_rejects_implicit_work_reselection() -> None:
    selected = backend.FabricShadowControllerBackend(
        FakeAPI(MemoryFiles()), MemoryFiles(), selected_work_id=WORK_ID
    )
    config = FabricCandidateAConfig.production_shadow()

    with pytest.raises(backend.LiveShadowBackendError, match="reselected"):
        selected.committed_route("other-work", config=config)
    with pytest.raises(backend.LiveShadowBackendError, match="plan-pinned"):
        selected.register({"work_id": "other-work"}, config=config)
    with pytest.raises(backend.LiveShadowBackendError, match="exact plan"):
        selected.claim("other-work", config=config)


def test_backend_sjd_state_selected_route_and_fixed_onelake_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    selected = backend.FabricShadowControllerBackend(
        api, files, selected_work_id=WORK_ID
    )

    state = selected.sjd_state(
        backend.SJD_DISPLAY_NAMES["control"]
    )
    assert state is not None
    assert state["display_name"] == backend.SJD_DISPLAY_NAMES["control"]
    assert state["schedules"] == []
    with pytest.raises(backend.LiveShadowBackendError, match="outside"):
        selected.sjd_state("not-fixed")

    rows = legacy_rows()
    monkeypatch.setattr(
        selected,
        "_ensure_snapshot",
        lambda **values: {
            "eligible_routes": [
                {
                    "work_id": WORK_ID,
                    "rows": {
                        "work": rows.work,
                        "attempt": rows.attempt,
                        "publication": rows.publication,
                        "committed_view": rows.committed_view,
                    },
                }
            ]
        },
    )
    assert selected.read_legacy_rows().work["work_id"] == WORK_ID
    assert len(selected.eligible_legacy_rows()) == 1
    assert backend.ShadowAzureOneLakeFiles._path(
        "Files/_shadow/people-counter/candidate-a/v1/controller/x.json"
    ).startswith(LAKEHOUSE_ID + "/Files/_shadow/")
    with pytest.raises(backend.LiveShadowBackendError, match="escaped"):
        backend.ShadowAzureOneLakeFiles._path(
            "Files/people-counter/not-shadow.json"
        )


def test_backend_authorization_append_and_replay_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    selected = backend.FabricShadowControllerBackend(
        api, files, selected_work_id=WORK_ID
    )
    plan, receipt = plan_and_receipt()
    from people_counter.fabric_production_shadow import ingest_legacy_source

    source = ingest_legacy_source(legacy_rows())
    selected.prepare_plan_context(
        plan=plan,
        receipt=receipt,
        safety_token=plan.safety_token,
        source=source,
    )
    migration = MigrationJournalProof(
        backend.MIGRATION_ID, "APPLIED", "9" * 64, "7" * 64
    )
    monkeypatch.setattr(selected, "migration_proof", lambda: migration)
    calls: list[Mapping[str, Any]] = []

    def invoke(job: str, command: str, payload=None):
        assert (job, command) == ("control", "authorize")
        calls.append(dict(payload))
        return {"protocol_state": "COMMITTED"}

    monkeypatch.setattr(selected, "_invoke", invoke)
    authorization_id = hashlib.sha256(
        json.dumps(
            {
                "expires_at": plan.expires_at.isoformat(),
                "plan_sha256": plan.sha256,
                "work_id": WORK_ID,
                "work_identity_sha256": plan.identity_sha256,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    allowlist = {
        **identity().as_dict(),
        "plan_sha256": plan.sha256,
        "approved_at": receipt.reviewed_at.isoformat(),
        "approved_by": receipt.reviewer,
    }
    audit = {
        "audit_id": authorization_id,
        "work_id": WORK_ID,
        "plan_sha256": plan.sha256,
        "shadow_attempt_id": "AUTHORIZATION",
        "legacy_attempt_id": plan.identity_sha256,
        "comparison_sha256": hashlib.sha256(
            plan.safety_token.encode()
        ).hexdigest(),
        "critical_findings": 0,
        "recorded_at": receipt.reviewed_at.isoformat(),
    }

    selected.append_authorization(allowlist, audit)

    assert calls[0]["authorization"]["work"] == identity().as_dict()
    assert calls[0]["authorization"]["legacy_output_sha256"] == HASH_F
    assert selected.authorization_result(plan.sha256).row.authorization_id == (
        authorization_id
    )
    with pytest.raises(backend.LiveShadowBackendError, match="plan hash"):
        selected.authorization_result("0" * 64)


def test_backend_authorization_result_reconstructs_exact_stored_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = backend.FabricShadowControllerBackend(
        FakeAPI(MemoryFiles()), MemoryFiles(), selected_work_id=WORK_ID
    )
    plan, receipt = plan_and_receipt()
    from people_counter.fabric_production_shadow import ingest_legacy_source

    selected.prepare_plan_context(
        plan=plan,
        receipt=receipt,
        safety_token=plan.safety_token,
        source=ingest_legacy_source(legacy_rows()),
    )
    authorization_id = hashlib.sha256(
        json.dumps(
            {
                "expires_at": plan.expires_at.isoformat(),
                "plan_sha256": plan.sha256,
                "work_id": WORK_ID,
                "work_identity_sha256": plan.identity_sha256,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    allow = {
        **identity().as_dict(),
        "plan_sha256": plan.sha256,
        "approved_at": receipt.reviewed_at.isoformat(),
        "approved_by": receipt.reviewer,
    }
    audit = {
        "audit_id": authorization_id,
        "work_id": WORK_ID,
        "plan_sha256": plan.sha256,
        "shadow_attempt_id": "AUTHORIZATION",
        "legacy_attempt_id": plan.identity_sha256,
        "comparison_sha256": hashlib.sha256(
            plan.safety_token.encode()
        ).hexdigest(),
        "critical_findings": 0,
        "recorded_at": receipt.reviewed_at.isoformat(),
    }
    monkeypatch.setattr(selected, "read_allowlist", lambda work_id: [allow])
    monkeypatch.setattr(selected, "read_audit", lambda value: [audit])
    monkeypatch.setattr(
        selected,
        "migration_proof",
        lambda: MigrationJournalProof(
            backend.MIGRATION_ID, "APPLIED", "9" * 64, "7" * 64
        ),
    )
    from people_counter.fabric_production_shadow import authorization_intent

    expected_row = AuthorizationRow(
        authorization_id=authorization_id,
        plan_sha256=plan.sha256,
        work=plan.work,
        work_identity_sha256=plan.identity_sha256,
        expires_at=plan.expires_at,
        authorized_at=receipt.reviewed_at,
        safety_token_sha256=hashlib.sha256(
            plan.safety_token.encode()
        ).hexdigest(),
        migration_journal_sha256="7" * 64,
        reviewer=receipt.reviewer,
    )
    committed = (
        "Files/_shadow/people-counter/candidate-a/v1/controller/live/"
        f"authorization/{authorization_id}/30-committed.json"
    )
    selected.files.create_bytes(
        committed,
        json.dumps(
            {
                "state": "COMMITTED",
                "intent": authorization_intent(
                    expected_row, allow, audit
                ).as_dict(),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode(),
    )

    result = selected.authorization_result(plan.sha256)

    assert result.replayed
    assert result.row.authorization_id == authorization_id

    selected._authorization = None
    monkeypatch.setattr(
        selected,
        "read_allowlist",
        lambda work_id: [{**allow, "approved_by": "conflict"}],
    )
    with pytest.raises(backend.LiveShadowBackendError, match="conflict"):
        selected.authorization_result(plan.sha256)


def test_backend_rejects_nonzero_result_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    selected = backend.FabricShadowControllerBackend(
        api,
        files,
        monotonic=lambda: 0.0,
        sleep=lambda value: None,
    )
    monkeypatch.setattr(
        selected,
        "_rest_snapshot",
        lambda: {"reflex": {"id": REFLEX_ID, "active": False}},
    )
    original = api.run_sjd

    def nonzero(item_id: str, arguments: str):
        instance = original(item_id, arguments)
        invocation = shlex.split(arguments)[2]
        evidence = json.loads(files.values[result_path(invocation)])
        evidence["exit_code"] = 7
        files.values[result_path(invocation)] = (
            json.dumps(
                evidence, allow_nan=False, separators=(",", ":"), sort_keys=True
            ).encode()
            + b"\n"
        )
        return instance

    monkeypatch.setattr(api, "run_sjd", nonzero)

    with pytest.raises(backend.LiveShadowBackendError, match="nonzero"):
        selected._invoke("control", "status")


def test_backend_recovers_safe_failure_when_job_has_no_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    selected = backend.FabricShadowControllerBackend(
        api,
        files,
        monotonic=lambda: 0.0,
        sleep=lambda value: None,
    )
    monkeypatch.setattr(
        selected,
        "_rest_snapshot",
        lambda: {"reflex": {"id": REFLEX_ID, "active": False}},
    )

    def missing_result(item_id: str, arguments: str) -> str:
        api.run_arguments = arguments
        return "job-001"

    monkeypatch.setattr(api, "run_sjd", missing_result)

    with pytest.raises(backend.LiveShadowBackendError, match="no result"):
        selected._invoke("control", "status")


def test_absent_sjd_is_validated_before_request_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    api = FakeAPI(files, include_sjds=False)
    selected = backend.FabricShadowControllerBackend(api, files)
    monkeypatch.setattr(
        selected,
        "_rest_snapshot",
        lambda: pytest.fail("request construction must not start"),
    )

    with pytest.raises(
        backend.LiveShadowBackendError,
        match="exactly one deployed control shadow SJD is required",
    ):
        selected._invoke("control", "snapshot")

    assert files.values == {}
    assert api.run_arguments == ""


def test_definition_mismatch_is_validated_before_request_write() -> None:
    files = MemoryFiles()
    api = FakeAPI(files)
    control_id = next(
        str(item["id"])
        for item in api.items
        if item.get("displayName") == backend.SJD_DISPLAY_NAMES["control"]
    )
    api.definitions[("SparkJobDefinition", control_id)] = {
        "definition": {"format": "SparkJobDefinitionV2", "parts": []}
    }
    selected = backend.FabricShadowControllerBackend(api, files)

    with pytest.raises(
        backend.LiveShadowBackendError,
        match="definition",
    ):
        selected._invoke("control", "snapshot")

    assert files.values == {}
    assert api.run_arguments == ""


def test_status_classifies_old_request_only_artifacts_as_unstarted() -> None:
    files = MemoryFiles()
    api = FakeAPI(files, include_sjds=False)
    orphan_ids = ("orphan-request-1", "orphan-request-2")
    for invocation in orphan_ids:
        files.values[request_path(invocation)] = b"old signed request\n"
    files.list_invocations = lambda: list(orphan_ids)  # type: ignore[attr-defined]
    selected = backend.FabricShadowControllerBackend(api, files)

    value = selected.status()

    assert value["predeployment_state"]["ready"] is False
    assert value["invocation_diagnostics"] == [
        {
            "classification": "UNSTARTED_DIAGNOSTIC",
            "invocation_id": invocation,
        }
        for invocation in orphan_ids
    ]
    assert set(files.values) == {request_path(value) for value in orphan_ids}

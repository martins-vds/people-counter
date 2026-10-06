from __future__ import annotations

import base64
import hashlib
import io
import json
import zipfile
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

import people_counter.fabric_production_migration_tool as tool
from people_counter.fabric_canary_tool import HTTPResponse, StaticTokenProvider
from people_counter.fabric_production_migration import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    WORKSPACE_ID,
    canonical_json,
)
from people_counter.fabric_production_migration_sjd import (
    build_migration_sjd_definition,
)
from people_counter.fabric_reflex_definition import REFLEX_ID


FIXTURE = Path(__file__).parent / "fixtures" / "reflex_entities_live_sanitized.json"
NOW = 2_000.0
RUN_ID = "migration-run-001"
OWNER = "reviewer"
LEASE = "one-use-lease"
SAFETY = "reviewed-safety-token"


def definition(path: str, content: bytes) -> dict[str, object]:
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(content).decode(),
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }


def environment_state(wheel: str = "people_counter-0.7.1-py3-none-any.whl"):
    compute = {"runtimeVersion": "2.0"}
    published = {
        "libraries": (
            ([{"libraryType": "Custom", "name": wheel}] if wheel else [])
            + [{"libraryType": "External", "name": "numpy", "version": "2.4.6"}]
        )
    }
    staged = {
        "customLibraries": {
            "jarFiles": [],
            "pyFiles": [],
            "rTarFiles": [],
            "wheelFiles": [wheel] if wheel else [],
        },
        "environmentYml": "dependencies:\n  - pip:\n      - numpy==2.4.6\n",
    }
    return {
        "item": {
            "id": ENVIRONMENT_ID,
            "properties": {"publishDetails": {"state": "Success"}},
        },
        "published_compute": compute,
        "published_libraries": published,
        "staged_compute": deepcopy(compute),
        "staged_libraries": staged,
    }


def fixed_items(include_sjd: bool = True) -> list[dict[str, object]]:
    values = [
        {
            "displayName": tool.ENVIRONMENT_NAME,
            "id": ENVIRONMENT_ID,
            "type": "Environment",
        },
        {
            "displayName": tool.LAKEHOUSE_NAME,
            "id": LAKEHOUSE_ID,
            "type": "Lakehouse",
        },
        {
            "displayName": tool.REFLEX_NAME,
            "id": REFLEX_ID,
            "type": "Reflex",
        },
    ]
    values.extend(
        {"displayName": name, "id": item_id, "type": "DataPipeline"}
        for name, item_id in tool.WRITER_ITEMS
    )
    if include_sjd:
        values.append(
            {
                "displayName": tool.MIGRATION_SJD_NAME,
                "id": "11111111-1111-4111-8111-111111111111",
                "type": "SparkJobDefinition",
            }
        )
    return values


class FakeAPI:
    def __init__(self, *, include_sjd: bool = True) -> None:
        self.items = fixed_items(include_sjd)
        self.definitions: dict[tuple[str, str], Mapping[str, Any]] = {
            ("Reflex", REFLEX_ID): definition(
                "ReflexEntities.json", FIXTURE.read_bytes()
            )
        }
        for name, item_id in tool.WRITER_ITEMS:
            self.definitions[("DataPipeline", item_id)] = definition(
                "pipeline-content.json", canonical_json({"name": name}).encode()
            )
        if include_sjd:
            self.definitions[
                ("SparkJobDefinition", "11111111-1111-4111-8111-111111111111")
            ] = build_migration_sjd_definition()
        self.schedules: dict[str, list[Mapping[str, Any]]] = {
            item_id: [] for _, item_id in tool.WRITER_ITEMS
        }
        self.jobs: dict[str, list[Mapping[str, Any]]] = {
            item_id: [] for _, item_id in tool.WRITER_ITEMS
        }
        self.environment = environment_state()
        self.deleted: list[str] = []
        self.uploads: list[tuple[str, bytes]] = []
        self.published = False
        self.updated: list[str] = []
        self.run_arguments = ""
        self.poll_states: list[Mapping[str, Any]] = [{"status": "Completed"}]

    def list_items(self):
        return deepcopy(self.items)

    def get_definition(self, item_type: str, item_id: str):
        return deepcopy(self.definitions[(item_type, item_id)])

    def list_schedules(self, item_id: str, job_type: str):
        assert job_type == "Pipeline"
        return deepcopy(self.schedules[item_id])

    def list_job_instances(self, item_id: str):
        return deepcopy(self.jobs[item_id])

    def environment_state(self):
        return deepcopy(self.environment)

    def delete_staged_environment_library(self, name: str):
        self.deleted.append(name)
        self.environment["staged_libraries"]["customLibraries"]["wheelFiles"] = []  # type: ignore[index]

    def upload_environment_wheel(self, name: str, content: bytes):
        self.uploads.append((name, content))
        self.environment["staged_libraries"]["customLibraries"]["wheelFiles"] = [name]  # type: ignore[index]

    def publish_environment(self):
        self.published = True
        external = [
            item
            for item in self.environment["published_libraries"]["libraries"]  # type: ignore[index]
            if item["libraryType"] != "Custom"
        ]
        wheels = self.environment["staged_libraries"]["customLibraries"]["wheelFiles"]  # type: ignore[index]
        self.environment["published_libraries"]["libraries"] = [  # type: ignore[index]
            {"libraryType": "Custom", "name": name} for name in wheels
        ] + external
        return {"status": "Succeeded"}

    def create_sjd(self, payload: Mapping[str, Any]):
        item_id = "11111111-1111-4111-8111-111111111111"
        self.items.append(
            {
                "displayName": tool.MIGRATION_SJD_NAME,
                "id": item_id,
                "type": "SparkJobDefinition",
            }
        )
        self.definitions[("SparkJobDefinition", item_id)] = deepcopy(payload)
        return {"id": item_id}

    def update_sjd(self, item_id: str, payload: Mapping[str, Any]):
        self.updated.append(item_id)
        self.definitions[("SparkJobDefinition", item_id)] = deepcopy(payload)
        return {"id": item_id}

    def run_sjd(self, item_id: str, arguments: str):
        assert item_id == "11111111-1111-4111-8111-111111111111"
        self.run_arguments = arguments
        return "22222222-2222-4222-8222-222222222222"

    def get_job_instance(self, item_id: str, job_instance_id: str):
        assert item_id == "11111111-1111-4111-8111-111111111111"
        assert job_instance_id == "22222222-2222-4222-8222-222222222222"
        return self.poll_states.pop(0)


class MemoryFiles:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.creates: list[str] = []
        self.exists_calls: list[str] = []

    def exists(self, path: str) -> bool:
        self.exists_calls.append(path)
        return path in self.values

    def read_bytes(self, path: str) -> bytes:
        return self.values[path]

    def create_bytes(self, path: str, content: bytes) -> None:
        if path in self.values:
            raise FileExistsError(path)
        self.values[path] = content
        self.creates.append(path)


def test_snapshot_is_canonical_external_only_and_exact_allowlist() -> None:
    api = FakeAPI()
    first_id = tool.WRITER_ITEMS[0][1]
    api.schedules[first_id] = [
        {"id": "schedule-1", "enabled": False, "configuration": {"interval": 10}}
    ]
    api.jobs[first_id] = [
        {
            "id": "job-1",
            "status": "Completed",
            "jobType": "Pipeline",
            "invokeType": "Manual",
            "startTimeUtc": "2026-10-01T00:00:00Z",
            "endTimeUtc": "2026-10-01T00:01:00Z",
        }
    ]

    observed = tool.capture_snapshot(api, now=NOW)

    assert observed["schema"].endswith("-v2")
    assert observed["artifact_binding"] == {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }
    assert len(observed["item_definitions"]) == len(tool.WRITER_ITEMS)
    assert observed["reflex"]["rule"]["enabled"] is False
    assert observed["writer_schedules"][0]["schedule_id"] == "schedule-1"
    assert observed["fabric_jobs"] == []
    assert observed["fabric_job_inventory"] == {
        "active_count": 0,
        "all_jobs_sha256": hashlib.sha256(
            canonical_json(
                [
                    {
                        "end_time_utc": "2026-10-01T00:01:00Z",
                        "invoke_type": "Manual",
                        "item_id": first_id,
                        "item_name": tool.WRITER_ITEMS[0][0],
                        "job_type": "Pipeline",
                        "run_id": "job-1",
                        "start_time_utc": "2026-10-01T00:00:00Z",
                        "state": "Completed",
                    }
                ]
            ).encode()
        ).hexdigest(),
        "state_counts": {"completed": 1},
        "total_count": 1,
    }
    for forbidden in ("control_writer_row", "tables", "leases", "pointers", "gold"):
        assert forbidden not in observed
    assert tool.capture_snapshot(api, now=NOW) == observed


def test_recovery_snapshot_requires_exact_terminal_failed_job() -> None:
    class RecoveryAPI(FakeAPI):
        recovery_state: dict[str, object] = {
            "endTimeUtc": "2026-10-04T23:39:47.0438663",
            "failureReason": {
                "errorCode": "Spark_User",
                "message": "failed",
            },
            "id": tool.FAILED_RECOVERY_JOB_ID,
            "itemId": "11111111-1111-4111-8111-111111111111",
            "jobType": tool.JOB_TYPE_SJD,
            "startTimeUtc": "2026-10-04T23:28:57.9082748",
            "status": "Failed",
        }

        def get_job_instance(self, item_id: str, job_instance_id: str):
            if job_instance_id == tool.FAILED_RECOVERY_JOB_ID:
                return deepcopy(self.recovery_state)
            return super().get_job_instance(item_id, job_instance_id)

    api = RecoveryAPI()
    snapshot = tool.capture_recovery_snapshot(api, now=NOW)
    assert snapshot["lock_recovery"] == {
        "end_time_utc": "2026-10-04T23:39:47.0438663",
        "failure_reason_sha256": hashlib.sha256(
            tool._canonical_bytes(
                {
                    "error_code": "Spark_User",
                    "is_retriable": None,
                    "message": "failed",
                }
            )
        ).hexdigest(),
        "invocation_id": tool.FAILED_RECOVERY_INVOCATION_ID,
        "item_id": "11111111-1111-4111-8111-111111111111",
        "job_id": tool.FAILED_RECOVERY_JOB_ID,
        "job_state_sha256": hashlib.sha256(
            tool._canonical_bytes(
                {
                    "end_time_utc": "2026-10-04T23:39:47.0438663",
                    "failure": {
                        "error_code": "Spark_User",
                        "is_retriable": None,
                        "message": "failed",
                    },
                    "invoke_type": None,
                    "item_id": "11111111-1111-4111-8111-111111111111",
                    "job_id": tool.FAILED_RECOVERY_JOB_ID,
                    "job_type": tool.JOB_TYPE_SJD,
                    "root_activity_id": None,
                    "start_time_utc": "2026-10-04T23:28:57.9082748",
                    "status": "Failed",
                }
            )
        ).hexdigest(),
        "job_type": tool.JOB_TYPE_SJD,
        "start_time_utc": "2026-10-04T23:28:57.9082748",
        "status": "Failed",
    }
    api.recovery_state["status"] = "Completed"
    with pytest.raises(tool.ControllerError, match="exact failed SJD"):
        tool.capture_recovery_snapshot(api, now=NOW)


def test_recovery_plan_review_and_execution_binding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(tool, "LOCAL_STATE_ROOT", tmp_path / "state")
    token = "recover-v1:" + "a" * 64
    body = {
        "binding": {},
        "evidence": {
            "failed_invocation_id": tool.FAILED_RECOVERY_INVOCATION_ID,
            "failed_job_id": tool.FAILED_RECOVERY_JOB_ID,
        },
        "evidence_sha256": "b" * 64,
        "recovery_token": token,
        "schema": "people-counter-production-lock-recovery-v2",
    }
    plan = {
        **body,
        "plan_sha256": hashlib.sha256(
            tool._canonical_bytes(body)
        ).hexdigest(),
    }
    artifact = tool.store_recovery_plan(plan)
    assert artifact["recovery_token_sha256"] == hashlib.sha256(
        token.encode()
    ).hexdigest()
    files = MemoryFiles()
    receipt = tool.review_recovery(files, now=NOW)
    assert receipt["failed_job_id"] == tool.FAILED_RECOVERY_JOB_ID
    assert receipt["path"] in files.values
    assert tool.validate_recovery_execution(token, now=NOW + 1) == artifact
    with pytest.raises(tool.ControllerError, match="binding is invalid"):
        tool.validate_recovery_execution("wrong", now=NOW + 1)


@pytest.mark.parametrize(
    "change",
    [
        "receipt-hash",
        "expired",
        "missing-expiry",
        "plan",
        "receipt-token",
        "artifact-token",
    ],
)
def test_recovery_execution_rejects_each_independent_binding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    change: str,
) -> None:
    monkeypatch.setattr(tool, "LOCAL_STATE_ROOT", tmp_path / change)
    token = "recover-v1:" + "a" * 64
    body = {
        "binding": {},
        "evidence": {
            "failed_invocation_id": tool.FAILED_RECOVERY_INVOCATION_ID,
            "failed_job_id": tool.FAILED_RECOVERY_JOB_ID,
        },
        "evidence_sha256": "b" * 64,
        "recovery_token": token,
        "schema": "people-counter-production-lock-recovery-v2",
    }
    plan = {
        **body,
        "plan_sha256": hashlib.sha256(
            tool._canonical_bytes(body)
        ).hexdigest(),
    }
    tool.store_recovery_plan(plan)
    tool.review_recovery(MemoryFiles(), now=NOW)
    receipt_path = tool._local_path(
        "recovery-reviews-v2", tool.FAILED_RECOVERY_JOB_ID
    )
    artifact_path = tool._local_path(
        "recovery-plans-v2", tool.FAILED_RECOVERY_JOB_ID
    )
    receipt = json.loads(receipt_path.read_bytes())
    artifact = json.loads(artifact_path.read_bytes())
    if change == "receipt-hash":
        receipt["receipt_sha256"] = "0" * 64
    elif change == "expired":
        receipt["expires_at"] = NOW - 1
    elif change == "missing-expiry":
        del receipt["expires_at"]
    elif change == "plan":
        receipt["plan_sha256"] = "0" * 64
    elif change == "receipt-token":
        receipt["recovery_token_sha256"] = "0" * 64
    else:
        artifact["recovery_token_sha256"] = "0" * 64
    if change not in {"receipt-hash", "artifact-token"}:
        receipt["receipt_sha256"] = hashlib.sha256(
            tool._canonical_bytes(
                {
                    key: value
                    for key, value in receipt.items()
                    if key != "receipt_sha256"
                }
            )
        ).hexdigest()
    receipt_path.write_text(canonical_json(receipt))
    artifact_path.write_text(canonical_json(artifact))
    with pytest.raises(tool.ControllerError) as refused:
        tool.validate_recovery_execution(token, now=NOW)
    assert str(refused.value) == (
        "recovery execution token or review receipt binding is invalid"
    )
    if change == "missing-expiry":
        with pytest.raises(tool.ControllerError) as missing_at_epoch:
            tool.validate_recovery_execution(token, now=0)
        assert str(missing_at_epoch.value) == str(refused.value)

    if change == "expired":
        receipt["expires_at"] = NOW
        receipt["receipt_sha256"] = hashlib.sha256(
            tool._canonical_bytes(
                {
                    key: value
                    for key, value in receipt.items()
                    if key != "receipt_sha256"
                }
            )
        ).hexdigest()
        receipt_path.write_text(canonical_json(receipt))
        artifact_path.write_text(
            canonical_json(
                tool._read_local(
                    "recovery-plans-v2", tool.FAILED_RECOVERY_JOB_ID
                )
            )
        )
        assert tool.validate_recovery_execution(token, now=NOW)


@pytest.mark.parametrize("execute", [False, True])
def test_recover_controller_inspection_and_execution_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execute: bool,
) -> None:
    monkeypatch.setattr(tool, "LOCAL_STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(
        tool, "capture_recovery_snapshot", lambda api, now: {"captured_at": now}
    )
    monkeypatch.setattr(
        tool.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="recoverynonce"),
    )
    token = "recover-v1:" + "a" * 64
    body = {
        "binding": {},
        "evidence": {
            "failed_invocation_id": tool.FAILED_RECOVERY_INVOCATION_ID,
            "failed_job_id": tool.FAILED_RECOVERY_JOB_ID,
        },
        "evidence_sha256": "b" * 64,
        "recovery_token": token,
        "schema": "people-counter-production-lock-recovery-v2",
    }
    plan = {
        **body,
        "plan_sha256": hashlib.sha256(
            tool._canonical_bytes(body)
        ).hexdigest(),
    }
    if execute:
        monkeypatch.setattr(
            tool,
            "validate_recovery_execution",
            lambda supplied, now: {"validated": supplied, "now": now},
        )
    seen: dict[str, object] = {}

    def invoke(*args, **kwargs):
        seen.update(kwargs)
        return tool.InvocationResult(
            "recover-lock",
            tool.FAILED_RECOVERY_RUN_ID,
            "recovery-job",
            {"status": "cleared"} if execute else plan,
        )

    monkeypatch.setattr(tool, "invoke", invoke)
    args = SimpleNamespace(
        failed_invocation_id=tool.FAILED_RECOVERY_INVOCATION_ID,
        failed_job_id=tool.FAILED_RECOVERY_JOB_ID,
        recovery_execute=execute,
        recovery_token=token if execute else None,
        run_id=tool.FAILED_RECOVERY_RUN_ID,
    )
    result = tool._recover_controller_command(
        args,
        FakeAPI(),
        files=MemoryFiles(),
        clock=lambda: NOW,
    )
    assert result["execute"] is execute
    assert seen["recovery_execute"] is execute
    if execute:
        assert "local_recovery_plan" not in result
    else:
        assert result["local_recovery_plan"]["plan_sha256"] == plan["plan_sha256"]


def test_snapshot_rejects_unknown_jobs_schedules_and_item_binding() -> None:
    item_id = tool.WRITER_ITEMS[0][1]
    api = FakeAPI()
    api.jobs[item_id] = [
        {"id": "job", "status": "Mystery", "jobType": "Pipeline"}
    ]
    with pytest.raises(tool.ControllerError, match="unknown Fabric job state"):
        tool.capture_snapshot(api, now=NOW)

    api = FakeAPI()
    api.schedules[item_id] = [{"id": "schedule", "enabled": "false"}]
    with pytest.raises(tool.ControllerError, match="enabled state is unknown"):
        tool.capture_snapshot(api, now=NOW)

    api = FakeAPI()
    api.items[0]["displayName"] = "wrong"
    with pytest.raises(tool.ControllerError, match="binding mismatch"):
        tool.capture_snapshot(api, now=NOW)


def test_inventory_hmac_nonce_redaction_and_create_only_readback() -> None:
    api = FakeAPI()
    values = iter((b"k" * 32, b"n" * 16))
    signed = tool.sign_inventory(
        tool.capture_snapshot(api, now=NOW),
        RUN_ID,
        random_bytes=lambda size: next(values),
    )
    envelope = json.loads(signed.envelope_bytes)
    payload_bytes = canonical_json(envelope["payload"]).encode()

    assert envelope["signature_sha256"] == __import__("hmac").new(
        b"k" * 32, payload_bytes, hashlib.sha256
    ).hexdigest()
    assert signed.hmac_key == b"k" * 32
    assert signed.payload["nonce"] == (b"n" * 16).hex()
    assert signed.path == tool.inventory_path(RUN_ID)
    assert "kkkk" not in canonical_json(signed.redacted_summary())

    files = MemoryFiles()
    tool.upload_inventory(files, signed)
    assert files.exists_calls == [signed.path]
    assert files.read_bytes(signed.path) == signed.envelope_bytes
    with pytest.raises(FileExistsError) as collision:
        tool.upload_inventory(files, signed)
    assert collision.value.args == (signed.path,)

    corrupt = MemoryFiles()

    def corrupt_create(path: str, content: bytes) -> None:
        corrupt.values[path] = content + b"x"

    corrupt.create_bytes = corrupt_create  # type: ignore[method-assign]
    with pytest.raises(tool.ControllerError, match="readback hash mismatch"):
        tool.upload_inventory(corrupt, signed)


@pytest.mark.parametrize(
    "values",
    [
        (b"k" * 31, b"n" * 16),
        (b"k" * 32, b"n" * 15),
    ],
)
def test_inventory_signing_requires_both_exact_random_lengths(
    values: tuple[bytes, bytes],
) -> None:
    supplied = iter(values)
    with pytest.raises(tool.ControllerError, match="wrong length"):
        tool.sign_inventory(
            {"schema": "test"},
            RUN_ID,
            random_bytes=lambda requested: next(supplied),
        )


def test_inventory_signing_requests_key_then_nonce_lengths() -> None:
    requested: list[int] = []

    def random_bytes(size: int) -> bytes:
        requested.append(size)
        return b"k" * size

    signed = tool.sign_inventory({"schema": "test"}, RUN_ID, random_bytes=random_bytes)

    assert requested == [32, 16]
    assert signed.payload["nonce"] == (b"k" * 16).hex()


def test_deploy_preview_and_exact_environment_sjd_policy() -> None:
    api = FakeAPI()
    wheel = b"PK deterministic-wheel"
    preview = tool.deploy(
        api,
        execute=False,
        wheel_builder=lambda: (wheel, hashlib.sha256(wheel).hexdigest()),
    )
    assert preview["execute"] is False
    assert not api.deleted and not api.uploads and not api.updated

    result = tool.deploy(
        api,
        execute=True,
        wheel_builder=lambda: (wheel, hashlib.sha256(wheel).hexdigest()),
    )
    assert api.deleted == ["people_counter-0.7.1-py3-none-any.whl"]
    assert api.uploads == [(tool.PROJECT_WHEEL, wheel)]
    assert api.published
    assert api.updated == ["11111111-1111-4111-8111-111111111111"]
    assert result["readback"] == "verified"

    bad = build_migration_sjd_definition()
    metadata_part = bad["definition"]["parts"][1]  # type: ignore[index]
    metadata = json.loads(base64.b64decode(metadata_part["payload"]))
    metadata["commandLineArguments"] = "--unsafe"
    metadata_part["payload"] = base64.b64encode(
        canonical_json(metadata).encode()
    ).decode()
    with pytest.raises(tool.ControllerError, match="bindings or saved defaults"):
        tool._validate_sjd_semantics(bad)


def test_deploy_skips_republish_when_exact_environment_is_already_current() -> None:
    api = FakeAPI()
    api.environment = environment_state(tool.PROJECT_WHEEL)
    wheel = b"PK current-wheel"

    result = tool.deploy(
        api,
        execute=True,
        wheel_builder=lambda: (wheel, hashlib.sha256(wheel).hexdigest()),
    )

    assert result["environment_action"] == "already_current"
    assert api.deleted == []
    assert api.uploads == []
    assert not api.published
    assert result["readback"] == "verified"


def test_sjd_readback_accepts_only_service_formatting_and_platform_part() -> None:
    definition = build_migration_sjd_definition()
    metadata_part = definition["definition"]["parts"][1]  # type: ignore[index]
    metadata = json.loads(base64.b64decode(metadata_part["payload"]))
    metadata_part["payload"] = base64.b64encode(
        json.dumps(metadata, indent=2).encode()
    ).decode()
    definition["definition"]["parts"].append(  # type: ignore[index]
        {
            "path": ".platform",
            "payload": base64.b64encode(b'{"version":"2.0"}').decode(),
            "payloadType": "InlineBase64",
        }
    )

    tool._validate_sjd_semantics(definition)

    definition["definition"]["parts"].append(  # type: ignore[index]
        {
            "path": "unexpected.txt",
            "payload": base64.b64encode(b"unexpected").decode(),
            "payloadType": "InlineBase64",
        }
    )
    with pytest.raises(tool.ControllerError, match="exactly Main and metadata"):
        tool._validate_sjd_semantics(definition)


def test_build_wheel_uses_exact_version_and_verifies_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    wheel_path = tmp_path / tool.PROJECT_WHEEL
    monkeypatch.setattr(tool, "DIST_WHEEL", wheel_path)
    observed: dict[str, object] = {}

    def runner(arguments, *, check, env):
        observed["arguments"] = arguments
        observed["epoch"] = env["SOURCE_DATE_EPOCH"]
        with zipfile.ZipFile(wheel_path, "w") as archive:
            archive.writestr(
                "people_counter-0.9.11.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: people-counter\nVersion: 0.9.11\n",
            )
        return SimpleNamespace(returncode=0)

    content, digest = tool.build_wheel(runner=runner)

    assert content.startswith(b"PK")
    assert digest == hashlib.sha256(content).hexdigest()
    assert observed == {
        "arguments": ["uv", "build", "--wheel", "--out-dir", str(tmp_path)],
        "epoch": "315532800",
    }


def plan_evidence() -> dict[str, object]:
    plan_sha = "a" * 64
    return {
        "compatibility": {"compatible": True},
        "operations": [{"kind": "create_table_if_not_exists"}],
        "plan_sha256": plan_sha,
        "rollback_manifest": "stop routing and ignore additive structures",
        "safety_token": "<redacted>",
        "safety_token_sha256": hashlib.sha256(SAFETY.encode()).hexdigest(),
    }


def test_run_poll_result_and_secret_arguments_are_redactable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeAPI()
    api.poll_states = [{"status": "Running"}, {"status": "Completed"}]
    signed = tool.sign_inventory(tool.capture_snapshot(api, now=NOW), RUN_ID)
    files = MemoryFiles()
    invocation_uuid = "33333333-3333-4333-8333-333333333333"
    monkeypatch.setattr(tool.uuid, "uuid4", lambda: invocation_uuid)
    files.values[tool.report_path(RUN_ID, invocation_uuid, "plan")] = (
        canonical_json(plan_evidence()).encode()
    )
    ticks = iter((NOW, NOW, NOW + 1))

    result = tool.invoke(
        api,
        files,
        signed,
        command="plan",
        owner=OWNER,
        lease_token=LEASE,
        clock=lambda: next(ticks),
        sleep=lambda _: None,
    )

    assert result.evidence["plan_sha256"] == "a" * 64
    assert result.job_instance_id == "22222222-2222-4222-8222-222222222222"
    assert "--inventory-run-id" in api.run_arguments
    assert "--invocation-id 33333333-3333-4333-8333-333333333333" in api.run_arguments
    assert signed.hmac_key.hex() in api.run_arguments
    redacted = tool._redacted_arguments(
        [
            "--inventory-hmac-key",
            signed.hmac_key.hex(),
            "--lease-token",
            LEASE,
            "--safety-token",
            SAFETY,
        ]
    )
    assert redacted == [
        "--inventory-hmac-key",
        "<redacted>",
        "--lease-token",
        "<redacted>",
        "--safety-token",
        "<redacted>",
    ]
    assert tool._redacted_arguments(
        [
            f"--inventory-hmac-key={signed.hmac_key.hex()}",
            f"--lease-token={LEASE}",
            f"--safety-token={SAFETY}",
            "--owner",
            OWNER,
        ]
    ) == [
        "--inventory-hmac-key=<redacted>",
        "--lease-token=<redacted>",
        "--safety-token=<redacted>",
        "--owner",
        OWNER,
    ]


def test_diagnose_invocation_has_no_owner_lock_or_mutation_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeAPI()
    signed = tool.sign_inventory(tool.capture_snapshot(api, now=NOW), RUN_ID)
    files = MemoryFiles()
    invocation_uuid = "44444444-4444-4444-8444-444444444444"
    monkeypatch.setattr(tool.uuid, "uuid4", lambda: invocation_uuid)
    files.values[tool.diagnose_path(RUN_ID, invocation_uuid)] = canonical_json(
        {
            "checks": [{"name": "all", "passed": True}],
            "status": "passed",
        }
    ).encode()

    result = tool.invoke(
        api,
        files,
        signed,
        command="diagnose",
        clock=lambda: NOW,
        sleep=lambda _: None,
    )

    assert result.command == "diagnose"
    assert result.evidence["status"] == "passed"
    assert "--inventory-hmac-key" in api.run_arguments
    assert "--owner" not in api.run_arguments
    assert "--lease-token" not in api.run_arguments
    assert "--execute" not in api.run_arguments
    assert "--plan-sha256" not in api.run_arguments
    assert "--safety-token" not in api.run_arguments


@pytest.mark.parametrize("command", ["verify", "status"])
def test_report_invocations_read_their_exact_evidence_paths(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    api = FakeAPI()
    signed = tool.sign_inventory(tool.capture_snapshot(api, now=NOW), RUN_ID)
    files = MemoryFiles()
    invocation_uuid = "45454545-4545-4545-8545-454545454545"
    monkeypatch.setattr(tool.uuid, "uuid4", lambda: invocation_uuid)
    path = tool.report_path(RUN_ID, invocation_uuid, command)
    files.values[path] = canonical_json(
        {"command": command, "status": "observed"}
    ).encode()

    result = tool.invoke(
        api,
        files,
        signed,
        command=command,
        owner=OWNER,
        lease_token=LEASE,
        clock=lambda: NOW,
        sleep=lambda _: None,
    )

    assert result.evidence == {"command": command, "status": "observed"}
    assert files.read_bytes(path)
    assert api.run_arguments.startswith(command + " ")


def test_failed_job_recovers_redacted_create_only_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeAPI()
    signed = tool.sign_inventory(tool.capture_snapshot(api, now=NOW), RUN_ID)
    invocation_uuid = "55555555-5555-4555-8555-555555555555"
    monkeypatch.setattr(tool.uuid, "uuid4", lambda: invocation_uuid)
    secret = signed.hmac_key.hex()
    api.poll_states = [
        {
            "failureReason": f"driver echoed {secret} {LEASE}",
            "status": "Failed",
        }
    ]
    path = tool.failure_path(RUN_ID, invocation_uuid, "live")
    payload = canonical_json(
        {
            "exception": {
                "message": "catalog binding missing",
                "traceback": "sanitized",
                "type": "LiveMigrationError",
            },
            "stage": "spark-binding",
        }
    ).encode()
    files = MemoryFiles()
    files.values[path] = payload

    with pytest.raises(tool.ControllerError) as observed:
        tool.invoke(
            api,
            files,
            signed,
            command="plan",
            owner=OWNER,
            lease_token=LEASE,
            clock=lambda: NOW,
            sleep=lambda _: None,
        )

    message = str(observed.value)
    assert secret not in message
    assert LEASE not in message
    assert path in message
    assert hashlib.sha256(payload).hexdigest() in message
    assert "spark-binding" in message


def test_apply_invocation_reads_exact_result_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeAPI()
    signed = tool.sign_inventory(tool.capture_snapshot(api, now=NOW), "apply-inventory")
    files = MemoryFiles()
    plan_sha = "a" * 64
    files.values[tool.result_path(RUN_ID, plan_sha)] = (
        canonical_json({"plan_sha256": plan_sha, "status": "applied"}).encode()
    )
    monkeypatch.setattr(
        tool.uuid, "uuid4", lambda: "33333333-3333-4333-8333-333333333333"
    )

    result = tool.invoke(
        api,
        files,
        signed,
        command="apply",
        migration_run_id=RUN_ID,
        owner=OWNER,
        lease_token=LEASE,
        plan_sha256=plan_sha,
        safety_token=SAFETY,
        clock=lambda: NOW,
        sleep=lambda _: None,
    )

    assert result.evidence["status"] == "applied"
    assert result.run_id == RUN_ID
    assert result.job_instance_id == "22222222-2222-4222-8222-222222222222"
    assert f"--inventory-run-id {signed.run_id}" in api.run_arguments
    assert f"--run-id {RUN_ID}" in api.run_arguments
    assert f"--owner {OWNER}" in api.run_arguments
    assert "--execute" in api.run_arguments
    assert f"--plan-sha256 {plan_sha}" in api.run_arguments
    assert f"--safety-token {SAFETY}" in api.run_arguments


@pytest.mark.parametrize(
    ("plan_sha256", "safety_token"),
    [(None, SAFETY), ("a" * 64, None)],
)
def test_apply_invocation_requires_both_review_bindings(
    plan_sha256: str | None, safety_token: str | None
) -> None:
    api = FakeAPI()
    signed = tool.sign_inventory(tool.capture_snapshot(api, now=NOW), RUN_ID)

    with pytest.raises(tool.ControllerError, match="requires plan hash"):
        tool.invoke(
            api,
            MemoryFiles(),
            signed,
            command="apply",
            owner=OWNER,
            lease_token=LEASE,
            plan_sha256=plan_sha256,
            safety_token=safety_token,
            clock=lambda: NOW,
            sleep=lambda _: None,
        )


def test_review_receipt_binds_plan_inventory_token_and_expiry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tool, "LOCAL_STATE_ROOT", tmp_path / "state")
    api = FakeAPI()
    snapshot = tool.capture_snapshot(api, now=NOW)
    signed = tool.sign_inventory(snapshot, RUN_ID)
    artifact = tool.store_plan(signed, plan_evidence())
    receipt = tool.review_plan(RUN_ID, now=NOW + 1)

    assert receipt["summary"]["safety_token"] == "<redacted>"
    assert SAFETY not in canonical_json(receipt)
    assert (
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=tool.capture_snapshot(api, now=NOW + 2),
            now=NOW + 2,
        )
        == artifact
    )

    with pytest.raises(tool.ControllerError, match="plan SHA"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="b" * 64,
            safety_token=SAFETY,
            fresh_snapshot=snapshot,
            now=NOW + 2,
        )
    with pytest.raises(tool.ControllerError, match="safety token"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token="wrong",
            fresh_snapshot=snapshot,
            now=NOW + 2,
        )
    with pytest.raises(tool.ControllerError, match="expired"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=snapshot,
            now=NOW + tool.REVIEW_MAX_AGE_SECONDS + 2,
        )


def test_apply_review_rejects_missing_tampered_stale_and_changed_inventory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tool, "LOCAL_STATE_ROOT", tmp_path / "state")
    api = FakeAPI()
    snapshot = tool.capture_snapshot(api, now=NOW)
    signed = tool.sign_inventory(snapshot, RUN_ID)
    tool.store_plan(signed, plan_evidence())
    with pytest.raises(tool.ControllerError, match="review"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=snapshot,
            now=NOW,
        )

    tool.review_plan(RUN_ID, now=NOW)
    changed = deepcopy(snapshot)
    changed["environment"] = {"changed": True}
    with pytest.raises(tool.ControllerError, match="fresh snapshot differs"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=changed,
            now=NOW + 1,
        )

    with pytest.raises(tool.ControllerError, match="not fresh enough"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=snapshot,
            now=NOW + tool.INVENTORY_APPLY_MAX_AGE_SECONDS + 1,
        )

    assert tool.validate_apply_review(
        RUN_ID,
        plan_sha256="a" * 64,
        safety_token=SAFETY,
        fresh_snapshot=snapshot,
        now=NOW + tool.INVENTORY_APPLY_MAX_AGE_SECONDS,
    )

    fresh_now = NOW + tool.INVENTORY_APPLY_MAX_AGE_SECONDS + 1
    assert tool.validate_apply_review(
        RUN_ID,
        plan_sha256="a" * 64,
        safety_token=SAFETY,
        fresh_snapshot=tool.capture_snapshot(api, now=fresh_now),
        now=fresh_now,
    )
    metadata_only = tool.capture_snapshot(api, now=NOW + 2)
    metadata_only["workspace_items"]["sha256"] = "f" * 64
    metadata_only["fabric_job_inventory"]["all_jobs_sha256"] = "e" * 64
    metadata_only["nonce"] = "different"
    assert tool.validate_apply_review(
        RUN_ID,
        plan_sha256="a" * 64,
        safety_token=SAFETY,
        fresh_snapshot=metadata_only,
        now=NOW + 2,
    )
    writer_drift = tool.capture_snapshot(api, now=NOW + 2)
    writer_drift["writer_schedules"] = [
        {
            "definition_sha256": "d" * 64,
            "enabled": True,
            "schedule_id": "new-writer",
        }
    ]
    with pytest.raises(tool.ControllerError, match="fresh snapshot differs"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=writer_drift,
            now=NOW + 2,
        )

    review_path = tool._local_path("reviews", RUN_ID)
    receipt = json.loads(review_path.read_bytes())
    receipt["reviewed_at"] = NOW - 1
    review_path.write_text(canonical_json(receipt))
    with pytest.raises(tool.ControllerError, match="receipt integrity mismatch"):
        tool.validate_apply_review(
            RUN_ID,
            plan_sha256="a" * 64,
            safety_token=SAFETY,
            fresh_snapshot=snapshot,
            now=NOW + 1,
        )


def test_cli_snapshot_is_read_only_and_fixed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeAPI()
    files = MemoryFiles()
    output = io.StringIO()
    assert (
        tool.main(
            ["snapshot", "--run-id", RUN_ID],
            api=api,
            files=files,
            clock=lambda: NOW,
            output=output,
            errors=io.StringIO(),
        )
        == 0
    )
    result = json.loads(output.getvalue())
    assert result["status"] == "read_only"
    assert files.creates == []
    parser = tool._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["snapshot", "--workspace-id", "arbitrary"])


def test_cli_apply_consumes_review_and_fresh_run_specific_inventory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tool, "LOCAL_STATE_ROOT", tmp_path / "state")
    api = FakeAPI()
    files = MemoryFiles()
    snapshot = tool.capture_snapshot(api, now=NOW)
    reviewed = tool.sign_inventory(snapshot, RUN_ID)
    tool.store_plan(reviewed, plan_evidence())
    tool.review_plan(RUN_ID, now=NOW)
    apply_inventory_id = f"{RUN_ID}.apply.44444444444444444444444444444444"
    monkeypatch.setattr(
        tool.uuid,
        "uuid4",
        lambda: SimpleNamespace(
            hex="44444444444444444444444444444444",
            __str__=lambda self: "55555555-5555-4555-8555-555555555555",
        ),
    )

    class UUIDValue:
        hex = "44444444444444444444444444444444"

        def __str__(self) -> str:
            return "55555555-5555-4555-8555-555555555555"

    monkeypatch.setattr(tool.uuid, "uuid4", lambda: UUIDValue())
    files.values[tool.result_path(RUN_ID, "a" * 64)] = canonical_json(
        {"status": "applied"}
    ).encode()
    output = io.StringIO()
    errors = io.StringIO()

    code = tool.main(
        [
            "apply",
            "--execute",
            "--run-id",
            RUN_ID,
            "--owner",
            OWNER,
            "--lease-token",
            LEASE,
            "--plan-sha256",
            "a" * 64,
            "--safety-token",
            SAFETY,
        ],
        api=api,
        files=files,
        clock=lambda: NOW + 1,
        output=output,
        errors=errors,
    )

    assert code == 0, errors.getvalue()
    result = json.loads(output.getvalue())
    assert result["result"]["status"] == "applied"
    assert tool.inventory_path(apply_inventory_id) in files.creates
    assert SAFETY not in output.getvalue()
    assert LEASE not in output.getvalue()


class Transport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Mapping[str, str], bytes | None]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        self.calls.append((method, url, headers, body))
        return HTTPResponse(200, {}, b'{"value":[]}')


def test_onelake_create_makes_fixed_parent_directories_before_file() -> None:
    created: list[str] = []
    file_calls: list[tuple[object, ...]] = []

    class Directory:
        def __init__(self, path: str) -> None:
            self.path = path

        def create_directory(self) -> None:
            created.append(self.path)

    class File:
        def __init__(self, path: str) -> None:
            self.path = path

        def create_file(self, **kwargs: object) -> None:
            file_calls.append(("create", self.path, kwargs))

        def append_data(self, content: bytes, *, offset: int, length: int) -> None:
            file_calls.append(("append", self.path, content, offset, length))

        def flush_data(self, offset: int) -> None:
            file_calls.append(("flush", self.path, offset))

    class FileSystem:
        def get_directory_client(self, path: str) -> Directory:
            return Directory(path)

        def get_file_client(self, path: str) -> File:
            return File(path)

    files = object.__new__(tool.AzureOneLakeFiles)
    files._filesystem = FileSystem()
    path = tool.inventory_path("fresh-run")

    files.create_bytes(path, b"inventory")

    assert created[0] == f"{LAKEHOUSE_ID}/Files/people-counter"
    assert created[-1].endswith(
        "/Files/people-counter/migrations/people_counter_ca_0001/inventory"
    )
    checked = f"{LAKEHOUSE_ID}/{path}"
    assert file_calls[0][0:2] == ("create", checked)
    assert file_calls[1:] == [
        ("append", checked, b"inventory", 0, len(b"inventory")),
        ("flush", checked, len(b"inventory")),
    ]


def test_rest_client_uses_exact_fixed_schedule_and_job_apis() -> None:
    transport = Transport()
    client = tool.FabricRESTController(
        StaticTokenProvider("secret-token"), transport=transport
    )
    item_id = tool.WRITER_ITEMS[0][1]
    assert client.list_schedules(item_id, "Pipeline") == []
    assert client.list_job_instances(item_id) == []
    urls = [value[1] for value in transport.calls]
    assert urls[0].endswith(f"/items/{item_id}/jobs/Pipeline/schedules")
    assert urls[1].endswith(f"/items/{item_id}/jobs/instances")
    assert transport.calls[0][2]["Authorization"] == "Bearer secret-token"


class ContinuationTransport:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        self.urls.append(url)
        if len(self.urls) == 1:
            return HTTPResponse(
                200,
                {},
                b'{"value":[{"id":"first"}],"continuationUri":'
                b'"https://wabi-west-us3-a-primary-redirect.analysis.windows.net/'
                b'v1/workspaces/workspace/items?continuationToken=next"}',
            )
        return HTTPResponse(200, {}, b'{"value":[{"id":"second"}]}')


def test_rest_client_preserves_absolute_regional_continuation_uri() -> None:
    transport = ContinuationTransport()
    client = tool.FabricRESTController(
        StaticTokenProvider("secret-token"), transport=transport
    )

    assert client.list_items() == [{"id": "first"}, {"id": "second"}]
    assert transport.urls[1] == (
        "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/"
        "v1/workspaces/workspace/items?continuationToken=next"
    )


class LROTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        self.calls.append((method, url))
        if len(self.calls) == 1:
            return HTTPResponse(
                202,
                {"Location": "https://api.fabric.microsoft.com/v1/operations/op-1"},
                b"null",
            )
        if len(self.calls) == 2:
            return HTTPResponse(
                200,
                {},
                b'{"status":"Running"}',
            )
        return HTTPResponse(
            200,
            {"Location": "https://api.fabric.microsoft.com/v1/resources/item-1"},
            b'{"status":"Succeeded"}',
        ) if len(self.calls) == 3 else HTTPResponse(200, {}, b'{"id":"item-1"}')


def test_rest_client_polls_lro_and_fetches_resource() -> None:
    transport = LROTransport()
    ticks = iter((0.0, 0.0, 1.0))
    client = tool.FabricRESTController(
        StaticTokenProvider("token"),
        transport=transport,
        sleep=lambda _: None,
        monotonic=lambda: next(ticks),
    )

    result = client.create_sjd(build_migration_sjd_definition())

    assert result == {"id": "item-1"}
    assert [method for method, _ in transport.calls] == ["POST", "GET", "GET", "GET"]


def test_rest_client_requests_exact_sjd_v2_definition_format() -> None:
    transport = Transport()
    client = tool.FabricRESTController(
        StaticTokenProvider("secret-token"), transport=transport
    )

    client.get_definition("SparkJobDefinition", "item-id")

    assert transport.calls[0][1].endswith(
        "/sparkJobDefinitions/item-id/getDefinition?format=SparkJobDefinitionV2"
    )


class RunTransport:
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        assert method == "POST"
        assert url.endswith(
            "/sparkJobDefinitions/11111111-1111-4111-8111-111111111111"
            "/jobs/sparkjob/instances"
        )
        assert json.loads(body)["executionData"]["commandLineArguments"] == "plan"
        return HTTPResponse(
            202,
            {"Location": "https://api.fabric.microsoft.com/v1/jobs/job-123"},
            b"",
        )


def test_rest_run_sjd_reads_instance_from_location() -> None:
    client = tool.FabricRESTController(
        StaticTokenProvider("token"), transport=RunTransport()
    )

    assert (
        client.run_sjd("11111111-1111-4111-8111-111111111111", "plan")
        == "job-123"
    )

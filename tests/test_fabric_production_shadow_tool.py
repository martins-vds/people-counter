from __future__ import annotations

import io
import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import pytest

import people_counter.fabric_production_shadow_tool as tool
from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    PRODUCTION_FILES_ROOT,
    PRODUCTION_SHADOW_FILES_ROOT,
    WORKSPACE_ID,
)
from people_counter.fabric_production_migration import MIGRATION_ID
from people_counter.fabric_production_routing import AllowlistRow, sha256_json
from people_counter.fabric_production_shadow import (
    AuthorizationResult,
    AuthorizationRow,
    CommittedRoute,
    LegacySourceRows,
    MigrationJournalProof,
    ProductionShadowError,
    RuntimeProvenance,
    ShadowQuiescence,
)
from people_counter.fabric_production_shadow_jobs import (
    SJD_NAMES,
    build_sjd_v2_definition,
)
from people_counter.fabric_reflex_definition import REFLEX_ID


NOW = 1_799_000_000.0
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64
WORK_ID = "work-001"
ATTEMPT_ID = "attempt-legacy-001"


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
        "attempt_id": ATTEMPT_ID,
        "output_path": f"{PRODUCTION_FILES_ROOT}attempts/work={WORK_ID}/data.json",
        "output_sha256": HASH_F,
    }
    return LegacySourceRows(
        work={
            **shared,
            "committed_attempt_id": ATTEMPT_ID,
            "payload": {"duration_seconds": 10.0, "runtime_key": "cpu"},
            "status": "SUCCEEDED",
        },
        attempt={**shared, "status": "SUCCEEDED"},
        publication={**shared, "publication_sequence": 7},
        committed_view={**shared, "publication_sequence": 7},
    )


class MemoryFiles:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.creates: list[str] = []

    def exists(self, path: str) -> bool:
        return path in self.values

    def read_bytes(self, path: str) -> bytes:
        return self.values[path]

    def create_bytes(self, path: str, content: bytes) -> None:
        if path in self.values:
            raise FileExistsError(path)
        self.values[path] = content
        self.creates.append(path)


class MemoryLocal:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}

    def create_bytes(self, path: str, content: bytes, *, mode: int) -> None:
        if path in self.values:
            raise FileExistsError(path)
        self.values[path] = content
        self.modes[path] = mode

    def read_bytes(self, path: str) -> bytes:
        return self.values[path]

    def file_mode(self, path: str) -> int:
        return self.modes[path]


class FakeBackend:
    def __init__(self) -> None:
        self.rows = legacy_rows()
        self.sjds: dict[str, Mapping[str, Any]] = {}
        self.deployed: list[str] = []
        self.allowlists: list[dict[str, Any]] = []
        self.audits: list[dict[str, Any]] = []
        self.schemas: dict[str, tuple[tuple[str, str, bool], ...]] = {}
        self.registered: list[Mapping[str, Any]] = []
        self.reconciliations: list[Mapping[str, Any]] = []
        self.processed = 0
        self.mutate_legacy = False
        self.deploy_error = False
        self.deployment_plan: Mapping[str, Any] | None = None
        self.migration_plan_sha256 = HASH_A

    def artifact_binding(self) -> Mapping[str, str]:
        return {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "migration_id": MIGRATION_ID,
            "workspace_id": WORKSPACE_ID,
        }

    def predeploy_snapshot(self) -> Mapping[str, Any]:
        return {
            "captured_at": datetime.fromtimestamp(NOW, timezone.utc).isoformat(),
            "environment": {
                "runtime": "2.0",
                "published_custom_wheels": ["people_counter-0.8.10-py3-none-any.whl"],
                "public_packages": ["numpy"],
            },
            "pipelines": [
                {"id": "writer-1", "display_name": "writer", "active_jobs": []}
            ],
            "reflex": {"id": REFLEX_ID, "active": False},
            "sjds": [],
        }

    def fixed_deployment_spec(self) -> Mapping[str, Any]:
        return {
            "artifact_binding": self.artifact_binding(),
            "definitions": {
                job: {
                    "definition_sha256": hashlib.sha256(
                        tool._canonical(build_sjd_v2_definition(job))
                    ).hexdigest(),
                    "display_name": display_name,
                }
                for job, display_name in tool.SJD_DISPLAY_NAMES
            },
            "environment": {
                "project_version": "0.9.11",
                "project_wheel": "people_counter-0.9.11-py3-none-any.whl",
                "public_libraries_preserved": True,
                "sole_custom_wheel": True,
            },
            "source_sha256": HASH_D,
            "wheel_sha256": HASH_E,
        }

    def deploy_fixed_definition(
        self, plan: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        assert "work_id" not in json.dumps(plan)
        self.deployment_plan = deepcopy(plan)
        deployed = []
        for job, display_name in tool.SJD_DISPLAY_NAMES:
            value = self.deploy_sjd(display_name, build_sjd_v2_definition(job))
            deployed.append(
                {
                    "job": job,
                    "display_name": display_name,
                    "definition_sha256": hashlib.sha256(
                        tool._canonical(value["definition"])
                    ).hexdigest(),
                }
            )
        return {"sjds": deployed, "wheel_sha256": HASH_E}

    def shadow_deployment_state(self) -> Mapping[str, Any]:
        return {
            "ready": all(
                display_name in self.sjds
                for _, display_name in tool.SJD_DISPLAY_NAMES
            )
        }

    def sjd_state(self, display_name: str) -> Mapping[str, Any] | None:
        return deepcopy(self.sjds.get(display_name))

    def read_legacy_rows(self) -> LegacySourceRows:
        return deepcopy(self.rows)

    def migration_proof(self) -> MigrationJournalProof:
        return MigrationJournalProof(
            MIGRATION_ID, "APPLIED", self.migration_plan_sha256, HASH_B
        )

    def quiescence(self) -> ShadowQuiescence:
        return ShadowQuiescence(
            datetime.fromtimestamp(NOW, timezone.utc),
            REFLEX_ID,
            False,
        )

    def status(self) -> Mapping[str, Any]:
        return {"authorization_count": len(self.allowlists), "state": "OFFLINE_FAKE"}

    def deploy_sjd(
        self, display_name: str, definition: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        assert display_name in dict(tool.SJD_DISPLAY_NAMES).values()
        if self.mutate_legacy:
            work = dict(self.rows.work)
            work["payload"] = {"duration_seconds": 11.0, "runtime_key": "cpu"}
            self.rows = LegacySourceRows(
                work, self.rows.attempt, self.rows.publication, self.rows.committed_view
            )
        if self.deploy_error:
            raise ProductionShadowError("offline deploy failed")
        state = {"definition": deepcopy(definition), "display_name": display_name}
        self.sjds[display_name] = state
        self.deployed.append(display_name)
        return state

    def read_allowlist(self, work_id: str) -> Sequence[Mapping[str, Any]]:
        return [row for row in self.allowlists if row["work_id"] == work_id]

    def append_allowlist(self, row: Mapping[str, Any]) -> None:
        self.allowlists.append(dict(row))

    def read_audit(
        self, authorization_id: str
    ) -> Sequence[Mapping[str, Any]]:
        return [
            row
            for row in self.audits
            if row["audit_id"] == authorization_id
        ]

    def append_audit(self, row: Mapping[str, Any]) -> None:
        self.audits.append(dict(row))

    def append_authorization(
        self,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None:
        if self.read_allowlist(str(allowlist["work_id"])) or self.read_audit(
            str(audit["audit_id"])
        ):
            raise ProductionShadowError("atomic authorization conflict")
        self.allowlists.append(dict(allowlist))
        self.audits.append(dict(audit))

    def table_schema(
        self, table_name: str
    ) -> Sequence[tuple[str, str, bool]] | None:
        return self.schemas.get(table_name)

    def create_table(
        self,
        table_name: str,
        schema: Sequence[tuple[str, str, bool]],
    ) -> None:
        self.schemas[table_name] = tuple(schema)

    def authorization_result(self, plan_sha256: str) -> AuthorizationResult:
        allowlist = next(
            row for row in self.allowlists if row["plan_sha256"] == plan_sha256
        )
        audit = next(
            row for row in self.audits if row["plan_sha256"] == plan_sha256
        )
        row = AuthorizationRow(
            authorization_id=str(audit["audit_id"]),
            plan_sha256=plan_sha256,
            work=identity(),
            work_identity_sha256=str(audit["legacy_attempt_id"]),
            expires_at=datetime.fromtimestamp(NOW + 600, timezone.utc),
            authorized_at=datetime.fromisoformat(str(allowlist["approved_at"])),
            safety_token_sha256=str(audit["comparison_sha256"]),
            migration_journal_sha256=HASH_B,
            reviewer=str(allowlist["approved_by"]),
        )
        return AuthorizationResult(row, False, False)

    def runtime_provenance(self) -> RuntimeProvenance:
        return RuntimeProvenance(
            "0.9.11", HASH_C, "3.13.7", "4.1.1", "21.0.1"
        )

    def committed_route(self, work_id: str, *, config: Any) -> None:
        assert work_id == WORK_ID
        assert config.files_root == PRODUCTION_SHADOW_FILES_ROOT
        return None

    def register(self, registration: Mapping[str, Any], *, config: Any) -> None:
        assert config.files_root == PRODUCTION_SHADOW_FILES_ROOT
        self.registered.append(dict(registration))

    def claim(self, work_id: str, *, config: Any) -> Mapping[str, Any]:
        return {"attempt_id": "shadow-attempt-001", "work_id": work_id}

    def process(
        self,
        claim: Mapping[str, Any],
        *,
        config: Any,
        provenance: RuntimeProvenance,
    ) -> Mapping[str, Any]:
        self.processed += 1
        return {"record_count": 1, "work_id": claim["work_id"]}

    def seal(
        self,
        claim: Mapping[str, Any],
        output: Mapping[str, Any],
        *,
        config: Any,
    ) -> None:
        assert output["work_id"] == claim["work_id"]

    def publish(
        self,
        claim: Mapping[str, Any],
        *,
        authorization: AuthorizationRow,
        config: Any,
        provenance: RuntimeProvenance,
    ) -> CommittedRoute:
        return self._route(
            authorization,
            attempt_id=str(claim["attempt_id"]),
            shadow=True,
            provenance=provenance,
        )

    def _route(
        self,
        authorization: AuthorizationRow,
        *,
        attempt_id: str,
        shadow: bool,
        provenance: RuntimeProvenance | None = None,
    ) -> CommittedRoute:
        return CommittedRoute(
            work_id=WORK_ID,
            attempt_id=attempt_id,
            logical_identity_sha256=HASH_D,
            fence=2,
            pointer_fence=2,
            output_path=(
                f"{PRODUCTION_SHADOW_FILES_ROOT}outputs/work={WORK_ID}/"
                f"attempt={attempt_id}/records.json"
                if shadow
                else f"{PRODUCTION_FILES_ROOT}attempts/work={WORK_ID}/data.json"
            ),
            output_sha256=HASH_F,
            sealed=True,
            committed=True,
            pointer_attempt_id=attempt_id,
            publication_sequence=8 if shadow else 7,
            publication_count=1,
            authorization_id=authorization.authorization_id,
            plan_sha256=authorization.plan_sha256,
            provenance=provenance or self.runtime_provenance(),
            identity=identity(),
            records=({"count": 3},),
            logical_total=3.0,
            frame_count=10,
            timestamp=datetime.fromtimestamp(NOW, timezone.utc),
        )

    def legacy_route(self, work_id: str) -> CommittedRoute:
        authorization = self.authorization_result(self.allowlists[0]["plan_sha256"])
        return self._route(
            authorization.row,
            attempt_id=ATTEMPT_ID,
            shadow=False,
        )

    def shadow_route(self, work_id: str) -> CommittedRoute:
        authorization = self.authorization_result(self.allowlists[0]["plan_sha256"])
        return self._route(
            authorization.row,
            attempt_id="shadow-attempt-001",
            shadow=True,
        )

    def append_reconciliation(self, row: Mapping[str, Any]) -> None:
        self.reconciliations.append(dict(row))


def invoke(
    backend: FakeBackend,
    local: MemoryLocal,
    files: MemoryFiles,
    *arguments: str,
    now: float = NOW,
) -> tuple[int, dict[str, Any] | None, str]:
    output = io.StringIO()
    errors = io.StringIO()
    code = tool.main(
        list(arguments),
        backend=backend,
        local=local,
        files=files,
        clock=lambda: now,
        output=output,
        errors=errors,
    )
    value = json.loads(output.getvalue()) if output.getvalue() else None
    return code, value, errors.getvalue()


def prepare_review(
    backend: FakeBackend,
    local: MemoryLocal,
    files: MemoryFiles,
) -> tuple[str, str]:
    code, planned, error = invoke(backend, local, files, "plan")
    assert (code, error) == (0, "")
    assert planned is not None
    plan_sha = planned["local_plan"]["plan_sha256"]
    code, reviewed, error = invoke(
        backend,
        local,
        files,
        "review",
        "--plan-sha256",
        plan_sha,
        "--reviewer",
        "operator",
    )
    assert (code, error) == (0, "")
    assert reviewed is not None
    return plan_sha, reviewed["safety_token"]


def test_parser_has_only_fixed_policy_surface_and_safe_snapshot_default() -> None:
    parser = tool._build_parser()
    assert parser.parse_args([]).command == "snapshot"
    assert set(vars(parser.parse_args([]))) == {
        "command",
        "deployment_plan_sha256",
        "deployment_token",
        "execute",
        "plan_sha256",
        "predeploy_snapshot_sha256",
        "reviewer",
        "safety_token",
        "work_id",
        "auth",
    }
    assert set(parser._actions[1].choices) == tool.READ_ONLY_COMMANDS | tool.MUTATING_COMMANDS
    for forbidden in (
        "--workspace",
        "--lakehouse",
        "--environment",
        "--namespace",
        "--table",
        "--path",
        "--resource",
        "--sql",
        "--code",
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(["snapshot", forbidden, "unsafe"])


@pytest.mark.parametrize("command", sorted(tool.MUTATING_COMMANDS))
def test_every_mutating_command_requires_execute(command: str) -> None:
    backend = FakeBackend()
    local = MemoryLocal()
    files = MemoryFiles()

    code, value, error = invoke(backend, local, files, command)

    assert code == 2
    assert value is None
    assert "without explicit --execute" in error
    assert not backend.deployed
    assert not backend.allowlists
    assert not backend.schemas
    assert not files.creates


def test_snapshot_uses_fixed_identities_names_and_exact_legacy_row_hashes() -> None:
    backend = FakeBackend()

    snapshot, source = tool.capture_snapshot(backend, now=NOW)

    assert snapshot["artifact_binding"] == {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "migration_id": MIGRATION_ID,
        "workspace_id": WORKSPACE_ID,
    }
    assert [item["job"] for item in snapshot["sjds"]] == list(SJD_NAMES)
    assert [item["display_name"] for item in snapshot["sjds"]] == [
        name for _, name in tool.SJD_DISPLAY_NAMES
    ]
    expected_rows = legacy_rows()
    expected_hashes = {
        "attempt": sha256_json(dict(expected_rows.attempt)),
        "committed_view": sha256_json(dict(expected_rows.committed_view)),
        "publication": sha256_json(dict(expected_rows.publication)),
        "work": sha256_json(dict(expected_rows.work)),
    }
    assert dict(source.row_hashes) == expected_hashes
    assert snapshot["legacy"]["row_hashes"] == expected_hashes


def test_plan_is_canonical_review_is_separate_0600_and_token_is_deterministic() -> None:
    first_backend, second_backend = FakeBackend(), FakeBackend()
    first_local, second_local = MemoryLocal(), MemoryLocal()
    files = MemoryFiles()

    first_sha, first_token = prepare_review(first_backend, first_local, files)
    second_sha, second_token = prepare_review(second_backend, second_local, files)

    assert (first_sha, first_token) == (second_sha, second_token)
    plan_path = f"plans/{first_sha}.json"
    review_path = f"reviews/{first_sha}.json"
    assert first_local.values[plan_path].endswith(b"\n")
    assert json.loads(first_local.values[plan_path])["plan_sha256"] == first_sha
    assert first_local.modes[plan_path] == 0o644
    assert first_local.modes[review_path] == 0o600
    assert first_token == tool.load_plan(first_local, first_sha).safety_token
    assert first_token.encode() not in first_local.values[plan_path]


def test_signed_inventory_is_verified_create_only_and_redacted() -> None:
    inventory = tool.sign_inventory(
        {"fixed": True},
        hmac_key=b"k" * 32,
        nonce=b"n" * 16,
    )
    files = MemoryFiles()

    tool.upload_inventory(files, inventory)

    assert files.creates == [inventory.path]
    assert inventory.path.startswith(f"{tool.CONTROLLER_ROOT}/inventories/")
    assert inventory.redacted_summary()["hmac_key"] == "<redacted>"
    assert inventory.redacted_summary()["nonce"] == "<redacted>"
    assert "kkkk" not in json.dumps(inventory.redacted_summary())
    with pytest.raises(FileExistsError):
        tool.upload_inventory(files, inventory)


def test_file_local_artifacts_are_create_only_bounded_and_mode_exact() -> None:
    root = Path(".people-counter-production-shadow-tool-test")
    assert not root.exists()
    local = tool.FileLocalArtifacts(root)
    try:
        local.create_bytes("plans/value.json", b"{}\n", mode=0o600)
        assert local.read_bytes("plans/value.json") == b"{}\n"
        assert local.file_mode("plans/value.json") == 0o600
        with pytest.raises(FileExistsError):
            local.create_bytes("plans/value.json", b"changed", mode=0o600)
        with pytest.raises(tool.ShadowControllerError, match="escaped fixed root"):
            local.read_bytes("../outside")
    finally:
        (root / "plans" / "value.json").unlink(missing_ok=True)
        (root / "plans").rmdir()
        root.rmdir()


def test_reviewed_offline_flow_uses_fixed_definitions_and_create_only_evidence() -> None:
    backend = FakeBackend()
    local = MemoryLocal()
    files = MemoryFiles()
    plan_sha, token = prepare_review(backend, local, files)

    results: dict[str, dict[str, Any]] = {}
    for command in (
        "authorize",
        "bootstrap",
        "process",
        "compare",
        "reconcile",
    ):
        code, value, error = invoke(
            backend,
            local,
            files,
            command,
            "--execute",
            "--plan-sha256",
            plan_sha,
            "--safety-token",
            token,
        )
        assert (code, error) == (0, "")
        assert value is not None
        results[command] = value
        assert value["command"] == command
        assert value["legacy_unchanged"] is True
        assert value["plan_sha256"] == plan_sha
        assert value["inventory"]["hmac_key"] == "<redacted>"
        assert value["evidence_path"].startswith(tool.CONTROLLER_ROOT)
        evidence = json.loads(files.values[value["evidence_path"]])
        assert set(evidence) == {
            "command",
            "completed_at",
            "inventory_id",
            "inventory_payload_sha256",
            "legacy_proof",
            "plan_sha256",
            "result",
            "schema",
        }
        assert evidence["command"] == command
        assert evidence["completed_at"] == datetime.fromtimestamp(
            NOW, timezone.utc
        ).isoformat()
        assert evidence["inventory_id"] == value["inventory"]["inventory_id"]
        assert (
            evidence["inventory_payload_sha256"]
            == value["inventory"]["payload_sha256"]
        )
        assert evidence["legacy_proof"] == {
            "after_sha256": evidence["legacy_proof"]["before_sha256"],
            "before_sha256": evidence["legacy_proof"]["before_sha256"],
            "unchanged": True,
            "work_id": WORK_ID,
        }
        assert evidence["plan_sha256"] == plan_sha
        assert evidence["result"] == value["result"]
        assert evidence["schema"] == tool.EVIDENCE_SCHEMA

    assert not backend.deployed
    assert len(backend.allowlists) == len(backend.audits) == 1
    assert len(backend.schemas) == 8
    assert backend.processed == 1
    assert results["compare"]["result"]["passed"] is True
    assert results["reconcile"]["result"]["passed"] is True
    assert len(backend.reconciliations) == 1
    assert len(files.creates) == 10
    assert all(path.startswith(tool.CONTROLLER_ROOT) for path in files.creates)


def prepare_deployment(
    backend: FakeBackend, local: MemoryLocal
) -> tuple[str, str, str]:
    snapshot = tool.capture_predeploy_snapshot(backend, local, now=NOW)
    plan = tool.build_deployment_plan(
        backend,
        local,
        str(snapshot["snapshot_sha256"]),
        now=NOW,
    )
    reviewed = tool.review_deployment_plan(
        local,
        str(plan["deployment_plan_sha256"]),
        "reviewer-1",
        now=NOW,
    )
    return (
        str(snapshot["snapshot_sha256"]),
        str(plan["deployment_plan_sha256"]),
        str(reviewed["deployment_token"]),
    )


def test_fixed_deployment_is_rest_only_workless_and_exact() -> None:
    backend = FakeBackend()
    local = MemoryLocal()
    files = MemoryFiles()
    before = deepcopy(backend.rows)
    predeploy_sha, plan_sha, token = prepare_deployment(backend, local)

    code, value, error = invoke(
        backend,
        local,
        files,
        "deploy",
        "--execute",
        "--deployment-plan-sha256",
        plan_sha,
        "--deployment-token",
        token,
    )

    assert (code, error) == (0, "")
    assert value is not None
    assert value["scope"] == "publish-project-wheel-and-upsert-three-shadow-sjds"
    assert value["deployment_plan_sha256"] == plan_sha
    assert backend.rows == before
    assert len(backend.deployed) == 3
    assert not files.creates
    artifact = json.loads(local.values[f"deploy-plans/{plan_sha}.json"])
    plan = artifact["deployment_plan"]
    assert local.modes[f"deploy-plans/{plan_sha}.json"] == 0o644
    assert set(artifact) == {
        "deployment_plan",
        "deployment_plan_sha256",
        "deployment_token_sha256",
        "schema",
    }
    assert plan == {
        "artifact_binding": backend.artifact_binding(),
        "created_at": datetime.fromtimestamp(NOW, timezone.utc).isoformat(),
        "environment_policy_sha256": hashlib.sha256(
            tool._canonical(backend.predeploy_snapshot()["environment"])
        ).hexdigest(),
        "expires_at": datetime.fromtimestamp(
            NOW + tool.AUTHORIZATION_MAX_AGE.total_seconds(), timezone.utc
        ).isoformat(),
        "operation": "publish-project-wheel-and-upsert-three-shadow-sjds",
        "predeploy_snapshot_sha256": predeploy_sha,
        "schema": tool.DEPLOY_PLAN_SCHEMA,
        "spec": backend.fixed_deployment_spec(),
        "writers_inactive": True,
    }
    assert backend.deployment_plan == plan
    encoded = json.dumps(artifact, sort_keys=True)
    assert predeploy_sha in encoded
    assert WORK_ID not in encoded
    assert "authorization_id" not in encoded
    assert '"command":' not in encoded


@pytest.mark.parametrize(
    "forbidden",
    ("work_id", "authorization_id", "table", "command", "data_path"),
)
def test_fixed_deployment_plan_rejects_every_data_plane_scope(
    forbidden: str,
) -> None:
    backend = FakeBackend()
    local = MemoryLocal()
    snapshot = tool.capture_predeploy_snapshot(backend, local, now=NOW)
    original = backend.fixed_deployment_spec
    backend.fixed_deployment_spec = lambda: {  # type: ignore[method-assign]
        **original(),
        forbidden: "forbidden",
    }

    with pytest.raises(tool.ShadowControllerError, match="data-plane scope"):
        tool.build_deployment_plan(
            backend,
            local,
            str(snapshot["snapshot_sha256"]),
            now=NOW,
        )


def test_fixed_deployment_hash_token_receipt_and_expiry_fail_closed() -> None:
    backend = FakeBackend()
    local = MemoryLocal()
    files = MemoryFiles()
    _, plan_sha, token = prepare_deployment(backend, local)

    code, value, error = invoke(
        backend,
        local,
        files,
        "deploy",
        "--execute",
        "--deployment-plan-sha256",
        plan_sha,
        "--deployment-token",
        token[:-1] + ("0" if token[-1] != "0" else "1"),
    )
    assert code == 2
    assert value is None
    assert "deployment token differs" in error
    assert not backend.deployed
    local.modes[f"deploy-reviews/{plan_sha}.json"] = 0o644
    code, value, error = invoke(
        backend,
        local,
        files,
        "deploy",
        "--execute",
        "--deployment-plan-sha256",
        plan_sha,
        "--deployment-token",
        token,
    )
    assert code == 2
    assert value is None
    assert "receipt differs" in error
    assert not backend.deployed
    local.modes[f"deploy-reviews/{plan_sha}.json"] = 0o600
    code, value, error = invoke(
        backend,
        local,
        files,
        "deploy",
        "--execute",
        "--deployment-plan-sha256",
        plan_sha,
        "--deployment-token",
        token,
        now=NOW + 3601,
    )
    assert code == 2
    assert value is None
    assert "expired" in error
    assert not backend.deployed


def test_predeployment_snapshot_is_clean_and_creates_no_onelake_request() -> None:
    backend = FakeBackend()
    local = MemoryLocal()
    files = MemoryFiles()

    code, value, error = invoke(
        backend,
        local,
        files,
        "snapshot",
    )
    assert (code, error) == (0, "")
    assert value == {
        "execute": False,
        "one_lake_write": False,
        "schema": "people-counter-production-shadow-predeployment-state-v1",
        "state": {"ready": False},
    }
    assert not backend.deployed
    assert not files.creates


def test_no_backend_constructs_fixed_live_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import people_counter.fabric_canary_tool as canary
    import people_counter.fabric_production_shadow_backend as live

    output, errors = io.StringIO(), io.StringIO()
    backend = FakeBackend()
    files = MemoryFiles()
    captured: dict[str, object] = {}

    monkeypatch.setattr(canary, "make_token_provider", lambda mode: ("token", mode))
    monkeypatch.setattr(live, "ShadowAzureOneLakeFiles", lambda: files)
    monkeypatch.setattr(
        live,
        "ShadowFabricREST",
        lambda token: captured.setdefault("api", ("api", token)),
    )

    def construct(api, evidence, *, selected_work_id, clock):
        captured.update(
            api=api,
            evidence=evidence,
            selected_work_id=selected_work_id,
            clock=clock,
        )
        return backend

    monkeypatch.setattr(live, "FabricShadowControllerBackend", construct)

    code = tool.main([], output=output, errors=errors)

    assert code == 0
    assert json.loads(output.getvalue())["execute"] is False
    assert errors.getvalue() == ""
    assert captured["evidence"] is files
    assert captured["selected_work_id"] is None

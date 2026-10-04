"""Delta-backed Candidate A control plane for Microsoft Fabric.

PySpark, Delta, and notebook utilities are imported only by operations that
need them.  Every mutable transition runs under :class:`ControlWriter`; an
ambiguous failure therefore retains the global lock and fails closed.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any, Protocol
from uuid import uuid4

from people_counter.fabric_candidate_a import FabricCandidateAConfig
from people_counter.fabric_control import ControlWriter
from people_counter.sjd_control import (
    CLAIM_ENVELOPE_VERSION,
    BatchValidationError,
    ClaimedBatch,
    ClaimedWork,
    ControlBatchState,
    ImmutableConflictError,
    LeaseBudgetError,
    LeaseLostError,
    RecoveryReport,
    RegisteredWork,
    ReconciliationFinding,
    ReplayRequest,
    SQLiteControlStore,
    _claim_admission_settings,
    _largest_admissible_claim_prefix,
)


_SCHEMAS = {
    "locks": "lock_name string, owner_id string, acquired_at timestamp",
    "work": (
        "work_id string, payload_json string, payload_sha256 string, "
        "runtime_key string, duration_seconds double, config_sha256 string, "
        "release_digest string, status string, attempt_count long, "
        "max_attempts long, original_max_attempts long, fence long, "
        "available_at double, lease_owner string, lease_attempt_id string, "
        "lease_expires_at double, committed_attempt_id string, "
        "publication_sequence long, replay_generation long, "
        "last_replay_id string, last_error string, created_at double, "
        "updated_at double"
    ),
    "batches": (
        "batch_id string, owner string, runtime_key string, status string, "
        "lease_expires_at double, item_count long, membership_sha256 string, "
        "envelope_version long, envelope_path string, envelope_sha256 string, "
        "created_at double, sealed_at double, committed_at double"
    ),
    "batch_members": (
        "batch_id string, ordinal long, work_id string, attempt_id string, "
        "fence long, payload_sha256 string"
    ),
    "attempts": (
        "attempt_id string, work_id string, batch_id string, fence long, "
        "status string, lease_expires_at double, payload_sha256 string, "
        "output_path string, output_sha256 string, terminal_succeeded boolean, "
        "records_json string, recovery_outcome string, created_at double, "
        "sealed_at double"
    ),
    "publications": (
        "publication_sequence long, work_id string, attempt_id string, "
        "batch_id string, output_path string, output_sha256 string, "
        "published_at double"
    ),
    "replay_requests": (
        "replay_id string, work_id string, operator string, reason string, "
        "generation long, requested_at double"
    ),
    "reconciliation_findings": (
        "finding_id string, finding_type string, severity string, "
        "entity_key string, details_json string, first_seen_at double, "
        "last_seen_at double, resolved_at double"
    ),
}

_KEYS = {
    "work": ("work_id",),
    "batches": ("batch_id",),
    "batch_members": ("batch_id", "ordinal"),
    "attempts": ("attempt_id",),
    "publications": ("publication_sequence",),
    "replay_requests": ("replay_id",),
    "reconciliation_findings": ("finding_id",),
}


class OneLakeFiles(Protocol):
    def exists(self, path: str) -> bool: ...

    def read_text(self, path: str) -> str: ...

    def create_text(self, path: str, content: str) -> None: ...


class NotebookUtilsOneLakeFiles:
    """Small create-only wrapper around the Fabric notebook file API."""

    @staticmethod
    def _fs() -> Any:
        import notebookutils

        return notebookutils.fs

    def exists(self, path: str) -> bool:
        return bool(self._fs().exists(path))

    def read_text(self, path: str) -> str:
        return str(self._fs().head(path, 100 * 1024 * 1024))

    def create_text(self, path: str, content: str) -> None:
        if self.exists(path):
            raise FileExistsError(path)
        result = self._fs().put(path, content, False)
        if result is False:
            raise OSError(f"OneLake create failed for {path}")


class OneLakeEnvelopeWriter:
    """Write and verify immutable, content-addressed claim envelopes."""

    def __init__(
        self,
        root: str,
        files: OneLakeFiles | None = None,
    ) -> None:
        self.root = root.rstrip("/")
        expected = FabricCandidateAConfig().file_path("control").rstrip("/")
        if self.root != expected:
            raise ValueError(
                f"claim envelopes require the fixed Candidate A control root {expected!r}"
            )
        self.files = files or NotebookUtilsOneLakeFiles()

    def write(self, batch_id: str, envelope: Mapping[str, Any]) -> tuple[str, str]:
        _safe_segment(batch_id)
        content = _canonical(envelope)
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        path = (
            f"{self.root}/claims/batch={batch_id}/"
            f"envelope-{digest}.json"
        )
        try:
            self.files.create_text(path, content)
        except FileExistsError:
            if self.files.read_text(path) != content:
                raise ImmutableConflictError(
                    f"claim envelope conflicts at {path}"
                )
        if self.files.read_text(path) != content:
            raise BatchValidationError(
                f"claim envelope readback differs at {path}"
            )
        return path, digest

    def read(self, path: str, expected_sha256: str) -> dict[str, Any]:
        content = self.files.read_text(path)
        actual = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if actual != expected_sha256:
            raise BatchValidationError(
                f"claim envelope digest mismatch: expected "
                f"{expected_sha256}, got {actual}"
            )
        try:
            value = json.loads(content)
        except json.JSONDecodeError as error:
            raise BatchValidationError("claim envelope is not valid JSON") from error
        if not isinstance(value, dict):
            raise BatchValidationError("claim envelope must be an object")
        return value


class FabricControlStoreImpl:
    """Real Delta implementation of the Candidate A control contract."""

    def __init__(
        self,
        spark_session: Any,
        *,
        config: FabricCandidateAConfig | None = None,
        files: OneLakeFiles | None = None,
        clock: Callable[[], float] = time.time,
        id_factory: Callable[[], str] | None = None,
        auto_bootstrap: bool = True,
    ) -> None:
        if spark_session is None:
            raise ValueError("spark_session is required")
        self.spark = spark_session
        self.config = config or FabricCandidateAConfig()
        self._clock = clock
        self._id_factory = id_factory or (lambda: str(uuid4()))
        self.envelopes = OneLakeEnvelopeWriter(
            self.config.file_path("control"), files
        )
        self.tables = {
            suffix: self.config.table(suffix) for suffix in _SCHEMAS
        }
        if auto_bootstrap:
            self.bootstrap()
        self.writer = ControlWriter(self.spark, self.tables["locks"])

    def bootstrap(self) -> None:
        """Create every Delta table with an explicit schema and seed one lock."""
        for suffix, schema in _SCHEMAS.items():
            name = self.tables[suffix]
            if self.spark.catalog.tableExists(name):
                continue
            if suffix == "locks":
                frame = self.spark.createDataFrame(
                    [("global", None, None)], schema=schema
                )
            else:
                frame = self.spark.createDataFrame([], schema=schema)
            frame.write.format("delta").mode("errorifexists").saveAsTable(name)
        rows = self._rows("locks")
        if len(rows) != 1 or rows[0].get("lock_name") != "global":
            raise RuntimeError(
                "Candidate A lock table must contain exactly one global row"
            )
        if (rows[0].get("owner_id") is None) != (
            rows[0].get("acquired_at") is None
        ):
            raise RuntimeError(
                "Candidate A lock ownership and timestamp are inconsistent"
            )

    def now(self) -> float:
        return _finite(self._clock(), "clock")

    def register(
        self,
        work_id: str,
        payload: Mapping[str, Any],
        *,
        runtime_key: str,
        duration_seconds: float,
        config_sha256: str,
        release_digest: str,
        max_attempts: int = 3,
        available_at: float | None = None,
    ) -> RegisteredWork:
        identity = _text(work_id, "work_id")
        runtime = _text(runtime_key, "runtime_key")
        duration = _positive(duration_seconds, "duration_seconds")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        payload_json = _canonical(dict(payload))
        payload_digest = hashlib.sha256(payload_json.encode()).hexdigest()
        now = self.now()

        def operation() -> RegisteredWork:
            rows = self._rows("work")
            existing = _one(rows, "work_id", identity)
            if existing is not None:
                immutable = (
                    existing["payload_sha256"],
                    existing["runtime_key"],
                    float(existing["duration_seconds"]),
                    existing["config_sha256"],
                    existing["release_digest"],
                )
                supplied = (
                    payload_digest,
                    runtime,
                    duration,
                    _text(config_sha256, "config_sha256"),
                    _text(release_digest, "release_digest"),
                )
                if immutable != supplied:
                    raise ImmutableConflictError(
                        f"work {identity!r} was registered with different content"
                    )
                return _registered(existing)
            row = {
                "work_id": identity,
                "payload_json": payload_json,
                "payload_sha256": payload_digest,
                "runtime_key": runtime,
                "duration_seconds": duration,
                "config_sha256": _text(config_sha256, "config_sha256"),
                "release_digest": _text(release_digest, "release_digest"),
                "status": "READY",
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "original_max_attempts": max_attempts,
                "fence": 0,
                "available_at": (
                    now
                    if available_at is None
                    else _finite(available_at, "available_at")
                ),
                "lease_owner": None,
                "lease_attempt_id": None,
                "lease_expires_at": None,
                "committed_attempt_id": None,
                "publication_sequence": None,
                "replay_generation": 0,
                "last_replay_id": None,
                "last_error": None,
                "created_at": now,
                "updated_at": now,
            }
            rows.append(row)
            self._replace("work", rows)
            return _registered(row)

        return self.writer.run(operation)

    def claim(
        self,
        owner: str,
        *,
        max_items: int,
        lease_seconds: float,
        minimum_speed_x: float | None = None,
        safety_factor: float | None = None,
        margin_seconds: float | None = None,
        minimum_items: int = 1,
        allowed_work_ids: Collection[str] | None = None,
        process_profile: object | None = None,
        peak_rss_bytes: int | None = None,
    ) -> ClaimedBatch | None:
        owner_id = _text(owner, "owner")
        if type(max_items) is not int or not 1 <= max_items <= 100:
            raise ValueError("max_items must be between 1 and 100")
        if type(minimum_items) is not int or not 1 <= minimum_items <= max_items:
            raise ValueError("minimum_items must be between 1 and max_items")
        lease = _positive(lease_seconds, "lease_seconds")
        speed, factor, margin, profile_workers, profile_name = (
            _claim_admission_settings(
                process_profile,
                minimum_speed_x=minimum_speed_x,
                safety_factor=safety_factor,
                margin_seconds=margin_seconds,
                peak_rss_bytes=peak_rss_bytes,
            )
        )
        if factor < 1 or margin < 0:
            raise ValueError("invalid claim admission settings")
        scope = (
            None
            if allowed_work_ids is None
            else {_text(item, "allowed_work_id") for item in allowed_work_ids}
        )

        def operation() -> ClaimedBatch | None:
            now = self.now()
            work = self._rows("work")
            eligible = [
                row
                for row in work
                if row["status"] == "READY"
                and float(row["available_at"]) <= now
                and (scope is None or row["work_id"] in scope)
            ]
            if not eligible:
                return None
            first_by_runtime: dict[str, float] = {}
            counts: dict[str, int] = {}
            for row in eligible:
                runtime = str(row["runtime_key"])
                first_by_runtime[runtime] = min(
                    first_by_runtime.get(runtime, math.inf),
                    float(row["created_at"]),
                )
                counts[runtime] = counts.get(runtime, 0) + 1
            candidates = [
                runtime
                for runtime, count in counts.items()
                if count >= minimum_items
            ]
            if not candidates:
                return None
            runtime = min(candidates, key=lambda key: (first_by_runtime[key], key))
            selected = sorted(
                (row for row in eligible if row["runtime_key"] == runtime),
                key=lambda row: (float(row["created_at"]), str(row["work_id"])),
            )[:max_items]
            selected = self._admissible_prefix(
                selected,
                minimum_items,
                lease,
                speed,
                factor,
                margin,
                profile_workers,
            )
            batch_id = self._id_factory()
            expires = now + lease
            claims = [
                ClaimedWork(
                    str(row["work_id"]),
                    self._id_factory(),
                    int(row["fence"]) + 1,
                    str(row["payload_sha256"]),
                )
                for row in selected
            ]
            membership = _membership(claims)
            envelope = {
                "schema_version": CLAIM_ENVELOPE_VERSION,
                "batch_id": batch_id,
                "owner": owner_id,
                "runtime_key": runtime,
                "claimed_at": now,
                "lease_expires_at": expires,
                "membership_sha256": membership,
                "admission": {
                    "process_profile": profile_name,
                    "worker_count": min(profile_workers, len(selected)),
                    "minimum_speed_x": speed,
                    "safety_factor": factor,
                    "margin_seconds": margin,
                },
                "items": [
                    {
                        "ordinal": ordinal,
                        "work_id": claim.work_id,
                        "attempt_id": claim.attempt_id,
                        "fence": claim.fence,
                        "payload_sha256": claim.payload_sha256,
                        "payload": json.loads(row["payload_json"]),
                        "config_sha256": row["config_sha256"],
                        "release_digest": row["release_digest"],
                        "duration_seconds": row["duration_seconds"],
                        "runtime_key": runtime,
                    }
                    for ordinal, (row, claim) in enumerate(
                        zip(selected, claims, strict=True)
                    )
                ],
            }
            path, digest = self.envelopes.write(batch_id, envelope)
            batches = self._rows("batches")
            batches.append(
                {
                    "batch_id": batch_id,
                    "owner": owner_id,
                    "runtime_key": runtime,
                    "status": "LEASED",
                    "lease_expires_at": expires,
                    "item_count": len(claims),
                    "membership_sha256": membership,
                    "envelope_version": CLAIM_ENVELOPE_VERSION,
                    "envelope_path": path,
                    "envelope_sha256": digest,
                    "created_at": now,
                    "sealed_at": None,
                    "committed_at": None,
                }
            )
            members = self._rows("batch_members")
            attempts = self._rows("attempts")
            by_work = {str(row["work_id"]): row for row in work}
            for ordinal, claim in enumerate(claims):
                row = by_work[claim.work_id]
                row.update(
                    {
                        "status": "LEASED",
                        "attempt_count": int(row["attempt_count"]) + 1,
                        "fence": claim.fence,
                        "lease_owner": owner_id,
                        "lease_attempt_id": claim.attempt_id,
                        "lease_expires_at": expires,
                        "updated_at": now,
                    }
                )
                members.append(
                    {
                        "batch_id": batch_id,
                        "ordinal": ordinal,
                        "work_id": claim.work_id,
                        "attempt_id": claim.attempt_id,
                        "fence": claim.fence,
                        "payload_sha256": claim.payload_sha256,
                    }
                )
                attempts.append(
                    {
                        "attempt_id": claim.attempt_id,
                        "work_id": claim.work_id,
                        "batch_id": batch_id,
                        "fence": claim.fence,
                        "status": "LEASED",
                        "lease_expires_at": expires,
                        "payload_sha256": claim.payload_sha256,
                        "output_path": None,
                        "output_sha256": None,
                        "terminal_succeeded": None,
                        "records_json": None,
                        "recovery_outcome": None,
                        "created_at": now,
                        "sealed_at": None,
                    }
                )
            self._replace_many(
                work=work,
                batches=batches,
                batch_members=members,
                attempts=attempts,
            )
            return ClaimedBatch(
                batch_id, path, digest, runtime, expires, tuple(claims)
            )

        return self.writer.run(operation)

    def load_claim_envelope_with_digest(
        self, batch_id: str
    ) -> tuple[dict[str, Any], str]:
        batch = _require_one(self._rows("batches"), "batch_id", batch_id)
        digest = str(batch["envelope_sha256"])
        envelope = self.envelopes.read(str(batch["envelope_path"]), digest)
        if envelope.get("batch_id") != batch_id:
            raise BatchValidationError("claim envelope batch mismatch")
        return envelope, digest

    def process_batch_state(self, envelope: Any) -> ControlBatchState:
        batch = self._verified_process_batch(envelope)
        return ControlBatchState(
            str(batch["status"]), float(batch["lease_expires_at"])
        )

    def assert_process_fence(self, envelope: Any) -> None:
        state = self.process_batch_state(envelope)
        if state.status == "COMMITTED":
            return
        if state.status not in {"LEASED", "SEALED"} or state.lease_expires_at <= self.now():
            raise LeaseLostError(
                f"batch {envelope.batch_id} does not own a live fence"
            )

    def heartbeat_process(
        self, envelope: Any, extension_seconds: float
    ) -> None:
        extension = _positive(extension_seconds, "extension_seconds")

        def operation() -> None:
            now = self.now()
            batch = self._verified_process_batch(envelope)
            if batch["status"] != "LEASED" or float(batch["lease_expires_at"]) <= now:
                raise LeaseLostError("heartbeat lost the batch lease")
            expires = max(float(batch["lease_expires_at"]), now + extension)
            batch["lease_expires_at"] = expires
            attempts = self._rows("attempts")
            work = self._rows("work")
            expected = {item.attempt_id: item for item in envelope.items}
            changed = 0
            for attempt in attempts:
                if attempt["attempt_id"] in expected:
                    attempt["lease_expires_at"] = expires
            for row in work:
                item = next(
                    (candidate for candidate in envelope.items if candidate.work_id == row["work_id"]),
                    None,
                )
                if (
                    item is not None
                    and row["status"] == "LEASED"
                    and row["lease_attempt_id"] == item.attempt_id
                    and int(row["fence"]) == item.fence
                ):
                    row["lease_expires_at"] = expires
                    row["updated_at"] = now
                    changed += 1
            if changed != len(envelope.items):
                raise LeaseLostError("heartbeat work cardinality changed")
            self._replace_many(
                batches=self._rows_with_replacement(
                    "batches", "batch_id", envelope.batch_id, batch
                ),
                attempts=attempts,
                work=work,
            )

        self.writer.run(operation)

    def seal_batch(
        self,
        batch_id: str,
        outputs: Sequence[Mapping[str, Any]],
        *,
        envelope_sha256: str,
        membership_sha256: str,
    ) -> None:
        normalized = SQLiteControlStore._normalize_outputs(outputs)

        def operation() -> None:
            now = self.now()
            batches = self._rows("batches")
            batch = _require_one(batches, "batch_id", batch_id)
            if (
                batch["envelope_sha256"] != envelope_sha256
                or batch["membership_sha256"] != membership_sha256
            ):
                raise BatchValidationError("envelope or membership hash mismatch")
            if batch["status"] == "SEALED":
                attempts = [
                    row for row in self._rows("attempts") if row["batch_id"] == batch_id
                ]
                observed = {
                    (
                        row["work_id"],
                        row["attempt_id"],
                        row["output_path"],
                        row["output_sha256"],
                        bool(row["terminal_succeeded"]),
                        row["records_json"],
                    )
                    for row in attempts
                }
                supplied = {
                    (
                        row["work_id"],
                        row["attempt_id"],
                        row["output_path"],
                        row["output_sha256"],
                        row["succeeded"],
                        _canonical(row["records"]),
                    )
                    for row in normalized
                }
                if observed != supplied:
                    raise ImmutableConflictError("sealed outputs differ")
                return
            if batch["status"] != "LEASED" or float(batch["lease_expires_at"]) <= now:
                raise LeaseLostError("batch no longer owns a live lease")
            members = [
                row for row in self._rows("batch_members") if row["batch_id"] == batch_id
            ]
            expected = {(row["work_id"], row["attempt_id"]) for row in members}
            supplied = {(row.get("work_id"), row.get("attempt_id")) for row in normalized}
            if len(normalized) != len(expected) or supplied != expected:
                raise BatchValidationError(
                    "sealed output cardinality/membership differs from claim"
                )
            attempts = self._rows("attempts")
            work = {row["work_id"]: row for row in self._rows("work")}
            by_attempt = {row["attempt_id"]: row for row in attempts}
            for output in normalized:
                attempt = by_attempt.get(output["attempt_id"])
                current = work.get(output["work_id"])
                if (
                    attempt is None
                    or current is None
                    or attempt["status"] != "LEASED"
                    or current["status"] != "LEASED"
                    or current["lease_attempt_id"] != attempt["attempt_id"]
                    or int(current["fence"]) != int(attempt["fence"])
                ):
                    raise LeaseLostError(
                        f"attempt {output['attempt_id']} lost its work fence"
                    )
                attempt.update(
                    {
                        "status": "SEALED",
                        "output_path": _text(output.get("output_path"), "output_path"),
                        "output_sha256": _sha(output.get("output_sha256"), "output_sha256"),
                        "terminal_succeeded": output["succeeded"],
                        "records_json": _canonical(output["records"]),
                        "sealed_at": now,
                    }
                )
            batch["status"] = "SEALED"
            batch["sealed_at"] = now
            self._replace_many(batches=batches, attempts=attempts)

        self.writer.run(operation)

    def commit_batch(self, batch_id: str) -> tuple[int, ...]:
        def operation() -> tuple[int, ...]:
            now = self.now()
            batches = self._rows("batches")
            batch = _require_one(batches, "batch_id", batch_id)
            publications = self._rows("publications")
            if batch["status"] == "COMMITTED":
                return tuple(
                    sorted(
                        int(row["publication_sequence"])
                        for row in publications
                        if row["batch_id"] == batch_id
                    )
                )
            if batch["status"] != "SEALED" or float(batch["lease_expires_at"]) <= now:
                raise LeaseLostError("only a live sealed batch can be committed")
            attempts = self._rows("attempts")
            selected = [row for row in attempts if row["batch_id"] == batch_id]
            if len(selected) != int(batch["item_count"]):
                raise BatchValidationError("attempt cardinality differs from batch")
            work_rows = self._rows("work")
            work = {row["work_id"]: row for row in work_rows}
            next_sequence = max(
                (int(row["publication_sequence"]) for row in publications),
                default=0,
            )
            sequences: list[int] = []
            for attempt in sorted(selected, key=lambda row: row["work_id"]):
                row = work[attempt["work_id"]]
                if (
                    attempt["status"] != "SEALED"
                    or row["status"] != "LEASED"
                    or row["lease_attempt_id"] != attempt["attempt_id"]
                    or int(row["fence"]) != int(attempt["fence"])
                ):
                    raise LeaseLostError(
                        f"attempt {attempt['attempt_id']} cannot advance pointer"
                    )
                if bool(attempt["terminal_succeeded"]):
                    next_sequence += 1
                    if publications and next_sequence <= max(
                        int(item["publication_sequence"]) for item in publications
                    ):
                        raise LeaseLostError("publication sequence is not monotonic")
                    publications.append(
                        {
                            "publication_sequence": next_sequence,
                            "work_id": row["work_id"],
                            "attempt_id": attempt["attempt_id"],
                            "batch_id": batch_id,
                            "output_path": attempt["output_path"],
                            "output_sha256": attempt["output_sha256"],
                            "published_at": now,
                        }
                    )
                    row["status"] = "SUCCEEDED"
                    row["committed_attempt_id"] = attempt["attempt_id"]
                    row["publication_sequence"] = next_sequence
                    attempt["status"] = "SUCCEEDED"
                    sequences.append(next_sequence)
                else:
                    row["status"] = (
                        "DEAD"
                        if int(row["attempt_count"]) >= int(row["max_attempts"])
                        else "READY"
                    )
                    row["available_at"] = now
                    row["last_error"] = "processing failed"
                    attempt["status"] = "FAILED"
                row["lease_owner"] = None
                row["lease_attempt_id"] = None
                row["lease_expires_at"] = None
                row["updated_at"] = now
            batch["status"] = "COMMITTED"
            batch["committed_at"] = now
            self._replace_many(
                batches=batches,
                attempts=attempts,
                work=work_rows,
                publications=publications,
            )
            return tuple(sequences)

        return self.writer.run(operation)

    def recover(self, *, now: float | None = None) -> RecoveryReport:
        timestamp = self.now() if now is None else _finite(now, "now")

        def operation() -> RecoveryReport:
            work = self._rows("work")
            attempts = self._rows("attempts")
            batches = self._rows("batches")
            retried = dead = 0
            by_attempt = {row["attempt_id"]: row for row in attempts}
            for row in work:
                if row["status"] != "LEASED" or float(row["lease_expires_at"]) > timestamp:
                    continue
                outcome = (
                    "DEAD"
                    if int(row["attempt_count"]) >= int(row["max_attempts"])
                    else "READY"
                )
                dead += outcome == "DEAD"
                retried += outcome == "READY"
                attempt = by_attempt.get(row["lease_attempt_id"])
                if attempt is not None and attempt["status"] in {"LEASED", "SEALED"}:
                    attempt["status"] = "EXPIRED"
                    attempt["recovery_outcome"] = outcome
                row.update(
                    {
                        "status": outcome,
                        "available_at": timestamp,
                        "lease_owner": None,
                        "lease_attempt_id": None,
                        "lease_expires_at": None,
                        "last_error": "lease expired",
                        "updated_at": timestamp,
                    }
                )
            for batch in batches:
                if (
                    batch["status"] in {"LEASED", "SEALED"}
                    and float(batch["lease_expires_at"]) <= timestamp
                ):
                    batch["status"] = "EXPIRED"
            self._replace_many(work=work, attempts=attempts, batches=batches)
            return RecoveryReport(retried + dead, retried, dead)

        return self.writer.run(operation)

    def replay(
        self,
        work_id: str,
        *,
        operator: str,
        reason: str,
        additional_attempts: int = 1,
    ) -> ReplayRequest:
        if type(additional_attempts) is not int or additional_attempts < 1:
            raise ValueError("additional_attempts must be positive")

        def operation() -> ReplayRequest:
            now = self.now()
            work = self._rows("work")
            row = _require_one(work, "work_id", work_id)
            if row["status"] == "LEASED":
                raise LeaseLostError("leased work cannot be replayed")
            replay_id = self._id_factory()
            generation = int(row["replay_generation"]) + 1
            row.update(
                {
                    "status": "READY",
                    "max_attempts": max(
                        int(row["max_attempts"]),
                        int(row["attempt_count"]) + additional_attempts,
                    ),
                    "available_at": now,
                    "committed_attempt_id": None,
                    "publication_sequence": None,
                    "replay_generation": generation,
                    "last_replay_id": replay_id,
                    "updated_at": now,
                }
            )
            requests = self._rows("replay_requests")
            requests.append(
                {
                    "replay_id": replay_id,
                    "work_id": work_id,
                    "operator": _text(operator, "operator"),
                    "reason": _text(reason, "reason"),
                    "generation": generation,
                    "requested_at": now,
                }
            )
            self._replace_many(work=work, replay_requests=requests)
            return ReplayRequest(
                replay_id, work_id, operator, reason, generation, now
            )

        return self.writer.run(operation)

    def reconcile(
        self, *, now: float | None = None
    ) -> list[ReconciliationFinding]:
        timestamp = self.now() if now is None else _finite(now, "now")

        def operation() -> list[ReconciliationFinding]:
            detected: dict[str, tuple[str, str, dict[str, Any]]] = {}
            attempts = {row["attempt_id"]: row for row in self._rows("attempts")}
            publications = self._rows("publications")
            publication_by_work = {row["work_id"]: row for row in publications}
            for row in self._rows("work"):
                attempt = attempts.get(row["committed_attempt_id"])
                publication = publication_by_work.get(row["work_id"])
                if row["committed_attempt_id"] is not None and (
                    attempt is None
                    or attempt["status"] != "SUCCEEDED"
                    or publication is None
                    or publication["attempt_id"] != row["committed_attempt_id"]
                ):
                    detected[f"INVALID_COMMITTED_POINTER:{row['work_id']}"] = (
                        "INVALID_COMMITTED_POINTER",
                        "ERROR",
                        {"committed_attempt_id": row["committed_attempt_id"]},
                    )
                if (
                    row["status"] == "LEASED"
                    and float(row["lease_expires_at"]) <= timestamp
                ):
                    detected[f"EXPIRED_LEASE:{row['work_id']}"] = (
                        "EXPIRED_LEASE",
                        "ERROR",
                        {"lease_expires_at": row["lease_expires_at"]},
                    )
            findings_rows = self._rows("reconciliation_findings")
            current = {row["finding_id"]: row for row in findings_rows}
            active_ids: set[str] = set()
            for entity, (kind, severity, details) in detected.items():
                finding_id = "finding-" + hashlib.sha256(entity.encode()).hexdigest()
                active_ids.add(finding_id)
                if finding_id in current:
                    current[finding_id]["last_seen_at"] = timestamp
                    current[finding_id]["resolved_at"] = None
                else:
                    _, entity_key = entity.split(":", 1)
                    row = {
                        "finding_id": finding_id,
                        "finding_type": kind,
                        "severity": severity,
                        "entity_key": entity_key,
                        "details_json": _canonical(details),
                        "first_seen_at": timestamp,
                        "last_seen_at": timestamp,
                        "resolved_at": None,
                    }
                    findings_rows.append(row)
                    current[finding_id] = row
            for row in findings_rows:
                if row["finding_id"] not in active_ids and row["resolved_at"] is None:
                    row["resolved_at"] = timestamp
            self._replace("reconciliation_findings", findings_rows)
            return [
                ReconciliationFinding(
                    str(row["finding_id"]),
                    str(row["finding_type"]),
                    str(row["severity"]),
                    str(row["entity_key"]),
                    json.loads(row["details_json"]),
                    float(row["first_seen_at"]),
                    float(row["last_seen_at"]),
                )
                for row in findings_rows
                if row["resolved_at"] is None
            ]

        return self.writer.run(operation)

    def _verified_process_batch(self, envelope: Any) -> dict[str, Any]:
        batch = _require_one(
            self._rows("batches"), "batch_id", envelope.batch_id
        )
        if (
            batch["envelope_sha256"] != envelope.envelope_sha256
            or batch["membership_sha256"] != envelope.membership_sha256
        ):
            raise BatchValidationError("authoritative envelope identity changed")
        attempts = [
            row
            for row in self._rows("attempts")
            if row["batch_id"] == envelope.batch_id
        ]
        work = {row["work_id"]: row for row in self._rows("work")}
        expected = {
            (item.work_id, item.attempt_id, item.fence, item.payload_sha256)
            for item in envelope.items
        }
        observed = {
            (
                row["work_id"],
                row["attempt_id"],
                int(row["fence"]),
                row["payload_sha256"],
            )
            for row in attempts
        }
        if observed != expected:
            raise LeaseLostError("authoritative attempt membership changed")
        if batch["status"] in {"LEASED", "SEALED"}:
            for attempt in attempts:
                row = work[attempt["work_id"]]
                if (
                    row["status"] != "LEASED"
                    or row["lease_attempt_id"] != attempt["attempt_id"]
                    or int(row["fence"]) != int(attempt["fence"])
                ):
                    raise LeaseLostError(
                        f"attempt {attempt['attempt_id']} lost its work fence"
                    )
        return batch

    def _admissible_prefix(
        self,
        rows: list[dict[str, Any]],
        minimum_items: int,
        lease: float,
        speed: float,
        factor: float,
        margin: float,
        profile_workers: int,
    ) -> list[dict[str, Any]]:
        selected, _, _ = _largest_admissible_claim_prefix(
            rows,
            minimum_items=minimum_items,
            profile_workers=profile_workers,
            minimum_speed_x=speed,
            safety_factor=factor,
            margin_seconds=margin,
            lease_seconds=lease,
        )
        return list(selected)

    def _rows(self, suffix: str) -> list[dict[str, Any]]:
        self.spark.catalog.refreshTable(self.tables[suffix])
        return [
            row.asDict(recursive=True)
            for row in self.spark.table(self.tables[suffix]).collect()
        ]

    def _replace_many(self, **tables: list[dict[str, Any]]) -> None:
        for suffix, rows in tables.items():
            self._replace(suffix, rows)

    def _replace(self, suffix: str, rows: list[dict[str, Any]]) -> None:
        keys = _KEYS[suffix]
        identities = [tuple(row.get(key) for key in keys) for row in rows]
        if len(identities) != len(set(identities)):
            raise BatchValidationError(f"duplicate {suffix} table identity")
        frame = self.spark.createDataFrame(rows, schema=_SCHEMAS[suffix])
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "false"
        ).saveAsTable(self.tables[suffix])
        observed = self._rows(suffix)
        if _normalized_rows(observed, keys) != _normalized_rows(rows, keys):
            raise BatchValidationError(
                f"{suffix} exact readback/cardinality verification failed"
            )

    def _rows_with_replacement(
        self,
        suffix: str,
        key: str,
        value: object,
        replacement: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows = self._rows(suffix)
        count = 0
        for index, row in enumerate(rows):
            if row[key] == value:
                rows[index] = dict(replacement)
                count += 1
        if count != 1:
            raise BatchValidationError(
                f"{suffix} replacement cardinality is {count}, expected 1"
            )
        return rows


def _normalized_rows(
    rows: Sequence[Mapping[str, Any]], keys: Sequence[str]
) -> list[str]:
    return sorted(
        (_canonical(dict(row)) for row in rows),
        key=lambda value: tuple(
            str(json.loads(value).get(key)) for key in keys
        ),
    )


def _registered(row: Mapping[str, Any]) -> RegisteredWork:
    return RegisteredWork(
        str(row["work_id"]),
        str(row["status"]),
        str(row["payload_sha256"]),
        str(row["runtime_key"]),
        float(row["duration_seconds"]),
        int(row["attempt_count"]),
        int(row["max_attempts"]),
        int(row["original_max_attempts"]),
        None
        if row.get("committed_attempt_id") is None
        else str(row["committed_attempt_id"]),
        None
        if row.get("publication_sequence") is None
        else int(row["publication_sequence"]),
        None if row.get("last_replay_id") is None else str(row["last_replay_id"]),
    )


def _one(
    rows: Sequence[dict[str, Any]], key: str, value: object
) -> dict[str, Any] | None:
    matches = [row for row in rows if row.get(key) == value]
    if len(matches) > 1:
        raise BatchValidationError(f"duplicate {key}={value!r}")
    return matches[0] if matches else None


def _require_one(
    rows: Sequence[dict[str, Any]], key: str, value: object
) -> dict[str, Any]:
    row = _one(rows, key, value)
    if row is None:
        raise KeyError(value)
    return row


def _membership(items: Sequence[ClaimedWork]) -> str:
    content = _canonical(
        [
            {
                "work_id": item.work_id,
                "attempt_id": item.attempt_id,
                "fence": item.fence,
                "payload_sha256": item.payload_sha256,
            }
            for item in items
        ]
    )
    return hashlib.sha256(content.encode()).hexdigest()


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            default=lambda item: item.isoformat(),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("value must be finite JSON data") from error


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _finite(value: object, name: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ValueError(f"{name} must be finite")
    return float(value)


def _positive(value: object, name: str) -> float:
    result = _finite(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


def _sha(value: object, name: str) -> str:
    text = _text(value, name)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return text


def _safe_segment(value: str) -> str:
    text = _text(value, "path segment")
    pure = PurePosixPath(text)
    if len(pure.parts) != 1 or text in {".", ".."}:
        raise ValueError(f"unsafe path segment {text!r}")
    return text

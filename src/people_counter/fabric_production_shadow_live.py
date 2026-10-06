"""Installed-wheel live control plane for the fixed production shadow.

The host sends a one-use, HMAC-authenticated request through the fixed
production-shadow evidence root.  This module accepts only enumerated
operations; it accepts no table, path, SQL, workspace, Lakehouse, Environment,
or Fabric item identifier.  Spark remains authoritative for migration,
quiescence, legacy-route, authorization, and shadow-table state.

Authorization is deliberately *not* called an ACID transaction.  It is a
serialized, idempotent two-table protocol protected by the exact
``people_counter_control_writer`` compare-and-set lock and an immutable
create-only intent ledger.  A partial/ambiguous operation retains that lock.
Only an exact replay whose intent proves the same reviewed operation may
finish the missing append and release it.
"""

from __future__ import annotations

import hashlib
import hmac
import importlib.metadata
import json
import platform
import sys
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any, Protocol, TextIO
from urllib.parse import unquote, urlsplit

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    FABRIC_RUNTIME,
    LAKEHOUSE_ID,
    PRODUCTION_FILES_ROOT,
    PRODUCTION_SHADOW_FILES_ROOT,
    WORKSPACE_ID,
    FabricCandidateAConfig,
)
from people_counter.fabric_production_migration import (
    ApplyStatus,
    JOURNAL_TABLE,
    MIGRATION_ID,
    JournalEntry,
    journal_integrity_errors,
)
from people_counter.fabric_production_migration_live import (
    DISPATCHER_LEASE_TABLE,
    LOCK_TABLE,
    REGISTRATION_LEASE_TABLE,
    WORK_TABLE,
    delta_control_writer_cas,
)
from people_counter.fabric_production_migration_tool import WRITER_ITEMS
from people_counter.fabric_production_shadow import (
    PACKAGE_VERSION,
    PRODUCTION_ALLOWLIST_TABLE,
    PRODUCTION_AUDIT_TABLE,
    AuthorizationIntent,
    AuthorizationProtocolState,
    ProductionShadowError,
    SHADOW_SCHEMAS,
    authorization_intent,
    classify_authorization_protocol,
)
from people_counter.fabric_production_routing import canonical_bytes, sha256_json
from people_counter.fabric_reflex_definition import REFLEX_ID
from people_counter.fabric_spark_canonical import (
    SparkCanonicalizationError,
    canonical_spark_json_bytes,
    spark_sha256,
)


LIVE_SCHEMA = "people-counter-production-shadow-live-request-v1"
RESULT_SCHEMA = "people-counter-production-shadow-live-result-v1"
FAILURE_SCHEMA = "people-counter-production-shadow-live-failure-v1"
LIVE_ROOT = f"{PRODUCTION_SHADOW_FILES_ROOT}controller/live"
CONTROL_COMMANDS = frozenset(
    {
        "snapshot",
        "status",
        "authorize",
        "bootstrap",
        "register",
        "claim",
        "recover-exact",
        "route",
    }
)
PROCESS_COMMANDS = frozenset({"process"})
RECONCILE_COMMANDS = frozenset({"compare", "reconcile"})
_ACTIVE_WORK = frozenset({"LEASED", "RUNNING", "STAGING", "WRITING"})
_HEX = frozenset("0123456789abcdef")
_HEX64 = frozenset("0123456789abcdef")
_IDENTITY_HASH_FIELDS = (
    "camera_sha256",
    "location_sha256",
    "model_sha256",
    "source_sha256",
    "config_sha256",
)
_FIXED_MODEL_FILES = {
    "r18": (
        "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
        "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
    ),
    "r50": (
        "Files/models/rtdetr_osnet/rtdetr_v2_r50vd/model.safetensors",
        "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
    ),
}
_FIXED_CONFIG_FILES = {
    "r18": (
        "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/config.json",
        "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
    ),
    "r50": (
        "Files/models/rtdetr_osnet/rtdetr_v2_r50vd/config.json",
        "Files/models/rtdetr_osnet/rtdetr_v2_r50vd/preprocessor_config.json",
    ),
}


class LiveShadowError(RuntimeError):
    """A signed request, Spark readback, or fixed operation was refused."""


class ShadowEvidenceFiles(Protocol):
    def exists(self, path: str) -> bool: ...

    def read_bytes(self, path: str) -> bytes: ...

    def create_bytes(self, path: str, content: bytes) -> None: ...


class NotebookShadowEvidenceFiles:
    """Create-only notebookutils adapter confined to the shadow live root."""

    @staticmethod
    def _fs() -> Any:
        import notebookutils

        return notebookutils.fs

    @staticmethod
    def _path(path: str) -> str:
        candidate = PurePosixPath(path)
        if (
            "\\" in path
            or "\x00" in path
            or candidate.is_absolute()
            or str(candidate) != path
            or not path.startswith(LIVE_ROOT + "/")
            or ".." in candidate.parts
        ):
            raise LiveShadowError("evidence path escaped the fixed shadow root")
        return path

    def exists(self, path: str) -> bool:
        return bool(self._fs().exists(self._path(path)))

    def read_bytes(self, path: str) -> bytes:
        value = self._fs().head(self._path(path), 100 * 1024 * 1024)
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        raise OSError("notebookutils.fs.head returned unsupported content")

    def create_bytes(self, path: str, content: bytes) -> None:
        checked = self._path(path)
        if self.exists(checked):
            raise FileExistsError(checked)
        if self._fs().put(checked, content.decode("utf-8"), False) is False:
            raise OSError(f"OneLake create failed for {checked}")
        if self.read_bytes(checked) != content:
            raise OSError(f"OneLake create readback differs for {checked}")


def _safe_invocation(value: str) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or not value[0].isalnum()
        or any(not (item.isalnum() or item in "._-") for item in value)
    ):
        raise LiveShadowError("invocation ID is not a safe fixed-root segment")
    return value


def request_path(invocation_id: str) -> str:
    return f"{LIVE_ROOT}/invocations/{_safe_invocation(invocation_id)}/request.json"


def result_path(invocation_id: str) -> str:
    return f"{LIVE_ROOT}/invocations/{_safe_invocation(invocation_id)}/result.json"


def failure_path(invocation_id: str) -> str:
    return f"{LIVE_ROOT}/invocations/{_safe_invocation(invocation_id)}/failure.json"


def started_path(invocation_id: str) -> str:
    return f"{LIVE_ROOT}/invocations/{_safe_invocation(invocation_id)}/started.json"


def intent_path(authorization_id: str, stage: str) -> str:
    if (
        len(authorization_id) != 64
        or any(character not in _HEX for character in authorization_id)
    ):
        raise LiveShadowError("authorization ID is not lowercase SHA-256")
    if stage not in {"00-prepared", "10-allowlist", "20-audit", "30-committed"}:
        raise LiveShadowError("authorization intent stage is not fixed")
    return (
        f"{LIVE_ROOT}/authorization/{authorization_id}/"
        f"{stage}.json"
    )


def route_binding_path(work_id: str) -> str:
    if (
        not isinstance(work_id, str)
        or not 1 <= len(work_id) <= 128
        or not work_id[0].isalnum()
        or any(
            not (character.isalnum() or character in "._=-")
            for character in work_id
        )
    ):
        raise LiveShadowError("route-binding work identity is invalid")
    return f"{LIVE_ROOT}/routes/{work_id}/binding.json"


def validate_synthetic_route_context(
    context: Mapping[str, Any], work_id: str
) -> dict[str, Any]:
    """Validate the exact non-legacy process binding without exposing values."""

    expected_keys = {
        "authorization_id",
        "comparison_mode",
        "identity",
        "plan_sha256",
        "provenance",
        "reviewed_at",
        "reviewer",
        "route_mode",
        "source_evidence_sha256",
    }
    if set(context) != expected_keys:
        raise LiveShadowError("synthetic route binding fields differ")
    identity = context.get("identity")
    if (
        context.get("route_mode") != "SHADOW_SYNTHETIC"
        or context.get("comparison_mode") != "NO_LEGACY_BASELINE"
        or not isinstance(identity, Mapping)
        or identity.get("work_id") != work_id
        or set(identity) != {"work_id", *_IDENTITY_HASH_FIELDS}
    ):
        raise LiveShadowError("synthetic route mode or identity differs")
    for name in (
        "authorization_id",
        "plan_sha256",
        "source_evidence_sha256",
        *tuple(_IDENTITY_HASH_FIELDS),
    ):
        value = context.get(name) if name in context else identity.get(name)
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in _HEX64 for character in value)
        ):
            raise LiveShadowError("synthetic route hash identity differs")
    return dict(context)


def _canonical(value: object) -> bytes:
    return canonical_bytes(value)


def _create(files: ShadowEvidenceFiles, path: str, value: object) -> None:
    content = _canonical(value) + b"\n"
    if files.exists(path):
        if files.read_bytes(path) != content:
            raise LiveShadowError(f"create-only evidence conflicts at {path}")
        return
    files.create_bytes(path, content)
    if files.read_bytes(path) != content:
        raise LiveShadowError(f"create-only evidence readback differs at {path}")


def _start_once(
    files: ShadowEvidenceFiles, invocation_id: str, command: str, request: object
) -> None:
    path = started_path(invocation_id)
    if files.exists(path):
        raise LiveShadowError("signed invocation was already consumed")
    content = _canonical(
        {
            "schema": "people-counter-shadow-invocation-started-v1",
            "invocation_id": invocation_id,
            "command": command,
            "request_sha256": hashlib.sha256(_canonical(request)).hexdigest(),
        }
    ) + b"\n"
    files.create_bytes(path, content)
    if files.read_bytes(path) != content:
        raise LiveShadowError("invocation start readback differs")


def _normalize(value: object) -> object:
    if isinstance(value, datetime):
        observed = value
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        return observed.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return _normalize(asdict(value))
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise LiveShadowError("internal JSON mapping key is not a string")
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise LiveShadowError("internal value is not canonical JSON")


def _row(
    value: Any,
    *,
    table: str,
    row_index: int,
) -> dict[str, Any]:
    normalized = json.loads(
        canonical_spark_json_bytes(
            value,
            stage=f"spark-row-normalization:{table}",
            path=f"$[{row_index}]",
        )
    )
    if not isinstance(normalized, dict):
        raise LiveShadowError("Spark row is not an object")
    return normalized


def _exact_rows(
    observed: Sequence[Mapping[str, Any]],
    expected: Mapping[str, Any],
) -> tuple[bool, bool]:
    rows = [json.loads(_canonical(_normalize(dict(item)))) for item in observed]
    expected_row = json.loads(_canonical(_normalize(dict(expected))))
    if not rows:
        return False, False
    return len(rows) == 1 and rows[0] == expected_row, True


def _route_payload(
    *,
    work_id: str,
    attempt_id: str,
    identity: Mapping[str, Any],
    attempt: Mapping[str, Any],
    publication: Mapping[str, Any],
    records: object,
    context: Mapping[str, Any],
    shadow: bool,
    pointer_fence: object | None = None,
    pointer_attempt_id: object | None = None,
) -> dict[str, Any]:
    """Normalize the fixed route readback consumed by the host controller."""

    if not isinstance(records, list) or any(
        not isinstance(item, Mapping) for item in records
    ):
        raise LiveShadowError("committed route records are not a list of objects")
    output_path = str(attempt.get("output_path"))
    expected_root = (
        PRODUCTION_SHADOW_FILES_ROOT if shadow else PRODUCTION_FILES_ROOT
    )
    if not output_path.startswith(expected_root):
        raise LiveShadowError("committed route output escaped its fixed namespace")
    fence = int(attempt.get("fence", 0))
    published_at = publication.get("published_at")
    if isinstance(published_at, (int, float)):
        timestamp = datetime.fromtimestamp(
            float(published_at), timezone.utc
        ).isoformat()
    elif isinstance(published_at, str):
        timestamp = published_at
    else:
        timestamp = datetime.fromtimestamp(0, timezone.utc).isoformat()
    numeric_values = [
        float(item.get("count", 0.0))
        for item in records
        if isinstance(item.get("count", 0.0), (int, float))
        and not isinstance(item.get("count", 0.0), bool)
    ]
    return {
        "work_id": work_id,
        "attempt_id": attempt_id,
        "logical_identity_sha256": sha256_json(dict(identity)),
        "fence": fence,
        "pointer_fence": (
            fence if pointer_fence is None else int(pointer_fence)
        ),
        "output_path": output_path,
        "output_sha256": str(attempt.get("output_sha256")),
        "sealed": str(attempt.get("status")) in {"SEALED", "SUCCEEDED"},
        "committed": True,
        "pointer_attempt_id": (
            attempt_id
            if pointer_attempt_id is None
            else str(pointer_attempt_id)
        ),
        "publication_sequence": int(
            publication.get("publication_sequence", 0)
        ),
        "publication_count": 1,
        "authorization_id": str(context.get("authorization_id")),
        "plan_sha256": str(context.get("plan_sha256")),
        "provenance": dict(context.get("provenance", {})),
        "identity": dict(identity),
        "records": [dict(item) for item in records],
        "logical_total": sum(numeric_values),
        "frame_count": int(
            attempt.get("frame_count", len(records))
        ),
        "timestamp": timestamp,
    }


def _exact_recovery_inventory(
    rest: Mapping[str, Any],
    *,
    group: str,
    identity_fields: tuple[str, str],
    expected: set[tuple[str, str]],
    require_unique_item_ids: bool = False,
) -> list[Mapping[str, Any]]:
    """Return one exact signed inventory, rejecting omissions and duplicates."""

    values = rest.get(group)
    if not isinstance(values, list) or len(values) != len(expected):
        raise LiveShadowError(
            "exact recovery REST inventory is not complete and inactive"
        )
    if any(not isinstance(value, Mapping) for value in values):
        raise LiveShadowError(
            "exact recovery REST inventory is not complete and inactive"
        )
    items = [value for value in values if isinstance(value, Mapping)]
    observed = {
        (str(item.get(identity_fields[0])), str(item.get(identity_fields[1])))
        for item in items
    }
    item_ids = [item.get("id") for item in items]
    if observed != expected or (
        require_unique_item_ids
        and (
            any(not isinstance(item_id, str) or not item_id for item_id in item_ids)
            or len(set(item_ids)) != len(expected)
        )
    ):
        raise LiveShadowError(
            "exact recovery REST inventory is not complete and inactive"
        )
    return items


def _require_no_active_recovery_jobs(
    items: Sequence[Mapping[str, Any]],
) -> None:
    for item in items:
        active_jobs = item.get("active_jobs")
        if not isinstance(active_jobs, list):
            raise LiveShadowError(
                "exact recovery REST inventory is not complete and inactive"
            )
        if active_jobs:
            raise LiveShadowError(
                "exact recovery rejected active Fabric jobs"
            )


def _require_inactive_recovery_snapshot(rest: Mapping[str, Any]) -> None:
    """Require complete signed job inventories and no active writer."""

    reflex = rest.get("reflex")
    if (
        not isinstance(reflex, Mapping)
        or reflex.get("id") != REFLEX_ID
        or reflex.get("active") is not False
    ):
        raise LiveShadowError(
            "exact recovery REST inventory is not complete and inactive"
        )
    pipelines = _exact_recovery_inventory(
        rest,
        group="pipelines",
        identity_fields=("id", "display_name"),
        expected={
            (identifier, display_name)
            for display_name, identifier in WRITER_ITEMS
        },
    )
    sjds = _exact_recovery_inventory(
        rest,
        group="sjds",
        identity_fields=("job", "display_name"),
        expected={
            (job, f"pc-ca-production-shadow-{job}-v001")
            for job in ("control", "process", "reconcile")
        },
        require_unique_item_ids=True,
    )
    _require_no_active_recovery_jobs([*pipelines, *sjds])


class SparkShadowControl:
    """Fixed-table Spark implementation used only by installed-wheel SJDs."""

    def __init__(
        self,
        spark: Any,
        files: ShadowEvidenceFiles,
        *,
        clock: Callable[[], float] = time.time,
        artifact_reader: Callable[[str], bool] | None = None,
    ) -> None:
        self.spark = spark
        self.files = files
        self.clock = clock
        self.config = FabricCandidateAConfig.production_shadow()
        self.artifact_reader = (
            artifact_reader
            if artifact_reader is not None
            else getattr(spark, "artifact_reader", None)
        )

    def _table_rows(self, table: str) -> list[dict[str, Any]]:
        return [
            _row(item, table=table, row_index=index)
            for index, item in enumerate(self.spark.table(table).collect())
        ]

    def _maybe_rows(self, table: str) -> list[dict[str, Any]]:
        if not self.spark.catalog.tableExists(table):
            return []
        return self._table_rows(table)

    def _key_rows(
        self, table: str, key: str, value: object
    ) -> list[dict[str, Any]]:
        return [
            item
            for item in self._maybe_rows(table)
            if _normalize(item.get(key)) == _normalize(value)
        ]

    def migration_proof(self) -> dict[str, Any]:
        values: list[JournalEntry] = []
        for item in self._table_rows(JOURNAL_TABLE):
            if item.get("migration_id") != MIGRATION_ID:
                raise LiveShadowError("migration journal identity is not exact")
            try:
                values.append(
                    JournalEntry(
                        journal_id=str(item["journal_id"]),
                        migration_id=str(item["migration_id"]),
                        migration_version=int(item["migration_version"]),
                        status=ApplyStatus(str(item["status"])),
                        plan_sha256=str(item["plan_sha256"]),
                        before_sha256=str(item["before_sha256"]),
                        after_sha256=str(item["after_sha256"]),
                        receipts_sha256=str(item["receipts_sha256"]),
                        previous_evidence_sha256=str(
                            item["previous_evidence_sha256"]
                        ),
                        evidence_sha256=str(item["evidence_sha256"]),
                        error_text=str(item.get("error_text") or ""),
                    )
                )
            except (KeyError, TypeError, ValueError) as error:
                raise LiveShadowError(
                    "migration journal row is invalid"
                ) from error
        ordered: list[JournalEntry] = []
        remaining = list(values)
        previous = "0" * 64
        while remaining:
            matches = [
                item
                for item in remaining
                if item.previous_evidence_sha256 == previous
            ]
            if len(matches) != 1:
                raise LiveShadowError(
                    "migration journal chain is not exact"
                )
            selected = matches[0]
            ordered.append(selected)
            remaining.remove(selected)
            previous = selected.evidence_sha256
        if journal_integrity_errors(ordered):
            raise LiveShadowError("migration journal success is not exact")
        terminal = ordered[-1]
        plan = terminal.plan_sha256
        if len(plan) != 64:
            raise LiveShadowError("migration journal plan hash is invalid")
        return {
            "migration_id": MIGRATION_ID,
            "status": terminal.status.name,
            "plan_sha256": plan,
            "journal_sha256": sha256_json(
                [entry.to_dict() for entry in ordered]
            ),
        }

    def quiescence(self, request: Mapping[str, Any]) -> dict[str, Any]:
        rest = request.get("rest_snapshot")
        if not isinstance(rest, Mapping):
            raise LiveShadowError("signed REST snapshot is missing")
        reflex = rest.get("reflex")
        if not isinstance(reflex, Mapping):
            raise LiveShadowError("signed exact Reflex snapshot is missing")
        if reflex.get("id") != REFLEX_ID:
            raise LiveShadowError("signed Reflex identity differs")
        pipelines = rest.get("pipelines")
        if not isinstance(pipelines, list):
            raise LiveShadowError("signed writer pipeline inventory is missing")
        expected_pipelines = {
            identifier for _, identifier in WRITER_ITEMS
        }
        observed_pipelines = {
            str(value.get("id"))
            for value in pipelines
            if isinstance(value, Mapping)
        }
        if observed_pipelines != expected_pipelines:
            raise LiveShadowError("signed writer pipeline inventory differs")
        rest_writers = self._active_rest_writers(pipelines)
        active_work, active_leases = self._active_spark_writers()
        lock_rows = self._table_rows(LOCK_TABLE)
        if len(lock_rows) != 1 or lock_rows[0].get("lock_name") != "global":
            raise LiveShadowError("control writer row is not exact")
        return {
            "observed_at": datetime.fromtimestamp(
                self.clock(), timezone.utc
            ).isoformat(),
            "reflex_id": str(reflex.get("id")),
            "reflex_active": bool(reflex.get("active")),
            "active_writer_ids": sorted(rest_writers + active_work),
            "active_lease_ids": sorted(active_leases),
            "control_owner_id": lock_rows[0].get("owner_id"),
        }

    @staticmethod
    def _active_rest_writers(
        pipelines: Sequence[object],
    ) -> list[str]:
        active: list[str] = []
        for value in pipelines:
            if not isinstance(value, Mapping):
                continue
            jobs = value.get("active_jobs")
            if not isinstance(jobs, list):
                continue
            active.extend(
                f"{value['id']}:{job.get('id') or job.get('jobInstanceId')}"
                for job in jobs
                if isinstance(job, Mapping)
            )
        return active

    def _active_spark_writers(self) -> tuple[list[str], list[str]]:
        active_work = [
            f"{WORK_TABLE}:{row.get('work_id')}:{row.get('status')}"
            for row in self._maybe_rows(WORK_TABLE)
            if row.get("status") in _ACTIVE_WORK
        ]
        active_leases: list[str] = []
        now = float(self.clock())
        for table in (DISPATCHER_LEASE_TABLE, REGISTRATION_LEASE_TABLE):
            for row in self._maybe_rows(table):
                owner = row.get("owner_id")
                if owner in (None, ""):
                    continue
                expiry = row.get("expires_at")
                try:
                    if isinstance(expiry, datetime):
                        observed_expiry = expiry
                    elif isinstance(expiry, str):
                        observed_expiry = datetime.fromisoformat(expiry)
                    elif isinstance(expiry, (int, float)) and not isinstance(
                        expiry, bool
                    ):
                        observed_expiry = datetime.fromtimestamp(
                            float(expiry), timezone.utc
                        )
                    else:
                        raise TypeError("unsupported lease expiry")
                    if observed_expiry.tzinfo is None:
                        observed_expiry = observed_expiry.replace(
                            tzinfo=timezone.utc
                        )
                    epoch = observed_expiry.timestamp()
                except (
                    TypeError,
                    ValueError,
                    OverflowError,
                    OSError,
                ) as error:
                    raise LiveShadowError(
                        f"{table} active lease expiry is invalid"
                    ) from error
                if epoch > now:
                    active_leases.append(
                        f"{table}:{row.get('lock_name')}:{owner}"
                    )
        return active_work, active_leases

    @staticmethod
    def _payload(work: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        """Adapt only documented legacy payload encodings.

        The adapter never derives identity hashes from labels, timestamps, or
        other human-readable metadata.  Legacy ``video_work`` rows may store
        the immutable source and canonical config outside ``payload_json``;
        those exact fields are copied into the normalized payload without
        changing their values.
        """

        payload = work.get("payload")
        if isinstance(payload, Mapping):
            return dict(payload), True
        encoded = work.get("payload_json")
        if isinstance(encoded, str):
            try:
                value = json.loads(encoded)
            except json.JSONDecodeError:
                return {}, False
            if isinstance(value, dict):
                return value, True
            return {}, False

        source = work.get("source_uri")
        config_json = work.get("config_json")
        if not isinstance(source, str) or not isinstance(config_json, str):
            return {}, False
        try:
            config = json.loads(config_json)
        except json.JSONDecodeError:
            return {}, False
        if not isinstance(config, dict):
            return {}, False
        normalized_source = SparkShadowControl._legacy_source_path(source)
        if normalized_source is None:
            return {}, False
        return {
            **config,
            "source_video": normalized_source,
            "source_sha256": work.get("expected_sha256"),
        }, True

    @staticmethod
    def _legacy_source_path(value: str) -> str | None:
        mount = "/lakehouse/default/"
        if value.startswith(mount):
            relative = value[len(mount) :]
        elif value.startswith("Files/"):
            relative = value
        else:
            fixed_prefix = (
                f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
                f"{LAKEHOUSE_ID}/"
            )
            if value.startswith(fixed_prefix):
                relative = value[len(fixed_prefix) :]
            else:
                parsed = urlsplit(value)
                if (
                    parsed.scheme.lower() not in {"abfs", "abfss", "https"}
                    or parsed.query
                    or parsed.fragment
                ):
                    return None
                if parsed.scheme.lower() == "https" and (
                    parsed.username or parsed.password or parsed.port
                ):
                    return None
                decoded = unquote(parsed.path)
                segments = [item for item in decoded.split("/") if item]
                try:
                    incoming = segments.index("incoming")
                except ValueError:
                    return None
                relative = "Files/videos/" + "/".join(segments[incoming:])
        candidate = PurePosixPath(relative)
        if (
            not relative.startswith("Files/")
            or "\\" in relative
            or "\x00" in relative
            or candidate.is_absolute()
            or str(candidate) != relative
            or ".." in candidate.parts
        ):
            return None
        return mount + relative

    @staticmethod
    def _hash_state(value: object) -> tuple[bool, bool, bool]:
        present = value is not None
        string = isinstance(value, str)
        valid = string and len(value) == 64 and all(
            character in _HEX64 for character in value
        )
        return present, string, valid

    @staticmethod
    def _candidate_id(work: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            b"people-counter-legacy-route-candidate-v1\0"
            + canonical_spark_json_bytes(
                dict(work), stage="legacy-route-candidate-id"
            )
        ).hexdigest()

    def _artifact_readable(self, path: str) -> bool:
        if self.artifact_reader is not None:
            try:
                return bool(self.artifact_reader(path))
            except Exception:
                return False
        normalized = self._legacy_source_path(path)
        if normalized is None:
            return False
        relative = normalized[len("/lakehouse/default/") :]
        uri = (
            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
            f"{LAKEHOUSE_ID}/{relative}"
        )
        stream = None
        try:
            jvm = self.spark._jvm
            configuration = self.spark._jsc.hadoopConfiguration()
            java_uri = jvm.java.net.URI(uri)
            java_path = jvm.org.apache.hadoop.fs.Path(uri)
            filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(
                java_uri, configuration
            )
            if not filesystem.exists(java_path) or not filesystem.isFile(java_path):
                return False
            stream = filesystem.open(java_path)
            stream.read()
            return True
        except Exception:
            return False
        finally:
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    pass

    def _artifact_sha256(self, path: str) -> str | None:
        normalized = self._legacy_source_path(path)
        if normalized is None:
            return None
        try:
            digest = hashlib.sha256()
            with open(normalized, "rb") as stream:
                for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError:
            pass
        relative = normalized[len("/lakehouse/default/") :]
        uri = (
            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
            f"{LAKEHOUSE_ID}/{relative}"
        )
        stream = None
        digest_stream = None
        try:
            jvm = self.spark._jvm
            configuration = self.spark._jsc.hadoopConfiguration()
            java_uri = jvm.java.net.URI(uri)
            java_path = jvm.org.apache.hadoop.fs.Path(uri)
            filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(
                java_uri, configuration
            )
            stream = filesystem.open(java_path)
            digest = jvm.java.security.MessageDigest.getInstance("SHA-256")
            digest_stream = jvm.java.security.DigestInputStream(stream, digest)
            buffer = self.spark.sparkContext._gateway.new_array(
                jvm.byte, 8 * 1024 * 1024
            )
            while digest_stream.read(buffer) != -1:
                pass
            return bytes(
                int(value) & 0xFF for value in digest.digest()
            ).hex()
        except Exception:
            return None
        finally:
            for candidate in (digest_stream, stream):
                if candidate is not None:
                    try:
                        candidate.close()
                    except Exception:
                        pass

    def synthetic_candidate(self) -> dict[str, Any] | None:
        """Build one byte-verified, non-legacy-baseline synthetic input."""

        candidates: list[tuple[int, str, dict[str, Any]]] = []
        for work in self._table_rows(WORK_TABLE):
            if work.get("status") != "SUCCEEDED":
                continue
            payload, supported = self._payload(work)
            source_path = payload.get("source_video")
            if not supported or not isinstance(source_path, str):
                continue
            config_json = work.get("config_json")
            config_sha = work.get("config_sha256")
            expected_source = work.get("expected_sha256")
            size = work.get("expected_size_bytes")
            if (
                not isinstance(config_json, str)
                or not isinstance(config_sha, str)
                or hashlib.sha256(config_json.encode()).hexdigest() != config_sha
                or not isinstance(expected_source, str)
                or not isinstance(size, int)
                or isinstance(size, bool)
                or size <= 0
            ):
                continue
            observed_source = self._artifact_sha256(source_path)
            if observed_source != expected_source:
                continue
            detector = str(payload.get("detector_model", "r18"))
            model_paths = _FIXED_MODEL_FILES.get(detector, ())
            config_paths = _FIXED_CONFIG_FILES.get(detector, ())
            artifact_hashes = {
                path: self._artifact_sha256(path)
                for path in (*model_paths, *config_paths)
            }
            if (
                not model_paths
                or not config_paths
                or any(value is None for value in artifact_hashes.values())
            ):
                continue
            model_sha = sha256_json(
                {
                    "schema": "people-counter-fixed-model-artifacts-v1",
                    "artifacts": {
                        path: artifact_hashes[path] for path in model_paths
                    },
                }
            )
            canonical_config_sha = hashlib.sha256(
                canonical_spark_json_bytes(
                    json.loads(config_json),
                    stage="synthetic-config-canonical",
                )
            ).hexdigest()
            if canonical_config_sha != config_sha:
                continue
            identity = {
                "camera_sha256": sha256_json(
                    {
                        "schema": "people-counter-shadow-synthetic-camera-v1",
                        "fixture_class": "retained-non-sensitive-synthetic",
                    }
                ),
                "location_sha256": sha256_json(
                    {
                        "schema": "people-counter-shadow-synthetic-location-v1",
                        "namespace": PRODUCTION_SHADOW_FILES_ROOT,
                    }
                ),
                "model_sha256": model_sha,
                "source_sha256": observed_source,
                "config_sha256": config_sha,
            }
            work_id = (
                f"shadow-synthetic-{observed_source[:16]}-{config_sha[:8]}"
            )
            normalized_payload = {
                **payload,
                "source_video": source_path,
                "source_sha256": observed_source,
                "duration_seconds": float(work["duration_seconds"]),
                "runtime_key": str(work.get("runtime_sha256") or config_sha),
            }
            evidence = {
                "comparison_mode": "NO_LEGACY_BASELINE",
                "source_bytes_verified": True,
                "config_canonical_verified": True,
                "model_bytes_verified": True,
                "source_sha256": observed_source,
                "config_sha256": config_sha,
                "model_sha256": model_sha,
                "artifact_set_sha256": sha256_json(artifact_hashes),
            }
            candidates.append(
                (
                    size,
                    self._candidate_id(work),
                    {
                        "candidate_id": self._candidate_id(work),
                        "comparison_mode": "NO_LEGACY_BASELINE",
                        "identity": {"work_id": work_id, **identity},
                        "payload": normalized_payload,
                        "source_evidence_sha256": sha256_json(evidence),
                        "evidence": evidence,
                    },
                )
            )
        return min(candidates, default=(0, "", None))[-1]

    def synthetic_diagnostics(self) -> list[dict[str, Any]]:
        """Explain synthetic qualification using opaque IDs and booleans only."""

        result: list[dict[str, Any]] = []
        for work in self._table_rows(WORK_TABLE):
            if work.get("status") != "SUCCEEDED":
                continue
            payload, supported = self._payload(work)
            source_path = payload.get("source_video")
            config_json = work.get("config_json")
            config_sha = work.get("config_sha256")
            expected_source = work.get("expected_sha256")
            source_digest = (
                self._artifact_sha256(source_path)
                if isinstance(source_path, str)
                else None
            )
            detector = str(payload.get("detector_model", "r18"))
            model_paths = _FIXED_MODEL_FILES.get(detector, ())
            config_paths = _FIXED_CONFIG_FILES.get(detector, ())
            model_readable = bool(model_paths) and all(
                self._artifact_sha256(path) is not None for path in model_paths
            )
            config_artifacts_readable = bool(config_paths) and all(
                self._artifact_sha256(path) is not None for path in config_paths
            )
            config_bytes_match = (
                isinstance(config_json, str)
                and isinstance(config_sha, str)
                and hashlib.sha256(config_json.encode()).hexdigest() == config_sha
            )
            observations = {
                "payload_supported": supported,
                "source_path_supported": isinstance(source_path, str)
                and self._legacy_source_path(source_path) is not None,
                "source_bytes_readable": source_digest is not None,
                "source_bytes_match_registered_hash": (
                    isinstance(expected_source, str)
                    and source_digest == expected_source
                ),
                "config_document_hash_match": config_bytes_match,
                "model_artifacts_readable": model_readable,
                "config_artifacts_readable": config_artifacts_readable,
            }
            codes = [
                code
                for field, code in (
                    ("payload_supported", "UNSUPPORTED_LEGACY_WORK_SCHEMA"),
                    ("source_path_supported", "UNSUPPORTED_SOURCE_PATH"),
                    ("source_bytes_readable", "MISSING_READABLE_SOURCE_ARTIFACT"),
                    (
                        "source_bytes_match_registered_hash",
                        "SOURCE_BYTES_HASH_MISMATCH",
                    ),
                    ("config_document_hash_match", "CONFIG_DOCUMENT_HASH_MISMATCH"),
                    ("model_artifacts_readable", "MISSING_READABLE_MODEL_ARTIFACT"),
                    (
                        "config_artifacts_readable",
                        "MISSING_READABLE_CONFIG_ARTIFACT",
                    ),
                )
                if not observations[field]
            ]
            result.append(
                {
                    "candidate_id": self._candidate_id(work),
                    "eligible": not codes,
                    "observations": observations,
                    "rejection_codes": codes,
                }
            )
        return sorted(result, key=lambda value: value["candidate_id"])

    @staticmethod
    def _identity_value(
        work: Mapping[str, Any], payload: Mapping[str, Any], field: str
    ) -> object:
        if field in work:
            return work.get(field)
        if field in payload:
            return payload.get(field)
        if field == "source_sha256":
            return work.get("expected_sha256")
        return None

    def _route_inventory(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return eligible routes and redaction-safe per-row diagnostics."""

        all_work = self._table_rows(WORK_TABLE)
        work_rows = sorted(
            (row for row in all_work if row.get("status") == "SUCCEEDED"),
            key=self._candidate_id,
        )
        work_ids = [row.get("work_id") for row in work_rows]
        attempts_table = (
            "people_counter_video_attempts"
            if self.spark.catalog.tableExists("people_counter_video_attempts")
            else "people_counter_video_attempts_committed"
        )
        attempts_supported = self.spark.catalog.tableExists(attempts_table)
        attempts = self._maybe_rows(attempts_table)
        publications_supported = self.spark.catalog.tableExists(
            "people_counter_ca_publications"
        )
        publications = self._maybe_rows("people_counter_ca_publications")
        views_supported = self.spark.catalog.tableExists(
            "people_counter_runs_committed"
        )
        committed = self._maybe_rows("people_counter_runs_committed")
        routes: list[dict[str, Any]] = []
        diagnostics: list[dict[str, Any]] = []

        for work in work_rows:
            candidate_id = self._candidate_id(work)
            raw_work_id = work.get("work_id")
            work_id = raw_work_id if isinstance(raw_work_id, str) else ""
            pointer = work.get("committed_attempt_id")
            attempt_id = pointer if isinstance(pointer, str) else ""
            payload, payload_supported = self._payload(work)
            attempt_by_work = [
                row for row in attempts if row.get("work_id") == raw_work_id
            ]
            attempt_matches = [
                row
                for row in attempt_by_work
                if row.get("attempt_id") == pointer
            ]
            publication_matches = [
                row
                for row in publications
                if row.get("work_id") == raw_work_id
                and row.get("attempt_id") == pointer
            ]
            view_matches = [
                row
                for row in committed
                if row.get("work_id") == raw_work_id
                and (
                    row.get("attempt_id") == pointer
                    or row.get("committed_attempt_id") == pointer
                )
            ]
            fields: dict[str, bool] = {
                "work_id_present": raw_work_id is not None,
                "work_id_string": isinstance(raw_work_id, str),
                "status_string": isinstance(work.get("status"), str),
                "status_succeeded": work.get("status") == "SUCCEEDED",
                "pointer_present": pointer is not None,
                "pointer_string": isinstance(pointer, str),
                "payload_supported": payload_supported,
            }
            joins: dict[str, bool] = {
                "work_present": True,
                "work_unique": work_ids.count(raw_work_id) == 1,
                "attempt_table_supported": attempts_supported,
                "attempt_present": bool(attempt_matches),
                "attempt_unique": len(attempt_matches) == 1,
                "attempt_work_identity_match": bool(attempt_matches)
                and attempt_matches[0].get("work_id") == raw_work_id,
                "attempt_pointer_match": bool(attempt_matches)
                and attempt_matches[0].get("attempt_id") == pointer,
                "attempt_succeeded": len(attempt_matches) == 1
                and attempt_matches[0].get("status") == "SUCCEEDED",
                "publication_table_supported": publications_supported,
                "publication_present": bool(publication_matches),
                "publication_unique": len(publication_matches) == 1,
                "view_table_supported": views_supported,
                "view_present": bool(view_matches),
                "view_unique": len(view_matches) == 1,
            }
            rejection_codes: set[str] = set()
            if not joins["work_unique"]:
                rejection_codes.add("DUPLICATE_AMBIGUOUS_WORK_ROW")
            if not isinstance(pointer, str) or not pointer:
                rejection_codes.add("MISSING_POINTER")
            if not attempts_supported:
                rejection_codes.add("UNSUPPORTED_LEGACY_ATTEMPT_SCHEMA")
            if not attempt_matches:
                rejection_codes.add("MISSING_ATTEMPT_JOIN")
                if attempt_by_work:
                    rejection_codes.add("POINTER_MISMATCH")
            elif len(attempt_matches) != 1:
                rejection_codes.add("DUPLICATE_AMBIGUOUS_ATTEMPT_ROW")
            elif attempt_matches[0].get("status") != "SUCCEEDED":
                rejection_codes.add("ATTEMPT_NOT_SUCCEEDED")
            if not publications_supported:
                rejection_codes.add("UNSUPPORTED_LEGACY_PUBLICATION_SCHEMA")
            if not publication_matches:
                rejection_codes.add("MISSING_PUBLICATION_JOIN")
            elif len(publication_matches) != 1:
                rejection_codes.add("DUPLICATE_AMBIGUOUS_PUBLICATION_ROW")
            if not views_supported:
                rejection_codes.add("UNSUPPORTED_LEGACY_VIEW_SCHEMA")
            if not view_matches:
                rejection_codes.add("MISSING_COMMITTED_VIEW_JOIN")
            elif len(view_matches) != 1:
                rejection_codes.add("DUPLICATE_AMBIGUOUS_COMMITTED_VIEW_ROW")
            if not payload_supported:
                rejection_codes.add("UNSUPPORTED_LEGACY_WORK_SCHEMA")

            identity: dict[str, str] = {"work_id": work_id}
            identity_rows = [
                row
                for row in (
                    work,
                    *attempt_matches[:1],
                    *publication_matches[:1],
                    *view_matches[:1],
                )
                if isinstance(row, Mapping)
            ]
            for field in _IDENTITY_HASH_FIELDS:
                value = self._identity_value(work, payload, field)
                present, string, valid = self._hash_state(value)
                fields[f"{field}_present"] = present
                fields[f"{field}_string"] = string
                fields[f"{field}_valid"] = valid
                present_row_values = [
                    row.get(field) for row in identity_rows if field in row
                ]
                fields[f"{field}_identity_match"] = all(
                    item == value for item in present_row_values
                )
                label = field.removesuffix("_sha256").upper()
                if not present:
                    rejection_codes.add(f"MISSING_{label}_SHA256")
                elif not valid:
                    rejection_codes.add(f"INVALID_{label}_SHA256")
                elif not fields[f"{field}_identity_match"]:
                    rejection_codes.add(f"{label}_IDENTITY_MISMATCH")
                if valid:
                    identity[field] = str(value)

            attempt = attempt_matches[0] if len(attempt_matches) == 1 else {}
            publication = (
                publication_matches[0] if len(publication_matches) == 1 else {}
            )
            view = view_matches[0] if len(view_matches) == 1 else {}
            output_values = [
                row.get("output_path")
                for row in (attempt, publication, view)
                if "output_path" in row
            ]
            output_path = attempt.get("output_path")
            fields["output_path_present"] = output_path is not None
            fields["output_path_string"] = isinstance(output_path, str)
            fields["output_path_identity_match"] = bool(output_values) and len(
                set(output_values)
            ) == 1
            fields["output_path_production_owned"] = (
                isinstance(output_path, str)
                and output_path.startswith(PRODUCTION_FILES_ROOT)
            )
            if output_path is None:
                rejection_codes.add("MISSING_OUTPUT_PATH")
            elif not fields["output_path_production_owned"]:
                rejection_codes.add("OUTPUT_PATH_NOT_PRODUCTION_OWNED")
            elif not fields["output_path_identity_match"]:
                rejection_codes.add("OUTPUT_PATH_IDENTITY_MISMATCH")

            output_hash_values = [
                row.get("output_sha256")
                for row in (attempt, publication, view)
                if "output_sha256" in row
            ]
            output_sha = attempt.get("output_sha256")
            output_present, output_string, output_valid = self._hash_state(output_sha)
            fields["output_sha256_present"] = output_present
            fields["output_sha256_string"] = output_string
            fields["output_sha256_valid"] = output_valid
            fields["output_sha256_identity_match"] = bool(
                output_hash_values
            ) and len(set(output_hash_values)) == 1
            if not output_present:
                rejection_codes.add("MISSING_OUTPUT_SHA256")
            elif not output_valid:
                rejection_codes.add("INVALID_OUTPUT_SHA256")
            elif not fields["output_sha256_identity_match"]:
                rejection_codes.add("OUTPUT_SHA256_IDENTITY_MISMATCH")

            source_path = payload.get("source_video")
            fields["source_artifact_path_present"] = source_path is not None
            fields["source_artifact_path_supported"] = (
                isinstance(source_path, str)
                and self._legacy_source_path(source_path) is not None
            )
            fields["source_artifact_readable"] = (
                fields["source_artifact_path_supported"]
                and self._artifact_readable(str(source_path))
            )
            if not fields["source_artifact_readable"]:
                rejection_codes.add("MISSING_READABLE_SOURCE_ARTIFACT")

            detector = str(payload.get("detector_model", "r18"))
            model_paths = _FIXED_MODEL_FILES.get(detector, ())
            config_paths = _FIXED_CONFIG_FILES.get(detector, ())
            fields["model_artifact_schema_supported"] = bool(model_paths)
            fields["model_artifact_readable"] = bool(model_paths) and all(
                self._artifact_readable(path) for path in model_paths
            )
            fields["config_artifact_schema_supported"] = bool(config_paths)
            fields["config_artifact_readable"] = bool(config_paths) and all(
                self._artifact_readable(path) for path in config_paths
            )
            if not fields["model_artifact_readable"]:
                rejection_codes.add("MISSING_READABLE_MODEL_ARTIFACT")
            if not fields["config_artifact_readable"]:
                rejection_codes.add("MISSING_READABLE_CONFIG_ARTIFACT")

            eligible = not rejection_codes
            diagnostic = {
                "candidate_id": candidate_id,
                "eligible": eligible,
                "fields": fields,
                "joins": joins,
                "rejection_codes": sorted(rejection_codes),
            }
            diagnostics.append(diagnostic)
            if not eligible:
                continue

            shared = {
                **identity,
                "attempt_id": attempt_id,
                "output_path": str(output_path),
                "output_sha256": str(output_sha),
            }
            rows = {
                "work": {
                    **work,
                    **shared,
                    "payload": payload,
                    "status": "SUCCEEDED",
                    "committed_attempt_id": attempt_id,
                },
                "attempt": {**attempt, **shared, "status": "SUCCEEDED"},
                "publication": {**publication, **shared},
                "committed_view": {**view, **shared},
            }
            routes.append(
                {
                    "work_id": work_id,
                    "attempt_id": attempt_id,
                    "identity": identity,
                    "output_sha256": str(output_sha),
                    "source_rows_sha256": sha256_json(
                        {
                            key: spark_sha256(
                                value,
                                stage=f"spark-row-canonical-hash:{key}",
                            )
                            for key, value in sorted(rows.items())
                        }
                    ),
                    "rows": rows,
                }
            )
        return routes, diagnostics

    def eligible_routes(self) -> list[dict[str, Any]]:
        """Return a deterministic inventory of complete succeeded legacy routes."""
        return self._route_inventory()[0]

    def route_diagnostics(self) -> list[dict[str, Any]]:
        """Return only opaque IDs, enumerated codes, and boolean observations."""

        return self._route_inventory()[1]

    def status(
        self,
        request: Mapping[str, Any],
        *,
        eligible_route_count: int | None = None,
    ) -> dict[str, Any]:
        shadow = {}
        for suffix in SHADOW_SCHEMAS:
            table = self.config.table(suffix)
            shadow[table] = (
                len(self._table_rows(table))
                if self.spark.catalog.tableExists(table)
                else None
            )
        authorization_id = request.get("authorization_id")
        recovery: dict[str, Any] | None = None
        if isinstance(authorization_id, str):
            stages = {
                stage: self.files.exists(intent_path(authorization_id, stage))
                for stage in (
                    "00-prepared",
                    "10-allowlist",
                    "20-audit",
                    "30-committed",
                )
            }
            recovery = {
                "authorization_id": authorization_id,
                "stages": stages,
                "partial": stages["00-prepared"] and not stages["30-committed"],
                "recovery": (
                    "exact authorize replay required"
                    if stages["00-prepared"] and not stages["30-committed"]
                    else None
                ),
            }
        return {
            "migration": self.migration_proof(),
            "quiescence": self.quiescence(request),
            "eligible_route_count": (
                len(self.eligible_routes())
                if eligible_route_count is None
                else eligible_route_count
            ),
            "shadow_tables": shadow,
            "authorization_recovery": recovery,
        }

    def snapshot(self, request: Mapping[str, Any]) -> dict[str, Any]:
        eligible_routes, route_diagnostics = self._route_inventory()
        synthetic_candidate = self.synthetic_candidate()
        selected_work = request.get("selected_work_id")
        authorization_id = request.get("authorization_id")
        authorization = {
            "allowlist": (
                []
                if not isinstance(selected_work, str)
                else self._key_rows(
                    PRODUCTION_ALLOWLIST_TABLE, "work_id", selected_work
                )
            ),
            "audit": (
                []
                if not isinstance(authorization_id, str)
                else self._key_rows(
                    PRODUCTION_AUDIT_TABLE, "audit_id", authorization_id
                )
            ),
        }
        return {
            "migration": self.migration_proof(),
            "quiescence": self.quiescence(request),
            "eligible_routes": eligible_routes,
            "route_diagnostics": route_diagnostics,
            "synthetic_candidate": synthetic_candidate,
            "synthetic_diagnostics": self.synthetic_diagnostics(),
            "authorization": authorization,
            "status": self.status(
                request, eligible_route_count=len(eligible_routes)
            ),
        }

    def route(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Read one exact committed legacy or shadow route."""

        work_id = str(request.get("selected_work_id"))
        context = request.get("route_context")
        if not isinstance(context, Mapping):
            raise LiveShadowError("route authorization context is missing")
        kind = request.get("route_kind")
        if kind == "legacy":
            matches = [
                item for item in self.eligible_routes()
                if item["work_id"] == work_id
            ]
            if len(matches) != 1:
                raise LiveShadowError("exact plan-pinned legacy route is unavailable")
            source = matches[0]
            attempt = source["rows"]["attempt"]
            publication = source["rows"]["publication"]
            view = source["rows"]["committed_view"]
            records = view.get("records", view.get("records_json", []))
            if isinstance(records, str):
                records = json.loads(records)
            return _route_payload(
                work_id=work_id,
                attempt_id=str(source["attempt_id"]),
                identity=dict(source["identity"]),
                attempt=attempt,
                publication=publication,
                records=records,
                context=context,
                shadow=False,
            )
        if kind != "shadow":
            raise LiveShadowError("route kind is not fixed")
        work_rows = self._key_rows(
            self.config.table("work"), "work_id", work_id
        )
        if not work_rows:
            return {"route": None}
        if len(work_rows) != 1 or work_rows[0].get("status") != "SUCCEEDED":
            raise LiveShadowError("shadow work exists but is not exactly committed")
        attempt_id = str(work_rows[0].get("committed_attempt_id"))
        attempts = self._key_rows(
            self.config.table("attempts"), "attempt_id", attempt_id
        )
        publications = [
            item
            for item in self._key_rows(
                self.config.table("publications"), "work_id", work_id
            )
            if str(item.get("attempt_id")) == attempt_id
        ]
        if len(attempts) != 1 or len(publications) != 1:
            raise LiveShadowError("shadow pointer/publication readback is not exact")
        binding_path = route_binding_path(work_id)
        if not self.files.exists(binding_path):
            raise LiveShadowError("committed shadow route binding is missing")
        binding = json.loads(self.files.read_bytes(binding_path))
        if (
            not isinstance(binding, Mapping)
            or binding.get("route_context") != context
        ):
            raise LiveShadowError("committed shadow route binding differs")
        stored_context = binding["route_context"]
        if not isinstance(stored_context, Mapping):
            raise LiveShadowError("committed shadow route context is invalid")
        records = attempts[0].get("records_json", "[]")
        if isinstance(records, str):
            records = json.loads(records)
        return _route_payload(
            work_id=work_id,
            attempt_id=attempt_id,
            identity=dict(stored_context["identity"]),
            attempt={
                **attempts[0],
                "fence": attempts[0].get(
                    "fence", work_rows[0].get("fence")
                ),
            },
            publication=publications[0],
            records=records,
            context=stored_context,
            shadow=True,
            pointer_fence=work_rows[0].get("fence"),
            pointer_attempt_id=work_rows[0].get("committed_attempt_id"),
        )

    def _owner(self) -> str | None:
        rows = self._table_rows(LOCK_TABLE)
        if len(rows) != 1 or rows[0].get("lock_name") != "global":
            raise LiveShadowError("control writer row is not exact")
        value = rows[0].get("owner_id")
        return None if value is None else str(value)

    def _acquire_or_resume(
        self, owner: str, authorization_id: str
    ) -> None:
        observed = self._owner()
        cas = delta_control_writer_cas(self.spark)
        if observed is None:
            cas(None, owner)
            if self._owner() != owner:
                raise LiveShadowError("authorization lock CAS readback failed")
            return
        prefix = f"pc-shadow-auth-{authorization_id[:24]}-"
        rows = self._table_rows(LOCK_TABLE)
        acquired = rows[0].get("acquired_at")
        try:
            acquired_at = (
                acquired
                if isinstance(acquired, datetime)
                else datetime.fromisoformat(str(acquired))
            )
            if acquired_at.tzinfo is None:
                acquired_at = acquired_at.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError) as error:
            raise LiveShadowError(
                "retained authorization lock timestamp is invalid"
            ) from error
        stale = (
            datetime.fromtimestamp(self.clock(), timezone.utc) - acquired_at
        ).total_seconds() >= 300.0
        intent_exists = self.files.exists(
            intent_path(authorization_id, "00-prepared")
        )
        if (
            not observed.startswith(prefix)
            or not stale
            or not intent_exists
        ):
            raise LiveShadowError(
                "control writer is active or not an exact stale partial "
                f"authorization: {observed}"
            )
        cas(observed, owner)
        if self._owner() != owner:
            raise LiveShadowError("authorization recovery lock CAS readback failed")

    def _release(self, owner: str) -> None:
        if self._owner() != owner:
            raise LiveShadowError("authorization lock owner changed")
        delta_control_writer_cas(self.spark)(owner, None)
        if self._owner() is not None:
            raise LiveShadowError("authorization lock release readback failed")

    def _append(self, table: str, row: Mapping[str, Any]) -> None:
        frame = self.spark.createDataFrame([dict(row)])
        frame.write.format("delta").mode("append").saveAsTable(table)
        self.spark.catalog.refreshTable(table)

    def _read_intent(self, expected: AuthorizationIntent) -> AuthorizationIntent | None:
        path = intent_path(expected.authorization_id, "00-prepared")
        if not self.files.exists(path):
            return None
        value = json.loads(self.files.read_bytes(path))
        if not isinstance(value, Mapping) or value.get("intent") is None:
            raise LiveShadowError("authorization intent evidence is invalid")
        return AuthorizationIntent(**dict(value["intent"]))

    def _authorization_expiry(
        self,
        raw: Mapping[str, Any],
        row: Mapping[str, Any],
        *,
        allow_expired_recovery: bool,
    ) -> datetime:
        expires_at = raw.get("expires_at")
        receipt_reviewed_at = raw.get("receipt_reviewed_at")
        if not isinstance(expires_at, str) or not isinstance(
            receipt_reviewed_at, str
        ):
            raise LiveShadowError("authorization expiry/receipt is missing")
        try:
            expiry = datetime.fromisoformat(expires_at)
            reviewed = datetime.fromisoformat(receipt_reviewed_at)
        except ValueError as error:
            raise LiveShadowError(
                "authorization expiry/receipt is invalid"
            ) from error
        if (
            expiry.tzinfo is None
            or reviewed.tzinfo is None
            or not reviewed < expiry
            or (
                not allow_expired_recovery
                and datetime.fromtimestamp(self.clock(), timezone.utc) >= expiry
            )
            or str(row.get("expires_at")) != expires_at
        ):
            raise LiveShadowError("authorization is expired or receipt differs")
        return expiry

    def _validate_authorization_snapshot(
        self,
        request: Mapping[str, Any],
        raw: Mapping[str, Any],
        expected_intent: AuthorizationIntent,
    ) -> None:
        migration = self.migration_proof()
        if migration["plan_sha256"] != raw.get("migration_plan_sha256"):
            raise LiveShadowError("migration proof drifted after review")
        fresh = self.quiescence(request)
        retained_owner = fresh["control_owner_id"]
        recoverable_owner = (
            isinstance(retained_owner, str)
            and retained_owner.startswith(
                f"pc-shadow-auth-{expected_intent.authorization_id[:24]}-"
            )
        )
        if (
            fresh["reflex_active"] is not False
            or fresh["active_writer_ids"]
            or fresh["active_lease_ids"]
            or (retained_owner is not None and not recoverable_owner)
        ):
            raise LiveShadowError("authorization quiescence preflight failed")
        eligible = {
            item["work_id"]: item for item in self.eligible_routes()
        }
        selected = eligible.get(expected_intent.work_id)
        if (
            selected is None
            or selected["identity"] != raw.get("work")
            or selected["source_rows_sha256"]
            != raw.get("legacy_source_rows_sha256")
            or selected["output_sha256"] != raw.get("legacy_output_sha256")
        ):
            raise LiveShadowError("plan-pinned legacy route drifted")

    def _authorization_preflight(
        self,
        request: Mapping[str, Any],
        *,
        allow_expired_recovery: bool = False,
    ) -> dict[str, Any]:
        raw = request.get("authorization")
        if not isinstance(raw, Mapping):
            raise LiveShadowError("signed authorization payload is missing")
        allowlist = raw.get("allowlist")
        audit = raw.get("audit")
        row = raw.get("row")
        if not all(isinstance(item, Mapping) for item in (allowlist, audit, row)):
            raise LiveShadowError("authorization rows are not objects")
        expected_intent = authorization_intent_from_request(
            dict(row), dict(allowlist), dict(audit)
        )
        expiry = self._authorization_expiry(
            raw, row, allow_expired_recovery=allow_expired_recovery
        )
        supplied = raw.get("intent")
        if (
            not isinstance(supplied, Mapping)
            or AuthorizationIntent(**dict(supplied)) != expected_intent
        ):
            raise LiveShadowError("authorization intent differs from signed rows")
        self._validate_authorization_snapshot(request, raw, expected_intent)

        allow_rows = self._key_rows(
            PRODUCTION_ALLOWLIST_TABLE, "work_id", expected_intent.work_id
        )
        audit_rows = self._key_rows(
            PRODUCTION_AUDIT_TABLE,
            "audit_id",
            expected_intent.authorization_id,
        )
        allow_exact, allow_present = _exact_rows(allow_rows, dict(allowlist))
        audit_exact, audit_present = _exact_rows(audit_rows, dict(audit))
        existing_intent = self._read_intent(expected_intent)
        state = classify_authorization_protocol(
            existing_intent,
            expected_intent,
            allowlist_exact=allow_exact,
            allowlist_present=allow_present,
            audit_exact=audit_exact,
            audit_present=audit_present,
        )
        if state is AuthorizationProtocolState.CONFLICT:
            raise LiveShadowError("authorization protocol conflict; no repair allowed")
        return {
            "allowlist": dict(allowlist),
            "audit": dict(audit),
            "intent": expected_intent,
            "expiry": expiry,
            "allow_exact": allow_exact,
            "audit_exact": audit_exact,
            "state": state,
            "allow_expired_recovery": allow_expired_recovery,
        }

    def authorize(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Execute/recover the serialized idempotent two-table protocol."""

        raw = request.get("authorization")
        supplied = raw.get("intent") if isinstance(raw, Mapping) else None
        authorization_id = (
            supplied.get("authorization_id")
            if isinstance(supplied, Mapping)
            else None
        )
        if not isinstance(authorization_id, str):
            raise LiveShadowError("authorization identity is missing")
        prepared_path = intent_path(authorization_id, "00-prepared")
        had_prepared = self.files.exists(prepared_path)
        invocation_id = request.get("invocation_id", request.get("nonce"))
        if not isinstance(invocation_id, str):
            raise LiveShadowError("authorization invocation identity is missing")
        owner = (
            f"pc-shadow-auth-{authorization_id[:24]}-"
            f"{hashlib.sha256(invocation_id.encode()).hexdigest()[:8]}"
        )
        self._acquire_or_resume(owner, authorization_id)
        try:
            prepared = self._authorization_preflight(
                request, allow_expired_recovery=had_prepared
            )
        except BaseException:
            if not had_prepared:
                self._release(owner)
            raise
        expected_intent = prepared["intent"]
        if not isinstance(expected_intent, AuthorizationIntent):
            raise LiveShadowError("internal authorization intent is invalid")
        self._append_authorization(prepared)
        self._release(owner)
        state = prepared["state"]
        if not isinstance(state, AuthorizationProtocolState):
            raise LiveShadowError("internal authorization state is invalid")
        return {
            "authorization_id": expected_intent.authorization_id,
            "protocol_state": "COMMITTED",
            "recovered_from": state.value,
            "lock_retained": False,
        }

    def _append_authorization(self, prepared: Mapping[str, Any]) -> None:
        expected_intent = prepared["intent"]
        expiry = prepared["expiry"]
        allowlist = prepared["allowlist"]
        audit = prepared["audit"]
        if (
            not isinstance(expected_intent, AuthorizationIntent)
            or not isinstance(expiry, datetime)
            or not isinstance(allowlist, Mapping)
            or not isinstance(audit, Mapping)
        ):
            raise LiveShadowError("internal authorization preflight is invalid")
        allow_exact = prepared["allow_exact"] is True
        audit_exact = prepared["audit_exact"] is True
        try:
            if (
                prepared.get("allow_expired_recovery") is not True
                and datetime.fromtimestamp(self.clock(), timezone.utc) >= expiry
            ):
                raise LiveShadowError(
                    "authorization expired before serialized append"
                )
            _create(
                self.files,
                intent_path(expected_intent.authorization_id, "00-prepared"),
                {
                    "schema": "people-counter-shadow-authorization-intent-v1",
                    "state": "PREPARED",
                    "intent": expected_intent.as_dict(),
                },
            )
            if not allow_exact:
                self._append(PRODUCTION_ALLOWLIST_TABLE, dict(allowlist))
                allow_rows = self._key_rows(
                    PRODUCTION_ALLOWLIST_TABLE,
                    "work_id",
                    expected_intent.work_id,
                )
                allow_exact, _ = _exact_rows(allow_rows, dict(allowlist))
                if not allow_exact:
                    raise LiveShadowError("allowlist append readback is ambiguous")
            _create(
                self.files,
                intent_path(expected_intent.authorization_id, "10-allowlist"),
                {
                    "schema": "people-counter-shadow-authorization-intent-v1",
                    "state": "ALLOWLIST_APPENDED",
                    "intent": expected_intent.as_dict(),
                },
            )
            if not audit_exact:
                self._append(PRODUCTION_AUDIT_TABLE, dict(audit))
                audit_rows = self._key_rows(
                    PRODUCTION_AUDIT_TABLE,
                    "audit_id",
                    expected_intent.authorization_id,
                )
                audit_exact, _ = _exact_rows(audit_rows, dict(audit))
                if not audit_exact:
                    raise LiveShadowError("audit append readback is ambiguous")
            _create(
                self.files,
                intent_path(expected_intent.authorization_id, "20-audit"),
                {
                    "schema": "people-counter-shadow-authorization-intent-v1",
                    "state": "AUDIT_APPENDED",
                    "intent": expected_intent.as_dict(),
                },
            )
            if not allow_exact or not audit_exact:
                raise LiveShadowError("authorization exact readback failed")
            _create(
                self.files,
                intent_path(expected_intent.authorization_id, "30-committed"),
                {
                    "schema": "people-counter-shadow-authorization-intent-v1",
                    "state": "COMMITTED",
                    "intent": expected_intent.as_dict(),
                },
            )
        except BaseException:
            # Fail closed: an ambiguous/partial operation intentionally retains
            # its unique owner for a later exact stale replay recovery.
            raise

    def bootstrap(self) -> dict[str, Any]:
        from people_counter.fabric_candidate_a_control import FabricControlStoreImpl

        FabricControlStoreImpl(self.spark, config=self.config, auto_bootstrap=True)
        return {"tables": sorted(self.config.table(name) for name in SHADOW_SCHEMAS)}

    def register(self, request: Mapping[str, Any]) -> dict[str, Any]:
        from people_counter.fabric_candidate_a_control import FabricControlStoreImpl

        value = request.get("registration")
        if not isinstance(value, Mapping):
            raise LiveShadowError("fixed registration payload is missing")
        store = FabricControlStoreImpl(self.spark, config=self.config)
        payload = value.get("payload")
        if not isinstance(payload, Mapping):
            raise LiveShadowError("registration payload is not an object")
        result = store.register(
            str(value["work_id"]),
            dict(payload),
            runtime_key=str(payload.get("runtime_key", "cpu")),
            duration_seconds=float(payload["duration_seconds"]),
            config_sha256=str(value["config_sha256"]),
            release_digest=str(value["source_rows_sha256"]),
            max_attempts=1,
        )
        return _normalize(asdict(result))  # type: ignore[return-value]

    def claim(self, request: Mapping[str, Any]) -> dict[str, Any]:
        from people_counter.fabric_candidate_a_control import FabricControlStoreImpl

        work_id = str(request.get("work_id"))
        store = FabricControlStoreImpl(self.spark, config=self.config)
        claimed = store.claim(
            f"pc-shadow-{work_id}",
            max_items=1,
            minimum_items=1,
            lease_seconds=3600.0,
            minimum_speed_x=0.05,
            safety_factor=1.25,
            margin_seconds=60.0,
            allowed_work_ids=[work_id],
        )
        if claimed is None or len(claimed.items) != 1:
            raise LiveShadowError("exact one-work shadow claim was not produced")
        result = _normalize(asdict(claimed))
        if result["items"][0]["work_id"] != work_id:  # type: ignore[index]
            raise LiveShadowError("claim silently reselected a different work")
        return result  # type: ignore[return-value]

    def recover_exact(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Recover one reviewed expired synthetic claim, or fail closed."""

        from people_counter.fabric_candidate_a_control import FabricControlStoreImpl

        recovery = request.get("recovery")
        if not isinstance(recovery, Mapping):
            raise LiveShadowError("exact recovery payload is missing")
        expected_fields = {
            "attempt_id",
            "batch_id",
            "envelope_sha256",
            "failure_sha256",
            "fence",
            "lease_expires_at",
            "membership_sha256",
            "owner",
            "process_invocation_id",
            "request_sha256",
            "route_binding_sha256",
            "work_id",
        }
        if set(recovery) != expected_fields:
            raise LiveShadowError("exact recovery fields differ")
        rest = request.get("rest_snapshot")
        if not isinstance(rest, Mapping):
            raise LiveShadowError("exact recovery REST snapshot is missing")
        _require_inactive_recovery_snapshot(rest)
        work_id = str(recovery["work_id"])
        route_path = route_binding_path(work_id)
        if (
            not self.files.exists(route_path)
            or hashlib.sha256(self.files.read_bytes(route_path)).hexdigest()
            != recovery["route_binding_sha256"]
        ):
            raise LiveShadowError("exact recovery route binding differs")
        process_invocation = _safe_invocation(
            str(recovery["process_invocation_id"])
        )
        process_request_path = request_path(process_invocation)
        process_failure_path = failure_path(process_invocation)
        if (
            not self.files.exists(process_request_path)
            or not self.files.exists(process_failure_path)
            or self.files.exists(result_path(process_invocation))
            or hashlib.sha256(
                self.files.read_bytes(process_request_path)
            ).hexdigest()
            != recovery["request_sha256"]
            or hashlib.sha256(
                self.files.read_bytes(process_failure_path)
            ).hexdigest()
            != recovery["failure_sha256"]
        ):
            raise LiveShadowError("exact recovery process evidence differs")
        failure = json.loads(self.files.read_bytes(process_failure_path))
        if (
            not isinstance(failure, Mapping)
            or failure.get("command") != "process"
            or failure.get("error_type") != "ProcessValidationError"
        ):
            raise LiveShadowError("exact recovery failure is not the reviewed process")
        store = FabricControlStoreImpl(
            self.spark, config=self.config, auto_bootstrap=False
        )
        return store.recover_exact(
            work_id=work_id,
            batch_id=str(recovery["batch_id"]),
            attempt_id=str(recovery["attempt_id"]),
            owner=str(recovery["owner"]),
            fence=int(recovery["fence"]),
            lease_expires_at=float(recovery["lease_expires_at"]),
            envelope_sha256=str(recovery["envelope_sha256"]),
            membership_sha256=str(recovery["membership_sha256"]),
        )

    def reconcile(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Verify one plan-pinned passing reconciliation; never scan all work."""

        work_id = request.get("selected_work_id")
        value = request.get("reconciliation")
        if (
            not isinstance(work_id, str)
            or not isinstance(value, Mapping)
            or value.get("work_id") != work_id
            or value.get("passed") is not True
            or value.get("finding_ids") != []
            or value.get("finding_types") != []
        ):
            raise LiveShadowError(
                "reconciliation request is not one exact passing plan work"
            )
        return {
            "work_id": work_id,
            "reconciled": True,
            "reconciliation_sha256": sha256_json(dict(value)),
        }


def authorization_intent_from_request(
    row: Mapping[str, Any],
    allowlist: Mapping[str, Any],
    audit: Mapping[str, Any],
) -> AuthorizationIntent:
    """Narrow parser used by Spark without reconstructing controller classes."""

    expected = {
        "authorization_id": str(row.get("authorization_id")),
        "work_id": str(row.get("work_id")),
        "plan_sha256": str(row.get("plan_sha256")),
        "work_identity_sha256": str(row.get("work_identity_sha256")),
        "allowlist_sha256": sha256_json(dict(allowlist)),
        "audit_sha256": sha256_json(dict(audit)),
    }
    return AuthorizationIntent(**expected)


def _load_request(
    files: ShadowEvidenceFiles,
    invocation_id: str,
    hmac_key_hex: str,
) -> dict[str, Any]:
    try:
        key = bytes.fromhex(hmac_key_hex)
    except ValueError as error:
        raise LiveShadowError("request HMAC key is not hexadecimal") from error
    if len(key) != 32:
        raise LiveShadowError("request HMAC key must be exactly 32 bytes")
    envelope = json.loads(files.read_bytes(request_path(invocation_id)))
    if not isinstance(envelope, Mapping):
        raise LiveShadowError("request envelope is not an object")
    payload = envelope.get("payload")
    if not isinstance(payload, Mapping) or payload.get("schema") != LIVE_SCHEMA:
        raise LiveShadowError("request payload schema differs")
    encoded = _canonical(dict(payload))
    digest = hashlib.sha256(encoded).hexdigest()
    signature = hmac.new(key, encoded, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(str(envelope.get("payload_sha256")), digest):
        raise LiveShadowError("request payload hash differs")
    if not hmac.compare_digest(str(envelope.get("signature")), signature):
        raise LiveShadowError("request signature differs")
    if payload.get("invocation_id") != invocation_id:
        raise LiveShadowError("request invocation identity differs")
    created_at = payload.get("created_at")
    try:
        created = datetime.fromisoformat(str(created_at))
    except ValueError as error:
        raise LiveShadowError("request creation timestamp is invalid") from error
    now = datetime.now(timezone.utc)
    if (
        created.tzinfo is None
        or created > now + timedelta(seconds=60)
        or (now - created).total_seconds() > 3600.0
    ):
        raise LiveShadowError("signed request is outside its validity window")
    binding = payload.get("artifact_binding")
    if binding != {
        "workspace_id": WORKSPACE_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "environment_id": ENVIRONMENT_ID,
        "migration_id": MIGRATION_ID,
    }:
        raise LiveShadowError("request artifact binding differs")
    return dict(payload)


def _runtime() -> dict[str, str]:
    spark = _spark()
    try:
        package_version = importlib.metadata.version("people-counter")
    except importlib.metadata.PackageNotFoundError as error:
        raise LiveShadowError("installed people-counter wheel is missing") from error
    java = str(
        spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    )
    observed = {
        "package_version": package_version,
        "python_version": platform.python_version(),
        "spark_version": str(spark.version),
        "java_version": java,
        "fabric_runtime": FABRIC_RUNTIME,
    }
    if (
        package_version != PACKAGE_VERSION
        or not observed["python_version"].startswith("3.13")
        or not observed["spark_version"].startswith("4.1.1")
        or not observed["java_version"].startswith("21")
    ):
        raise LiveShadowError("installed wheel/runtime provenance differs")
    return observed


def _safe_failure_reason(error: BaseException) -> dict[str, str]:
    """Return durable allowlisted diagnostics without persisting raw values."""

    message = str(error)
    rules = (
        ("route-mode fixed Candidate A root", "ATTEMPT_ROOT_MODE_MISMATCH", "attempt_store.root"),
        ("process route mode", "PROCESS_ROUTE_MODE_INVALID", "route_context.route_mode"),
        ("synthetic route", "SYNTHETIC_ROUTE_INVALID", "route_context"),
        ("model identity", "MODEL_IDENTITY_MISMATCH", "route_context.identity.model_sha256"),
        ("video/config identity", "INPUT_IDENTITY_MISMATCH", "claim.identity"),
        ("lease admission rejected", "LEASE_ADMISSION_REJECTED", "claim.lease_expires_at"),
        ("payload SHA-256", "PAYLOAD_IDENTITY_MISMATCH", "claim.payload_sha256"),
        ("staged ", "STAGED_RECORD_INVALID", "attempt.records"),
    )
    for fragment, code, path in rules:
        if fragment in message:
            return {"reason_code": code, "reason_path": path}
    return {
        "reason_code": "UNCLASSIFIED_VALIDATION_FAILURE",
        "reason_path": "process",
    }


def _spark() -> Any:
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _dispatch(
    job: str,
    command: str,
    request: Mapping[str, Any],
    control: SparkShadowControl,
) -> object:
    allowed = {
        "control": CONTROL_COMMANDS,
        "process": PROCESS_COMMANDS,
        "reconcile": RECONCILE_COMMANDS,
    }
    if command not in allowed.get(job, frozenset()):
        raise LiveShadowError("command is not allowed for this fixed SJD")
    if command == "snapshot":
        return control.snapshot(request)
    if command == "status":
        return control.status(request)
    if command == "authorize":
        return control.authorize(request)
    if command == "bootstrap":
        return control.bootstrap()
    if command == "register":
        return control.register(request)
    if command == "claim":
        return control.claim(request)
    if command == "recover-exact":
        return control.recover_exact(request)
    if command == "route":
        return control.route(request)
    if command == "process":
        from people_counter.fabric_candidate_a_jobs import process_main

        batch_id = str(request.get("batch_id"))
        work_id = request.get("selected_work_id")
        context = request.get("route_context")
        if not batch_id or batch_id in {"None", "*", "all"}:
            raise LiveShadowError("fixed process batch identity is missing")
        if not isinstance(work_id, str) or not isinstance(context, Mapping):
            raise LiveShadowError("fixed process route binding is missing")
        route_mode = context.get("route_mode")
        if route_mode == "SHADOW_SYNTHETIC":
            context = validate_synthetic_route_context(context, work_id)
        elif route_mode is not None:
            raise LiveShadowError("unsupported fixed process route mode")
        _create(
            control.files,
            route_binding_path(work_id),
            {
                "schema": "people-counter-shadow-route-binding-v1",
                "work_id": work_id,
                "route_context": dict(context),
            },
        )
        exit_code = process_main(
            ["--batch-id", batch_id, "--mode", "sdk"],
            config=FabricCandidateAConfig.production_shadow(),
            route_mode=(
                "SHADOW_SYNTHETIC"
                if route_mode == "SHADOW_SYNTHETIC"
                else "PRODUCTION_SHADOW"
            ),
            route_identity=(
                dict(context["identity"])
                if route_mode == "SHADOW_SYNTHETIC"
                else None
            ),
        )
        if exit_code != 0:
            raise LiveShadowError(f"shadow SDK process returned {exit_code}")
        return {"batch_id": batch_id, "processed": True}
    if command in {"compare", "reconcile"}:
        return control.reconcile(request)
    raise LiveShadowError("unsupported fixed live command")


def main(
    argv: Sequence[str] | None = None,
    *,
    job: str = "control",
    files: ShadowEvidenceFiles | None = None,
    spark: Any | None = None,
    output: TextIO = sys.stdout,
    errors: TextIO = sys.stderr,
) -> int:
    """Run one signed fixed operation and create exact result/failure evidence."""

    import argparse

    parser = argparse.ArgumentParser(prog=f"pc-production-shadow-{job}-sjd")
    parser.add_argument("command", choices=tuple(sorted(
        CONTROL_COMMANDS | PROCESS_COMMANDS | RECONCILE_COMMANDS
    )))
    parser.add_argument("--invocation-id", required=True)
    parser.add_argument("--request-hmac-key", required=True)
    arguments = parser.parse_args(argv)
    evidence = files or NotebookShadowEvidenceFiles()
    invocation = _safe_invocation(arguments.invocation_id)
    try:
        request = _load_request(
            evidence, invocation, arguments.request_hmac_key
        )
        if request.get("command") != arguments.command:
            raise LiveShadowError("signed request command differs")
        runtime = _runtime()
        selected_spark = spark or _spark()
        _start_once(evidence, invocation, arguments.command, request)
        result = _dispatch(
            job,
            arguments.command,
            request,
            SparkShadowControl(selected_spark, evidence),
        )
        value = {
            "schema": RESULT_SCHEMA,
            "command": arguments.command,
            "invocation_id": invocation,
            "exit_code": 0,
            "runtime": runtime,
            "result": _normalize(result),
        }
        _create(evidence, result_path(invocation), value)
        print(_canonical(value).decode(), file=output)
        return 0
    except BaseException as error:
        failure = {
            "schema": FAILURE_SCHEMA,
            "command": arguments.command,
            "invocation_id": invocation,
            "exit_code": 1,
            "error_type": type(error).__name__,
            "error_sha256": hashlib.sha256(
                str(error).encode("utf-8", errors="replace")
            ).hexdigest(),
            "traceback_sha256": hashlib.sha256(
                "".join(traceback.format_exception(error)).encode(
                    "utf-8", errors="replace"
                )
            ).hexdigest(),
        }
        if isinstance(error, SparkCanonicalizationError):
            failure["canonicalization"] = error.safe_metadata()
        if type(error).__name__.endswith("ValidationError"):
            failure["reason"] = _safe_failure_reason(error)
        try:
            _create(evidence, failure_path(invocation), failure)
        except BaseException:
            pass
        print(f"production-shadow {job} failed: {type(error).__name__}", file=errors)
        return 2

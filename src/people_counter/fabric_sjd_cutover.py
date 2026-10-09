"""Fail-closed additive cutover into the stable Fabric SJD namespace."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any

from people_counter.fabric_sjd import FabricSjdConfig, TABLE_SUFFIXES


CUTOVER_ID = "people_counter_sjd_0001"
CA_PREFIX = "people_counter_ca_"
RETIREMENT_ROOT = (
    "Files/people-counter/sjd/v1/retirements/candidate-a"
)
WRITER_ITEM_IDS = frozenset(
    {
        "5548a877-38e0-4933-bf2d-0285250637d2",
        "12e33015-5c82-4ea0-a54e-fe145694ad02",
        "2e415a3e-336a-42bf-9e3e-ee7c10c1722f",
        "61d98ddf-0a4d-45b7-a21b-fa577f3f909e",
        "dd4fc19b-7276-4402-9d30-76676f36555b",
        "a26b27a5-99db-4c95-9b2f-c048949a8246",
        "f6153ed1-ef84-4270-98ae-c17398dadb61",
        "09d2e5d0-c90a-4a22-885b-b29688983d70",
        "bc5209e3-177c-4729-b647-eb48fd33ec95",
    }
)
STOPPED_STATES = frozenset({"Completed", "Cancelled", "Canceled", "Failed"})
CA_SHADOW_ONLY_TABLES = frozenset(
    {
        "people_counter_ca_routing_allowlist",
        "people_counter_ca_shadow_audit",
    }
)
CA_SYNTHETIC_MARKERS = (
    "/_benchmark/",
    "/_canary/",
    "/_shadow/",
    "candidate-a",
    "synthetic",
    "benchmark",
    "canary",
)
REVIEWED_SYNTHETIC_WORK_EVIDENCE = frozenset(
    {
        (
            "sjd-live-0944-pytorch-001",
            "e51851892b94d9b3f984c95d5bfd4cd580c64e13f7e5ec0d2bb924a05718e1d4",
        )
    }
)
TRANSFERABLE_SUFFIXES = frozenset(
    {
        "work",
        "batches",
        "batch_members",
        "attempts",
        "publications",
        "replay_requests",
        "reconciliation_findings",
        "gold_checkpoints",
        "semantic_refresh_outbox",
    }
)
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")

_CONTROL_SCHEMAS = {
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
    "gold_checkpoints": (
        "stage string, source_key string, publication_sequence long, "
        "source_versions_json string, target_versions_json string, "
        "completed_at timestamp"
    ),
    "semantic_refresh_outbox": (
        "outbox_id long, dedupe_key string, reason string, payload_json string, "
        "created_at timestamp, acked_at timestamp, acked_by string"
    ),
    "migration_journal": (
        "journal_id string, cutover_id string, phase string, status string, "
        "source_snapshot_sha256 string, target_snapshot_sha256 string, "
        "evidence_sha256 string, previous_evidence_sha256 string, "
        "created_at timestamp"
    ),
    "retirement_journal": (
        "journal_id string, bundle_id string, status string, "
        "manifest_path string, manifest_sha256 string, "
        "zero_writer_proof_sha256 string, zero_routing_proof_sha256 string, "
        "rollback_proof_sha256 string, created_at timestamp"
    ),
}


class CutoverError(RuntimeError):
    """A stable-schema cutover precondition or readback failed."""


@dataclass(frozen=True)
class WriterState:
    item_id: str
    state: str


@dataclass(frozen=True)
class SourceTableState:
    name: str
    exists: bool
    version: int | None
    row_count: int
    schema_sha256: str | None


@dataclass(frozen=True)
class StoppedWriterGate:
    captured_at: float
    writer_states: tuple[WriterState, ...]
    source_tables: tuple[SourceTableState, ...]
    routing_to_ca: int
    evidence_sha256: str


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def frame_content_sha256(frame: Any) -> str:
    """Hash every row in deterministic order without collecting the table."""
    from pyspark.sql import functions as spark_functions

    columns = [
        spark_functions.col(f"`{name.replace('`', '``')}`")
        for name in frame.columns
    ]
    rows = (
        frame.select(
            spark_functions.to_json(
                spark_functions.struct(*columns),
                options={"ignoreNullFields": "false"},
            ).alias("canonical_row")
        )
        .orderBy("canonical_row")
        .toLocalIterator()
    )
    digest = hashlib.sha256()
    for row in rows:
        value = row["canonical_row"]
        if not isinstance(value, str):
            raise CutoverError("canonical row serialization is not a string")
        digest.update(value.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def stable_table_schemas() -> dict[str, str]:
    """Return the exact additive stable schema allowlist."""
    from people_counter.sjd_gold import _SPARK_FIELDS

    schemas = dict(_CONTROL_SCHEMAS)
    for suffix, fields in _SPARK_FIELDS.items():
        schemas[suffix] = ", ".join(
            f"{name} {data_type}" for name, data_type, _nullable in fields
        )
    if set(schemas) != set(TABLE_SUFFIXES):
        raise CutoverError("stable schema definitions differ from fixed allowlist")
    return schemas


def classify_ca_table(name: str) -> str:
    if name in CA_SHADOW_ONLY_TABLES:
        return "SHADOW_ONLY"
    if not name.startswith(CA_PREFIX):
        raise CutoverError(f"table is outside Candidate A namespace: {name!r}")
    suffix = name[len(CA_PREFIX) :]
    if suffix in TRANSFERABLE_SUFFIXES:
        return "TRANSFERABLE_DURABLE_SCHEMA"
    return "EVIDENCE_ONLY"


def classify_work_payload(payload_json: str) -> str:
    try:
        value = json.loads(payload_json)
    except json.JSONDecodeError as error:
        raise CutoverError("work payload is invalid JSON") from error
    if not isinstance(value, dict):
        raise CutoverError("work payload must be an object")
    normalized = canonical_bytes(value).decode("ascii").lower()
    if any(marker in normalized for marker in CA_SYNTHETIC_MARKERS):
        return "SYNTHETIC_OR_SHADOW"
    source = value.get("source_video")
    if not isinstance(source, str) or not (
        source.startswith("/lakehouse/default/Files/incoming/")
        or source.startswith("Files/incoming/")
    ):
        return "UNCLASSIFIED_REFUSE"
    return "DURABLE_PRODUCTION"


def validate_stopped_writer_gate(
    gate: StoppedWriterGate,
    observed_sources: Sequence[SourceTableState],
    *,
    now: float | None = None,
    maximum_age_seconds: float = 900.0,
) -> None:
    current = time.time() if now is None else now
    if (
        not math.isfinite(gate.captured_at)
        or gate.captured_at > current
        or current - gate.captured_at > maximum_age_seconds
    ):
        raise CutoverError("stopped-writer gate is future-dated or stale")
    if (
        len(gate.writer_states) != len(WRITER_ITEM_IDS)
        or len({item.item_id for item in gate.writer_states})
        != len(gate.writer_states)
    ):
        raise CutoverError("stopped-writer gate writer evidence is not unique")
    states = {item.item_id: item.state for item in gate.writer_states}
    if set(states) != set(WRITER_ITEM_IDS):
        raise CutoverError("stopped-writer gate has an incomplete writer allowlist")
    active = sorted(
        item_id for item_id, state in states.items() if state not in STOPPED_STATES
    )
    if active:
        raise CutoverError(f"production writers are not stopped: {active!r}")
    if gate.routing_to_ca != 0:
        raise CutoverError("production routing to Candidate A is not zero")
    if tuple(observed_sources) != gate.source_tables:
        raise CutoverError("Candidate A source Delta versions/counts changed after gate")
    unsigned = {
        "captured_at": gate.captured_at,
        "routing_to_ca": gate.routing_to_ca,
        "source_tables": [asdict(item) for item in gate.source_tables],
        "writer_states": [asdict(item) for item in gate.writer_states],
    }
    if gate.evidence_sha256 != sha256(unsigned):
        raise CutoverError("stopped-writer gate evidence hash differs")


def create_stable_tables(spark: Any) -> dict[str, str]:
    """Add every allowlisted table and verify its exact field readback."""
    config = FabricSjdConfig()
    schemas = stable_table_schemas()
    observed: dict[str, str] = {}
    for suffix in sorted(schemas):
        name = config.table(suffix)
        expected_schema = spark.createDataFrame([], schema=schemas[suffix]).schema
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS `{name}` ({schemas[suffix]}) "
            "USING DELTA "
            f"TBLPROPERTIES ('people_counter.cutover_id'='{CUTOVER_ID}',"
            "'people_counter.namespace'='stable-sjd')"
        )
        fields = spark.table(name).schema
        if fields.json() != expected_schema.json():
            raise CutoverError(f"stable table schema readback differs for {name}")
        details = spark.sql(f"DESCRIBE DETAIL `{name}`").limit(2).collect()
        if len(details) != 1:
            raise CutoverError(f"stable table detail readback differs for {name}")
        detail = details[0].asDict(recursive=True)
        properties = dict(detail.get("properties") or {})
        expected_properties = {
            "people_counter.cutover_id": CUTOVER_ID,
            "people_counter.namespace": "stable-sjd",
        }
        if (
            str(detail.get("format", "")).lower() != "delta"
            or any(
                properties.get(key) != value
                for key, value in expected_properties.items()
            )
        ):
            raise CutoverError(f"stable table provider/properties differ for {name}")
        if suffix == "locks":
            lock_rows = spark.table(name).limit(2).collect()
            if not lock_rows:
                (
                    spark.createDataFrame(
                        [("global", None, None)],
                        schema=schemas[suffix],
                    )
                    .write.format("delta")
                    .mode("append")
                    .option("txnAppId", f"{CUTOVER_ID}:bootstrap-lock")
                    .option("txnVersion", "0")
                    .saveAsTable(name)
                )
                spark.catalog.refreshTable(name)
                lock_rows = spark.table(name).limit(2).collect()
            if (
                len(lock_rows) != 1
                or lock_rows[0]["lock_name"] != "global"
                or lock_rows[0]["owner_id"] is not None
                or lock_rows[0]["acquired_at"] is not None
            ):
                raise CutoverError("stable global lock row readback differs")
        observed[name] = sha256(
            {
                "properties": expected_properties,
                "provider": "delta",
                "schema": json.loads(fields.json()),
            }
        )
    return observed


def source_table_state(spark: Any, name: str) -> SourceTableState:
    if not spark.catalog.tableExists(name):
        return SourceTableState(name, False, None, 0, None)
    schema_json = spark.table(name).schema.json()
    count = int(spark.table(name).count())
    history = spark.sql(f"DESCRIBE HISTORY `{name}` LIMIT 1").collect()
    if len(history) != 1:
        raise CutoverError(f"Delta history is unavailable for {name}")
    version = int(history[0]["version"])
    return SourceTableState(
        name,
        True,
        version,
        count,
        hashlib.sha256(schema_json.encode("utf-8")).hexdigest(),
    )


def snapshot_ca_tables(spark: Any, names: Sequence[str]) -> tuple[SourceTableState, ...]:
    if len(set(names)) != len(names):
        raise CutoverError("Candidate A snapshot table allowlist contains duplicates")
    for name in names:
        classify_ca_table(name)
    return tuple(source_table_state(spark, name) for name in sorted(names))


def append_journal_once(
    spark: Any,
    *,
    suffix: str,
    journal_id: str,
    row: Mapping[str, Any],
) -> None:
    config = FabricSjdConfig()
    table = config.table(suffix)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", journal_id) is None:
        raise CutoverError("journal ID is not canonical")
    existing = spark.table(table).where(f"journal_id = '{journal_id}'").limit(2).collect()
    expected = {"journal_id": journal_id, **dict(row)}
    for key, value in expected.items():
        if not isinstance(value, datetime):
            continue
        if value.tzinfo is None or value.utcoffset() is None:
            raise CutoverError(f"journal timestamp {key} must be timezone-aware")
        expected[key] = value.astimezone(timezone.utc).replace(tzinfo=None)
    if existing:
        if len(existing) != 1 or existing[0].asDict(recursive=True) != expected:
            raise CutoverError(f"journal conflict for {journal_id}")
        return
    (
        spark.createDataFrame([expected], schema=stable_table_schemas()[suffix])
        .write.format("delta")
        .mode("append")
        .option("txnAppId", f"{CUTOVER_ID}:{suffix}:{journal_id}")
        .option("txnVersion", "0")
        .saveAsTable(table)
    )
    readback = spark.table(table).where(f"journal_id = '{journal_id}'").limit(2).collect()
    if len(readback) != 1 or readback[0].asDict(recursive=True) != expected:
        raise CutoverError(f"journal exact readback failed for {journal_id}")


def migrate_durable_rows(
    spark: Any,
    *,
    durable_work_ids: Sequence[str],
    gate: StoppedWriterGate,
    source_names: Sequence[str],
) -> dict[str, int]:
    """Copy only reviewed production-rooted rows under a stopped-writer CAS."""
    observed = snapshot_ca_tables(spark, source_names)
    validate_stopped_writer_gate(gate, observed)
    work_ids = tuple(sorted(set(durable_work_ids)))
    if any(not value or "'" in value for value in work_ids):
        raise CutoverError("durable work allowlist contains an unsafe identifier")
    if "people_counter_ca_work" not in source_names:
        if work_ids:
            raise CutoverError("durable work cannot be selected without source work")
        return {}
    source_work = spark.table("people_counter_ca_work")
    selected = source_work.where(
        "false" if not work_ids else "work_id IN (" + ",".join(
            f"'{value}'" for value in work_ids
        ) + ")"
    ).collect()
    if {str(row["work_id"]) for row in selected} != set(work_ids):
        raise CutoverError("durable work allowlist does not exactly match source rows")
    invalid = [
        str(row["work_id"])
        for row in selected
        if classify_work_payload(str(row["payload_json"])) != "DURABLE_PRODUCTION"
    ]
    if invalid:
        raise CutoverError(f"non-production work was selected for migration: {invalid!r}")
    if not work_ids:
        return {suffix: 0 for suffix in sorted(TRANSFERABLE_SUFFIXES)}
    raise CutoverError(
        "non-empty durable migration requires a reviewed table-specific graph plan"
    )


def retirement_bundle_path(bundle_id: str) -> str:
    if re.fullmatch(r"v[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}", bundle_id) is None:
        raise CutoverError("retirement bundle ID is not canonical")
    return f"{RETIREMENT_ROOT}/{bundle_id}"


def _history_json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported Delta history value: {type(value).__name__}")


def _history_json(row: Any) -> dict[str, Any]:
    return json.loads(
        json.dumps(
            row.asDict(recursive=True),
            allow_nan=False,
            default=_history_json_default,
        )
    )


def archive_ca_tables(
    spark: Any,
    *,
    bundle_id: str,
    table_names: Sequence[str],
    path_exists: Any | None = None,
) -> dict[str, Any]:
    """Create immutable Delta copies and return a hashable retirement manifest."""
    root = retirement_bundle_path(bundle_id)
    states = snapshot_ca_tables(spark, table_names)
    entries: list[dict[str, Any]] = []
    for state in states:
        classification = classify_ca_table(state.name)
        path = f"{root}/tables/{state.name}"
        if state.exists:
            source = spark.table(state.name)
            source_content_sha256 = frame_content_sha256(source)
            if path_exists is None or not path_exists(path):
                (
                    source
                    .write.format("delta")
                    .mode("errorifexists")
                    .save(path)
                )
            archived = spark.read.format("delta").load(path)
            if archived.count() != state.row_count:
                raise CutoverError(f"retirement archive count differs for {state.name}")
            archived_schema_sha256 = hashlib.sha256(
                archived.schema.json().encode("utf-8")
            ).hexdigest()
            if archived_schema_sha256 != state.schema_sha256:
                raise CutoverError(
                    f"retirement archive schema differs for {state.name}"
                )
            archive_content_sha256 = frame_content_sha256(archived)
            if archive_content_sha256 != source_content_sha256:
                raise CutoverError(
                    f"retirement archive content differs for {state.name}"
                )
            history = [
                _history_json(row)
                for row in spark.sql(
                    f"DESCRIBE HISTORY `{state.name}` LIMIT 1"
                ).collect()
            ]
        else:
            archive_content_sha256 = None
            history = []
        entries.append(
            {
                **asdict(state),
                "archive_path": path if state.exists else None,
                "content_sha256": archive_content_sha256,
                "classification": classification,
                "history": history,
                "history_sha256": sha256(history),
            }
        )
    manifest = {
        "bundle_id": bundle_id,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "cutover_id": CUTOVER_ID,
        "schema": "people-counter-ca-retirement-v1",
        "tables": entries,
    }
    return {**manifest, "manifest_sha256": sha256(manifest)}


def validate_retirement_archives(
    spark: Any,
    manifest: Mapping[str, Any],
    expected_sources: Sequence[SourceTableState],
) -> dict[str, Any]:
    """Prove every existing source has an exact readable Delta rollback copy."""
    unsigned = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    if manifest.get("manifest_sha256") != sha256(unsigned):
        raise CutoverError("retirement manifest digest differs")
    bundle_id = str(manifest.get("bundle_id"))
    root = retirement_bundle_path(bundle_id)
    entries = manifest.get("tables")
    if not isinstance(entries, list):
        raise CutoverError("retirement manifest tables are invalid")
    by_name = {
        str(entry.get("name")): entry
        for entry in entries
        if isinstance(entry, Mapping)
    }
    expected = {state.name: state for state in expected_sources}
    if set(by_name) != set(expected):
        raise CutoverError("retirement manifest table allowlist differs")
    rollback: list[dict[str, Any]] = []
    for name in sorted(expected):
        state = expected[name]
        entry = by_name[name]
        observed_state = SourceTableState(
            name=str(entry.get("name")),
            exists=bool(entry.get("exists")),
            version=(
                None if entry.get("version") is None else int(entry["version"])
            ),
            row_count=int(entry.get("row_count", -1)),
            schema_sha256=(
                None
                if entry.get("schema_sha256") is None
                else str(entry["schema_sha256"])
            ),
        )
        if observed_state != state:
            raise CutoverError(f"retirement manifest source differs for {name}")
        archive_path = entry.get("archive_path")
        if not state.exists:
            if archive_path is not None or entry.get("content_sha256") is not None:
                raise CutoverError(f"missing source has archive evidence for {name}")
            continue
        expected_archive_path = f"{root}/tables/{name}"
        if archive_path != expected_archive_path:
            raise CutoverError(f"retirement archive path is invalid for {name}")
        archived = spark.read.format("delta").load(archive_path)
        if int(archived.count()) != state.row_count:
            raise CutoverError(f"retirement archive count differs for {name}")
        schema_digest = hashlib.sha256(
            archived.schema.json().encode("utf-8")
        ).hexdigest()
        if schema_digest != state.schema_sha256:
            raise CutoverError(f"retirement archive schema differs for {name}")
        content_digest = frame_content_sha256(archived)
        if (
            re.fullmatch(r"[0-9a-f]{64}", str(entry.get("content_sha256")))
            is None
            or content_digest != entry.get("content_sha256")
        ):
            raise CutoverError(f"retirement archive content differs for {name}")
        rollback.append(
            {
                "archive_path": archive_path,
                "name": name,
                "restore_sql": (
                    f"CREATE TABLE `{name}` USING DELTA "
                    f"LOCATION '{archive_path}'"
                ),
                "row_count": state.row_count,
                "schema_sha256": state.schema_sha256,
                "content_sha256": content_digest,
            }
        )
    proof = {
        "schema": "people-counter-ca-rollback-proof-v1",
        "tables": rollback,
    }
    return {**proof, "rollback_proof_sha256": sha256(proof)}


def retirement_manifest_sources(
    manifest: Mapping[str, Any],
) -> tuple[SourceTableState, ...]:
    entries = manifest.get("tables")
    if not isinstance(entries, list):
        raise CutoverError("retirement manifest tables are invalid")
    sources = tuple(
        SourceTableState(
            name=str(entry.get("name")),
            exists=bool(entry.get("exists")),
            version=None if entry.get("version") is None else int(entry["version"]),
            row_count=int(entry.get("row_count", -1)),
            schema_sha256=(
                None
                if entry.get("schema_sha256") is None
                else str(entry["schema_sha256"])
            ),
        )
        for entry in entries
        if isinstance(entry, Mapping)
    )
    if len(sources) != len(entries):
        raise CutoverError("retirement manifest table entry is invalid")
    return sources


def retire_archived_ca_tables(
    spark: Any,
    *,
    gate: StoppedWriterGate,
    manifest: Mapping[str, Any],
    table_names: Sequence[str],
    path_exists: Any,
    absence_timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    """Drop only freshly gated, exactly archived Candidate A tables."""
    names = tuple(sorted(table_names))
    if len(names) != len(set(names)):
        raise CutoverError("retirement table allowlist contains duplicates")
    current = snapshot_ca_tables(spark, names)
    validate_stopped_writer_gate(gate, gate.source_tables)
    gated_by_name = {state.name: state for state in gate.source_tables}
    if set(gated_by_name) != set(names):
        raise CutoverError("retirement gate table allowlist differs")
    for observed in current:
        gated = gated_by_name[observed.name]
        if observed.exists and observed != gated:
            raise CutoverError(
                f"Candidate A source changed after gate: {observed.name}"
            )
    expected = retirement_manifest_sources(manifest)
    rollback = validate_retirement_archives(spark, manifest, expected)
    expected_by_name = {state.name: state for state in expected}
    if set(expected_by_name) != set(names):
        raise CutoverError("retirement manifest table allowlist differs")
    for observed in current:
        source = expected_by_name[observed.name]
        if observed.exists and observed != source:
            raise CutoverError(
                f"Candidate A source changed during retirement: {observed.name}"
            )
        if observed.exists and not source.exists:
            raise CutoverError(
                f"Candidate A source appeared during retirement: {observed.name}"
            )
    for source in expected:
        observed_by_name = {
            item.name: item for item in snapshot_ca_tables(spark, names)
        }
        observed = observed_by_name[source.name]
        if observed.exists:
            if observed != source:
                raise CutoverError(
                    f"Candidate A source changed during retirement: {source.name}"
                )
            spark.sql(f"DROP TABLE `{source.name}`")
        table_path = f"Tables/{source.name}"
        deadline = time.monotonic() + absence_timeout_seconds
        while spark.catalog.tableExists(source.name) or path_exists(table_path):
            if time.monotonic() >= deadline:
                raise CutoverError(
                    f"retired table or managed path remains: {source.name}"
                )
            time.sleep(0.25)
    after = snapshot_ca_tables(spark, names)
    unexpected = [state.name for state in after if state.exists]
    if unexpected:
        raise CutoverError(f"retired tables remain: {unexpected!r}")
    zero_routing = {
        "routing_to_ca": gate.routing_to_ca,
        "schema": "people-counter-ca-zero-routing-proof-v1",
    }
    result = {
        "bundle_id": str(manifest.get("bundle_id")),
        "dropped_tables": [state.name for state in expected if state.exists],
        "post_retirement": [asdict(state) for state in after],
        "retired_at": datetime.now(timezone.utc).isoformat(),
        "rollback": rollback,
        "schema": "people-counter-ca-retirement-result-v1",
        "zero_routing_proof_sha256": sha256(zero_routing),
        "zero_writer_proof_sha256": gate.evidence_sha256,
    }
    return {**result, "retirement_result_sha256": sha256(result)}


def synthetic_work_inventory(spark: Any) -> dict[str, Any]:
    """Inventory explicitly named stable validation work without mutating it."""
    config = FabricSjdConfig()
    rows = (
        spark.table(config.table("work"))
        .where("work_id LIKE 'sjd-live-%'")
        .orderBy("work_id")
        .collect()
    )
    work = [_history_json(row) for row in rows]
    result = {
        "schema": "people-counter-sjd-synthetic-work-inventory-v1",
        "work": work,
    }
    return {**result, "inventory_sha256": sha256(result)}


def complete_synthetic_cleanup(
    spark: Any,
    archive_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify an immutable cleanup archive and idempotently finish deletion."""
    cleanup_id = str(archive_manifest.get("cleanup_id"))
    work_evidence = archive_manifest.get("work_evidence")
    batch_ids = archive_manifest.get("batch_ids")
    archived_tables = archive_manifest.get("archived_tables")
    unsigned = {
        key: value
        for key, value in archive_manifest.items()
        if key != "archive_manifest_sha256"
    }
    if (
        archive_manifest.get("schema")
        != "people-counter-sjd-synthetic-cleanup-archive-v1"
        or re.fullmatch(
            r"v[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}",
            cleanup_id,
        )
        is None
        or not isinstance(work_evidence, Mapping)
        or not work_evidence
        or any(
            not isinstance(item, str)
            or re.fullmatch(
                r"sjd-live-[0-9]{4}-[A-Za-z0-9._-]+",
                item,
            )
            is None
            or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
            for item, digest in work_evidence.items()
        )
        or not isinstance(batch_ids, list)
        or any(
            not isinstance(item, str)
            or re.fullmatch(r"[A-Za-z0-9._-]+", item) is None
            for item in batch_ids
        )
        or not isinstance(archived_tables, Mapping)
        or archive_manifest.get("archive_manifest_sha256") != sha256(unsigned)
    ):
        raise CutoverError("synthetic cleanup archive manifest is invalid")
    work_ids = sorted(work_evidence)
    config = FabricSjdConfig()
    quoted_ids = ", ".join(f"'{item}'" for item in work_ids)
    quoted_batches = ", ".join(f"'{item}'" for item in batch_ids)
    filters = {
        "attempts": f"work_id IN ({quoted_ids})",
        "batch_members": f"work_id IN ({quoted_ids})",
        "publications": f"work_id IN ({quoted_ids})",
        "replay_requests": f"work_id IN ({quoted_ids})",
        "work": f"work_id IN ({quoted_ids})",
    }
    if batch_ids:
        filters["batches"] = f"batch_id IN ({quoted_batches})"
    expected_tables = {config.table(suffix) for suffix in filters}
    if set(archived_tables) != expected_tables:
        raise CutoverError("synthetic cleanup archive table allowlist differs")
    root = f"Files/people-counter/sjd/v1/validation-cleanups/{cleanup_id}"
    for table in sorted(expected_tables):
        entry = archived_tables[table]
        if not isinstance(entry, Mapping):
            raise CutoverError(f"synthetic cleanup archive is invalid for {table}")
        count = int(entry.get("row_count", -1))
        schema_digest = str(entry.get("schema_sha256"))
        content_digest = entry.get("content_sha256")
        path = entry.get("archive_path")
        if count < 0 or re.fullmatch(r"[0-9a-f]{64}", schema_digest) is None:
            raise CutoverError(f"synthetic cleanup archive is invalid for {table}")
        if count == 0:
            if path is not None or content_digest is not None:
                raise CutoverError(
                    f"empty synthetic cleanup archive has evidence for {table}"
                )
            continue
        expected_path = f"{root}/tables/{table}"
        if path != expected_path:
            raise CutoverError(f"synthetic cleanup archive path differs for {table}")
        readback = spark.read.format("delta").load(expected_path)
        if int(readback.count()) != count:
            raise CutoverError(f"synthetic archive count differs for {table}")
        readback_schema_digest = hashlib.sha256(
            readback.schema.json().encode("utf-8")
        ).hexdigest()
        if readback_schema_digest != schema_digest:
            raise CutoverError(f"synthetic archive schema differs for {table}")
        if (
            re.fullmatch(r"[0-9a-f]{64}", str(content_digest)) is None
            or frame_content_sha256(readback) != content_digest
        ):
            raise CutoverError(f"synthetic archive content differs for {table}")
    for suffix, predicate in filters.items():
        spark.sql(f"DELETE FROM `{config.table(suffix)}` WHERE {predicate}")
    remaining = {
        config.table(suffix): int(
            spark.table(config.table(suffix)).where(predicate).count()
        )
        for suffix, predicate in filters.items()
    }
    if any(remaining.values()):
        raise CutoverError(f"synthetic rows remain after cleanup: {remaining!r}")
    result = {
        "archive_manifest": dict(archive_manifest),
        "schema": "people-counter-sjd-synthetic-cleanup-v1",
    }
    return {**result, "cleanup_sha256": sha256(result)}


def archive_and_delete_synthetic_work(
    spark: Any,
    *,
    cleanup_id: str,
    work_evidence: Mapping[str, str],
    write_archive_manifest: Any | None = None,
    archive_exists: Any | None = None,
) -> dict[str, Any]:
    """Archive and remove only explicitly allowlisted stable validation rows."""
    if re.fullmatch(r"v[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}", cleanup_id) is None:
        raise CutoverError("synthetic cleanup ID is not canonical")
    ids = tuple(sorted(work_evidence))
    if not ids:
        raise CutoverError("synthetic cleanup work evidence is empty")
    if any(re.fullmatch(r"sjd-live-[0-9]{4}-[A-Za-z0-9._-]+", item) is None for item in ids):
        raise CutoverError("synthetic cleanup work ID is outside the fixed prefix")
    if any(
        re.fullmatch(r"[0-9a-f]{64}", str(work_evidence[item])) is None
        for item in ids
    ):
        raise CutoverError("synthetic cleanup payload digest is invalid")
    config = FabricSjdConfig()
    quoted_ids = ", ".join(f"'{item}'" for item in ids)
    selected_work = (
        spark.table(config.table("work"))
        .where(f"work_id IN ({quoted_ids})")
        .collect()
    )
    if {str(row["work_id"]) for row in selected_work} != set(ids):
        raise CutoverError("synthetic cleanup allowlist does not exactly match work")
    unsafe_work = [
        str(row["work_id"])
        for row in selected_work
        if str(row["payload_sha256"])
        != work_evidence[str(row["work_id"])]
        or (
            classify_work_payload(str(row["payload_json"]))
            != "SYNTHETIC_OR_SHADOW"
            and (
                str(row["work_id"]),
                str(row["payload_sha256"]),
            )
            not in REVIEWED_SYNTHETIC_WORK_EVIDENCE
        )
    ]
    if unsafe_work:
        raise CutoverError(
            f"synthetic cleanup payload evidence differs: {unsafe_work!r}"
        )
    nonterminal_work = [
        str(row["work_id"])
        for row in selected_work
        if str(row["status"]) != "SUCCEEDED"
    ]
    if nonterminal_work:
        raise CutoverError(
            f"synthetic cleanup work is not terminal: {nonterminal_work!r}"
        )
    members = (
        spark.table(config.table("batch_members"))
        .where(f"work_id IN ({quoted_ids})")
        .collect()
    )
    batch_ids = tuple(sorted({str(row["batch_id"]) for row in members}))
    quoted_batches = ", ".join(f"'{item}'" for item in batch_ids)
    if batch_ids:
        selected_batches = (
            spark.table(config.table("batches"))
            .where(f"batch_id IN ({quoted_batches})")
            .collect()
        )
        if (
            {str(row["batch_id"]) for row in selected_batches} != set(batch_ids)
            or any(str(row["status"]) != "COMMITTED" for row in selected_batches)
        ):
            raise CutoverError("synthetic cleanup batches are not committed")
        all_members = (
            spark.table(config.table("batch_members"))
            .where(f"batch_id IN ({quoted_batches})")
            .collect()
        )
        batch_work_ids = {str(row["work_id"]) for row in all_members}
        if not batch_work_ids.issubset(ids):
            raise CutoverError("synthetic cleanup would orphan shared batch members")
    filters = {
        "attempts": f"work_id IN ({quoted_ids})",
        "batch_members": f"work_id IN ({quoted_ids})",
        "publications": f"work_id IN ({quoted_ids})",
        "replay_requests": f"work_id IN ({quoted_ids})",
        "work": f"work_id IN ({quoted_ids})",
    }
    if batch_ids:
        filters["batches"] = f"batch_id IN ({quoted_batches})"
    root = (
        "Files/people-counter/sjd/v1/validation-cleanups/"
        f"{cleanup_id}"
    )
    archived: dict[str, dict[str, Any]] = {}
    for suffix, predicate in filters.items():
        table = config.table(suffix)
        frame = spark.table(table).where(predicate)
        count = int(frame.count())
        path = f"{root}/tables/{table}"
        schema_digest = hashlib.sha256(
            frame.schema.json().encode("utf-8")
        ).hexdigest()
        if count:
            if archive_exists is None or not archive_exists(path):
                frame.write.format("delta").mode("errorifexists").save(path)
            readback = spark.read.format("delta").load(path)
            if int(readback.count()) != count:
                raise CutoverError(f"synthetic archive count differs for {table}")
            readback_schema_digest = hashlib.sha256(
                readback.schema.json().encode("utf-8")
            ).hexdigest()
            if readback_schema_digest != schema_digest:
                raise CutoverError(f"synthetic archive schema differs for {table}")
            source_content_sha256 = frame_content_sha256(frame)
            archive_content_sha256 = frame_content_sha256(readback)
            if archive_content_sha256 != source_content_sha256:
                raise CutoverError(f"synthetic archive content differs for {table}")
        else:
            archive_content_sha256 = None
        archived[table] = {
            "archive_path": path if count else None,
            "content_sha256": archive_content_sha256,
            "row_count": count,
            "schema_sha256": schema_digest,
        }
    archive_manifest = {
        "archived_tables": archived,
        "batch_ids": list(batch_ids),
        "cleanup_id": cleanup_id,
        "schema": "people-counter-sjd-synthetic-cleanup-archive-v1",
        "work_evidence": {item: work_evidence[item] for item in ids},
    }
    archive_manifest = {
        **archive_manifest,
        "archive_manifest_sha256": sha256(archive_manifest),
    }
    if write_archive_manifest is not None:
        write_archive_manifest(archive_manifest)
    return complete_synthetic_cleanup(spark, archive_manifest)

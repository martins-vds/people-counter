"""Bounded Fabric job commands for the stable-schema cutover."""

from __future__ import annotations

import argparse
import base64
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any

from people_counter.fabric_sjd import FabricSjdConfig
from people_counter.fabric_sjd_cutover import (
    CA_PREFIX,
    CA_SHADOW_ONLY_TABLES,
    CUTOVER_ID,
    SourceTableState,
    StoppedWriterGate,
    TRANSFERABLE_SUFFIXES,
    WriterState,
    append_journal_once,
    archive_and_delete_synthetic_work,
    archive_ca_tables,
    canonical_bytes,
    complete_synthetic_cleanup,
    create_stable_tables,
    retire_archived_ca_tables,
    retirement_manifest_sources,
    sha256,
    snapshot_ca_tables,
    synthetic_work_inventory,
    validate_retirement_archives,
    validate_stopped_writer_gate,
    retirement_bundle_path,
)


CA_TABLE_ALLOWLIST = tuple(
    sorted(
        {
            f"{CA_PREFIX}{suffix}"
            for suffix in (
                "locks",
                "work",
                "batches",
                "batch_members",
                "attempts",
                "publications",
                "replay_requests",
                "reconciliation_findings",
                "gold_checkpoints",
                "semantic_refresh_outbox",
                "migration_journal",
                "gold_flow_minute",
                "gold_flow_hour",
                "gold_video",
                "gold_operations_hour",
                "gold_dim_date",
                "gold_dim_time",
                "gold_dim_camera",
                "gold_dim_location",
                "gold_dim_video",
                "gold_dim_model_config",
            )
        }
        | set(CA_SHADOW_ONLY_TABLES)
    )
)
EVIDENCE_ROOT = "Files/people-counter/sjd/v1/cutovers/people_counter_sjd_0001"
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024


def _spark() -> Any:
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def _write_immutable_json(path: str, value: Mapping[str, Any]) -> str:
    import notebookutils

    content = canonical_bytes(value).decode("utf-8")
    if len(content.encode("utf-8")) > MAX_EVIDENCE_BYTES:
        raise ValueError(f"immutable evidence exceeds size limit at {path}")
    if notebookutils.fs.exists(path):
        observed = notebookutils.fs.head(path, len(content.encode("utf-8")) + 1)
        observed = (
            observed.decode("utf-8") if isinstance(observed, bytes) else str(observed)
        )
        if observed != content:
            raise FileExistsError(path)
    elif notebookutils.fs.put(path, content, False) is not True:
        raise OSError(f"immutable evidence create failed at {path}")
    deadline = time.monotonic() + 15.0
    observed = ""
    while observed != content:
        observed = notebookutils.fs.head(path, len(content.encode("utf-8")) + 1)
        observed = (
            observed.decode("utf-8") if isinstance(observed, bytes) else str(observed)
        )
        if observed == content:
            break
        if time.monotonic() >= deadline:
            raise OSError(f"immutable evidence readback differs at {path}")
        time.sleep(0.25)
    if observed != content:
        raise OSError(f"immutable evidence readback differs at {path}")
    return sha256(value)


def _read_immutable_json(path: str) -> dict[str, Any]:
    import notebookutils

    observed = notebookutils.fs.head(path, MAX_EVIDENCE_BYTES + 1)
    raw = observed if isinstance(observed, bytes) else str(observed).encode("utf-8")
    if len(raw) > MAX_EVIDENCE_BYTES:
        raise ValueError(f"immutable evidence exceeds size limit at {path}")
    value = json.loads(raw)
    if not isinstance(value, dict) or canonical_bytes(value) != raw:
        raise ValueError(f"immutable evidence is not canonical at {path}")
    return value


def inventory(spark: Any, evidence_id: str) -> dict[str, Any]:
    config = FabricSjdConfig()
    sources = snapshot_ca_tables(spark, CA_TABLE_ALLOWLIST)
    targets = [
        {
            "exists": bool(spark.catalog.tableExists(config.table(suffix))),
            "name": config.table(suffix),
        }
        for suffix in sorted(config_table_suffixes())
    ]
    value = {
        "ca_tables": [asdict(item) for item in sources],
        "cutover_id": CUTOVER_ID,
        "evidence_id": evidence_id,
        "schema": "people-counter-sjd-cutover-inventory-v1",
        "stable_tables": targets,
        "transferable_suffixes": sorted(TRANSFERABLE_SUFFIXES),
    }
    digest = _write_immutable_json(
        f"{EVIDENCE_ROOT}/inventory/{evidence_id}.json", value
    )
    return {**value, "evidence_sha256": digest}


def bootstrap(spark: Any, evidence_id: str) -> dict[str, Any]:
    schemas = create_stable_tables(spark)
    value = {
        "cutover_id": CUTOVER_ID,
        "evidence_id": evidence_id,
        "schema": "people-counter-sjd-bootstrap-v1",
        "table_schema_sha256": schemas,
    }
    digest = _write_immutable_json(
        f"{EVIDENCE_ROOT}/bootstrap/{evidence_id}.json", value
    )
    return {**value, "evidence_sha256": digest}


def synthetic_inventory(spark: Any, evidence_id: str) -> dict[str, Any]:
    value = {
        **synthetic_work_inventory(spark),
        "cutover_id": CUTOVER_ID,
        "evidence_id": evidence_id,
    }
    digest = _write_immutable_json(
        f"{EVIDENCE_ROOT}/synthetic-inventory/{evidence_id}.json", value
    )
    return {**value, "evidence_sha256": digest}


def cleanup_synthetic(
    spark: Any,
    cleanup_id: str,
    work_evidence: Mapping[str, str],
) -> dict[str, Any]:
    import notebookutils

    archive_manifest_path = (
        "Files/people-counter/sjd/v1/validation-cleanups/"
        f"{cleanup_id}/manifest.json"
    )
    if notebookutils.fs.exists(archive_manifest_path):
        manifest = _read_immutable_json(archive_manifest_path)
        if manifest.get("work_evidence") != dict(sorted(work_evidence.items())):
            raise ValueError("synthetic cleanup retry allowlist differs")
        result = complete_synthetic_cleanup(spark, manifest)
    else:
        result = archive_and_delete_synthetic_work(
            spark,
            cleanup_id=cleanup_id,
            work_evidence=work_evidence,
            write_archive_manifest=lambda value: _write_immutable_json(
                archive_manifest_path,
                value,
            ),
            archive_exists=notebookutils.fs.exists,
        )
    _write_immutable_json(
        f"{EVIDENCE_ROOT}/synthetic-cleanup/{cleanup_id}.json",
        result,
    )
    return result


def config_table_suffixes() -> frozenset[str]:
    from people_counter.fabric_sjd import TABLE_SUFFIXES

    return TABLE_SUFFIXES


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-production-schema-cutover")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("inventory", "bootstrap", "synthetic-inventory"):
        command = commands.add_parser(name)
        command.add_argument("--evidence-id", required=True)
    archive = commands.add_parser("archive")
    archive.add_argument("--bundle-id", required=True)
    archive.add_argument("--gate-path", required=True)
    cleanup = commands.add_parser("cleanup-synthetic")
    cleanup.add_argument("--cleanup-id", required=True)
    cleanup.add_argument("--work-evidence", action="append", required=True)
    retire = commands.add_parser("retire")
    retire.add_argument("--bundle-id", required=True)
    retire.add_argument("--gate-path", required=True)
    return parser


def _stopped_gate(encoded: str) -> StoppedWriterGate:
    try:
        value = json.loads(base64.b64decode(encoded, validate=True))
        writer_states = tuple(
            WriterState(str(item["item_id"]), str(item["state"]))
            for item in value["writer_states"]
        )
        source_tables = tuple(
            SourceTableState(
                name=str(item["name"]),
                exists=bool(item["exists"]),
                version=(
                    None if item["version"] is None else int(item["version"])
                ),
                row_count=int(item["row_count"]),
                schema_sha256=(
                    None
                    if item["schema_sha256"] is None
                    else str(item["schema_sha256"])
                ),
            )
            for item in value["source_tables"]
        )
        return StoppedWriterGate(
            captured_at=float(value["captured_at"]),
            writer_states=writer_states,
            source_tables=source_tables,
            routing_to_ca=int(value["routing_to_ca"]),
            evidence_sha256=str(value["evidence_sha256"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("stopped-writer gate is invalid") from error


def _work_evidence(values: Sequence[str]) -> dict[str, str]:
    evidence: dict[str, str] = {}
    for value in values:
        try:
            work_id, payload_sha256 = value.split("=", 1)
        except ValueError as error:
            raise ValueError("work evidence must be work_id=payload_sha256") from error
        if work_id in evidence:
            raise ValueError(f"duplicate work evidence for {work_id}")
        evidence[work_id] = payload_sha256
    return evidence


def _stopped_gate_path(path: str) -> StoppedWriterGate:
    prefix = f"{EVIDENCE_ROOT}/gates/"
    if not path.startswith(prefix) or not path.endswith(".json"):
        raise ValueError("stopped-writer gate path is outside Fabric evidence")
    value = _read_immutable_json(path)
    return _stopped_gate(
        base64.b64encode(canonical_bytes(value)).decode("ascii")
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    spark = _spark()
    if arguments.command == "inventory":
        result = inventory(spark, arguments.evidence_id)
    elif arguments.command == "bootstrap":
        result = bootstrap(spark, arguments.evidence_id)
    elif arguments.command == "synthetic-inventory":
        result = synthetic_inventory(spark, arguments.evidence_id)
    elif arguments.command == "cleanup-synthetic":
        result = cleanup_synthetic(
            spark,
            arguments.cleanup_id,
            _work_evidence(arguments.work_evidence),
        )
    elif arguments.command == "archive":
        import notebookutils

        gate = _stopped_gate_path(arguments.gate_path)
        before = snapshot_ca_tables(spark, CA_TABLE_ALLOWLIST)
        validate_stopped_writer_gate(gate, before)
        manifest_path = (
            f"{retirement_bundle_path(arguments.bundle_id)}/manifest.json"
        )
        if notebookutils.fs.exists(manifest_path):
            result = _read_immutable_json(manifest_path)
            validate_retirement_archives(spark, result, before)
        else:
            result = archive_ca_tables(
                spark,
                bundle_id=arguments.bundle_id,
                table_names=CA_TABLE_ALLOWLIST,
                path_exists=notebookutils.fs.exists,
            )
        after = snapshot_ca_tables(spark, CA_TABLE_ALLOWLIST)
        validate_stopped_writer_gate(gate, after)
        _write_immutable_json(manifest_path, result)
    else:
        import notebookutils

        gate = _stopped_gate_path(arguments.gate_path)
        manifest_path = (
            f"{retirement_bundle_path(arguments.bundle_id)}/manifest.json"
        )
        if notebookutils.fs.exists(manifest_path):
            manifest = _read_immutable_json(manifest_path)
        else:
            manifest = archive_ca_tables(
                spark,
                bundle_id=arguments.bundle_id,
                table_names=CA_TABLE_ALLOWLIST,
                path_exists=notebookutils.fs.exists,
            )
        if manifest.get("bundle_id") != arguments.bundle_id:
            raise ValueError("retirement manifest bundle differs")
        _write_immutable_json(manifest_path, manifest)
        result_path = (
            f"{retirement_bundle_path(arguments.bundle_id)}/retirement.json"
        )
        if notebookutils.fs.exists(result_path):
            result = _read_immutable_json(result_path)
            unsigned_result = {
                key: value
                for key, value in result.items()
                if key != "retirement_result_sha256"
            }
            if (
                result.get("retirement_result_sha256")
                != sha256(unsigned_result)
                or result.get("bundle_id") != arguments.bundle_id
            ):
                raise ValueError("retirement result evidence differs")
            validate_stopped_writer_gate(gate, gate.source_tables)
            current = snapshot_ca_tables(spark, CA_TABLE_ALLOWLIST)
            if any(state.exists for state in current) or result.get(
                "post_retirement"
            ) != [asdict(state) for state in current]:
                raise ValueError("retirement result does not match current catalog")
            rollback = validate_retirement_archives(
                spark,
                manifest,
                retirement_manifest_sources(manifest),
            )
            if result.get("rollback") != rollback:
                raise ValueError("retirement rollback proof differs")
        else:
            result = retire_archived_ca_tables(
                spark,
                gate=gate,
                manifest=manifest,
                table_names=CA_TABLE_ALLOWLIST,
                path_exists=notebookutils.fs.exists,
            )
            _write_immutable_json(result_path, result)
        retired_at = datetime.fromisoformat(str(result["retired_at"]))
        if retired_at.tzinfo is None:
            raise ValueError("retirement timestamp lacks timezone")
        append_journal_once(
            spark,
            suffix="retirement_journal",
            journal_id=arguments.bundle_id,
            row={
                "bundle_id": arguments.bundle_id,
                "status": "RETIRED",
                "manifest_path": manifest_path,
                "manifest_sha256": manifest["manifest_sha256"],
                "zero_writer_proof_sha256": result[
                    "zero_writer_proof_sha256"
                ],
                "zero_routing_proof_sha256": result[
                    "zero_routing_proof_sha256"
                ],
                "rollback_proof_sha256": result["rollback"][
                    "rollback_proof_sha256"
                ],
                "created_at": retired_at,
            },
        )
    print(json.dumps(result, allow_nan=False, default=str, sort_keys=True))
    return 0

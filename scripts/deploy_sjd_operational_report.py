from __future__ import annotations

import copy
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from people_counter.fabric_rest import FabricRESTController
from people_counter.fabric_deployment_manifest import DeploymentManifest
from scripts.deploy_sjd_analytics import (
    ReportingDeploymentError,
    _decoded_part,
    _json_part,
    _parts,
    _replace_part,
    _set_platform_name,
    canonical_bytes,
    publish_reporting_bundle,
    reporting_arguments,
    validate_report_binding,
)


SOURCE_MODEL = {
    "name": "pc_operations_model",
}
SOURCE_REPORT = {
    "name": "pc_operations_report",
}
TARGET_MODEL_NAME = "pc_sjd_operations_model"
TARGET_REPORT_NAME = "pc_sjd_operations_report"
TARGET_DESCRIPTION = (
    "Stable people-counter SJD operational artifact; preserves the reviewed "
    "operations report and binds only to people_counter_sjd_* tables"
)
TABLE_BINDINGS = {
    "people_counter_video_work": "people_counter_sjd_gold_work_operations",
    "people_counter_video_attempts": (
        "people_counter_sjd_gold_attempt_operations"
    ),
    "people_counter_gold_operations_hour": (
        "people_counter_sjd_gold_operations_hour"
    ),
    "people_counter_reconciliation_findings": (
        "people_counter_sjd_reconciliation_findings"
    ),
}
REMOVED_TABLES = {
    "people_counter_event_receipts",
    "people_counter_replay_requests",
}
REQUIRED_PAGES = {"Attempt History", "Operations", "Backfill"}


class OperationalReportingDeploymentError(ReportingDeploymentError):
    pass


def _stable_reconciliation_tmdl(source: str) -> str:
    prefix = source.split("\tcolumn ", 1)[0]
    columns = (
        ("finding_id", "string"),
        ("finding_type", "string"),
        ("severity", "string"),
        ("entity_key", "string"),
        ("details_json", "string"),
        ("first_seen_at", "double"),
        ("last_seen_at", "double"),
        ("resolved_at", "double"),
    )
    body = []
    for name, data_type in columns:
        body.append(
            f"\tcolumn {name}\n"
            f"\t\tdataType: {data_type}\n"
            f"\t\tsummarizeBy: none\n"
            f"\t\tsourceColumn: {name}\n"
        )
    stable = TABLE_BINDINGS["people_counter_reconciliation_findings"]
    return (
        prefix.replace(
            "[dbo].[people_counter_reconciliation_findings]",
            f"[dbo].[{stable}]",
        )
        + "\n".join(body)
        + "\n"
        + "\tpartition people_counter_reconciliation_findings = entity\n"
        + "\t\tmode: directLake\n"
        + "\t\tsource\n"
        + f"\t\t\tentityName: {stable}\n"
        + "\t\t\tschemaName: dbo\n"
        + "\t\t\texpressionSource: 'DirectLake - people_counter_dev'\n"
    )


def _rewrite_relationships(text: str) -> str:
    blocks = text.split("\nrelationship ")
    retained = [blocks[0]]
    for block in blocks[1:]:
        if any(table in block for table in REMOVED_TABLES) or (
            "people_counter_reconciliation_findings" in block
        ):
            continue
        retained.append("relationship " + block)
    return "\n".join(retained)


def rewrite_model_definition(
    source: Mapping[str, Any],
    *,
    target_name: str = TARGET_MODEL_NAME,
) -> dict[str, Any]:
    result = copy.deepcopy(dict(source))
    parts = _parts(result)
    definition = result.get("definition")
    if not isinstance(definition, dict):
        raise OperationalReportingDeploymentError(
            "Fabric definition envelope is invalid"
        )
    retained_parts = []
    observed: set[str] = set()
    for part in parts:
        path = str(part["path"])
        if any(
            path == f"definition/tables/{table}.tmdl"
            for table in REMOVED_TABLES
        ):
            continue
        if not path.endswith(".tmdl"):
            retained_parts.append(part)
            continue
        text = _decoded_part(part).decode("utf-8")
        if path == (
            "definition/tables/"
            "people_counter_reconciliation_findings.tmdl"
        ):
            text = _stable_reconciliation_tmdl(text)
            observed.add("people_counter_reconciliation_findings")
        else:
            for legacy, stable in TABLE_BINDINGS.items():
                lineage = f"[dbo].[{legacy}]"
                entity = f"entityName: {legacy}"
                if lineage in text or entity in text:
                    observed.add(legacy)
                    text = text.replace(lineage, f"[dbo].[{stable}]")
                    text = text.replace(entity, f"entityName: {stable}")
        if path == "definition/model.tmdl":
            text = "\n".join(
                line
                for line in text.splitlines()
                if not any(
                    line.strip() == f"ref table {table}"
                    for table in REMOVED_TABLES
                )
            ) + "\n"
        elif path == "definition/relationships.tmdl":
            text = _rewrite_relationships(text)
        _replace_part(part, text.encode("utf-8"))
        retained_parts.append(part)
    definition["parts"] = retained_parts
    if observed != set(TABLE_BINDINGS):
        missing = sorted(set(TABLE_BINDINGS) - observed)
        raise OperationalReportingDeploymentError(
            "operational model is missing expected physical bindings: "
            f"{missing!r}"
        )
    _set_platform_name(retained_parts, target_name)
    validate_model_definition(result)
    return result


def validate_model_definition(value: Mapping[str, Any]) -> None:
    parts = _parts(value)
    text = "\n".join(
        _decoded_part(part).decode("utf-8")
        for part in parts
        if str(part["path"]).endswith(".tmdl")
    )
    for legacy, stable in TABLE_BINDINGS.items():
        _validate_table_binding(text, legacy, stable)
    if any(table in text for table in REMOVED_TABLES):
        raise OperationalReportingDeploymentError(
            "operational model retains an unsupported legacy-only table"
        )
    physical_entities = {
        line.partition(":")[2].strip()
        for line in text.splitlines()
        if line.strip().startswith("entityName:")
    }
    if not physical_entities or any(
        not entity.startswith("people_counter_sjd_")
        for entity in physical_entities
    ):
        raise OperationalReportingDeploymentError(
            "operational model has a non-SJD physical entity"
        )


def _validate_table_binding(text: str, legacy: str, stable: str) -> None:
    invalid_markers = (
        f"[dbo].[{legacy}]",
        f"entityName: {legacy}",
    )
    required_markers = (
        f"[dbo].[{stable}]",
        f"entityName: {stable}",
    )
    if any(marker in text for marker in invalid_markers) or any(
        marker not in text for marker in required_markers
    ):
        raise OperationalReportingDeploymentError(
            f"operational model binding validation failed for {legacy}"
        )


def rewrite_report_definition(
    source: Mapping[str, Any],
    *,
    source_model_id: str,
    target_model_id: str,
    target_model_name: str = TARGET_MODEL_NAME,
    target_report_name: str = TARGET_REPORT_NAME,
) -> dict[str, Any]:
    result = copy.deepcopy(dict(source))
    parts = _parts(result)
    matches = [part for part in parts if part["path"] == "definition.pbir"]
    if len(matches) != 1:
        raise OperationalReportingDeploymentError(
            "report definition must contain one definition.pbir part"
        )
    try:
        report = json.loads(_decoded_part(matches[0]))
        connection = report["datasetReference"]["byConnection"][
            "connectionString"
        ]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise OperationalReportingDeploymentError(
            "report dataset reference is invalid"
        ) from error
    source_catalog = f"initial catalog={SOURCE_MODEL['name']}"
    source_id = f"semanticmodelid={source_model_id}"
    if (
        not isinstance(connection, str)
        or connection.count(source_catalog) != 1
        or connection.count(source_id) != 1
    ):
        raise OperationalReportingDeploymentError(
            "report does not reference the expected source semantic model"
        )
    connection = connection.replace(
        source_catalog, f"initial catalog={target_model_name}"
    ).replace(source_id, f"semanticmodelid={target_model_id}")
    report["datasetReference"]["byConnection"]["connectionString"] = connection
    _replace_part(matches[0], canonical_bytes(report))
    _set_platform_name(parts, target_report_name)
    validate_report_definition(
        result,
        target_model_id=target_model_id,
        target_model_name=target_model_name,
    )
    return result


def validate_report_definition(
    value: Mapping[str, Any],
    *,
    target_model_id: str,
    target_model_name: str = TARGET_MODEL_NAME,
) -> None:
    parts = _parts(value)
    reference = next(
        (part for part in parts if part["path"] == "definition.pbir"),
        None,
    )
    if reference is None:
        raise OperationalReportingDeploymentError(
            "report definition.pbir is absent"
        )
    report = json.loads(_decoded_part(reference))
    connection = report["datasetReference"]["byConnection"]["connectionString"]
    validate_report_binding(
        connection, target_model_id=target_model_id, target_model_name=target_model_name,
    )
    _, layout = _json_part(parts, "report.json", "report layout")
    sections = layout.get("sections")
    pages = {
        section.get("displayName")
        for section in sections
        if isinstance(section, dict)
    } if isinstance(sections, list) else set()
    if not REQUIRED_PAGES.issubset(pages):
        raise OperationalReportingDeploymentError(
            "report required operational pages are missing"
        )


def deploy(
    output: Path, *, manifest: DeploymentManifest,
    client: FabricRESTController | None = None,
) -> dict[str, Any]:
    return publish_reporting_bundle(
        output, manifest=manifest, bundle="operational", client=client,
    )


def main(argv: Sequence[str] | None = None) -> int:
    output, manifest = reporting_arguments(argv)
    print(json.dumps(deploy(output, manifest=manifest), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

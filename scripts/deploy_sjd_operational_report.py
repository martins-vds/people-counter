from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from people_counter.fabric_canary_tool import make_token_provider
from people_counter.fabric_production_migration import WORKSPACE_ID
from people_counter.fabric_production_migration_tool import FabricRESTController
from scripts.deploy_sjd_analytics import (
    ReportingDeploymentError,
    _decoded_part,
    _get_definition,
    _json_part,
    _parts,
    _replace_part,
    _set_platform_name,
    _target_item,
    canonical_bytes,
    definition_sha256,
)


SOURCE_MODEL = {
    "id": "b108e56f-8559-4f84-8ae1-70ba58075353",
    "name": "pc_operations_model",
}
SOURCE_REPORT = {
    "id": "8b81cc28-cf18-4707-a245-afb98a8acbcc",
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
    if (
        f"initial catalog={target_model_name}" not in connection
        or f"semanticmodelid={target_model_id}" not in connection
        or f"semanticmodelid={SOURCE_MODEL['id']}" in connection
    ):
        raise OperationalReportingDeploymentError(
            "report target model binding is invalid"
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


def _deploy_definition(
    client: FabricRESTController,
    *,
    root: str,
    item_type: str,
    display_name: str,
    definition: Mapping[str, Any],
) -> dict[str, Any]:
    item = _target_item(client, item_type, display_name)
    if item is None:
        client._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/{root}",
            value={
                **dict(definition),
                "displayName": display_name,
                "description": TARGET_DESCRIPTION,
            },
            accepted=(201, 202),
        )
        item = _target_item(client, item_type, display_name)
        if item is None:
            raise OperationalReportingDeploymentError(
                f"created {item_type} {display_name} is not discoverable"
            )
    elif item.get("description") != TARGET_DESCRIPTION:
        raise OperationalReportingDeploymentError(
            f"refusing to overwrite unmanaged {item_type} {display_name}"
        )
    client._lro_json(
        "POST",
        f"/workspaces/{WORKSPACE_ID}/{root}/{item['id']}/updateDefinition",
        value=dict(definition),
        accepted=(200, 202),
    )
    return item


def deploy(output: Path) -> dict[str, Any]:
    client = FabricRESTController(make_token_provider("azure-cli"))
    source_model = _get_definition(
        client, "semanticModels", str(SOURCE_MODEL["id"])
    )
    source_report = _get_definition(
        client, "reports", str(SOURCE_REPORT["id"])
    )
    source_model_sha256 = definition_sha256(source_model)
    source_report_sha256 = definition_sha256(source_report)
    model_definition = rewrite_model_definition(source_model)
    model = _deploy_definition(
        client,
        root="semanticModels",
        item_type="SemanticModel",
        display_name=TARGET_MODEL_NAME,
        definition=model_definition,
    )
    model_id = str(model["id"])
    report_definition = rewrite_report_definition(
        source_report,
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=model_id,
    )
    report = _deploy_definition(
        client,
        root="reports",
        item_type="Report",
        display_name=TARGET_REPORT_NAME,
        definition=report_definition,
    )
    model_readback = _get_definition(client, "semanticModels", model_id)
    report_readback = _get_definition(client, "reports", str(report["id"]))
    validate_model_definition(model_readback)
    validate_report_definition(
        report_readback,
        target_model_id=model_id,
    )
    evidence = {
        "deployed_at": datetime.now(timezone.utc).isoformat(),
        "model": {
            "definition_sha256": definition_sha256(model_readback),
            "id": model_id,
            "name": TARGET_MODEL_NAME,
        },
        "removed_legacy_only_tables": sorted(REMOVED_TABLES),
        "report": {
            "definition_sha256": definition_sha256(report_readback),
            "id": str(report["id"]),
            "name": TARGET_REPORT_NAME,
        },
        "schema": "people-counter-sjd-operations-deployment-v1",
        "source_model_definition_sha256": source_model_sha256,
        "source_report_definition_sha256": source_report_sha256,
        "table_bindings": TABLE_BINDINGS,
        "workspace_id": WORKSPACE_ID,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args(argv)
    print(json.dumps(deploy(arguments.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

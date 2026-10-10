from __future__ import annotations

import base64
import json

import pytest

from scripts.deploy_sjd_operational_report import (
    OperationalReportingDeploymentError,
    REMOVED_TABLES,
    SOURCE_MODEL,
    TABLE_BINDINGS,
    TARGET_MODEL_NAME,
    TARGET_REPORT_NAME,
    rewrite_model_definition,
    rewrite_report_definition,
    validate_model_definition,
    validate_report_definition,
)


def _part(path: str, value: str | dict[str, object]) -> dict[str, str]:
    content = (
        json.dumps(value, separators=(",", ":"), sort_keys=True)
        if isinstance(value, dict)
        else value
    )
    return {
        "path": path,
        "payload": base64.b64encode(content.encode()).decode(),
        "payloadType": "InlineBase64",
    }


def _platform(item_type: str, name: str) -> dict[str, object]:
    return {
        "metadata": {"type": item_type, "displayName": name},
        "config": {"version": "2.0", "logicalId": "source"},
    }


def _table(name: str) -> str:
    measure = (
        "\n\tmeasure 'Open Reconciliation Errors' = "
        "COUNTROWS(people_counter_reconciliation_findings)\n"
        if name == "people_counter_video_work"
        else ""
    )
    return (
        f"table {name}\n"
        f"\tsourceLineageTag: [dbo].[{name}]\n"
        f"{measure}"
        "\tcolumn work_id\n"
        "\t\tdataType: string\n"
        "\t\tsourceColumn: work_id\n\n"
        f"\tpartition {name} = entity\n"
        "\t\tmode: directLake\n"
        "\t\tsource\n"
        f"\t\t\tentityName: {name}\n"
        "\t\t\tschemaName: dbo\n"
        "\t\t\texpressionSource: 'DirectLake - people_counter_dev'\n"
    )


def _model() -> dict[str, object]:
    names = list(TABLE_BINDINGS) + sorted(REMOVED_TABLES)
    relationships = "\n\n".join(
        [
            "relationship keep\n"
            "\tfromColumn: people_counter_video_attempts.work_id\n"
            "\ttoColumn: people_counter_video_work.work_id",
            "relationship remove-reconciliation\n"
            "\tfromColumn: people_counter_reconciliation_findings.work_id\n"
            "\ttoColumn: people_counter_video_work.work_id",
            "relationship remove-events\n"
            "\tfromColumn: people_counter_event_receipts.work_id\n"
            "\ttoColumn: people_counter_video_work.work_id",
        ]
    )
    parts = [
        _part(f"definition/tables/{name}.tmdl", _table(name))
        for name in names
    ]
    parts.extend(
        [
            _part(
                "definition/model.tmdl",
                "\n".join(f"ref table {name}" for name in names) + "\n",
            ),
            _part("definition/relationships.tmdl", relationships),
            _part(".platform", _platform("SemanticModel", "legacy")),
        ]
    )
    return {"definition": {"format": "TMDL", "parts": parts}}


def _report() -> dict[str, object]:
    connection = (
        "Data Source=powerbi://example;"
        f"initial catalog={SOURCE_MODEL['name']};"
        f"semanticmodelid={SOURCE_MODEL['id']}"
    )
    sections = [
        {"displayName": name, "visualContainers": []}
        for name in ("Attempt History", "Operations", "Backfill")
    ]
    return {
        "definition": {
            "format": "PBIR-Legacy",
            "parts": [
                _part(
                    "definition.pbir",
                    {
                        "datasetReference": {
                            "byConnection": {
                                "connectionString": connection,
                            }
                        }
                    },
                ),
                _part("report.json", {"sections": sections}),
                _part(".platform", _platform("Report", "legacy")),
            ],
        }
    }


def _decoded(value: dict[str, object], path: str) -> str:
    part = next(
        item
        for item in value["definition"]["parts"]
        if item["path"] == path
    )
    return base64.b64decode(part["payload"]).decode()


def test_rewrite_model_preserves_report_contract_with_only_sjd_entities() -> None:
    source = _model()

    result = rewrite_model_definition(source)

    validate_model_definition(result)
    paths = {
        part["path"] for part in result["definition"]["parts"]
    }
    for removed in REMOVED_TABLES:
        assert f"definition/tables/{removed}.tmdl" not in paths
        assert f"ref table {removed}" not in _decoded(
            result, "definition/model.tmdl"
        )
    for legacy, stable in TABLE_BINDINGS.items():
        table = _decoded(
            result, f"definition/tables/{legacy}.tmdl"
        )
        assert f"[dbo].[{stable}]" in table
        assert f"entityName: {stable}" in table
    work = _decoded(
        result,
        "definition/tables/people_counter_video_work.tmdl",
    )
    assert "measure 'Open Reconciliation Errors'" in work
    reconciliation = _decoded(
        result,
        "definition/tables/people_counter_reconciliation_findings.tmdl",
    )
    assert "column entity_key" in reconciliation
    assert "column resolved_at" in reconciliation
    relationships = _decoded(result, "definition/relationships.tmdl")
    assert "relationship keep" in relationships
    assert "remove-reconciliation" not in relationships
    assert "remove-events" not in relationships
    platform = json.loads(_decoded(result, ".platform"))
    assert platform["metadata"]["displayName"] == TARGET_MODEL_NAME
    assert source == _model()


def test_rewrite_model_fails_when_an_expected_source_table_is_missing() -> None:
    source = _model()
    source["definition"]["parts"] = [
        part
        for part in source["definition"]["parts"]
        if part["path"]
        != "definition/tables/people_counter_video_attempts.tmdl"
    ]

    with pytest.raises(
        OperationalReportingDeploymentError,
        match="missing expected physical bindings",
    ):
        rewrite_model_definition(source)


def test_rewrite_report_rebinds_model_and_preserves_operational_pages() -> None:
    source = _report()
    target_id = "11111111-2222-3333-4444-555555555555"

    result = rewrite_report_definition(
        source,
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
    )

    validate_report_definition(result, target_model_id=target_id)
    reference = json.loads(_decoded(result, "definition.pbir"))
    connection = reference["datasetReference"]["byConnection"][
        "connectionString"
    ]
    assert f"initial catalog={TARGET_MODEL_NAME}" in connection
    assert f"semanticmodelid={target_id}" in connection
    assert str(SOURCE_MODEL["id"]) not in connection
    pages = json.loads(_decoded(result, "report.json"))["sections"]
    assert [page["displayName"] for page in pages] == [
        "Attempt History",
        "Operations",
        "Backfill",
    ]
    platform = json.loads(_decoded(result, ".platform"))
    assert platform["metadata"]["displayName"] == TARGET_REPORT_NAME
    assert source == _report()


def test_validate_report_rejects_missing_required_page() -> None:
    result = rewrite_report_definition(
        _report(),
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id="target-id",
    )
    layout = json.loads(_decoded(result, "report.json"))
    layout["sections"].pop()
    part = next(
        item
        for item in result["definition"]["parts"]
        if item["path"] == "report.json"
    )
    part["payload"] = base64.b64encode(
        json.dumps(layout).encode()
    ).decode()

    with pytest.raises(
        OperationalReportingDeploymentError,
        match="required operational pages",
    ):
        validate_report_definition(result, target_model_id="target-id")

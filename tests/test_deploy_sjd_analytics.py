from __future__ import annotations

import base64
import hashlib
import json

import pytest

from scripts.deploy_sjd_analytics import (
    FORECAST_CAPACITIES,
    FORECAST_CAPACITY_TABLE,
    FORECAST_PAGE_DISPLAY_NAME,
    FORECAST_PAGE_NAME,
    FORECAST_PAGE_ORDINAL,
    FORECAST_VIDEO_HOURS,
    FORECAST_WORKLOAD_TABLE,
    ReportingDeploymentError,
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


def _platform(item_type: str, display_name: str) -> dict[str, object]:
    return {
        "metadata": {"type": item_type, "displayName": display_name},
        "config": {
            "version": "2.0",
            "logicalId": "00000000-0000-0000-0000-000000000000",
        },
    }


def _model() -> dict[str, object]:
    parts = [
        _part(
            f"definition/tables/{legacy}.tmdl",
            (
                f"table {legacy}\n"
                f"\tsourceLineageTag: [dbo].[{legacy}]\n"
                f"\tpartition {legacy} = entity\n"
                "\t\tmode: directLake\n"
                "\t\tsource\n"
                f"\t\t\tentityName: {legacy}\n"
                "\t\t\tschemaName: dbo\n"
            ),
        )
        for legacy in TABLE_BINDINGS
    ]
    parts.extend(
        [
            _part(
                "definition/model.tmdl",
                "\n".join(f"ref table {legacy}" for legacy in TABLE_BINDINGS)
                + "\n",
            ),
            _part(".platform", _platform("SemanticModel", "legacy")),
        ]
    )
    return {"definition": {"format": "TMDL", "parts": parts}}


def _report() -> dict[str, object]:
    connection = (
        "Data Source=powerbi://example;"
        f"initial catalog={SOURCE_MODEL['name']};"
        "integrated security=ClaimsToken;"
        f"semanticmodelid={SOURCE_MODEL['id']}"
    )
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
                _part(
                    "report.json",
                    {
                        "config": "{}",
                        "layoutOptimization": 0,
                        "resourcePackages": [],
                        "sections": [
                            {
                                "config": "{}",
                                "displayName": "Existing page",
                                "displayOption": 1,
                                "filters": "[]",
                                "height": 1080.0,
                                "name": "00000000000000000001",
                                "ordinal": 0,
                                "visualContainers": [],
                                "width": 1920.0,
                            }
                        ],
                    },
                ),
                _part(".platform", _platform("Report", "legacy")),
            ],
        }
    }


def _decoded(value: dict[str, object], path: str) -> str:
    parts = value["definition"]["parts"]
    part = next(item for item in parts if item["path"] == path)
    return base64.b64decode(part["payload"]).decode()


def test_rewrite_model_changes_only_physical_bindings_and_platform_name() -> None:
    source = _model()

    result = rewrite_model_definition(source)

    validate_model_definition(result)
    for legacy, stable in TABLE_BINDINGS.items():
        text = _decoded(result, f"definition/tables/{legacy}.tmdl")
        assert f"table {legacy}" in text
        assert f"[dbo].[{stable}]" in text
        assert f"entityName: {stable}" in text
        assert f"[dbo].[{legacy}]" not in text
        assert f"entityName: {legacy}" not in text
    platform = json.loads(_decoded(result, ".platform"))
    assert platform["metadata"]["displayName"] == TARGET_MODEL_NAME
    model = _decoded(result, "definition/model.tmdl")
    assert f"ref table '{FORECAST_WORKLOAD_TABLE}'" in model
    assert f"ref table '{FORECAST_CAPACITY_TABLE}'" in model
    workload = _decoded(
        result,
        f"definition/tables/{FORECAST_WORKLOAD_TABLE}.tmdl",
    )
    assert (
        f"SELECTEDVALUE('{FORECAST_WORKLOAD_TABLE}'[Video Hours], "
        f"{FORECAST_VIDEO_HOURS})"
    ) in workload
    assert (
        hashlib.sha256(workload.encode()).hexdigest()
        == "a9f9c97f8c4572a04529209322c2d1102ebb2cf00b307c29875bfddcef78cbc6"
    )
    capacity = _decoded(
        result,
        f"definition/tables/{FORECAST_CAPACITY_TABLE}.tmdl",
    )
    for value in FORECAST_CAPACITIES:
        assert f'{{"F{value}", {value}}}' in capacity
    assert "'Estimated Completion Days'" in capacity
    assert (
        hashlib.sha256(capacity.encode()).hexdigest()
        == "039fe1d910f040adb86a5590da22e237d988bae8dd029b3a4293fd26f1f31140"
    )
    assert source == _model()


def test_rewrite_model_requires_every_expected_gold_binding() -> None:
    source = _model()
    source["definition"]["parts"].pop(0)

    with pytest.raises(
        ReportingDeploymentError,
        match="missing expected physical bindings",
    ):
        rewrite_model_definition(source)


@pytest.mark.parametrize("marker", ("lineage", "entity"))
def test_validate_model_requires_both_stable_binding_markers(marker: str) -> None:
    result = rewrite_model_definition(_model())
    legacy, stable = next(iter(TABLE_BINDINGS.items()))
    path = f"definition/tables/{legacy}.tmdl"
    part = next(
        item for item in result["definition"]["parts"] if item["path"] == path
    )
    text = base64.b64decode(part["payload"]).decode()
    target = (
        f"[dbo].[{stable}]"
        if marker == "lineage"
        else f"entityName: {stable}"
    )
    part["payload"] = base64.b64encode(text.replace(target, "").encode()).decode()

    with pytest.raises(ReportingDeploymentError, match=legacy):
        validate_model_definition(result)


def test_validate_model_requires_forecast_tables() -> None:
    result = rewrite_model_definition(_model())
    result["definition"]["parts"] = [
        part
        for part in result["definition"]["parts"]
        if part["path"]
        != f"definition/tables/{FORECAST_CAPACITY_TABLE}.tmdl"
    ]

    with pytest.raises(
        ReportingDeploymentError,
        match="^semantic model Forecast tables are missing$",
    ):
        validate_model_definition(result)


def test_rewrite_report_binds_target_model_and_preserves_source() -> None:
    source = _report()
    target_id = "11111111-2222-3333-4444-555555555555"

    result = rewrite_report_definition(
        source,
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
    )

    validate_report_definition(result, target_model_id=target_id)
    report = json.loads(_decoded(result, "definition.pbir"))
    connection = report["datasetReference"]["byConnection"]["connectionString"]
    assert f"initial catalog={TARGET_MODEL_NAME}" in connection
    assert f"semanticmodelid={target_id}" in connection
    assert str(SOURCE_MODEL["id"]) not in connection
    platform = json.loads(_decoded(result, ".platform"))
    assert platform["metadata"]["displayName"] == TARGET_REPORT_NAME
    layout = json.loads(_decoded(result, "report.json"))
    assert [
        page["displayName"]
        for page in sorted(
            layout["sections"],
            key=lambda page: page.get("ordinal", 0),
        )
    ] == ["Existing page", FORECAST_PAGE_DISPLAY_NAME]
    forecast = layout["sections"][-1]
    assert forecast["name"] == FORECAST_PAGE_NAME
    assert forecast["ordinal"] == FORECAST_PAGE_ORDINAL
    forecast_digest = hashlib.sha256(
        json.dumps(
            forecast,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    assert (
        forecast_digest
        == "5ff2c137ddbae5c15b4e6dc4578d20669125b4225b021fb76d0135d740b699b2"
    )
    assert len(forecast["visualContainers"]) == 9
    rendered_config = "\n".join(
        visual["config"] for visual in forecast["visualContainers"]
    )
    assert "DIRECTIONAL ESTIMATE ONLY" in rendered_config
    assert "200,000 video-hours" in rendered_config
    assert "1.5899x aggregate real time" in rendered_config
    assert "ONNX batch-one: 1.3036x vs PyTorch 0.8142x" in rendered_config
    assert "nonlinear scaling between SKUs" in rendered_config
    assert f"{FORECAST_WORKLOAD_TABLE}.Video Hours" in rendered_config
    assert f"{FORECAST_CAPACITY_TABLE}.Estimated Throughput" in rendered_config
    assert (
        f"{FORECAST_CAPACITY_TABLE}.Estimated Completion Days"
        in rendered_config
    )
    assert f'"Value":"{FORECAST_VIDEO_HOURS}L"' in rendered_config
    assert source == _report()


def test_rewrite_report_is_idempotent_for_generated_forecast_page() -> None:
    target_id = "11111111-2222-3333-4444-555555555555"
    result = rewrite_report_definition(
        _report(),
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
    )
    pbir_part = next(
        part
        for part in result["definition"]["parts"]
        if part["path"] == "definition.pbir"
    )
    pbir = json.loads(base64.b64decode(pbir_part["payload"]))
    connection = pbir["datasetReference"]["byConnection"]["connectionString"]
    connection = connection.replace(
        f"initial catalog={TARGET_MODEL_NAME}",
        f"initial catalog={SOURCE_MODEL['name']}",
    ).replace(
        f"semanticmodelid={target_id}",
        f"semanticmodelid={SOURCE_MODEL['id']}",
    )
    pbir["datasetReference"]["byConnection"]["connectionString"] = connection
    pbir_part["payload"] = base64.b64encode(
        json.dumps(pbir).encode()
    ).decode()

    repeated = rewrite_report_definition(
        result,
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
    )

    assert json.loads(_decoded(repeated, "report.json")) == json.loads(
        _decoded(result, "report.json")
    )


def test_rewrite_report_supports_custom_target_names() -> None:
    target_id = "11111111-2222-3333-4444-555555555555"

    result = rewrite_report_definition(
        _report(),
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
        target_model_name="custom_sjd_model",
        target_report_name="custom_sjd_report",
    )

    validate_report_definition(
        result,
        target_model_id=target_id,
        target_model_name="custom_sjd_model",
    )
    report = json.loads(_decoded(result, "definition.pbir"))
    connection = report["datasetReference"]["byConnection"]["connectionString"]
    assert "initial catalog=custom_sjd_model" in connection
    platform = json.loads(_decoded(result, ".platform"))
    assert platform["metadata"]["displayName"] == "custom_sjd_report"


def test_validate_report_rejects_missing_forecast_page() -> None:
    target_id = "11111111-2222-3333-4444-555555555555"
    result = rewrite_report_definition(
        _report(),
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
    )
    layout_part = next(
        part
        for part in result["definition"]["parts"]
        if part["path"] == "report.json"
    )
    layout = json.loads(base64.b64decode(layout_part["payload"]))
    layout["sections"] = [
        page
        for page in layout["sections"]
        if page["name"] != FORECAST_PAGE_NAME
    ]
    layout_part["payload"] = base64.b64encode(
        json.dumps(layout).encode()
    ).decode()

    with pytest.raises(
        ReportingDeploymentError,
        match="^report Forecast page validation failed$",
    ):
        validate_report_definition(result, target_model_id=target_id)


def test_validate_report_accepts_fabric_forecast_normalization() -> None:
    target_id = "11111111-2222-3333-4444-555555555555"
    result = rewrite_report_definition(
        _report(),
        source_model_id=str(SOURCE_MODEL["id"]),
        target_model_id=target_id,
    )
    layout_part = next(
        part
        for part in result["definition"]["parts"]
        if part["path"] == "report.json"
    )
    layout = json.loads(base64.b64decode(layout_part["payload"]))
    forecast = next(
        page
        for page in layout["sections"]
        if page["name"] == FORECAST_PAGE_NAME
    )
    forecast["visualContainers"].reverse()
    for visual in forecast["visualContainers"]:
        visual["z"] = float(visual["z"])
        config = json.loads(visual["config"])
        position = config["layouts"][0]["position"]
        position["z"] = float(position["z"])
        visual["config"] = json.dumps(config)
    layout_part["payload"] = base64.b64encode(
        json.dumps(layout).encode()
    ).decode()

    validate_report_definition(result, target_model_id=target_id)


@pytest.mark.parametrize(
    ("path", "message"),
    (
        ("report.json", "report.json"),
        ("definition.pbir", "definition.pbir"),
    ),
)
def test_rewrite_report_requires_complete_report_definition(
    path: str,
    message: str,
) -> None:
    source = _report()
    source["definition"]["parts"] = [
        part
        for part in source["definition"]["parts"]
        if part["path"] != path
    ]

    with pytest.raises(ReportingDeploymentError, match=message):
        rewrite_report_definition(
            source,
            source_model_id=str(SOURCE_MODEL["id"]),
            target_model_id="11111111-2222-3333-4444-555555555555",
        )


@pytest.mark.parametrize("conflict", ("displayName", "name", "ordinal"))
def test_rewrite_report_refuses_conflicting_forecast_page(
    conflict: str,
) -> None:
    source = _report()
    layout_part = next(
        part
        for part in source["definition"]["parts"]
        if part["path"] == "report.json"
    )
    layout = json.loads(base64.b64decode(layout_part["payload"]))
    page = {
        "config": "{}",
        "displayName": "Different page",
        "displayOption": 1,
        "filters": "[]",
        "height": 1080.0,
        "name": "00000000000000000002",
        "ordinal": 1,
        "visualContainers": [],
        "width": 1920.0,
    }
    if conflict == "displayName":
        page["displayName"] = FORECAST_PAGE_DISPLAY_NAME
    elif conflict == "name":
        page["name"] = FORECAST_PAGE_NAME
    else:
        page["ordinal"] = FORECAST_PAGE_ORDINAL
    layout["sections"].append(page)
    layout_part["payload"] = base64.b64encode(
        json.dumps(layout).encode()
    ).decode()

    with pytest.raises(ReportingDeploymentError, match="Forecast"):
        rewrite_report_definition(
            source,
            source_model_id=str(SOURCE_MODEL["id"]),
            target_model_id="11111111-2222-3333-4444-555555555555",
        )


def test_rewrite_report_refuses_unexpected_source_model() -> None:
    with pytest.raises(
        ReportingDeploymentError,
        match="expected source semantic model",
    ):
        rewrite_report_definition(
            _report(),
            source_model_id="00000000-0000-0000-0000-000000000001",
            target_model_id="00000000-0000-0000-0000-000000000002",
        )


@pytest.mark.parametrize(
    "connection",
    (
        (
            "Data Source=powerbi://example;"
            f"initial catalog={TARGET_MODEL_NAME};"
            "integrated security=ClaimsToken;"
            f"semanticmodelid={SOURCE_MODEL['id']}"
        ),
        (
            "Data Source=powerbi://example;"
            f"initial catalog={TARGET_MODEL_NAME};"
            "integrated security=ClaimsToken;"
            "semanticmodelid=00000000-0000-0000-0000-000000000003"
        ),
    ),
)
def test_validate_report_requires_exact_target_and_excludes_source(
    connection: str,
) -> None:
    report = _report()
    part = report["definition"]["parts"][0]
    value = json.loads(base64.b64decode(part["payload"]))
    value["datasetReference"]["byConnection"]["connectionString"] = connection
    part["payload"] = base64.b64encode(
        json.dumps(value).encode()
    ).decode()

    with pytest.raises(ReportingDeploymentError, match="target model binding"):
        validate_report_definition(
            report,
            target_model_id="11111111-2222-3333-4444-555555555555",
        )

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from people_counter.fabric_canary_tool import make_token_provider
from people_counter.fabric_production_migration import WORKSPACE_ID
from people_counter.fabric_production_migration_tool import FabricRESTController


SOURCE_MODEL = {
    "id": "5dbaf3d1-9f86-4bcd-a590-f32c7fbea08c",
    "name": "pc_analytics_model",
    "sha256": "cf413a08ef8a8e23e084f37c031e3f6cdc53f8c919c6d6791492065ca5585120",
}
SOURCE_REPORT = {
    "id": "133a8648-ca75-409e-9154-1b89d67f3917",
    "name": "pc_analytics_report",
    "sha256": "ad2bd08b5aff60ed282bab085224e85d54a0ff63a8a40be5bbf8e31a8ec617d1",
}
TARGET_MODEL_NAME = "pc_sjd_analytics_model"
TARGET_REPORT_NAME = "pc_sjd_analytics_report"
TARGET_DESCRIPTION = (
    "Stable people-counter SJD analytics artifact; generated from reviewed "
    "legacy analytics definitions and bound only to people_counter_sjd_gold_*"
)
TABLE_BINDINGS = {
    "people_counter_gold_operations_hour": (
        "people_counter_sjd_gold_operations_hour"
    ),
    "people_counter_gold_flow_minute": "people_counter_sjd_gold_flow_minute",
    "people_counter_gold_video": "people_counter_sjd_gold_video",
    "people_counter_gold_flow_hour": "people_counter_sjd_gold_flow_hour",
    "people_counter_gold_dim_video": "people_counter_sjd_gold_dim_video",
    "people_counter_gold_dim_time": "people_counter_sjd_gold_dim_time",
    "people_counter_gold_dim_location": "people_counter_sjd_gold_dim_location",
    "people_counter_gold_dim_date": "people_counter_sjd_gold_dim_date",
    "people_counter_gold_dim_camera": "people_counter_sjd_gold_dim_camera",
    "people_counter_gold_dim_model_config": (
        "people_counter_sjd_gold_dim_model_config"
    ),
}
FORECAST_PAGE_DISPLAY_NAME = "Forecast"
FORECAST_PAGE_NAME = hashlib.sha256(
    b"people-counter-sjd-capacity-forecast-v1"
).hexdigest()[:20]
FORECAST_PAGE_ORDINAL = 5
FORECAST_BASELINE_PROFILE = "pytorch-r18-b1-1fps-1t"
FORECAST_BASELINE_CAPACITY = 64
FORECAST_BASELINE_THROUGHPUT = 1.5899
FORECAST_CAPACITIES = (64, 128, 256, 512, 1024, 2048, 4096, 8192)
FORECAST_VIDEO_HOURS = 200_000
FORECAST_MIN_VIDEO_HOURS = 1_000
FORECAST_MAX_VIDEO_HOURS = 1_000_000
FORECAST_VIDEO_HOURS_INCREMENT = 1_000
FORECAST_TARGET_DAYS = 30
FORECAST_USEFUL_UTILIZATION = 0.80
FORECAST_HEADROOM = 0.20
FORECAST_WORKLOAD_TABLE = "Forecast Workload"
FORECAST_CAPACITY_TABLE = "Forecast Capacity"


class ReportingDeploymentError(RuntimeError):
    pass


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def definition_sha256(value: Mapping[str, Any]) -> str:
    definition = value.get("definition")
    if not isinstance(definition, dict):
        raise ReportingDeploymentError("Fabric definition envelope is invalid")
    return hashlib.sha256(canonical_bytes(definition)).hexdigest()


def _parts(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    definition = value.get("definition")
    if not isinstance(definition, dict):
        raise ReportingDeploymentError("Fabric definition envelope is invalid")
    parts = definition.get("parts")
    if not isinstance(parts, list) or not parts:
        raise ReportingDeploymentError("Fabric definition has no parts")
    result = []
    for part in parts:
        if (
            not isinstance(part, dict)
            or not isinstance(part.get("path"), str)
            or part.get("payloadType") != "InlineBase64"
            or not isinstance(part.get("payload"), str)
        ):
            raise ReportingDeploymentError("Fabric definition part is invalid")
        result.append(part)
    return result


def _decoded_part(part: Mapping[str, Any]) -> bytes:
    try:
        return base64.b64decode(str(part["payload"]), validate=True)
    except (KeyError, ValueError) as error:
        raise ReportingDeploymentError("definition payload is not base64") from error


def _replace_part(part: dict[str, Any], content: bytes) -> None:
    part["payload"] = base64.b64encode(content).decode("ascii")


def _json_part(
    parts: Sequence[dict[str, Any]],
    path: str,
    label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    matches = [part for part in parts if part["path"] == path]
    if len(matches) != 1:
        raise ReportingDeploymentError(
            f"definition must contain one {path} part"
        )
    try:
        value = json.loads(_decoded_part(matches[0]))
    except (TypeError, json.JSONDecodeError) as error:
        raise ReportingDeploymentError(f"{label} is invalid") from error
    if not isinstance(value, dict):
        raise ReportingDeploymentError(f"{label} is invalid")
    return matches[0], value


def _set_platform_name(parts: Sequence[dict[str, Any]], name: str) -> None:
    matches = [part for part in parts if part["path"] == ".platform"]
    if len(matches) != 1:
        raise ReportingDeploymentError("definition must contain one .platform part")
    try:
        platform = json.loads(_decoded_part(matches[0]))
        platform["metadata"]["displayName"] = name
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ReportingDeploymentError(".platform metadata is invalid") from error
    _replace_part(matches[0], canonical_bytes(platform))


def _visual_name(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:20]


def _textbox(
    seed: str,
    lines: Sequence[str],
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    z: int,
    font_size: str,
    color: str = "#252423",
    font_family: str = "Segoe UI",
    font_weight: str | None = None,
) -> dict[str, Any]:
    name = _visual_name(f"forecast:{seed}")
    text_style = {
        "color": color,
        "fontFamily": font_family,
        "fontSize": font_size,
    }
    if font_weight is not None:
        text_style["fontWeight"] = font_weight
    paragraphs = [
        {
            "horizontalTextAlignment": "left",
            "textRuns": [
                {
                    "textStyle": text_style,
                    "value": line,
                }
            ],
        }
        for line in lines
    ]
    position = {
        "height": height,
        "tabOrder": z,
        "width": width,
        "x": x,
        "y": y,
        "z": z,
    }
    config = {
        "layouts": [{"id": 0, "position": position}],
        "name": name,
        "singleVisual": {
            "drillFilterOtherVisuals": True,
            "objects": {
                "general": [
                    {
                        "properties": {
                            "paragraphs": paragraphs,
                        }
                    }
                ]
            },
            "visualType": "textbox",
        },
    }
    return {
        "config": json.dumps(
            config,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "filters": "[]",
        "height": height,
        "width": width,
        "x": x,
        "y": y,
        "z": z,
    }


def _lineage_tag(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"people-counter:{seed}"))


def _pbi_id(seed: str) -> str:
    return hashlib.sha256(
        f"people-counter:{seed}".encode("utf-8")
    ).hexdigest()[:32]


def _forecast_workload_tmdl() -> str:
    return f"""table '{FORECAST_WORKLOAD_TABLE}'
\tlineageTag: {_lineage_tag("forecast-workload-table")}

\tmeasure 'Selected Video Hours' = SELECTEDVALUE('{FORECAST_WORKLOAD_TABLE}'[Video Hours], {FORECAST_VIDEO_HOURS})
\t\tformatString: #,0
\t\tlineageTag: {_lineage_tag("selected-video-hours")}

\tmeasure 'Bare Throughput Target' = DIVIDE([Selected Video Hours], {FORECAST_TARGET_DAYS} * 24)
\t\tformatString: 0.00
\t\tlineageTag: {_lineage_tag("bare-throughput-target")}

\tmeasure 'Planning Throughput Target' = DIVIDE([Bare Throughput Target] * {1 + FORECAST_HEADROOM:.2f}, {FORECAST_USEFUL_UTILIZATION:.2f})
\t\tformatString: 0.00
\t\tlineageTag: {_lineage_tag("planning-throughput-target")}

\tcolumn 'Video Hours'
\t\tformatString: #,0
\t\tlineageTag: {_lineage_tag("forecast-video-hours")}
\t\tsummarizeBy: none
\t\tsourceColumn: [Value]

\t\textendedProperty ParameterMetadata =
\t\t\t{{
\t\t\t  "version": 0
\t\t\t}}

\t\tannotation SummarizationSetBy = User

\tpartition '{FORECAST_WORKLOAD_TABLE}' = calculated
\t\tmode: import
\t\tsource = GENERATESERIES({FORECAST_MIN_VIDEO_HOURS}, {FORECAST_MAX_VIDEO_HOURS}, {FORECAST_VIDEO_HOURS_INCREMENT})

\tannotation PBI_Id = {_pbi_id("forecast-workload-table")}
"""


def _forecast_capacity_tmdl() -> str:
    capacity_rows = ",\n".join(
        f'\t\t\t\t{{"F{capacity}", {capacity}}}'
        for capacity in FORECAST_CAPACITIES
    )
    return f"""table '{FORECAST_CAPACITY_TABLE}'
\tlineageTag: {_lineage_tag("forecast-capacity-table")}

\tmeasure 'Estimated Throughput' = {FORECAST_BASELINE_THROUGHPUT:.4f} * DIVIDE(SELECTEDVALUE('{FORECAST_CAPACITY_TABLE}'[Capacity Units]), {FORECAST_BASELINE_CAPACITY})
\t\tformatString: 0.00
\t\tlineageTag: {_lineage_tag("estimated-throughput")}

\tmeasure 'Estimated Completion Days' = DIVIDE([Selected Video Hours], [Estimated Throughput] * 24)
\t\tformatString: #,0.0
\t\tlineageTag: {_lineage_tag("estimated-completion-days")}

\tcolumn 'Capacity SKU'
\t\tdataType: string
\t\tlineageTag: {_lineage_tag("capacity-sku")}
\t\tsummarizeBy: none
\t\tisNameInferred
\t\tisDataTypeInferred
\t\tsourceColumn: [Capacity SKU]
\t\tsortByColumn: 'Capacity Units'

\t\tannotation SummarizationSetBy = Automatic

\tcolumn 'Capacity Units'
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: {_lineage_tag("capacity-units")}
\t\tsummarizeBy: none
\t\tisNameInferred
\t\tisDataTypeInferred
\t\tsourceColumn: [Capacity Units]

\t\tannotation SummarizationSetBy = Automatic

\tpartition '{FORECAST_CAPACITY_TABLE}' = calculated
\t\tmode: import
\t\tsource =
\t\t\tDATATABLE(
\t\t\t\t"Capacity SKU", STRING,
\t\t\t\t"Capacity Units", INTEGER,
\t\t\t\t{{
{capacity_rows}
\t\t\t\t}}
\t\t\t)

\tannotation PBI_Id = {_pbi_id("forecast-capacity-table")}
"""


def _forecast_model_parts() -> dict[str, str]:
    return {
        (
            f"definition/tables/{FORECAST_WORKLOAD_TABLE}.tmdl"
        ): _forecast_workload_tmdl(),
        (
            f"definition/tables/{FORECAST_CAPACITY_TABLE}.tmdl"
        ): _forecast_capacity_tmdl(),
    }


def _visual(
    seed: str,
    single_visual: Mapping[str, Any],
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    z: int,
    filters: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    name = _visual_name(f"forecast:{seed}")
    position = {
        "height": height,
        "tabOrder": z,
        "width": width,
        "x": x,
        "y": y,
        "z": z,
    }
    config = {
        "layouts": [{"id": 0, "position": position}],
        "name": name,
        "singleVisual": dict(single_visual),
    }
    return {
        "config": json.dumps(
            config,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "filters": json.dumps(
            list(filters),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "height": height,
        "width": width,
        "x": x,
        "y": y,
        "z": z,
    }


def _measure_card(
    seed: str,
    table: str,
    measure: str,
    title: str,
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    z: int,
) -> dict[str, Any]:
    query_ref = f"{table}.{measure}"
    return _visual(
        seed,
        {
            "drillFilterOtherVisuals": True,
            "hasDefaultSort": True,
            "projections": {"Data": [{"queryRef": query_ref}]},
            "prototypeQuery": {
                "From": [{"Entity": table, "Name": "f", "Type": 0}],
                "OrderBy": [
                    {
                        "Direction": 2,
                        "Expression": {
                            "Measure": {
                                "Expression": {
                                    "SourceRef": {"Source": "f"}
                                },
                                "Property": measure,
                            }
                        },
                    }
                ],
                "Select": [
                    {
                        "Measure": {
                            "Expression": {"SourceRef": {"Source": "f"}},
                            "Property": measure,
                        },
                        "Name": query_ref,
                        "NativeReferenceName": measure,
                    }
                ],
                "Version": 2,
            },
            "vcObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {
                                "expr": {
                                    "Literal": {"Value": repr(title)}
                                }
                            },
                        }
                    }
                ]
            },
            "visualType": "cardVisual",
        },
        x=x,
        y=y,
        width=width,
        height=height,
        z=z,
    )


def _workload_slicer() -> dict[str, Any]:
    table = FORECAST_WORKLOAD_TABLE
    column = "Video Hours"
    query_ref = f"{table}.{column}"
    selection_filter = {
        "Version": 2,
        "From": [{"Entity": table, "Name": "f", "Type": 0}],
        "Where": [
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {
                                        "SourceRef": {"Source": "f"}
                                    },
                                    "Property": column,
                                }
                            }
                        ],
                        "Values": [
                            [
                                {
                                    "Literal": {
                                        "Value": f"{FORECAST_VIDEO_HOURS}L"
                                    }
                                }
                            ]
                        ],
                    }
                }
            }
        ],
    }
    return _visual(
        "workload-slicer",
        {
            "drillFilterOtherVisuals": True,
            "objects": {
                "data": [
                    {
                        "properties": {
                            "mode": {
                                "expr": {
                                    "Literal": {"Value": "'Dropdown'"}
                                }
                            },
                            "numericStart": {
                                "expr": {
                                    "Literal": {
                                        "Value": f"{FORECAST_VIDEO_HOURS}D"
                                    }
                                }
                            },
                        }
                    }
                ],
                "general": [
                    {
                        "properties": {
                            "filter": {"filter": selection_filter}
                        }
                    }
                ],
                "selection": [
                    {
                        "properties": {
                            "strictSingleSelect": {
                                "expr": {
                                    "Literal": {"Value": "true"}
                                }
                            }
                        }
                    }
                ],
                "slider": [
                    {
                        "properties": {
                            "show": {
                                "expr": {
                                    "Literal": {"Value": "true"}
                                }
                            }
                        }
                    }
                ],
            },
            "projections": {
                "Values": [{"active": True, "queryRef": query_ref}]
            },
            "prototypeQuery": {
                "From": [{"Entity": table, "Name": "f", "Type": 0}],
                "OrderBy": [
                    {
                        "Direction": 1,
                        "Expression": {
                            "Column": {
                                "Expression": {
                                    "SourceRef": {"Source": "f"}
                                },
                                "Property": column,
                            }
                        },
                    }
                ],
                "Select": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": "f"}},
                            "Property": column,
                        },
                        "Name": query_ref,
                        "NativeReferenceName": column,
                    }
                ],
                "Version": 2,
            },
            "vcObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {
                                "expr": {
                                    "Literal": {
                                        "Value": "'Video hours to process'"
                                    }
                                }
                            },
                        }
                    }
                ]
            },
            "visualType": "slicer",
        },
        x=60.0,
        y=220.0,
        width=470.0,
        height=180.0,
        z=2,
    )


def _capacity_table() -> dict[str, Any]:
    table = FORECAST_CAPACITY_TABLE
    sku = "Capacity SKU"
    throughput = "Estimated Throughput"
    completion = "Estimated Completion Days"
    return _visual(
        "capacity-table",
        {
            "drillFilterOtherVisuals": True,
            "projections": {
                "Values": [
                    {"queryRef": f"{table}.{sku}"},
                    {"queryRef": f"{table}.{throughput}"},
                    {"queryRef": f"{table}.{completion}"},
                ]
            },
            "prototypeQuery": {
                "From": [{"Entity": table, "Name": "f", "Type": 0}],
                "Select": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": "f"}},
                            "Property": sku,
                        },
                        "Name": f"{table}.{sku}",
                        "NativeReferenceName": sku,
                    },
                    {
                        "Measure": {
                            "Expression": {"SourceRef": {"Source": "f"}},
                            "Property": throughput,
                        },
                        "Name": f"{table}.{throughput}",
                        "NativeReferenceName": throughput,
                    },
                    {
                        "Measure": {
                            "Expression": {"SourceRef": {"Source": "f"}},
                            "Property": completion,
                        },
                        "Name": f"{table}.{completion}",
                        "NativeReferenceName": completion,
                    },
                ],
                "Version": 2,
            },
            "vcObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {
                                "expr": {
                                    "Literal": {
                                        "Value": (
                                            "'Capacity forecast for selected "
                                            "workload'"
                                        )
                                    }
                                }
                            },
                        }
                    }
                ]
            },
            "visualType": "tableEx",
        },
        x=60.0,
        y=420.0,
        width=1800.0,
        height=330.0,
        z=6,
    )


def _forecast_page() -> dict[str, Any]:
    visuals = [
        _textbox(
            "title",
            ("Capacity throughput forecast",),
            x=60.0,
            y=35.0,
            width=1800.0,
            height=80.0,
            z=0,
            font_size="30px",
            color="#118DFF",
            font_family="Segoe UI Semibold",
            font_weight="bold",
        ),
        _textbox(
            "baseline",
            (
                "Directional estimate based on the best measured F64 pilot",
                (
                    f"Baseline: {FORECAST_BASELINE_PROFILE} at "
                    f"{FORECAST_BASELINE_THROUGHPUT:.4f}x aggregate real time"
                ),
                (
                    f"Default workload: {FORECAST_VIDEO_HOURS:,} video-hours; "
                    "adjust it with the selector below."
                ),
            ),
            x=60.0,
            y=115.0,
            width=1800.0,
            height=95.0,
            z=1,
            font_size="15px",
        ),
        _workload_slicer(),
        _measure_card(
            "selected-workload",
            FORECAST_WORKLOAD_TABLE,
            "Selected Video Hours",
            "Selected video-hours",
            x=560.0,
            y=220.0,
            width=400.0,
            height=180.0,
            z=3,
        ),
        _measure_card(
            "bare-target",
            FORECAST_WORKLOAD_TABLE,
            "Bare Throughput Target",
            f"Bare target for {FORECAST_TARGET_DAYS} days (x)",
            x=990.0,
            y=220.0,
            width=400.0,
            height=180.0,
            z=4,
        ),
        _measure_card(
            "planning-target",
            FORECAST_WORKLOAD_TABLE,
            "Planning Throughput Target",
            (
                f"Planning target ({FORECAST_HEADROOM:.0%} headroom, "
                f"{FORECAST_USEFUL_UTILIZATION:.0%} utilization)"
            ),
            x=1420.0,
            y=220.0,
            width=440.0,
            height=180.0,
            z=5,
        ),
        _capacity_table(),
    ]
    visuals.extend(
        [
            _textbox(
                "warning",
                (
                    "DIRECTIONAL ESTIMATE ONLY — not a capacity guarantee.",
                    (
                        "Linear SKU scaling is an intentionally gross planning "
                        "approximation. Validate with a representative, "
                        "concurrent qualification run before resizing."
                    ),
                ),
                x=60.0,
                y=770.0,
                width=1800.0,
                height=95.0,
                z=7,
                font_size="16px",
                color="#C50F1F",
                font_weight="bold",
            ),
            _textbox(
                "assumptions",
                (
                    "Not modeled: Spark quotas and concurrency limits; executor "
                    "startup and idle tail; capacity throttling; storage/network "
                    "I/O; model-cache behavior; workload contention; retry and "
                    "failure overhead; or nonlinear scaling between SKUs.",
                    (
                        "Experimental single-video ONNX batch-one: 1.3036x vs "
                        "PyTorch 0.8142x (1.60x faster). It is not the planning "
                        "baseline until a comparable concurrent pilot is "
                        "qualified."
                    ),
                ),
                x=60.0,
                y=875.0,
                width=1800.0,
                height=145.0,
                z=8,
                font_size="14px",
            ),
        ]
    )
    return {
        "config": "{}",
        "displayName": FORECAST_PAGE_DISPLAY_NAME,
        "displayOption": 1,
        "filters": "[]",
        "height": 1080.0,
        "name": FORECAST_PAGE_NAME,
        "ordinal": FORECAST_PAGE_ORDINAL,
        "visualContainers": visuals,
        "width": 1920.0,
    }


def _normalized_forecast_page(page: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(page))
    visuals = result.get("visualContainers")
    if not isinstance(visuals, list):
        raise ReportingDeploymentError(
            "report Forecast visual containers are invalid"
        )
    normalized_visuals: dict[str, dict[str, Any]] = {}
    for visual in visuals:
        if not isinstance(visual, dict) or not isinstance(
            visual.get("config"),
            str,
        ):
            raise ReportingDeploymentError("report Forecast visual is invalid")
        try:
            config = json.loads(visual["config"])
        except json.JSONDecodeError as error:
            raise ReportingDeploymentError(
                "report Forecast visual config is invalid"
            ) from error
        if not isinstance(config, dict) or not isinstance(
            config.get("name"),
            str,
        ):
            raise ReportingDeploymentError(
                "report Forecast visual config is invalid"
            )
        name = config["name"]
        if name in normalized_visuals:
            raise ReportingDeploymentError(
                "report Forecast visual names are not unique"
            )
        normalized_visual = copy.deepcopy(visual)
        normalized_visual["config"] = config
        normalized_visuals[name] = normalized_visual
    result["visualContainers"] = normalized_visuals
    return result


def _forecast_pages_equal(
    observed: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> bool:
    return _normalized_forecast_page(observed) == _normalized_forecast_page(
        expected
    )


def _add_forecast_page(report: dict[str, Any]) -> None:
    sections = report.get("sections")
    if not isinstance(sections, list):
        raise ReportingDeploymentError("report layout sections are invalid")
    expected = _forecast_page()
    matches = [
        section
        for section in sections
        if isinstance(section, dict)
        and (
            section.get("displayName") == FORECAST_PAGE_DISPLAY_NAME
            or section.get("name") == FORECAST_PAGE_NAME
        )
    ]
    if len(matches) > 1:
        raise ReportingDeploymentError("report has conflicting Forecast pages")
    if matches:
        if not _forecast_pages_equal(matches[0], expected):
            raise ReportingDeploymentError("report has a conflicting Forecast page")
        return
    if any(
        isinstance(section, dict)
        and section.get("ordinal") == FORECAST_PAGE_ORDINAL
        for section in sections
    ):
        raise ReportingDeploymentError(
            "report Forecast page ordinal is already occupied"
        )
    sections.append(expected)


def _validate_forecast_page(report: Mapping[str, Any]) -> None:
    sections = report.get("sections")
    if not isinstance(sections, list):
        raise ReportingDeploymentError("report layout sections are invalid")
    matches = [
        section
        for section in sections
        if isinstance(section, dict)
        and (
            section.get("displayName") == FORECAST_PAGE_DISPLAY_NAME
            or section.get("name") == FORECAST_PAGE_NAME
        )
    ]
    if len(matches) != 1 or not _forecast_pages_equal(
        matches[0],
        _forecast_page(),
    ):
        raise ReportingDeploymentError("report Forecast page validation failed")


def rewrite_model_definition(
    source: Mapping[str, Any],
    *,
    target_name: str = TARGET_MODEL_NAME,
) -> dict[str, Any]:
    result = copy.deepcopy(dict(source))
    parts = _parts(result)
    observed: set[str] = set()
    for part in parts:
        if not str(part["path"]).endswith(".tmdl"):
            continue
        text = _decoded_part(part).decode("utf-8")
        for legacy, stable in TABLE_BINDINGS.items():
            lineage = f"[dbo].[{legacy}]"
            entity = f"entityName: {legacy}"
            if lineage in text or entity in text:
                observed.add(legacy)
                text = text.replace(lineage, f"[dbo].[{stable}]")
                text = text.replace(entity, f"entityName: {stable}")
        _replace_part(part, text.encode("utf-8"))
    if observed != set(TABLE_BINDINGS):
        missing = sorted(set(TABLE_BINDINGS) - observed)
        raise ReportingDeploymentError(
            f"semantic model is missing expected physical bindings: {missing!r}"
        )
    model_matches = [
        part for part in parts if part["path"] == "definition/model.tmdl"
    ]
    if len(model_matches) != 1:
        raise ReportingDeploymentError(
            "definition must contain one definition/model.tmdl part"
        )
    model_text = _decoded_part(model_matches[0]).decode("utf-8")
    forecast_parts = _forecast_model_parts()
    existing_paths = {str(part["path"]) for part in parts}
    conflicts = sorted(existing_paths.intersection(forecast_parts))
    if conflicts:
        raise ReportingDeploymentError(
            f"semantic model already contains Forecast artifacts: {conflicts!r}"
        )
    for table in (FORECAST_WORKLOAD_TABLE, FORECAST_CAPACITY_TABLE):
        model_text += f"\nref table '{table}'\n"
    _replace_part(model_matches[0], model_text.encode("utf-8"))
    definition = result.get("definition")
    if not isinstance(definition, dict) or not isinstance(
        definition.get("parts"),
        list,
    ):
        raise ReportingDeploymentError("Fabric definition envelope is invalid")
    for path, content in forecast_parts.items():
        definition["parts"].append(
            {
                "path": path,
                "payload": base64.b64encode(content.encode("utf-8")).decode(
                    "ascii"
                ),
                "payloadType": "InlineBase64",
            }
        )
    _set_platform_name(parts, target_name)
    validate_model_definition(result)
    return result


def _validate_stable_bindings(text: str) -> None:
    for legacy, stable in TABLE_BINDINGS.items():
        if (
            f"[dbo].[{legacy}]" in text
            or f"entityName: {legacy}" in text
            or f"[dbo].[{stable}]" not in text
            or f"entityName: {stable}" not in text
        ):
            raise ReportingDeploymentError(
                f"semantic model binding validation failed for {legacy}"
            )


def _validate_forecast_model_parts(
    parts: Sequence[Mapping[str, Any]],
) -> None:
    expected_paths = _forecast_model_parts()
    observed = {
        str(part["path"]): _decoded_part(part).decode("utf-8")
        for part in parts
        if str(part["path"]) in expected_paths
    }
    if set(observed) != set(expected_paths):
        raise ReportingDeploymentError(
            "semantic model Forecast tables are missing"
        )
    model = next(
        (
            _decoded_part(part).decode("utf-8")
            for part in parts
            if part["path"] == "definition/model.tmdl"
        ),
        "",
    )
    required_markers = {
        "workload default": (
            f"SELECTEDVALUE('{FORECAST_WORKLOAD_TABLE}'[Video Hours], "
            f"{FORECAST_VIDEO_HOURS})"
        ),
        "workload range": (
            f"GENERATESERIES({FORECAST_MIN_VIDEO_HOURS}, "
            f"{FORECAST_MAX_VIDEO_HOURS}, "
            f"{FORECAST_VIDEO_HOURS_INCREMENT})"
        ),
        "baseline throughput": f"{FORECAST_BASELINE_THROUGHPUT:.4f}",
        "completion measure": "'Estimated Completion Days'",
    }
    forecast_text = "\n".join(observed.values())
    for label, marker in required_markers.items():
        if marker not in forecast_text:
            raise ReportingDeploymentError(
                f"semantic model Forecast {label} is invalid"
            )
    if any(
        f'{{"F{capacity}", {capacity}}}' not in forecast_text
        for capacity in FORECAST_CAPACITIES
    ):
        raise ReportingDeploymentError(
            "semantic model Forecast capacity rows are invalid"
        )
    for table in (FORECAST_WORKLOAD_TABLE, FORECAST_CAPACITY_TABLE):
        if f"ref table '{table}'" not in model:
            raise ReportingDeploymentError(
                f"semantic model Forecast ref is missing for {table}"
            )


def validate_model_definition(value: Mapping[str, Any]) -> None:
    parts = _parts(value)
    text = "\n".join(
        _decoded_part(part).decode("utf-8")
        for part in parts
        if str(part["path"]).endswith(".tmdl")
    )
    _validate_stable_bindings(text)
    _validate_forecast_model_parts(parts)


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
        raise ReportingDeploymentError(
            "report definition must contain one definition.pbir part"
        )
    try:
        report = json.loads(_decoded_part(matches[0]))
        connection = report["datasetReference"]["byConnection"][
            "connectionString"
        ]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ReportingDeploymentError("report dataset reference is invalid") from error
    if not isinstance(connection, str):
        raise ReportingDeploymentError("report connection string is invalid")
    source_catalog = f"initial catalog={SOURCE_MODEL['name']}"
    source_id = f"semanticmodelid={source_model_id}"
    if connection.count(source_catalog) != 1 or connection.count(source_id) != 1:
        raise ReportingDeploymentError(
            "report does not reference the expected source semantic model"
        )
    connection = connection.replace(
        source_catalog, f"initial catalog={target_model_name}"
    ).replace(source_id, f"semanticmodelid={target_model_id}")
    report["datasetReference"]["byConnection"]["connectionString"] = connection
    _replace_part(matches[0], canonical_bytes(report))
    layout_part, layout = _json_part(
        parts,
        "report.json",
        "report layout",
    )
    _add_forecast_page(layout)
    _replace_part(layout_part, canonical_bytes(layout))
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
    part = next(
        (
            item
            for item in _parts(value)
            if item["path"] == "definition.pbir"
        ),
        None,
    )
    if part is None:
        raise ReportingDeploymentError("report definition.pbir is absent")
    report = json.loads(_decoded_part(part))
    connection = report["datasetReference"]["byConnection"]["connectionString"]
    if (
        f"initial catalog={target_model_name}" not in connection
        or f"semanticmodelid={target_model_id}" not in connection
        or f"semanticmodelid={SOURCE_MODEL['id']}" in connection
    ):
        raise ReportingDeploymentError("report target model binding is invalid")
    _, layout = _json_part(
        _parts(value),
        "report.json",
        "report layout",
    )
    _validate_forecast_page(layout)


def _get_definition(
    client: FabricRESTController,
    root: str,
    item_id: str,
) -> dict[str, Any]:
    return dict(
        client._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/{root}/{item_id}/getDefinition",
            accepted=(200, 202),
        )
    )


def _target_item(
    client: FabricRESTController,
    item_type: str,
    display_name: str,
) -> dict[str, Any] | None:
    matches = [
        dict(item)
        for item in client.list_items()
        if item.get("type") == item_type
        and item.get("displayName") == display_name
    ]
    if len(matches) > 1:
        raise ReportingDeploymentError(
            f"multiple {item_type} items named {display_name}"
        )
    return matches[0] if matches else None


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
            raise ReportingDeploymentError(
                f"created {item_type} {display_name} is not discoverable"
            )
    elif item.get("description") != TARGET_DESCRIPTION:
        raise ReportingDeploymentError(
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
    if definition_sha256(source_model) != SOURCE_MODEL["sha256"]:
        raise ReportingDeploymentError("source semantic model definition changed")
    if definition_sha256(source_report) != SOURCE_REPORT["sha256"]:
        raise ReportingDeploymentError("source report definition changed")

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
        "report": {
            "definition_sha256": definition_sha256(report_readback),
            "id": str(report["id"]),
            "name": TARGET_REPORT_NAME,
        },
        "schema": "people-counter-sjd-analytics-deployment-v1",
        "source_model_definition_sha256": SOURCE_MODEL["sha256"],
        "source_report_definition_sha256": SOURCE_REPORT["sha256"],
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

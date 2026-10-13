from __future__ import annotations

import base64
from pathlib import Path

from people_counter.fabric_sjd import ENVIRONMENT_ID, LAKEHOUSE_ID
from people_counter.fabric_sjd_definition import ENTRY_POINTS, build_sjd_definition


SJD_ROOT = (
    Path(__file__).resolve().parent.parent / "fabric" / "spark-job-definitions"
)


def expected_parts(role: str) -> dict[str, bytes]:
    parts = build_sjd_definition(role)["definition"]["parts"]
    return {
        part["path"]: base64.b64decode(part["payload"])
        for part in parts
    }


def test_canonical_entry_points_match_generated_definitions() -> None:
    for role in ENTRY_POINTS:
        assert (
            SJD_ROOT / role / "Main" / "main.py"
        ).read_bytes() == expected_parts(role)["Main/main.py"]


def test_metadata_template_matches_generated_definition() -> None:
    template = (
        SJD_ROOT / "common" / "SparkJobDefinitionV1.json.tmpl"
    ).read_bytes().rstrip(b"\n")
    rendered = template.replace(
        b"{{ .DefaultLakehouseArtifactId }}",
        LAKEHOUSE_ID.encode(),
    ).replace(
        b"{{ .EnvironmentArtifactId }}",
        ENVIRONMENT_ID.encode(),
    )

    for role in ENTRY_POINTS:
        assert rendered == expected_parts(role)["SparkJobDefinitionV1.json"]

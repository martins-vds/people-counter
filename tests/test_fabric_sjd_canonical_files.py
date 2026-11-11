from __future__ import annotations

import base64
import json
from pathlib import Path
import pytest
from people_counter.fabric_sjd import FabricSjdConfig
from people_counter.fabric_sjd_definition import ENTRY_POINTS, build_sjd_definition

from tests.fabric_fixtures import ENVIRONMENT_ID, LAKEHOUSE_ID, TEST_TARGET


SJD_ROOT = (
    Path(__file__).resolve().parent.parent / "fabric" / "spark-job-definitions"
)


def expected_parts(role: str) -> dict[str, bytes]:
    parts = build_sjd_definition(role, config=TEST_TARGET)["definition"]["parts"]
    return {
        part["path"]: base64.b64decode(part["payload"])
        for part in parts
    }


def test_canonical_entry_points_match_generated_definitions() -> None:
    for role in ENTRY_POINTS:
        assert (
            SJD_ROOT / role / "Main" / "main.py"
        ).read_bytes() == expected_parts(role)["Main/main.py"]


@pytest.mark.parametrize("files_root", [
    "Files/people-counter/sjd/v1/", "Files/people-counter/dev/v1/",
    "Files/people-counter/test/v1/",
])
def test_metadata_template_matches_generated_definition(files_root: str) -> None:
    config = FabricSjdConfig(
        workspace_id=TEST_TARGET.workspace_id, lakehouse_id=TEST_TARGET.lakehouse_id,
        environment_id=TEST_TARGET.environment_id, files_root=files_root,
    )
    import shlex
    arguments = shlex.join(config.namespace_arguments())
    template = (
        SJD_ROOT / "common" / "SparkJobDefinitionV1.json.tmpl"
    ).read_bytes().rstrip(b"\n")
    rendered = template.replace(
        b"{{ .DefaultLakehouseArtifactId }}",
        LAKEHOUSE_ID.encode(),
    ).replace(
        b"{{ .EnvironmentArtifactId }}",
        ENVIRONMENT_ID.encode(),
    ).replace(
        b"{{ .CommandLineArguments }}", arguments.encode(),
    )

    for role in ENTRY_POINTS:
        parts = build_sjd_definition(role, config=config, command_line_arguments=arguments)["definition"]["parts"]
        metadata = next(part for part in parts if part["path"] == "SparkJobDefinitionV1.json")
        assert json.loads(rendered) == json.loads(base64.b64decode(metadata["payload"]))

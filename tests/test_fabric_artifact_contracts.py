import base64
import copy
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest

from people_counter import fabric_benchmark_jobs as benchmark
from people_counter import fabric_namespace as namespace
from people_counter import fabric_sjd_definition as stable
from scripts import deploy_sjd_analytics as analytics
from scripts import prepare_manual_fabric_assets as prepare
from tests.fabric_fixtures import ROLE_IDS, TEST_TARGET
from tests.test_fabric_operations_portable import manifest


def canonical(value):
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


@pytest.mark.parametrize("module,entry_module,description", [
    (stable, "people_counter.fabric_sjd_jobs", "Generated stable production SJD entry point."),
    (namespace, "people_counter.fabric_sjd_runtime", "Generated Fabric SJD Fabric SJD entry point."),
    (benchmark, "people_counter.fabric_benchmark_jobs", "Generated Fabric SJD benchmark SJD entry point."),
])
@pytest.mark.parametrize("role", ["control", "process", "gold"])
@pytest.mark.parametrize("arguments", ["", "--owner opérateur --files-root Files/chosen/"])
def test_sjd_artifact_bytes_and_hash_inputs_are_canonical(
    module, entry_module, description, role, arguments,
):
    build = module.build_sjd_definition if module is stable else module.build_sjd_v2_definition
    expected_source = (
        f'"""{description}"""\n\n'
        f"from {entry_module} import {role}_main as main\n\n"
        'if __name__ == "__main__":\n'
        "    raise SystemExit(main())\n"
    ).encode()
    expected_metadata = canonical({
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": arguments,
        "defaultLakehouseArtifactId": TEST_TARGET.lakehouse_id,
        "environmentArtifactId": TEST_TARGET.environment_id,
        "executableFile": "main.py",
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    })
    actual = build(role, config=TEST_TARGET, command_line_arguments=arguments)
    assert actual == {"definition": {
        "format": "SparkJobDefinitionV2",
        "parts": [
            {"path": "Main/main.py", "payload": base64.b64encode(expected_source).decode(),
             "payloadType": "InlineBase64"},
            {"path": "SparkJobDefinitionV1.json", "payload": base64.b64encode(expected_metadata).decode(),
             "payloadType": "InlineBase64"},
        ],
    }}


@pytest.mark.parametrize("module", [stable, namespace, benchmark])
def test_invalid_definition_arguments_have_an_explicit_nul_diagnostic(module):
    build = module.build_sjd_definition if module is stable else module.build_sjd_v2_definition
    with pytest.raises(ValueError) as error:
        build("control", config=TEST_TARGET, command_line_arguments="\x00")
    assert str(error.value) == "command_line_arguments contains NUL"


def test_benchmark_export_is_a_complete_canonical_content_addressed_inventory(tmp_path):
    output = tmp_path / "nested" / "exports"
    arguments = {"control": "--owner chosen", "process": "--batch-id batch", "gold": "run"}
    expected_definitions = {
        role: benchmark.build_sjd_v2_definition(role, config=TEST_TARGET, command_line_arguments=value)
        for role, value in arguments.items()
    }
    original = copy.deepcopy(arguments)
    result = benchmark.export_sjd_definitions(
        output, config=TEST_TARGET, command_line_arguments=arguments,
    )
    assert arguments == original
    assert benchmark.build_all_sjd_v2_definitions(arguments, config=TEST_TARGET) == expected_definitions
    assert result == {
        role: {
            "path": str(output / f"{role}.SparkJobDefinitionV2.json"),
            "sha256": hashlib.sha256(canonical(definition) + b"\n").hexdigest(),
        }
        for role, definition in expected_definitions.items()
    }
    for role, definition in expected_definitions.items():
        assert Path(result[role]["path"]).read_bytes() == canonical(definition) + b"\n"
    assert benchmark.export_sjd_definitions(
        str(output), config=TEST_TARGET, command_line_arguments=arguments,
    ) == result


def test_benchmark_default_arguments_are_empty_for_every_exported_role(tmp_path):
    definitions = benchmark.build_all_sjd_v2_definitions(config=TEST_TARGET)
    assert set(definitions) == {"control", "process", "gold"}
    for role, value in definitions.items():
        assert value == benchmark.build_sjd_v2_definition(role, config=TEST_TARGET)
    assert set(benchmark.export_sjd_definitions(tmp_path, config=TEST_TARGET)) == set(definitions)


def test_benchmark_export_refuses_changed_files_and_symlink_paths(tmp_path):
    root = tmp_path / "exports"
    root.mkdir()
    path = root / "control.SparkJobDefinitionV2.json"
    path.write_bytes(b"older")
    with pytest.raises(FileExistsError) as error:
        benchmark.export_sjd_definitions(root, config=TEST_TARGET)
    assert str(error.value) == f"refusing to replace differing export {path}"
    assert path.read_bytes() == b"older"
    path.unlink()
    other = tmp_path / "other.json"
    other.write_bytes(b"preserve")
    path.symlink_to(other)
    with pytest.raises(ValueError) as error:
        benchmark.export_sjd_definitions(root, config=TEST_TARGET)
    assert str(error.value) == "refusing to overwrite a symlink"
    assert other.read_bytes() == b"preserve"


def test_benchmark_unknown_jobs_are_sorted_and_named_in_the_error():
    with pytest.raises(ValueError) as error:
        benchmark.build_all_sjd_v2_definitions({"zebra": "", "other": ""}, config=TEST_TARGET)
    assert str(error.value) == "unknown benchmark jobs: ['other', 'zebra']"


def test_prepared_administrative_artifact_has_exact_canonical_bytes(tmp_path):
    root = Path(__file__).resolve().parents[1]
    output = tmp_path / "nested" / "prepared.json"
    digest = prepare.prepare_assets(root, output)
    document = json.loads(output.read_bytes())
    expected = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    assert output.read_bytes() == expected
    assert digest == hashlib.sha256(expected).hexdigest()
    for stage, template in document["deploymentTemplates"].items():
        assert template == json.loads((root / f"src/people_counter/data/deploy/{stage}.json").read_bytes())


def test_reporting_evidence_persistence_is_utf8_sorted_and_newline_terminated(monkeypatch, tmp_path):
    selected = manifest()
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [{"displayName": "chosen-workspace"}, {"displayName": "chosen_lakehouse"}]
    monkeypatch.setattr(analytics, "ensure_item", Mock(side_effect=[
        ROLE_IDS["control"], ROLE_IDS["gold"],
    ]))
    output = tmp_path / "nested" / "reports" / "evidence.json"
    result = analytics.deploy(output, manifest=selected, client=client)
    assert output.read_bytes() == (json.dumps(result, indent=2, sort_keys=True) + "\n").encode()


def test_benchmark_export_rejects_a_file_or_symlink_as_the_destination(tmp_path):
    destination = tmp_path / "not-a-directory"
    destination.write_text("preserve")
    with pytest.raises(ValueError) as error:
        benchmark.export_sjd_definitions(destination, config=TEST_TARGET)
    assert str(error.value) == "export destination must be a real directory"
    assert destination.read_text() == "preserve"
    actual = tmp_path / "actual"
    actual.mkdir()
    link = tmp_path / "link"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError) as error:
        benchmark.export_sjd_definitions(link, config=TEST_TARGET)
    assert str(error.value) == "export destination cannot traverse symlinks"
    assert list(actual.iterdir()) == []


def test_preparation_reports_the_correct_checked_source_when_compilation_fails(monkeypatch, tmp_path):
    source = tmp_path / "src/people_counter"
    source.mkdir(parents=True)
    client = source / "fabric_rest.py"
    publisher = source / "manual_fabric_publication.py"
    monkeypatch.setattr(prepare, "read_definition_bundle", Mock(return_value={"definition": {}}))
    for bad_path, expected_name in (
        (client, "fabric_rest.py"), (publisher, "manual_fabric_publication.py"),
    ):
        client.write_text("VALUE = 1\n")
        publisher.write_text("VALUE = 1\n")
        bad_path.write_text("def incomplete(\n")
        with pytest.raises(SyntaxError) as error:
            prepare.prepare_assets(tmp_path, tmp_path / "output.json")
        assert error.value.filename == expected_name
        assert not (tmp_path / "output.json").exists()


def test_preparation_creates_more_than_one_missing_parent_directory(tmp_path):
    root = Path(__file__).resolve().parents[1]
    output = tmp_path / "missing" / "nested" / "prepared.json"
    digest = prepare.prepare_assets(root, output)
    assert digest == hashlib.sha256(output.read_bytes()).hexdigest()


def test_namespace_definition_default_arguments_are_empty_and_unknown_jobs_are_named():
    value = namespace.build_sjd_v2_definition("control", config=TEST_TARGET)
    metadata = json.loads(base64.b64decode(value["definition"]["parts"][1]["payload"]))
    assert metadata["commandLineArguments"] == ""
    with pytest.raises(ValueError) as error:
        namespace.thin_main_source("unknown")
    assert str(error.value) == "unsupported Fabric SJD job 'unknown'"


def test_reporting_loads_artifacts_from_its_own_repository_not_a_parent_checkout(monkeypatch, tmp_path):
    selected = manifest()
    client = Mock(workspace_id=selected.workspace_id)
    client._json.side_effect = [{"displayName": "workspace"}, {"displayName": "lakehouse"}]
    monkeypatch.setattr(analytics, "ensure_item", Mock(side_effect=[ROLE_IDS["control"], ROLE_IDS["gold"]]))
    with patch.object(
        analytics, "read_definition_bundle", wraps=analytics.read_definition_bundle,
    ) as reader:
        analytics.deploy(tmp_path / "evidence.json", manifest=selected, client=client)
    root = Path(analytics.__file__).resolve().parents[1] / "fabric"
    assert reader.call_args_list == [
        call(root / "semantic-models/analytics"),
        call(root / "reports/analytics"),
    ]


def test_preparation_cli_help_describes_its_checked_browser_only_publication_purpose(monkeypatch, capsys):
    import sys

    monkeypatch.setattr(sys, "argv", ["prepare", "--help"])
    with pytest.raises(SystemExit) as error:
        prepare.main()
    assert error.value.code == 0
    assert "Prepare checked repository definitions for browser-only publication." in capsys.readouterr().out


def test_preparation_cli_next_step_names_the_ui_and_does_not_claim_a_deployment(monkeypatch, tmp_path, capsys):
    import sys

    output = tmp_path / "prepared.json"
    monkeypatch.setattr(sys, "argv", ["prepare", "--output", str(output)])
    monkeypatch.setattr(prepare, "prepare_assets", Mock(return_value="a" * 64))
    prepare.main()
    assert capsys.readouterr().out.splitlines() == [
        f"Prepared {output.resolve()}", "SHA256 " + "a" * 64,
        "Upload through the Lakehouse UI. This command makes no cloud requests.",
    ]

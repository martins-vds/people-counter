import ast
import hashlib
import json
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from people_counter import backfill_operator as operator
from people_counter.inference_profiles import (
    artifact_paths, configuration_sha256, resolve_profile,
)
from people_counter.manifest import ManifestPublisherError, manifest_bytes
from people_counter.manifest_publisher import (
    PublisherConfig,
    run_publisher,
    validate_manifest_package,
)
from tests.test_manifest_publisher import FakeStorage, inspection, write_catalog


@pytest.fixture(autouse=True)
def forbid_live_cloud_clients(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Tests must not construct live cloud clients")

    monkeypatch.setattr(operator.OneLakeFiles, "__init__", forbidden)
    monkeypatch.setattr(operator.AzureDataLakeStorage, "__init__", forbidden)
    monkeypatch.setattr(
        operator.time, "sleep",
        lambda seconds: pytest.fail("Tests must inject any expected polling delay"),
    )


@pytest.fixture
def setup(tmp_path):
    video = tmp_path / "north/camera-17/20260101T120000Z.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"video")
    catalog = tmp_path / "catalog.csv"
    write_catalog(catalog)
    package = tmp_path / "package"
    run_publisher(
        PublisherConfig(
            catalog, None, tmp_path, None, "account", "footage",
            "staging", "incoming", tmp_path / "prepare.sqlite3",
            tmp_path / "reject.csv", tmp_path / "inventory.csv",
            10, True, output_dir=package, summary_report_path=tmp_path / "summary.json",
        ),
        storage=None,
        inspector=inspection,
    )
    config = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "environment_id": "33333333-3333-3333-3333-333333333333",
        "control_sjd_id": "44444444-4444-4444-4444-444444444444",
        "intake_mode": "explicit",
        "storage_account": "account",
        "filesystem": "footage",
        "shortcut_path": "Files/videos",
        "profile": resolve_profile(
            "rtdetr-r18-onnx-cpu",
            {path: "a" * 64 for path in artifact_paths("rtdetr-r18-onnx-cpu")},
        ),
    }
    config_path = tmp_path / "target.json"
    config_path.write_text(json.dumps(config))
    args = [
        "--manifest-package-dir", str(package),
        "--video-root", str(tmp_path),
        "--config", str(config_path),
        "--state-dir", str(tmp_path / "state"),
    ]
    return SimpleNamespace(
        root=tmp_path, package=package, config=config, config_path=config_path,
        args=args,
    )


@pytest.mark.parametrize("flags", [[], ["--wait"]])
def test_preview_is_offline_and_preserves_package(setup, capsys, flags):
    before = {p: p.read_bytes() for p in setup.package.rglob("*") if p.is_file()}
    with patch.object(operator, "AzureDataLakeStorage") as storage, patch.object(
        operator, "FabricAutomationClient"
    ) as fabric:
        assert operator.main(setup.args + flags) == 0
    storage.assert_not_called()
    fabric.assert_not_called()
    preview = json.loads(capsys.readouterr().out)
    assert preview["status"] == "preview"
    assert preview["item_count"] == 1
    assert preview["partition_count"] == 1
    assert type(preview["partition_count"]) is int
    assert preview["workspace_id"] == setup.config["workspace_id"]
    assert preview["lakehouse_id"] == setup.config["lakehouse_id"]
    assert preview["control_sjd_id"] == setup.config["control_sjd_id"]
    assert preview["destination"] == "abfss://footage@account.dfs.core.windows.net/incoming/"
    assert preview["profile_id"] == "rtdetr-r18-onnx-cpu"
    assert preview["package_sha256"] == hashlib.sha256(
        (setup.package / "manifest-package.json").read_bytes()
    ).hexdigest()
    assert "--submit" in preview["next_step"]
    assert not (setup.root / "state").exists()
    assert before == {p: p.read_bytes() for p in setup.package.rglob("*") if p.is_file()}


@pytest.mark.parametrize("change", [
    {"intake_mode": "arrival"},
    {"workspace_id": "../other-workspace"},
    {"workspace_id": None},
    {"storage_account": "https://evil.example"},
    {"filesystem": "../other"},
    {"shortcut_path": "Files/../outside"},
    {"shortcut_path": "/lakehouse/default/Files/videos"},
    {"shortcut_path": None},
    {"config_sha256": "not-a-digest"},
    {"table_directory": "Tables/../outside"},
    {"table_directory": None},
    {"profile": {"source_video": "/unreviewed"}},
    {"unexpected_option": True},
])
def test_invalid_config_fails_before_network(setup, change):
    setup.config_path.write_text(json.dumps({**setup.config, **change}))
    with patch.object(operator, "AzureDataLakeStorage") as storage, pytest.raises(
        (ValueError, ManifestPublisherError)
    ):
        operator.main(setup.args + ["--submit"])
    storage.assert_not_called()


@pytest.mark.parametrize("change", [
    {"source_video": "/unreviewed"},
    {"runtime_key": ""},
    {"batch_size": 0},
    {"batch_size": True},
    {"sample_fps": 0},
    {"sample_fps": float("inf")},
    {"sample_fps": True},
    {"model_artifact_sha256": {}},
    {"model_artifact_sha256": {"model.onnx": "bad-hash"}},
    {"model_artifact_sha256": {"../model.onnx": "a" * 64}},
])
def test_invalid_profile_fails_before_network(setup, change):
    profile = {**setup.config["profile"], **change}
    setup.config_path.write_text(json.dumps({**setup.config, "profile": profile}))
    with patch.object(operator, "OneLakeFiles") as files, pytest.raises(
        (ValueError, ManifestPublisherError)
    ):
        operator.main(setup.args + ["--submit"])
    files.assert_not_called()


def test_state_directory_cannot_be_inside_package(setup):
    args = setup.args[:-2] + ["--state-dir", str(setup.package / "state")]
    with pytest.raises(ValueError, match="outside"):
        operator.main(args)


@pytest.mark.parametrize("config", [[], {"intake_mode": "explicit"}])
def test_missing_configuration_fields_fail_before_cloud(setup, config):
    setup.config_path.write_text(json.dumps(config))
    with patch.object(operator, "OneLakeFiles") as files, pytest.raises(ValueError):
        operator.main(setup.args + ["--submit", "--wait"])
    files.assert_not_called()


def test_records_preserve_metadata_and_have_stable_configuration_bound_ids(setup):
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(
        setup.package, setup.root, setup.root / "state", config
    )
    validated = validate_manifest_package(publication)
    records = operator.registration_records(validated, config, "d" * 64)
    assert records == operator.registration_records(validated, config, "d" * 64)
    plan = validated.plans[0]
    record = records[0]
    assert record["work_id"].startswith("backfill-")
    assert len(record["work_id"]) == len("backfill-") + 64
    assert record["max_attempts"] == 3
    payload = record["payload"]
    assert payload["source_video"] == (
        f"/lakehouse/default/Files/videos/{plan.incoming_video_path}"
    )
    assert payload["source_sha256"] == plan.inspection.sha256
    assert payload["line"] == [0, 540, 1919, 540]
    assert payload["camera_id"] == "camera-17"
    assert payload["location_id"] == "north-entrance"
    assert payload["camera_timezone"] == "America/Denver"
    assert payload["captured_at_utc"] == "2026-01-01T12:00:00Z"
    assert record["duration_seconds"] == plan.inspection.duration_seconds
    assert record["config_sha256"] == configuration_sha256(payload)
    assert record["release_digest"] == "d" * 64
    assert record["work_id"] != operator.registration_records(
        validated, config, "e" * 64
    )[0]["work_id"]
    changed = replace(config, profile=resolve_profile(
        config.profile["profile_id"], config.profile["model_artifact_sha256"],
        overrides={"sample_fps": 2.0},
    ))
    assert record["work_id"] != operator.registration_records(
        validated, changed, "d" * 64
    )[0]["work_id"]
    second = replace(
        plan, relative_path="z.mp4", asset_id="b" * 64,
        incoming_video_path="incoming/z.mp4",
    )
    ordered = operator.registration_records(
        replace(validated, plans=(second, plan)), config, "d" * 64
    )
    assert ordered[0] == record
    assert ordered[1]["payload"]["source_video"].endswith("/incoming/z.mp4")


def cloud_mocks(setup):
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(
        setup.package, setup.root, setup.root / "state", config
    )
    plan = validate_manifest_package(publication).plans[0]
    storage = FakeStorage()
    files = Mock()
    files.read_bytes.return_value = manifest_bytes(
        plan, storage_account="account", filesystem="footage",
        source_etag='"video-etag"',
    )
    release = SimpleNamespace(
        digest="d" * 64,
        package_version="1.2.3",
    )
    return storage, files, release


def publication_shortcut(setup):
    parent, name = setup.config["shortcut_path"].rsplit("/", 1)
    return {
        "path": f"/{parent}", "name": name,
        "target": {"adlsGen2": {
            "location": f"https://{setup.config['storage_account']}.dfs.core.windows.net",
            "subpath": f"/{setup.config['filesystem']}",
        }},
    }


@pytest.mark.parametrize("target", [
    {"adlsGen2": {"location": "https://other.dfs.core.windows.net", "subpath": "/footage"}},
    {"adlsGen2": {"location": "https://account.dfs.core.windows.net", "subpath": "/other"}},
    {"adlsGen2": {"location": "https://account.dfs.core.windows.net", "subpath": "/footage/incoming"}},
    {"oneLake": {"path": "Files/videos"}},
    {"adlsGen2": None},
    {"adlsGen2": {"location": None, "subpath": "/footage"}},
    {"adlsGen2": {"location": "https://account.dfs.core.windows.net", "subpath": None}},
    None,
])
def test_shortcut_target_mismatch_stops_before_publication(setup, target):
    storage, files, _ = cloud_mocks(setup)
    client = Mock()
    shortcut = publication_shortcut(setup)
    shortcut["target"] = target
    client.list_pages.return_value = [shortcut]
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "FabricAutomationClient", return_value=client,
    ), patch.object(operator, "AzureDataLakeStorage") as adls, patch.object(
        operator, "publish_manifest_package",
    ) as publish, pytest.raises(ValueError, match="shortcut.*container root") as raised:
        operator.main(setup.args + ["--submit"])
    assert "Files/videos" in str(raised.value)
    assert "https://account.dfs.core.windows.net/footage" in str(raised.value)
    adls.assert_not_called()
    publish.assert_not_called()
    files.write_partition.assert_not_called()
    client.request_response.assert_not_called()
    assert not (setup.root / "state").exists()


@pytest.mark.parametrize("case", ["missing", "duplicate", "wrong-path", "wrong-name"])
def test_shortcut_must_resolve_unambiguously_in_the_target_lakehouse(setup, case):
    shortcut = publication_shortcut(setup)
    matches = [shortcut]
    if case == "missing":
        matches = []
    elif case == "duplicate":
        matches = [shortcut, shortcut]
    elif case == "wrong-path":
        shortcut["path"] = "/Files/other"
    else:
        shortcut["name"] = "other"
    client = Mock()
    client.list_pages.return_value = matches
    config = operator.load_config(setup.config_path)
    with pytest.raises(ValueError, match="exactly one.*Files/videos"):
        operator.verify_publication_shortcut(client, config)
    client.list_pages.assert_called_once_with(
        f"/workspaces/{config.workspace_id}/items/{config.lakehouse_id}/shortcuts"
    )
    client.request_response.assert_not_called()


@pytest.mark.parametrize("parent,location,subpath", [
    ("/Files", "https://account.dfs.core.windows.net", "/footage"),
    ("Files", "https://account.dfs.core.windows.net/", "footage/"),
])
def test_shortcut_root_validation_accepts_api_path_slash_variants(
    setup, parent, location, subpath,
):
    shortcut = publication_shortcut(setup)
    shortcut["path"] = parent
    shortcut["target"]["adlsGen2"].update(location=location, subpath=subpath)
    client = Mock()
    client.list_pages.return_value = [shortcut]
    operator.verify_publication_shortcut(client, operator.load_config(setup.config_path))


def test_shortcut_lookup_failure_preserves_cause_and_never_publishes(setup):
    from people_counter.fabric_automation import FabricApiError

    _, files, _ = cloud_mocks(setup)
    client = Mock()
    error = FabricApiError("Fabric GET shortcuts returned HTTP 403")
    client.list_pages.side_effect = error
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "FabricAutomationClient", return_value=client,
    ), patch.object(operator, "AzureDataLakeStorage") as adls, pytest.raises(
        FabricApiError, match="403",
    ) as raised:
        operator.main(setup.args + ["--submit"])
    assert raised.value is error
    adls.assert_not_called()
    files.write_partition.assert_not_called()
    client.request_response.assert_not_called()
    assert not (setup.root / "state").exists()


def test_shortcut_lookup_follows_pages_and_uses_the_exact_nested_path(setup):
    from people_counter.fabric_automation import HTTPResponse
    from tests.test_fabric_automation import _TokenProvider, _Transport

    setup.config["shortcut_path"] = "Files/source videos/selected"
    setup.config_path.write_text(json.dumps(setup.config))
    config = operator.load_config(setup.config_path)
    shortcut = publication_shortcut(setup)
    unrelated = {**shortcut, "path": "/Files/other"}
    continuation = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{config.workspace_id}"
        f"/items/{config.lakehouse_id}/shortcuts?continuationToken=next"
    )
    transport = _Transport([
        HTTPResponse(200, {}, json.dumps({
            "value": [unrelated], "continuationUri": continuation,
        }).encode()),
        HTTPResponse(200, {}, json.dumps({"value": [shortcut]}).encode()),
    ])
    client = operator.FabricAutomationClient(_TokenProvider(), transport=transport)
    operator.verify_publication_shortcut(client, config)
    assert [request["method"] for request in transport.requests] == ["GET", "GET"]
    assert transport.requests[0]["url"] == (
        f"https://api.fabric.microsoft.com/v1/workspaces/{config.workspace_id}"
        f"/items/{config.lakehouse_id}/shortcuts"
    )
    assert transport.requests[1]["url"] == continuation


def test_submit_publishes_verifies_then_registers_and_resumes(setup, capsys):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (
        202, {"Location": "https://api.fabric.microsoft.com/v1/jobs/job-1"}, b""
    )
    fabric.request_json.return_value = {"id": "job-1", "status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage) as storage_client, patch.object(
        operator, "OneLakeFiles", return_value=files
    ) as onelake_client, patch.object(
        operator, "installed_release", return_value=release
    ) as resolver, patch.object(operator, "FabricAutomationClient", return_value=fabric) as client:
        assert operator.main(setup.args + ["--submit"]) == 0
        assert operator.main(setup.args + ["--submit"]) == 0
    assert fabric.request_response.call_count == 1
    call = fabric.request_response.call_args
    assert "sparkJobDefinitions/44444444-4444-4444-4444-444444444444" in call.args[1]
    command = call.kwargs["value"]["executionData"]["commandLineArguments"]
    assert "--artifacts" in command
    assert "bulk-register --partition-path Files/people-counter/sjd/v1/intake/backfill/" in command
    assert "--max-items 1" in command
    content = files.write_partition.call_args.args[1]
    assert hashlib.sha256(content).hexdigest() in command
    record = json.loads(content)["items"][0]
    assert record["payload"]["source_sha256"] == hashlib.sha256(b"video").hexdigest()
    report = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert report["status"] == "registered"
    assert report["item_count"] == 1
    assert report["partition_count"] == 1
    assert report["processing_complete"] is False
    assert json.loads((setup.root / "state/registration-summary.json").read_text()) == report
    config = operator.load_config(setup.config_path)
    onelake_client.assert_called_with(config)
    resolver.assert_called_with()
    files.read_text.assert_not_called()
    storage_client.assert_called_with("account", "footage", chunk_size=8 * 1024 * 1024)
    assert fabric.list_pages.call_count == 2
    fabric.list_pages.assert_called_with(
        f"/workspaces/{config.workspace_id}/items/{config.lakehouse_id}/shortcuts"
    )
    assert client.call_args.args[0] == operator.AzureCliResourceTokenProvider(
        "https://api.fabric.microsoft.com",
    )
    assert isinstance(client.call_args.kwargs["transport"], operator.UrllibTransport)
    assert (setup.root / "state/publication.sqlite3").is_file()
    plan = json.loads((setup.root / "state/registration-plan.json").read_text())
    assert plan["target"] == {
        name: setup.config[name] for name in (
            "workspace_id", "lakehouse_id", "environment_id", "control_sjd_id",
            "storage_account", "filesystem",
        )
    }
    assert plan["package_sha256"] == hashlib.sha256(
        (setup.package / "manifest-package.json").read_bytes()
    ).hexdigest()
    for path in setup.config["profile"]["model_artifact_sha256"]:
        files.verify_video.assert_any_call(f"Files/models/{path}", None, "a" * 64)
    evidence = json.loads((setup.root / "state/publication-evidence.json").read_text())
    assert evidence[0]["work_id"] == record["work_id"]
    assert evidence[0]["source_manifest"]["source_etag"] == '"video-etag"'
    publication = operator.publication_config(setup.package, setup.root, setup.root / "state", config)
    video = validate_manifest_package(publication).plans[0]
    assert evidence[0]["manifest_path"] == f"{config.shortcut_path}/{video.incoming_manifest_path}"
    files.read_bytes.assert_called_with(evidence[0]["manifest_path"])
    files.verify_video.assert_any_call(
        f"{config.shortcut_path}/{video.incoming_video_path}",
        video.inspection.size_bytes, video.inspection.sha256,
    )
    assert files.write_partition.call_args.args[0].startswith(
        config.runtime_config().file_path("intake/backfill") + "/"
    )


@pytest.mark.parametrize("failure", ["publication", "shortcut", "model"])
def test_preflight_failure_never_submits_control(setup, failure):
    storage, files, release = cloud_mocks(setup)
    if failure == "shortcut":
        files.read_bytes.return_value = b"{}"
    if failure == "model":
        files.verify_video.side_effect = ValueError("model hash mismatch")
    publication = patch.object(
        operator, "publish_manifest_package",
        return_value=SimpleNamespace(rejected=1),
    ) if failure == "publication" else patch.object(
        operator, "publish_manifest_package", wraps=operator.publish_manifest_package
    )
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient") as fabric, publication, pytest.raises(
        (ValueError, RuntimeError), match="publication|manifest|model"
    ):
        fabric.return_value.list_pages.return_value = [publication_shortcut(setup)]
        operator.main(setup.args + ["--submit"])
    assert not fabric.return_value.request_response.called


@pytest.mark.parametrize("status", ["Failed", "Cancelled", "Deduped"])
def test_failed_job_is_not_success_and_retry_does_not_resubmit(setup, status):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job-1"}, b"")
    fabric.request_json.return_value = {"status": status}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        for _ in range(2):
            with pytest.raises(RuntimeError, match=status):
                operator.main(setup.args + ["--submit"])
    assert fabric.request_response.call_count == 1


def test_notebook_only_prepares_packages():
    path = Path(__file__).parents[1] / "notebooks/manifest_package_operator_tutorial.ipynb"
    notebook = json.loads(path.read_text())
    code = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )
    assert "RUN_PUBLICATION" not in code
    assert '"publish-manifests"' not in code
    assert "bulk-register" not in code
    assert "prepare_main" in code
    text = "\n".join("".join(cell["source"]) for cell in notebook["cells"])
    assert "backfill_operator" in text
    assert "10.3 Translate" not in text


def test_notebook_preparation_uses_kernel_interpreter():
    path = Path(__file__).parents[1] / "notebooks/manifest_package_operator_tutorial.ipynb"
    notebook = json.loads(path.read_text())
    cell = next(cell for cell in notebook["cells"] if cell["id"] == "prepare-first-pass")
    tree = ast.parse("".join(cell["source"]))
    assignment = ast.Module(body=[tree.body[0]], type_ignores=[])
    namespace = {
        "sys": sys,
        "CATALOG_PATH": Path("catalog.csv"),
        "VIDEO_ROOT": Path("videos"),
        "PARTITION_PREFIX": "samples/",
        "PACKAGE_DIR": Path("package"),
        "CHECKPOINT_PATH": Path("checkpoint.sqlite3"),
        "MAX_FILES": 10,
    }
    exec(compile(assignment, str(path), "exec"), namespace)
    command = namespace["prepare_base"]
    assert command[0] == sys.executable
    assert command[1] == "-c"
    assert command[3:] == [
        "--catalog", "catalog.csv",
        "--video-root", "videos",
        "--partition-prefix", "samples/",
        "--output-dir", "package",
        "--checkpoint", "checkpoint.sqlite3",
        "--max-files", "10",
    ]
    result = subprocess.run(
        command[:3] + ["--help"], check=True, capture_output=True, text=True,
    )
    assert "--catalog" in result.stdout
    assert "--output-dir" in result.stdout


@pytest.fixture
def backfill_notebook():
    path = Path(__file__).parents[1] / "notebooks/explicit_backfill_operator_tutorial.ipynb"
    notebook = json.loads(path.read_text())
    assert notebook["nbformat"] == 4
    cells = {}
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            assert cell["execution_count"] is None
            assert cell["outputs"] == []
            cells[cell["id"]] = compile("".join(cell["source"]), str(path), "exec")
            if cell["id"] == "workspace-setup":
                for fabric in (True, False):
                    tree = ast.parse("".join(cell["source"]))
                    assignment = next(
                        node for node in tree.body if isinstance(node, ast.Assign)
                        and any(isinstance(target, ast.Name) and target.id == "FABRIC_MODE"
                                for target in node.targets)
                    )
                    assignment.value = ast.Constant(fabric)
                    cells["setup-fabric" if fabric else "setup-local"] = compile(
                        ast.fix_missing_locations(tree), str(path), "exec",
                    )
    return cells


@pytest.fixture
def notebookutils_module(setup):
    return SimpleNamespace(
        runtime=SimpleNamespace(context={
            "defaultLakehouseWorkspaceId": setup.config["workspace_id"],
            "defaultLakehouseId": setup.config["lakehouse_id"],
        }),
        credentials=Mock(),
    )


@pytest.fixture
def backfill_notebook_run(backfill_notebook, setup, notebookutils_module):
    namespace = {}
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        exec(backfill_notebook["setup-fabric"], namespace)
    binding = {key: value for key, value in setup.config.items() if key != "profile"}
    catalog = setup.root / "backfill-catalog.json"
    models = setup.root / "models"
    for relative in artifact_paths(setup.config["profile"]["profile_id"]):
        path = models / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"test artifact {relative}".encode())
    namespace.update(
        PACKAGE_DIR=setup.package, VIDEO_ROOT=setup.root,
        CATALOG_PATH=catalog, STATE_DIR=setup.root / "state", MODELS_DIR=models,
        TARGET_BINDING=binding, TARGET="production",
    )
    for cell_id in (
        "create-catalog", "list-targets", "select-target", "select-profile",
        "preview-package", "submission-options",
    ):
        exec(backfill_notebook[cell_id], namespace)
    return namespace


def test_backfill_notebook_defaults_use_lakehouse_paths(backfill_notebook, notebookutils_module):
    namespace = {}
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        exec(backfill_notebook["setup-fabric"], namespace)
    assert namespace["PACKAGE_DIR"] == Path("/lakehouse/default/Files/backfill/manifest-package-tutorial")
    assert namespace["MODELS_DIR"] == Path("/lakehouse/default/Files/models")
    assert namespace["VIDEO_ROOT"] == Path("/lakehouse/default/Files/backfill/source-videos")
    assert not namespace["STATE_DIR"].is_relative_to(namespace["PACKAGE_DIR"])
    assert namespace["FABRIC_MODE"] is True
    assert namespace["TARGET_BINDING"]["workspace_id"] == notebookutils_module.runtime.context[
        "defaultLakehouseWorkspaceId"
    ]
    assert namespace["CATALOG_PATH"] == Path("/lakehouse/default/Files/backfill/config/backfill-catalog.json")
    assert namespace["STATE_DIR"] == Path("/lakehouse/default/Files/backfill/state/manifest-package-tutorial")
    assert namespace["TARGET"] == "production"
    assert namespace["PROFILE"] == "rtdetr-r18-onnx-cpu"


@pytest.mark.parametrize("subdirectory", ["", "notebooks"])
def test_backfill_notebook_local_defaults_match_manifest_tutorial(
    backfill_notebook, monkeypatch, subdirectory,
):
    root = Path(__file__).parents[1]
    monkeypatch.chdir(root / subdirectory)
    namespace = {}
    with patch.dict(sys.modules, {"notebookutils": None}):
        exec(backfill_notebook["setup-local"], namespace)
    assert namespace["REPO_ROOT"] == root
    assert namespace["PACKAGE_DIR"] == root / "samples/manifest-package-tutorial"
    assert namespace["VIDEO_ROOT"] == root
    assert namespace["MODELS_DIR"] == root / "offline-models"
    assert namespace["CATALOG_PATH"] == root / "config/backfill-catalog.json"
    assert namespace["STATE_DIR"] == root / "state/manifest-package-tutorial-backfill"
    assert namespace["TARGET"] == "production"
    assert namespace["PROFILE"] == "rtdetr-r18-onnx-cpu"


def test_backfill_notebook_switching_to_local_replaces_all_fabric_paths(
    backfill_notebook, notebookutils_module,
):
    namespace = {}
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        exec(backfill_notebook["setup-fabric"], namespace)
    with patch.dict(sys.modules, {"notebookutils": None}):
        exec(backfill_notebook["setup-local"], namespace)
    for name in ("PACKAGE_DIR", "VIDEO_ROOT", "MODELS_DIR", "CATALOG_PATH", "STATE_DIR"):
        assert not namespace[name].is_relative_to(Path("/lakehouse"))
    assert namespace["TARGET_BINDING"]["workspace_id"] == "<workspace-uuid>"
    assert namespace["TARGET_BINDING"]["lakehouse_id"] == "<lakehouse-uuid>"


def test_backfill_notebook_local_mode_rejects_working_directory_outside_repository(
    backfill_notebook, monkeypatch, tmp_path,
):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="repository"):
        exec(backfill_notebook["setup-local"], {})


def test_backfill_notebook_generates_catalog_from_real_model_bytes(backfill_notebook_run, setup):
    namespace = backfill_notebook_run
    catalog = json.loads(namespace["CATALOG_PATH"].read_text())
    assert "tutorial_only" not in catalog
    assert list(catalog["targets"]) == ["production"]
    config = operator.load_target(namespace["CATALOG_PATH"], "production", namespace["PROFILE"])
    hashes = config.profile["model_artifact_sha256"]
    assert hashes == {
        relative: hashlib.sha256((namespace["MODELS_DIR"] / relative).read_bytes()).hexdigest()
        for relative in artifact_paths(namespace["PROFILE"])
    }
    assert namespace["preview"]["workspace_id"] == config.workspace_id
    assert namespace["preview"]["effective_profile"]["model_artifact_sha256"] == hashes
    assert namespace["preview"]["status"] == "preview"
    assert namespace["preview"]["item_count"] == 1
    assert not (setup.root / "state").exists()


def test_backfill_notebook_catalog_is_retry_safe(
    backfill_notebook, backfill_notebook_run,
):
    namespace = backfill_notebook_run
    path = namespace["CATALOG_PATH"]
    before = path.read_bytes()
    exec(backfill_notebook["create-catalog"], namespace)
    assert path.read_bytes() == before
    (namespace["MODELS_DIR"] / artifact_paths(namespace["PROFILE"])[0]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="overwrite"):
        exec(backfill_notebook["create-catalog"], namespace)
    assert namespace["CATALOG_PATH"].read_bytes() == before


def test_backfill_notebook_missing_model_stops_catalog_creation(
    backfill_notebook, backfill_notebook_run,
):
    namespace = backfill_notebook_run
    namespace["CATALOG_PATH"].unlink()
    (namespace["MODELS_DIR"] / artifact_paths(namespace["PROFILE"])[0]).unlink()
    with pytest.raises(FileNotFoundError):
        exec(backfill_notebook["create-catalog"], namespace)
    assert not namespace["CATALOG_PATH"].exists()


def test_backfill_notebook_catalog_cannot_be_written_inside_package(
    backfill_notebook, backfill_notebook_run,
):
    namespace = backfill_notebook_run
    namespace["CATALOG_PATH"] = namespace["PACKAGE_DIR"] / "catalog.json"
    with pytest.raises(ValueError, match="outside"):
        exec(backfill_notebook["create-catalog"], namespace)
    assert not namespace["CATALOG_PATH"].exists()


def test_backfill_notebook_run_all_is_offline(backfill_notebook, backfill_notebook_run, setup):
    before = {p: p.read_bytes() for p in setup.package.rglob("*") if p.is_file()}
    with patch.object(operator, "submit") as submit, patch("builtins.input") as confirmation:
        exec(backfill_notebook["submit-backfill"], backfill_notebook_run)
        exec(backfill_notebook["inspect-evidence"], backfill_notebook_run)
    submit.assert_not_called()
    confirmation.assert_not_called()
    assert backfill_notebook_run["preview"]["status"] == "preview"
    assert backfill_notebook_run["preview"]["item_count"] == 1
    assert "--fabric" in backfill_notebook_run["backfill_args"]
    assert not (setup.root / "state").exists()
    assert before == {p: p.read_bytes() for p in setup.package.rglob("*") if p.is_file()}


@pytest.mark.parametrize("wait", [False, True])
def test_backfill_notebook_submission_reuses_reviewed_arguments(
    backfill_notebook, backfill_notebook_run, wait,
):
    namespace = backfill_notebook_run
    namespace["RUN_SUBMISSION"] = True
    namespace["WAIT_FOR_PROCESSING"] = wait
    namespace["SUBMISSION_CONFIRMATION"] = "SUBMIT production"
    report = {"status": "processed" if wait else "registered", "processing_complete": wait}
    run = Mock(side_effect=[namespace["preview"], report])
    namespace["run_backfill"] = run
    with patch("builtins.input", side_effect=AssertionError("Fabric must not request stdin")):
        exec(backfill_notebook["submit-backfill"], namespace)
    flags = ["--submit", "--wait"] if wait else ["--submit"]
    assert run.call_args_list[0].args[0] == namespace["backfill_args"]
    assert run.call_args_list[1].args[0] == [*namespace["backfill_args"], *flags]
    assert namespace["submission_report"] == report


def test_backfill_notebook_confirmation_rejects_submission(backfill_notebook, backfill_notebook_run):
    namespace = backfill_notebook_run
    namespace["RUN_SUBMISSION"] = True
    run = Mock(return_value=namespace["preview"])
    namespace["run_backfill"] = run
    with patch("builtins.input", side_effect=AssertionError("Fabric must not request stdin")), pytest.raises(
        ValueError, match="confirmation",
    ):
        exec(backfill_notebook["submit-backfill"], namespace)
    assert run.call_count == 1


def test_backfill_notebook_requires_new_review_after_changes(backfill_notebook, backfill_notebook_run):
    namespace = backfill_notebook_run
    namespace["RUN_SUBMISSION"] = True
    run = Mock(return_value={**namespace["preview"], "package_sha256": "changed"})
    namespace["run_backfill"] = run
    with patch("builtins.input") as confirmation, pytest.raises(RuntimeError, match="preview"):
        exec(backfill_notebook["submit-backfill"], namespace)
    confirmation.assert_not_called()
    assert run.call_count == 1


@pytest.mark.parametrize("code", [1, 130])
def test_backfill_notebook_command_failure_is_not_success(
    backfill_notebook, notebookutils_module, code,
):
    namespace = {}
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        exec(backfill_notebook["workspace-setup"], namespace)
    with patch.object(operator, "main", return_value=code), pytest.raises(
        RuntimeError, match=str(code),
    ):
        namespace["run_backfill"]([])


def test_backfill_notebook_keeps_progress_visible(backfill_notebook, notebookutils_module, capsys):
    namespace = {}
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        exec(backfill_notebook["workspace-setup"], namespace)

    def command(arguments):
        operator._progress("Processing: 1/1 committed")
        print('{"status": "processed", "processing_complete": true}')
        return 0

    with patch.object(operator, "main", side_effect=command):
        assert namespace["run_backfill"](["--submit", "--wait"]) == {
            "status": "processed", "processing_complete": True,
        }
    assert "Processing: 1/1 committed" in capsys.readouterr().err


@pytest.mark.parametrize("operation", ["download", "readall", "chunks"])
@pytest.mark.parametrize("failure", ["missing", "forbidden", "connection"])
def test_onelake_read_errors_identify_resource_and_preserve_cause(operation, failure):
    from azure.core.exceptions import (
        HttpResponseError, ResourceNotFoundError, ServiceRequestError,
    )

    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    files._workspace = "workspace"
    files._lakehouse = "lakehouse"
    file = Mock()
    files._file = Mock(return_value=file)
    if failure == "connection":
        error = ServiceRequestError("connection failed")
        guidance = "connectivity"
    else:
        error_type = ResourceNotFoundError if failure == "missing" else HttpResponseError
        error = error_type("The specified blob does not exist." if failure == "missing" else "Denied")
        error.status_code = 404 if failure == "missing" else 403
        error.response = SimpleNamespace(headers={"x-ms-request-id": "request-123"})
        guidance = "Files/models" if failure == "missing" else "permissions"
    path = "Files/models/rtdetr_osnet/model.onnx"
    if operation == "download":
        file.download_file.side_effect = error
    elif operation == "readall":
        file.download_file.return_value.readall.side_effect = error
    else:
        def chunks():
            yield b"partial"
            raise error

        file.download_file.return_value.chunks.return_value = chunks()

    with pytest.raises(RuntimeError) as raised:
        if operation == "chunks":
            files.verify_video(path, None, "a" * 64)
        else:
            files.read_text(path)
    message = str(raised.value)
    action = "verify size/SHA-256" if operation == "chunks" else "read"
    assert message.startswith(f"OneLake {action} failed:")
    assert f"abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/{path}" in message
    assert type(error).__name__ in message
    assert guidance in message
    files._file.assert_called_once_with(path)
    if failure != "connection":
        assert str(error.status_code) in message
        assert "request-123" in message
    assert raised.value.__cause__ is error


def test_missing_model_stops_before_publication(setup):
    from azure.core.exceptions import ResourceNotFoundError

    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    files._workspace = setup.config["workspace_id"]
    files._lakehouse = setup.config["lakehouse_id"]
    files._filesystem = Mock()
    files._filesystem.get_file_client.return_value.download_file.side_effect = (
        ResourceNotFoundError("The specified blob does not exist.")
    )
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "AzureDataLakeStorage",
    ) as storage, patch.object(operator, "FabricAutomationClient") as fabric, pytest.raises(
        RuntimeError, match="Files/models/",
    ):
        operator.main(setup.args + ["--submit"])
    storage.assert_not_called()
    fabric.assert_not_called()
    assert not (setup.root / "state").exists()


@pytest.mark.parametrize("bad_size,bad_digest", [(True, False), (False, True)])
def test_onelake_video_integrity_checks(bad_size, bad_digest):
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    file = Mock()
    file.download_file.return_value.chunks.return_value = [b"vi", b"deo"]
    files._file = Mock(return_value=file)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        files.verify_video(
            "Files/videos/video.mp4", 99 if bad_size else 5,
            "0" * 64 if bad_digest else hashlib.sha256(b"video").hexdigest(),
        )


def test_onelake_model_verification_does_not_require_video_size():
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    file = Mock()
    file.download_file.return_value.chunks.return_value = [b"model"]
    files._file = Mock(return_value=file)
    files.verify_video("Files/models/model.onnx", None, hashlib.sha256(b"model").hexdigest())


def test_onelake_video_verification_accumulates_all_chunks():
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    file = Mock()
    file.download_file.return_value.chunks.return_value = [b"vi", b"deo"]
    files._file = Mock(return_value=file)
    files.verify_video("Files/videos/video.mp4", 5, hashlib.sha256(b"video").hexdigest())
    files._file.assert_called_once_with("Files/videos/video.mp4")


@pytest.mark.parametrize("existing", [b"partition", b"changed"])
def test_onelake_partition_never_overwrites_existing_content(existing):
    from azure.core.exceptions import ResourceExistsError

    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    files._exists_error = ResourceExistsError
    files._lakehouse = "lakehouse"
    files._filesystem = Mock()
    files._filesystem.create_directory.side_effect = ResourceExistsError()
    file = Mock()
    file.upload_data.side_effect = ResourceExistsError()
    files._file = Mock(return_value=file)
    files.read_bytes = Mock(return_value=existing)
    if existing == b"partition":
        files.write_partition("Files/people-counter/dev/v1/intake/backfill/run/partition.json", b"partition")
    else:
        with pytest.raises(ValueError, match="conflicts"):
            files.write_partition("Files/people-counter/dev/v1/intake/backfill/run/partition.json", b"partition")
    from azure.core import MatchConditions

    file.upload_data.assert_called_once_with(
        b"partition", overwrite=True, match_condition=MatchConditions.IfMissing,
    )


def test_partition_readback_failure_is_explicit():
    files = operator.OneLakeFiles.__new__(operator.OneLakeFiles)
    files._lakehouse = "lakehouse"
    files._filesystem = Mock()
    files._file = Mock()
    files.read_bytes = Mock(return_value=b"changed")
    with pytest.raises(ValueError, match="readback mismatch"):
        files.write_partition("Files/people-counter/dev/v1/intake/backfill/run/partition.json", b"partition")


def test_partition_bounds_are_exact(setup):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    records = [{"work_id": f"work-{index}"} for index in range(1001)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(
        operator, "registration_records", return_value=records
    ), patch.object(
        operator, "publish_manifest_package",
        return_value=SimpleNamespace(rejected=0, published=1001, already_published=0),
    ), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ), patch.object(
        storage, "stat", return_value=SimpleNamespace(etag='"video-etag"')
    ):
        assert operator.main(setup.args + ["--submit"]) == 0
    partitions = files.write_partition.call_args_list
    assert [len(json.loads(call.args[1])["items"]) for call in partitions] == [1000, 1]
    commands = [
        call.kwargs["value"]["executionData"]["commandLineArguments"]
        for call in fabric.request_response.call_args_list
    ]
    assert commands[0].endswith("--max-items 1000")
    assert commands[1].endswith("--max-items 1")
    for call, command in zip(partitions, commands):
        assert hashlib.sha256(call.args[1]).hexdigest() in command


def test_unknown_submission_outcome_requires_recovery(setup):
    config = operator.load_config(setup.config_path)
    state = setup.root / "job.json"
    state.write_text('{"status":"submitting"}')
    client = Mock()
    with pytest.raises(RuntimeError, match="outcome unknown"):
        operator._register_partition(client, config, "partition.json", b"{}", 1, state)
    client.request_response.assert_not_called()


def test_pending_job_timeout_preserves_location_for_retry(setup):
    config = operator.load_config(setup.config_path)
    state = setup.root / "job.json"
    state.write_text('{"status":"submitted","location":"/jobs/job"}')
    client = Mock()
    client.request_json.return_value = {"status": "InProgress"}
    with patch.object(operator.time, "monotonic", side_effect=[0, 1, 1801]), patch.object(
        operator.time, "sleep"
    ), pytest.raises(TimeoutError, match="resume polling"):
        operator._register_partition(client, config, "partition.json", b"{}", 1, state)
    client.request_response.assert_not_called()
    assert json.loads(state.read_text())["location"] == "/jobs/job"


def test_state_directory_resumes_original_provenance_after_sdk_upgrade(setup):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        assert operator.main(setup.args + ["--submit"]) == 0
        original = json.loads((setup.root / "state/registration-plan.json").read_text())
        assert all(record["submitted_sdk_version"] == release.package_version for record in original["items"])
        plan = (setup.root / "state/registration-plan.json").read_bytes()
        release.digest = "e" * 64
        release.package_version = "0.10.2"
        assert operator.main(setup.args + ["--submit"]) == 0
        assert (setup.root / "state/registration-plan.json").read_bytes() == plan
        setup.config_path.write_text(json.dumps({
            **setup.config, "lakehouse_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        }))
        with pytest.raises(ValueError, match="different package"):
            operator.main(setup.args + ["--submit"])
    assert fabric.request_response.call_count == 1


def test_submission_resume_preserves_a_legacy_plan_without_schema_or_version_annotations(setup):
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(setup.package, setup.root, setup.root / "state", config)
    package = operator.validate_manifest_package(publication)
    records = operator.registration_records(package, config, "d" * 64, include_contract=False)
    for record in records:
        del record["submitted_sdk_version"]
    actual = operator._submission_records(
        package, config, operator.InstalledRelease("0.10.2", "e" * 64), {"items": records},
    )
    assert actual == records
    assert all("work_schema_version" not in record["payload"] for record in actual)


@pytest.mark.parametrize("field,value", [
    ("submitted_sdk_version", "0.9.0"), ("release_digest", "e" * 64),
])
def test_submission_resume_rejects_independently_mixed_provenance(setup, field, value):
    config = operator.load_config(setup.config_path)
    publication = operator.publication_config(setup.package, setup.root, setup.root / "state", config)
    package = operator.validate_manifest_package(publication)
    records = operator.registration_records(
        package, config, "d" * 64, submitted_sdk_version="0.10.1",
    )
    original = [*records, {**records[0], "work_id": "other", field: value}]
    with pytest.raises(ValueError) as error:
        operator._submission_records(
            package, config, operator.InstalledRelease("0.10.2", "e" * 64), {"items": original},
        )
    assert str(error.value) == "saved registration plan has inconsistent submission provenance"


def test_registration_in_progress_is_polled_once_then_persisted_as_completed(setup, tmp_path):
    config = operator.load_config(setup.config_path)
    state = tmp_path / "job.json"
    client = Mock()
    client.request_response.return_value = (202, {"Location": "/jobs/chosen-job"}, b"")
    client.request_json.side_effect = [
        {"status": "InProgress", "id": "chosen-job"},
        {"status": "Completed", "id": "chosen-job"},
    ]
    with patch.object(operator.time, "sleep") as sleep:
        result = operator._register_partition(
            client, config, "Files/intake/partition.json", b"{}", 1, state,
        )
    sleep.assert_called_once_with(15)
    assert client.request_json.call_count == 2
    assert result["status"] == "Completed"
    assert result["location"] == "/jobs/chosen-job"
    assert json.loads(state.read_text()) == result


def test_backfill_namespace_matches_the_deployed_control_job_and_retry_state(setup):
    root = "Files/people-counter/dev/v1/"
    setup.config_path.write_text(json.dumps({**setup.config, "files_root": root}))
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        assert operator.main(setup.args + ["--submit"]) == 0
        path = files.write_partition.call_args.args[0]
        assert path.startswith(root + "intake/backfill/")
        command = fabric.request_response.call_args.kwargs["value"]["executionData"]["commandLineArguments"]
        import shlex
        arguments = shlex.split(command)
        assert arguments[arguments.index("--files-root") + 1] == root
        assert arguments[arguments.index("--partition-path") + 1] == path
        assert json.loads((setup.root / "state/registration-plan.json").read_text())["target"]["files_root"] == root
        setup.config_path.write_text(json.dumps({**setup.config, "files_root": "Files/another-deployment/"}))
        with pytest.raises(ValueError, match="different package"):
            operator.main(setup.args + ["--submit"])
    assert fabric.request_response.call_count == 1


@pytest.mark.parametrize("wait", [False, True])
def test_wait_cli_only_monitors_after_submission(setup, capsys, wait):
    events = []
    connection = Mock()

    @contextmanager
    def reader(config):
        events.append("reader")
        yield connection

    def submitted(*args):
        events.append("submit")
        return {"status": "registered", "processing_complete": False}

    def monitored(*args):
        events.append("wait")
        assert args[0] is connection
        return {"status": "processed", "processing_complete": True}

    with patch.object(operator, "processing_reader", side_effect=reader), patch.object(
        operator, "submit", side_effect=submitted
    ), patch.object(operator, "wait_for_processing", side_effect=monitored):
        assert operator.main(setup.args + ["--submit"] + (["--wait"] if wait else [])) == 0
    assert events == (["reader", "submit", "wait"] if wait else ["submit"])
    result = json.loads(capsys.readouterr().out)
    assert result["processing_complete"] is wait
    assert result["status"] == ("processed" if wait else "registered")


def test_processing_progress_requires_every_exact_work_id_and_committed_success():
    rows = [
        {"work_id": "unrelated", "status": "SUCCEEDED", "committed_attempt_id": "other"},
        {"work_id": "one", "status": "SUCCEEDED", "committed_attempt_id": "attempt-1"},
        {"work_id": "two", "status": "SUCCEEDED", "committed_attempt_id": None},
        {"work_id": "three", "status": "LEASED", "committed_attempt_id": None},
    ]
    result = operator.processing_progress(["one", "two", "three", "four"], rows)
    assert result["item_count"] == 4
    assert result["completed"] == 1
    assert result["processing_complete"] is False
    assert result["counts"] == {
        "SUCCEEDED": 1, "WAITING_COMMIT": 1, "LEASED": 1, "WAITING_REGISTRATION": 1,
    }


@pytest.mark.parametrize("status", ["DEAD", "DEAD_LETTER", "FAILED", "QUARANTINED", "CANCELLED"])
def test_processing_progress_reports_terminal_failures(status):
    result = operator.processing_progress(["work"], [{
        "work_id": "work", "status": status, "committed_attempt_id": None,
        "last_error": "source could not be decoded",
    }])
    assert result["processing_complete"] is False
    assert result["failures"] == [{
        "work_id": "work", "status": status, "last_error": "source could not be decoded",
    }]


def test_processing_progress_rejects_duplicate_rows():
    row = {"work_id": "work", "status": "SUCCEEDED", "committed_attempt_id": "attempt"}
    with pytest.raises(RuntimeError, match="Duplicate"):
        operator.processing_progress(["work"], [row, row])


def processing_state(setup, ids=("one", "two")):
    state = setup.root / "state"
    state.mkdir(exist_ok=True)
    (state / "registration-plan.json").write_text(json.dumps({
        "items": [{"work_id": work_id} for work_id in ids],
    }))
    return state


def test_wait_polls_exact_ids_persists_progress_and_then_finishes(setup, capsys):
    config = operator.load_config(setup.config_path)
    state = processing_state(setup)
    connection = Mock()
    initial = [
        {"work_id": "one", "status": "READY", "committed_attempt_id": None},
        {"work_id": "two", "status": "LEASED", "committed_attempt_id": None},
    ]
    complete = [
        {"work_id": "one", "status": "SUCCEEDED", "committed_attempt_id": "attempt-1"},
        {"work_id": "two", "status": "SUCCEEDED", "committed_attempt_id": "attempt-2"},
    ]
    with patch.object(
        operator, "read_demo_delta", side_effect=[initial, complete]
    ) as read, patch.object(operator.time, "sleep") as sleep:
        report = operator.wait_for_processing(connection, config, state)
    assert report["status"] == "processed"
    assert report["processing_complete"] is True
    assert report["completed"] == 2
    assert report["committed_attempts"] == {"one": "attempt-1", "two": "attempt-2"}
    assert json.loads((state / "processing-summary.json").read_text()) == report
    assert read.call_count == 2
    for call in read.call_args_list:
        assert call.args == (
            connection,
            f"abfss://{config.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{config.lakehouse_id}/Tables/dbo/people_counter_sjd_",
            "work",
            ["work_id", "status", "committed_attempt_id", "last_error"],
            [("work_id", "in", ["one", "two"])],
        )
    sleep.assert_called_once_with(15)
    output = capsys.readouterr()
    assert output.out == ""
    assert "0/2" in output.err
    assert "2/2" in output.err
    assert "READY=1" in output.err
    assert "LEASED=1" in output.err


def test_wait_stops_on_dead_work_with_saved_diagnostics(setup):
    config = operator.load_config(setup.config_path)
    state = processing_state(setup, ("dead",))
    with patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "dead", "status": "DEAD", "committed_attempt_id": None,
        "last_error": "model unavailable",
    }]), patch.object(
        operator.time, "sleep", side_effect=AssertionError("Terminal work must stop the wait")
    ) as sleep, pytest.raises(
        RuntimeError, match="dead.*DEAD.*model unavailable"
    ):
        operator.wait_for_processing(Mock(), config, state)
    sleep.assert_not_called()
    report = json.loads((state / "processing-summary.json").read_text())
    assert report["status"] == "failed"
    assert report["failures"][0]["last_error"] == "model unavailable"


def test_wait_stops_on_incompatible_work_with_actionable_diagnostics(setup):
    config = operator.load_config(setup.config_path)
    state = processing_state(setup, ("blocked",))
    with patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "blocked", "status": "BLOCKED_INCOMPATIBLE",
        "committed_attempt_id": None,
        "last_error": json.dumps({"reason": "Unsupported payload schema: 2"}),
    }]), patch.object(operator.time, "sleep", side_effect=AssertionError("blocked work must stop polling")) as sleep:
        with pytest.raises(RuntimeError, match="blocked.*BLOCKED_INCOMPATIBLE.*payload schema"):
            operator.wait_for_processing(Mock(), config, state)
    sleep.assert_not_called()


def test_wait_read_failure_is_not_replaced_with_success(setup):
    state = processing_state(setup)
    with patch.object(
        operator, "read_demo_delta", side_effect=PermissionError("OneLake access denied")
    ), pytest.raises(PermissionError, match="denied"):
        operator.wait_for_processing(Mock(), operator.load_config(setup.config_path), state)
    assert not (state / "processing-summary.json").exists()


def test_processing_reader_preflights_table_and_closes_connection(setup):
    connection = Mock()
    duckdb = Mock()
    duckdb.connect.return_value = connection
    config = operator.load_config(setup.config_path)
    with patch.dict("sys.modules", {"duckdb": duckdb}), patch.object(
        operator, "read_demo_delta", return_value=[]
    ) as read:
        with operator.processing_reader(config) as actual:
            assert actual is connection
    connection.close.assert_called_once_with()
    assert read.call_args.args[2] == "work"
    assert read.call_args.args[4] == [("work_id", "in", ["backfill-monitor-preflight"])]
    setup_sql = " ".join(call.args[0] for call in connection.execute.call_args_list)
    assert "LOAD delta" in setup_sql
    assert "CHAIN 'cli'" in setup_sql
    assert "onelake.blob.fabric.microsoft.com" in setup_sql


def test_fabric_storage_credential_refreshes_notebook_tokens(notebookutils_module):
    import base64

    def token(expiration):
        body = base64.urlsafe_b64encode(json.dumps({"exp": expiration}).encode()).decode().rstrip("=")
        return f"header.{body}.signature"

    first, second = token(4102444800), token(4102444900)
    notebookutils_module.credentials.getToken.side_effect = [first, second]
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        credential = operator.NotebookStorageCredential()
        observed = [
            credential.get_token("https://storage.azure.com/.default"),
            credential.get_token("https://storage.azure.com/.default"),
        ]
    assert [(value.token, value.expires_on) for value in observed] == [
        (first, 4102444800), (second, 4102444900),
    ]
    assert [call.args for call in notebookutils_module.credentials.getToken.call_args_list] == [
        ("storage",), ("storage",),
    ]


@pytest.mark.parametrize("token", [
    "", "invalid", "header.e30.signature",
    "header.eyJleHAiOjF9.signature", "header.eyJleHAiOnRydWV9.signature",
])
def test_fabric_storage_credential_rejects_invalid_tokens(notebookutils_module, token):
    notebookutils_module.credentials.getToken.return_value = token
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), pytest.raises(
        RuntimeError, match="storage token",
    ):
        operator.NotebookStorageCredential().get_token("https://storage.azure.com/.default")


def test_fabric_api_token_uses_notebook_identity(notebookutils_module):
    notebookutils_module.credentials.getToken.return_value = "fabric-test-token"
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}):
        assert operator.NotebookFabricTokenProvider().get_token() == "fabric-test-token"
    notebookutils_module.credentials.getToken.assert_called_once_with("pbi")


def test_fabric_api_token_failure_has_no_cli_fallback(notebookutils_module):
    notebookutils_module.credentials.getToken.return_value = ""
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), pytest.raises(
        RuntimeError, match="empty API token",
    ):
        operator.NotebookFabricTokenProvider().get_token()


@pytest.mark.parametrize("padding", ["", "x", "xx", "xxx"])
def test_fabric_storage_token_padding_and_expiration_boundary(notebookutils_module, padding):
    import base64

    expiration = 4102444800
    payload = base64.urlsafe_b64encode(json.dumps({
        "exp": expiration, "padding": padding,
    }).encode()).decode().rstrip("=")
    notebookutils_module.credentials.getToken.return_value = f"header.{payload}.signature"
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), patch.object(
        operator.time, "time", return_value=expiration - 1,
    ):
        assert operator.NotebookStorageCredential().get_token().expires_on == expiration
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), patch.object(
        operator.time, "time", return_value=expiration,
    ), pytest.raises(RuntimeError, match="expired storage token"):
        operator.NotebookStorageCredential().get_token()


@pytest.mark.parametrize("wait", [False, True])
def test_fabric_submit_uses_notebook_credentials_and_local_sqlite(setup, notebookutils_module, wait):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    checkpoints = []
    spark = Mock()

    def committed_rows():
        plan = json.loads((setup.root / "state/registration-plan.json").read_text())
        return [
            SimpleNamespace(asDict=lambda item=item: {
                "work_id": item["work_id"], "status": "SUCCEEDED",
                "committed_attempt_id": "fabric-attempt", "last_error": None,
            })
            for item in plan["items"]
        ]

    spark.read.format.return_value.load.return_value.select.return_value.where.return_value.collect.side_effect = (
        committed_rows
    )

    def publish(config, *args, **kwargs):
        checkpoints.append(config.checkpoint_path)
        assert not config.checkpoint_path.is_relative_to(setup.root / "state")
        return operator_publish(config, *args, **kwargs)

    operator_publish = operator.publish_manifest_package
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), patch.object(
        operator, "AzureDataLakeStorage", return_value=storage,
    ) as adls, patch.object(
        operator, "OneLakeFiles", return_value=files,
    ) as onelake, patch.object(
        operator, "installed_release", return_value=release,
    ), patch.object(
        operator, "FabricAutomationClient", return_value=fabric,
    ) as client, patch.object(
        operator, "publish_manifest_package", side_effect=publish,
    ), patch.object(
        operator, "AzureCliResourceTokenProvider", side_effect=AssertionError("Must not use CLI"),
    ), patch(
        "pyspark.sql.SparkSession.getActiveSession", return_value=spark,
    ), patch(
        "pyspark.sql.functions.col", return_value=Mock(),
    ):
        args = setup.args + ["--fabric", "--submit"] + (["--wait"] if wait else [])
        assert operator.main(args) == 0
        assert operator.main(args) == 0
    assert adls.call_args.args == ("account", "footage")
    assert adls.call_args.kwargs["chunk_size"] == 8 * 1024 * 1024
    assert isinstance(adls.call_args.kwargs["credential"], operator.NotebookStorageCredential)
    assert onelake.call_args.args == (operator.load_config(setup.config_path),)
    assert onelake.call_args.kwargs["credential"] is adls.call_args.kwargs["credential"]
    assert isinstance(client.call_args.args[0], operator.NotebookFabricTokenProvider)
    fabric.list_pages.assert_called_with(
        f"/workspaces/{setup.config['workspace_id']}/items/{setup.config['lakehouse_id']}/shortcuts"
    )
    assert (setup.root / "state/registration-summary.json").is_file()
    assert len(set(checkpoints)) == 2
    assert not any(path.exists() for path in checkpoints)
    assert fabric.request_response.call_count == 1
    if wait:
        summary = json.loads((setup.root / "state/processing-summary.json").read_text())
        assert summary["status"] == "processed"
        assert summary["processing_complete"] is True
        assert set(summary["committed_attempts"].values()) == {"fabric-attempt"}


def test_onelake_supports_supplied_credential(setup, monkeypatch):
    credential = Mock()
    monkeypatch.undo()
    with patch("azure.storage.filedatalake.DataLakeServiceClient") as service, patch(
        "azure.identity.DefaultAzureCredential", side_effect=AssertionError("Must not use default"),
    ):
        operator.OneLakeFiles(operator.load_config(setup.config_path), credential=credential)
    assert service.call_args.kwargs["credential"] is credential


@pytest.mark.parametrize("field", ["defaultLakehouseId", "defaultLakehouseWorkspaceId"])
def test_fabric_target_mismatch_stops_before_writes(setup, notebookutils_module, field):
    notebookutils_module.runtime.context[field] = "wrong-target"
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), patch.object(
        operator, "AzureDataLakeStorage",
    ) as storage, pytest.raises(ValueError, match="attached default Lakehouse"):
        operator.main(setup.args + ["--fabric", "--submit"])
    storage.assert_not_called()
    assert not (setup.root / "state").exists()


def test_fabric_processing_reader_preflights_spark_without_duckdb(setup, notebookutils_module):
    spark = Mock()
    config = operator.load_config(setup.config_path)
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module, "duckdb": None}), patch(
        "pyspark.sql.SparkSession.getActiveSession", return_value=spark,
    ):
        with operator.processing_reader(config, fabric=True) as actual:
            assert actual is spark
    spark.read.format.assert_called_with("delta")
    spark.read.format.return_value.load.assert_called_once_with(operator._table_prefix(config) + "work")
    table = spark.read.format.return_value.load.return_value
    table.select.assert_called_once_with(*operator._WORK_COLUMNS)
    table.select.return_value.limit.assert_called_once_with(0)
    table.select.return_value.limit.return_value.collect.assert_called_once_with()
    spark.stop.assert_not_called()


def test_fabric_processing_table_error_prevents_publication(setup, notebookutils_module):
    spark = Mock()
    spark.read.format.return_value.load.side_effect = PermissionError("Work table access denied")
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), patch(
        "pyspark.sql.SparkSession.getActiveSession", return_value=spark,
    ), patch.object(operator, "submit") as submit, pytest.raises(PermissionError, match="denied"):
        operator.main(setup.args + ["--fabric", "--submit", "--wait"])
    submit.assert_not_called()


def test_fabric_wait_reads_fresh_delta_and_filters_exact_ids(setup):
    state = processing_state(setup, ("work",))
    spark = Mock()
    table = spark.read.format.return_value.load.return_value
    table.select.return_value.where.return_value.collect.side_effect = [
        [SimpleNamespace(asDict=lambda: {
            "work_id": "work", "status": "LEASED", "committed_attempt_id": None,
        })],
        [SimpleNamespace(asDict=lambda: {
            "work_id": "work", "status": "SUCCEEDED", "committed_attempt_id": "attempt",
        })],
    ]
    column = Mock()
    with patch("pyspark.sql.functions.col", return_value=column) as col, patch.object(
        operator.time, "sleep",
    ):
        report = operator.wait_for_processing(
            spark, operator.load_config(setup.config_path), state, fabric=True,
        )
    assert report["processing_complete"] is True
    assert report["committed_attempts"] == {"work": "attempt"}
    assert spark.read.format.return_value.load.call_count == 2
    col.assert_called_with("work_id")
    column.isin.assert_called_with(["work"])


def test_fabric_wait_preflight_failure_prevents_publication(setup, notebookutils_module):
    with patch.dict(sys.modules, {"notebookutils": notebookutils_module}), patch(
        "pyspark.sql.SparkSession.getActiveSession", return_value=None,
    ), patch.object(operator, "submit") as submit, pytest.raises(RuntimeError, match="Spark session"):
        operator.main(setup.args + ["--fabric", "--submit", "--wait"])
    submit.assert_not_called()


def test_wait_dependency_failure_prevents_submission(setup):
    with patch.dict("sys.modules", {"duckdb": None}), patch.object(
        operator, "submit"
    ) as submit, pytest.raises(RuntimeError, match="--extra demo"):
        operator.main(setup.args + ["--submit", "--wait"])
    submit.assert_not_called()


def test_ctrl_c_stops_monitoring_without_cancelling_and_can_resume(setup, capsys):
    connection = Mock()
    state = processing_state(setup, ("work",))
    fabric = Mock()

    @contextmanager
    def reader(config):
        yield connection

    with patch.object(operator, "processing_reader", side_effect=reader), patch.object(
        operator, "submit", return_value={"status": "registered", "processing_complete": False}
    ) as submit, patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "work", "status": "LEASED", "committed_attempt_id": None,
    }]), patch.object(operator.time, "sleep", side_effect=KeyboardInterrupt), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ):
        assert operator.main(setup.args + ["--submit", "--wait"]) == 130
        assert submit.call_count == 1
    assert not fabric.mock_calls
    assert json.loads((state / "processing-summary.json").read_text())["completed"] == 0
    output = capsys.readouterr()
    assert "Fabric" in output.err
    assert "continue" in output.err
    assert "--submit --wait" in output.err
    assert json.loads(output.out) == {
        "status": "monitoring_stopped", "processing_complete": False,
    }
    with patch.object(operator, "processing_reader", side_effect=reader), patch.object(
        operator, "submit", return_value={"status": "registered", "processing_complete": False}
    ), patch.object(operator, "read_demo_delta", return_value=[{
        "work_id": "work", "status": "SUCCEEDED", "committed_attempt_id": "attempt",
    }]), patch.object(
        operator.time, "sleep", side_effect=AssertionError("Resumed work is already complete")
    ):
        assert operator.main(setup.args + ["--submit", "--wait"]) == 0
    assert json.loads(capsys.readouterr().out)["processing_complete"] is True


def test_publication_and_registration_print_progress_to_stderr(setup, capsys):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(operator, "FabricAutomationClient", return_value=fabric):
        assert operator.main(setup.args + ["--submit"]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "registered"
    assert "Publishing 1" in output.err
    assert "Publication complete" in output.err
    assert "partition-00001" in output.err
    assert "Completed" in output.err


@pytest.mark.parametrize("directory", ["Tables", "Tables/dbo", "Tables/custom_schema"])
def test_monitor_uses_configured_lakehouse_table_directory(setup, directory):
    setup.config_path.write_text(json.dumps({**setup.config, "table_directory": directory}))
    config = operator.load_config(setup.config_path)
    assert operator._table_prefix(config) == (
        f"abfss://{config.workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{config.lakehouse_id}/{directory}/people_counter_sjd_"
    )


@pytest.mark.parametrize("ids", [[], ["work", "work"]])
def test_monitor_rejects_empty_or_repeated_expected_ids(ids):
    with pytest.raises(ValueError, match="nonempty, unique"):
        operator.processing_progress(ids, [])


@pytest.mark.parametrize("status", [None, ""])
def test_monitor_rejects_invalid_status(status):
    with pytest.raises(ValueError, match="Invalid work status"):
        operator.processing_progress(["work"], [{
            "work_id": "work", "status": status, "committed_attempt_id": None,
        }])


def test_processing_reader_permission_failure_closes_and_never_submits(setup):
    duckdb = Mock()
    connection = duckdb.connect.return_value
    with patch.dict("sys.modules", {"duckdb": duckdb}), patch.object(
        operator, "read_demo_delta", side_effect=PermissionError("work table access denied")
    ), patch.object(operator, "submit") as submit, pytest.raises(
        PermissionError, match="denied"
    ):
        operator.main(setup.args + ["--submit", "--wait"])
    connection.close.assert_called_once_with()
    submit.assert_not_called()


def test_wait_queries_bounded_batches_of_work_ids(setup):
    ids = tuple(f"work-{index}" for index in range(1001))
    state = processing_state(setup, ids)
    config = operator.load_config(setup.config_path)

    def rows(connection, prefix, suffix, columns, filters):
        return [
            {"work_id": work_id, "status": "SUCCEEDED", "committed_attempt_id": "attempt"}
            for work_id in filters[0][2]
        ]

    with patch.object(operator, "read_demo_delta", side_effect=rows) as read, patch.object(
        operator.time, "sleep", side_effect=AssertionError("All queried work must be complete")
    ):
        report = operator.wait_for_processing(Mock(), config, state)
    assert report["completed"] == 1001
    assert [len(call.args[4][0][2]) for call in read.call_args_list] == [1000, 1]


def test_wait_observes_new_delta_commits_and_ignores_unrelated_success(setup):
    import duckdb
    import pyarrow as pa
    from deltalake import write_deltalake

    table = setup.root / "tables/people_counter_sjd_work"
    table.parent.mkdir()
    state = processing_state(setup, ("selected",))
    config = operator.load_config(setup.config_path)

    def write(status, attempt, mode):
        write_deltalake(table, pa.table({
            "work_id": ["selected", "unrelated"],
            "status": [status, "SUCCEEDED"],
            "committed_attempt_id": pa.array([attempt, "other-attempt"], type=pa.string()),
            "last_error": pa.array([None, None], type=pa.string()),
        }), mode=mode)

    write("READY", None, "error")
    finished = False

    def finish_processing(_):
        nonlocal finished
        if finished:
            raise AssertionError("The next query must observe the committed Delta update")
        finished = True
        write("SUCCEEDED", "selected-attempt", "overwrite")

    with duckdb.connect() as connection:
        connection.execute("INSTALL delta; LOAD delta")
        with patch.object(
            operator, "_table_prefix", return_value=str(table.parent / "people_counter_sjd_")
        ), patch.object(
            operator.time, "sleep",
            side_effect=finish_processing,
        ) as sleep:
            result = operator.wait_for_processing(connection, config, state)
    sleep.assert_called_once_with(15)
    assert result["completed"] == 1
    assert result["committed_attempts"] == {"selected": "selected-attempt"}


@pytest.mark.parametrize("flag", [
    "--manifest-package-dir", "--video-root", "--config", "--state-dir",
])
def test_required_cli_paths_are_enforced_by_parser(setup, flag):
    args = list(setup.args)
    position = args.index(flag)
    del args[position:position + 2]
    with pytest.raises(SystemExit) as error:
        operator.main(args)
    assert error.value.code == 2


@pytest.mark.parametrize("count,partitions", [(1000, 1), (1001, 2)])
def test_preview_partition_count_respects_exact_boundaries(setup, capsys, count, partitions):
    with patch.object(operator, "validate_manifest_package", return_value=SimpleNamespace(
        plans=[None] * count, index_bytes=b"index",
    )):
        assert operator.main(setup.args) == 0
    assert json.loads(capsys.readouterr().out)["partition_count"] == partitions


def test_submit_wait_integrates_publication_registration_processing_and_resume(setup, capsys):
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"id": "job", "status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    connection = Mock()
    state = setup.root / "state"

    @contextmanager
    def reader(config):
        assert config.workspace_id == setup.config["workspace_id"]
        yield connection

    def completed_rows(*args):
        plan = json.loads((state / "registration-plan.json").read_text())
        ids = [row["work_id"] for row in plan["items"]]
        assert args[4] == [("work_id", "in", ids)]
        return [
            {"work_id": work_id, "status": "SUCCEEDED", "committed_attempt_id": "attempt"}
            for work_id in ids
        ]

    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ), patch.object(
        operator, "processing_reader", side_effect=reader
    ), patch.object(
        operator, "read_demo_delta", side_effect=completed_rows
    ), patch.object(
        operator.time, "sleep", side_effect=AssertionError("Processing is already complete")
    ):
        for _ in range(2):
            assert operator.main(setup.args + ["--submit", "--wait"]) == 0
            report = json.loads(capsys.readouterr().out)
            assert report["status"] == "processed"
            assert report["processing_complete"] is True
            assert report["completed"] == report["item_count"] == 1
            assert report["jobs"][0]["job"]["id"] == "job"
    assert fabric.request_response.call_count == 1
    assert json.loads((state / "processing-summary.json").read_text())["completed"] == 1


def named_catalog(setup):
    catalog = setup.root / "catalog.json"
    binding = {key: value for key, value in setup.config.items() if key != "profile"}
    catalog.write_text(json.dumps({
        "schema": operator.CATALOG_SCHEMA,
        "targets": {"production": {
            "binding": binding,
            "profiles": {setup.config["profile"]["profile_id"]: setup.config["profile"]},
        }},
    }))
    args = list(setup.args)
    position = args.index("--config")
    del args[position:position + 2]
    return catalog, args + [
        "--catalog", str(catalog), "--target", "production",
        "--profile", setup.config["profile"]["profile_id"],
    ]


def test_named_profile_preview_needs_no_handwritten_config_or_hashes(setup, capsys):
    catalog, args = named_catalog(setup)
    with patch.object(operator, "OneLakeFiles") as files:
        assert operator.main(args + ["--wait", "--batch-size", "4", "--sample-fps", "2"]) == 0
    files.assert_not_called()
    report = json.loads(capsys.readouterr().out)
    profile = report["effective_profile"]
    assert profile["batch_size"] == 4
    assert profile["sample_fps"] == 2.0
    assert profile["runtime_key"] != setup.config["profile"]["runtime_key"]
    stored = json.loads(catalog.read_text())["targets"]["production"]["profiles"]
    assert stored[profile["profile_id"]]["batch_size"] == 1


@pytest.mark.parametrize("selection", ["--list-targets", "--list-profiles"])
def test_operators_can_list_names_without_videos_or_cloud(setup, capsys, selection):
    catalog, _ = named_catalog(setup)
    args = ["--catalog", str(catalog), selection]
    if selection == "--list-profiles":
        args += ["--target", "production"]
    with patch.object(operator, "OneLakeFiles") as files:
        assert operator.main(args) == 0
    files.assert_not_called()
    output = capsys.readouterr().out
    if selection == "--list-targets":
        assert output.splitlines() == ["production"]
    else:
        assert "Target: production" in output
        assert "PROFILE" in output
        assert "rtdetr-r18-onnx-cpu" in output
        assert "r18" in output and "onnx" in output
        assert "sha256" not in output


@pytest.mark.parametrize("overrides", [
    ["--batch-size", "3"], ["--sample-fps", "0"],
    ["--sample-fps", "nan"], ["--sample-fps", "inf"],
])
def test_invalid_named_overrides_fail_before_cloud(setup, overrides):
    _, args = named_catalog(setup)
    with patch.object(operator, "OneLakeFiles") as files, pytest.raises(
        (ValueError, SystemExit)
    ):
        operator.main(args + ["--submit", *overrides])
    files.assert_not_called()


def test_catalog_profile_edit_is_detected_before_publication(setup):
    catalog, args = named_catalog(setup)
    raw = json.loads(catalog.read_text())
    raw["targets"]["production"]["profiles"]["rtdetr-r18-onnx-cpu"]["sample_fps"] = 9.0
    catalog.write_text(json.dumps(raw))
    with patch.object(operator, "OneLakeFiles") as files, pytest.raises(
        ValueError, match="profile"
    ):
        operator.main(args + ["--submit"])
    files.assert_not_called()


def test_named_target_submit_uses_derived_config_and_runtime_identities(setup, capsys):
    _, args = named_catalog(setup)
    storage, files, release = cloud_mocks(setup)
    fabric = Mock()
    fabric.request_response.return_value = (202, {"Location": "/jobs/job"}, b"")
    fabric.request_json.return_value = {"id": "job", "status": "Completed"}
    fabric.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "AzureDataLakeStorage", return_value=storage), patch.object(
        operator, "OneLakeFiles", return_value=files
    ), patch.object(
        operator, "installed_release", return_value=release
    ), patch.object(
        operator, "FabricAutomationClient", return_value=fabric
    ):
        assert operator.main(args + ["--submit", "--sample-fps", "2"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "registered"
    partition = json.loads(files.write_partition.call_args.args[1])
    record = partition["items"][0]
    assert record["payload"]["sample_fps"] == 2.0
    assert record["config_sha256"] == configuration_sha256(record["payload"])
    assert record["runtime_key"] == record["payload"]["runtime_key"]

import ast
import io
import json
import subprocess
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import people_counter.manifest_publisher as publisher
from people_counter.manifest import CatalogError
from tests.test_manifest_publisher import inspection


@pytest.fixture
def cells():
    path = Path(__file__).parents[1] / "notebooks/manifest_package_operator_tutorial.ipynb"
    notebook = json.loads(path.read_text())
    return {
        cell["id"]: "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    }


def run_setup(source, fabric, lakehouse, partition_prefix=None):
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            if "FABRIC_MODE" in names:
                node.value = ast.Constant(fabric)
            if "LAKEHOUSE_ROOT" in names:
                node.value = ast.Call(
                    func=ast.Name(id="Path", ctx=ast.Load()),
                    args=[ast.Constant(str(lakehouse))], keywords=[],
                )
            if (
                "PARTITION_PREFIX" in names
                and isinstance(node.value, ast.Constant)
                and partition_prefix is not None
            ):
                node.value = ast.Constant(partition_prefix)
    namespace = {}
    exec(compile(ast.fix_missing_locations(tree), "<notebook-setup>", "exec"), namespace)
    return namespace


@pytest.fixture
def fabric_setup(cells, tmp_path, monkeypatch, request):
    lakehouse = tmp_path / "lakehouse"
    (lakehouse / "Files/backfill/source-videos").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    namespace = run_setup(
        cells["workspace-setup"], True, lakehouse, getattr(request, "param", None),
    )
    yield namespace
    namespace["PREPARATION_STATE"].cleanup()


def test_notebook_has_no_saved_execution_state(cells):
    path = Path(__file__).parents[1] / "notebooks/manifest_package_operator_tutorial.ipynb"
    for cell in json.loads(path.read_text())["cells"]:
        if cell["cell_type"] == "code":
            assert cell["execution_count"] is None
            assert cell["outputs"] == []
            compile(cells[cell["id"]], str(path), "exec")


@pytest.mark.parametrize("subdirectory", ["", "notebooks"])
def test_local_defaults_preserve_sample_workflow(cells, tmp_path, monkeypatch, subdirectory):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='fixture'\n")
    (tmp_path / "samples").mkdir()
    (tmp_path / "notebooks").mkdir()
    monkeypatch.chdir(tmp_path / subdirectory)
    namespace = run_setup(cells["workspace-setup"], False, tmp_path / "unused")
    assert namespace["FABRIC_MODE"] is False
    assert namespace["REPO_ROOT"] == namespace["VIDEO_ROOT"] == tmp_path
    assert namespace["PACKAGE_DIR"] == tmp_path / "samples/manifest-package-tutorial"
    assert namespace["PARTITION_PREFIX"] == "samples/"
    assert namespace["CHECKPOINT_PATH"] == namespace["PACKAGE_DIR"] / "preparation.sqlite3"
    exec(cells["write-catalog"], namespace)
    exec(cells["write-exception-inventory"], namespace)
    assert "samples/" in namespace["CATALOG_PATH"].read_text()
    assert "samples/three_people_walking.mp4" in namespace["EXCEPTION_INVENTORY_PATH"].read_text()


def test_fabric_defaults_match_backfill_handoff(fabric_setup, tmp_path):
    lakehouse = tmp_path / "lakehouse"
    path = Path(__file__).parents[1] / "notebooks/explicit_backfill_operator_tutorial.ipynb"
    source = next(
        "".join(cell["source"]) for cell in json.loads(path.read_text())["cells"]
        if cell["id"] == "workspace-setup"
    )
    notebookutils = SimpleNamespace(runtime=SimpleNamespace(context={
        "defaultLakehouseWorkspaceId": "workspace",
        "defaultLakehouseId": "lakehouse",
    }))
    with patch.dict("sys.modules", {"notebookutils": notebookutils}):
        backfill = run_setup(source, True, lakehouse)
    assert fabric_setup["FABRIC_MODE"] is True
    for name in ("PACKAGE_DIR", "VIDEO_ROOT"):
        assert fabric_setup[name] == backfill[name]
    assert fabric_setup["PARTITION_PREFIX"] == "samples/"
    assert "REPO_ROOT" not in fabric_setup
    assert not fabric_setup["CHECKPOINT_PATH"].is_relative_to(lakehouse)
    assert fabric_setup["CHECKPOINT_PATH"].parent.is_dir()
    assert fabric_setup["CATALOG_PATH"] == lakehouse / "Files/backfill/config/camera_catalog.csv"


def test_missing_shortcut_fails_without_creating_source(cells, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match="Video root"):
        run_setup(cells["workspace-setup"], True, tmp_path / "missing")
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize("fabric", [False, True])
def test_missing_ffprobe_explains_environment_setup(cells, tmp_path, monkeypatch, fabric):
    (tmp_path / "samples").mkdir()
    (tmp_path / "pyproject.toml").write_text("[project]\nname='fixture'\n")
    (tmp_path / "lakehouse/Files/backfill/source-videos").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    with patch("shutil.which", return_value=None), pytest.raises(
        RuntimeError, match="ffprobe.*Conda ffmpeg.*restart",
    ):
        run_setup(cells["workspace-setup"], fabric, tmp_path / "lakehouse")
    assert not (tmp_path / "lakehouse/Files/backfill/config").exists()
    assert not (tmp_path / "samples/manifest-package-tutorial-inputs").exists()


def test_fabric_generates_identical_tutorial_metadata(
    cells, fabric_setup, tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    (repo / "samples").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='fixture'\n")
    monkeypatch.chdir(repo)
    local = run_setup(cells["workspace-setup"], False, tmp_path / "unused")
    for cell in ("write-catalog", "write-exception-inventory"):
        exec(cells[cell], fabric_setup)
        exec(cells[cell], local)
    for name in ("CATALOG_PATH", "EXCEPTION_INVENTORY_PATH"):
        assert fabric_setup[name].read_bytes() == local[name].read_bytes()
    with fabric_setup["CATALOG_PATH"].open(newline="") as handle:
        rows = list(fabric_setup["csv"].DictReader(handle))
    assert len(rows) == 3
    assert [row["frame_width"] for row in rows] == ["3626", "3840", "1920"]
    assert {row["source_path_prefix"] for row in rows} == {"samples/"}


@pytest.mark.parametrize("fabric", [False, True])
@pytest.mark.parametrize("prefix", ["camera-shortcut/", "footage/custom"])
def test_custom_prefix_updates_generated_metadata(cells, tmp_path, monkeypatch, fabric, prefix):
    (tmp_path / "samples").mkdir()
    (tmp_path / "pyproject.toml").write_text("[project]\nname='fixture'\n")
    (tmp_path / "lakehouse/Files/backfill/source-videos").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    namespace = run_setup(cells["workspace-setup"], fabric, tmp_path / "lakehouse", prefix)
    try:
        expected = prefix.rstrip("/") + "/"
        assert namespace["PARTITION_PREFIX"] == expected
        exec(cells["write-catalog"], namespace)
        exec(cells["write-exception-inventory"], namespace)
        with namespace["CATALOG_PATH"].open(newline="") as handle:
            rows = list(namespace["csv"].DictReader(handle))
        assert {row["source_path_prefix"] for row in rows} == {expected}
        assert [row["frame_width"] for row in rows] == ["3626", "3840", "1920"]
        with namespace["EXCEPTION_INVENTORY_PATH"].open(newline="") as handle:
            overrides = list(namespace["csv"].DictReader(handle))
        assert overrides == [{
            "video_relative_path": f"{expected}three_people_walking.mp4",
            "captured_at_utc": "2023-10-01T01:00:00Z",
        }]
    finally:
        if fabric:
            namespace["PREPARATION_STATE"].cleanup()


@pytest.mark.parametrize("prefix", ["", "../videos", "/videos", "abfss://container/videos"])
def test_invalid_prefix_fails_before_writing_metadata(cells, tmp_path, monkeypatch, prefix):
    (tmp_path / "Files/backfill/source-videos").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(CatalogError, match="Invalid relative path"):
        run_setup(cells["workspace-setup"], True, tmp_path, prefix)
    assert not (tmp_path / "Files/backfill/config").exists()


def test_fabric_preview_discovers_nested_shortcut_videos(cells, fabric_setup):
    root = fabric_setup["VIDEO_ROOT"]
    video = root / "samples/three_people_walking.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"video")
    runner = Mock(return_value=subprocess.CompletedProcess(
        [], 0, json.dumps({"streams": [{"width": 1920, "height": 1080}]}), "",
    ))
    fabric_setup["subprocess"] = SimpleNamespace(run=runner)
    exec(cells["ffprobe-preview"], fabric_setup)
    command = runner.call_args.args[0]
    assert command[0] == "ffprobe"
    assert command[-1] == str(video)
    assert runner.call_args.kwargs["cwd"] == root
    assert runner.call_args.kwargs["check"] is True


@pytest.mark.parametrize(
    "fabric_setup", ["samples/", "camera-shortcut/", "footage/custom"], indirect=True,
)
def test_fabric_prepares_and_validates_handoff_from_videos_only(cells, fabric_setup):
    prefix = fabric_setup["PARTITION_PREFIX"]
    video = fabric_setup["VIDEO_ROOT"] / prefix / "three_people_walking.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"video")
    unrelated = fabric_setup["VIDEO_ROOT"] / "outside-tutorial.mp4"
    unrelated.write_bytes(b"outside")
    assert not fabric_setup["CATALOG_PATH"].exists()
    assert not fabric_setup["EXCEPTION_INVENTORY_PATH"].exists()
    run_publisher = publisher.run_publisher

    def prepare(command, **kwargs):
        assert command[0] == fabric_setup["sys"].executable
        assert command[1] == "-c"
        assert kwargs["cwd"] == fabric_setup["VIDEO_ROOT"]
        output = io.StringIO()
        with redirect_stdout(output), patch.object(
            publisher, "run_publisher",
            side_effect=lambda config, **options: run_publisher(
                config, inspector=inspection, **options,
            ),
        ):
            result = publisher.prepare_main(command[3:])
        if kwargs["check"] and result:
            raise subprocess.CalledProcessError(result, command)
        return subprocess.CompletedProcess(command, result, output.getvalue(), "")

    runner = Mock(side_effect=prepare)
    fabric_setup["subprocess"] = SimpleNamespace(run=runner)
    with patch.object(publisher, "AzureDataLakeStorage") as storage:
        for cell in (
            "write-catalog", "prepare-first-pass", "inspect-rejections",
            "write-exception-inventory", "prepare-complete-package",
            "inspect-package", "verify-identities", "offline-validation",
        ):
            exec(cells[cell], fabric_setup)
    storage.assert_not_called()
    assert runner.call_count == 2
    first, complete = (call.args[0] for call in runner.call_args_list)
    assert first[first.index("--partition-prefix") + 1] == prefix
    assert "--inventory" not in first
    assert complete[complete.index("--inventory") + 1] == str(
        fabric_setup["EXCEPTION_INVENTORY_PATH"]
    )
    assert first[first.index("--checkpoint") + 1] == str(fabric_setup["CHECKPOINT_PATH"])
    package = fabric_setup["PACKAGE_DIR"]
    index = json.loads((package / "manifest-package.json").read_text())
    assert index["complete"] is True
    assert index["prepared"] == 1
    assert fabric_setup["generated_inventory"][0]["video_relative_path"] == video.relative_to(
        fabric_setup["VIDEO_ROOT"]
    ).as_posix()
    assert len(fabric_setup["validated_package"].plans) == 1
    assert fabric_setup["first_pass"].returncode == 2
    assert fabric_setup["complete_pass"].returncode == 0
    assert not list(package.rglob("*.sqlite3"))
    assert video.read_bytes() == b"video"
    assert unrelated.read_bytes() == b"outside"

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from people_counter import inference_profiles as profiles
from people_counter.local_spark import _config_from_work
from people_counter.sjd_control import normalize_registration_requests


@pytest.fixture
def model_tree(tmp_path):
    root = tmp_path / "models"
    for detector in ("rtdetr_v2_r18vd", "rtdetr_v2_r50vd"):
        for name in ("config.json", "preprocessor_config.json", "model.onnx", "model.safetensors"):
            path = root / "rtdetr_osnet" / detector / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(f"{detector}/{name}".encode())
    for name in ("osnet_ain_x0_25.pt", "osnet_ain_x0_25.onnx"):
        path = root / "rtdetr_osnet/libre_reid_osnet" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    return root


def resolved(model_tree, name="rtdetr-r18-onnx-cpu", **overrides):
    return profiles.prepare_profile(name, model_tree, overrides=overrides)


@pytest.mark.parametrize("detector", ["r18", "r50"])
@pytest.mark.parametrize("model_format", ["onnx", "pytorch"])
def test_preparation_selects_and_hashes_only_required_models(model_tree, detector, model_format):
    name = f"rtdetr-{detector}-{model_format}-cpu"
    profile = resolved(model_tree, name)
    assert profile["profile_id"] == name
    assert profile["detector_model"] == detector
    assert profile["model_format"] == model_format
    assert profile["device_variant"] == profile["device"] == "cpu"
    assert profile["batch_size"] == 1
    assert profile["sample_fps"] == 1.0
    assert profile["detection_threshold"] == 0.6
    assert profile["use_fp16"] is False
    hashes = profile["model_artifact_sha256"]
    assert len(hashes) == 4
    assert set(hashes) == set(profiles.artifact_paths(name))
    for relative, digest in hashes.items():
        assert digest == hashlib.sha256((model_tree / relative).read_bytes()).hexdigest()
    assert not any(("r18vd" if detector == "r50" else "r50vd") in path for path in hashes)
    assert len(profile["runtime_key"]) == 64
    assert len(profile["profile_sha256"]) == 64
    assert profile["runtime_key"] != profile["profile_sha256"]
    assert profiles.validate_profile(profile) == profile


def test_identities_are_derived_from_effective_settings_and_directed_line(model_tree):
    profile = resolved(model_tree)
    payload = {**profile, "line": [0, 100, 1919, 100]}
    first = profiles.configuration_sha256(payload)
    assert profile["profile_sha256"] == (
        "de93c80d7aea89898f64228b62cbb93360132eef6c253282578db2dcdf53f63e"
    )
    assert profile["runtime_key"] == (
        "491d9b12184fc26de6cdd8283bd651d801bb1020f9cf4993c0fb32901f3f2a77"
    )
    assert first == "0fe3b1a80ce5a84902cb0f2b9d05021bdc528c8d62875dc836115559a9bb5ca6"
    assert first == profiles.configuration_sha256(dict(reversed(list(payload.items()))))
    assert first != profiles.configuration_sha256({**payload, "line": [1919, 100, 0, 100]})
    for override in ({"batch_size": 2}, {"sample_fps": 2.0}):
        changed = resolved(model_tree, **override)
        assert changed["runtime_key"] != profile["runtime_key"]
        assert changed["profile_sha256"] != profile["profile_sha256"]
        assert profiles.configuration_sha256({**changed, "line": payload["line"]}) != first
    path = model_tree / profiles.artifact_paths(profile["profile_id"])[0]
    path.write_bytes(b"new reviewed model")
    assert resolved(model_tree)["runtime_key"] != profile["runtime_key"]


@pytest.mark.parametrize("change", [
    {"batch_size": 4}, {"sample_fps": 3.0}, {"detector_model": "r50"},
    {"model_format": "pytorch"}, {"profile_id": "unregistered"},
    {"device_variant": "gpu"}, {"use_fp16": True},
    {"runtime_key": "0" * 64}, {"profile_sha256": "0" * 64},
    {"profile_schema": "unknown"},
])
def test_resolved_profile_cannot_disagree_with_name_or_identity(model_tree, change):
    profile = resolved(model_tree)
    with pytest.raises(ValueError):
        profiles.validate_profile({**profile, **change})


@pytest.mark.parametrize("overrides", [
    {"batch_size": 3}, {"batch_size": True}, {"sample_fps": 0},
    {"sample_fps": float("inf")}, {"sample_fps": True},
    {"detector_model": "r50"}, {"device_variant": "gpu"},
])
def test_only_explicit_supported_overrides_are_allowed(model_tree, overrides):
    with pytest.raises(ValueError):
        resolved(model_tree, **overrides)


@pytest.mark.parametrize("threshold", [0.1, 0.35, 1])
def test_threshold_override_recalculates_managed_identities(model_tree, threshold):
    original = resolved(model_tree)
    changed = resolved(model_tree, detection_threshold=threshold)
    assert changed["detection_threshold"] == threshold
    assert changed["runtime_key"] != original["runtime_key"]
    assert changed["profile_sha256"] != original["profile_sha256"]
    assert profiles.validate_profile(changed) == changed


@pytest.mark.parametrize("threshold", [0, 0.099, -0.01, 1.01, float("nan"), float("inf"), True, "0.5"])
def test_threshold_override_rejects_invalid_values(model_tree, threshold):
    with pytest.raises(ValueError, match="detection_threshold"):
        resolved(model_tree, detection_threshold=threshold)


@pytest.mark.parametrize("name", ["supervision", "rfdetr-botsort", "rtdetr-r99-onnx-cpu"])
def test_unsupported_fabric_profiles_fail_before_model_access(tmp_path, name):
    with pytest.raises(ValueError, match="Unsupported"):
        profiles.prepare_profile(name, tmp_path / "does-not-exist")


def test_missing_or_unexpected_model_hashes_are_rejected(model_tree):
    profile = resolved(model_tree)
    for hashes in ({}, {**profile["model_artifact_sha256"], "other": "0" * 64}):
        with pytest.raises(ValueError, match="artifact"):
            profiles.resolve_profile(profile["profile_id"], hashes)
    missing = model_tree / profiles.artifact_paths(profile["profile_id"])[0]
    missing.unlink()
    with pytest.raises(FileNotFoundError):
        resolved(model_tree)


@pytest.mark.parametrize("line", [
    None, "1234", [0, 1, 2], [0, 1, 2, 3, 4], [False, 1, 2, 3],
    [0, 1, "two", 3], [0, 1, float("nan"), 3], [0, 1, float("inf"), 3],
])
def test_configuration_hash_rejects_invalid_directed_line(model_tree, line):
    with pytest.raises(ValueError, match="four finite coordinates"):
        profiles.configuration_sha256({**resolved(model_tree), "line": line})


def test_configuration_hash_requires_managed_profile():
    with pytest.raises(ValueError, match="managed inference profile"):
        profiles.configuration_sha256({"line": [0, 1, 2, 3]})


@pytest.mark.parametrize("digest", [None, "x" * 64, "a" * 63, "A" * 64])
def test_required_artifact_digests_must_be_lowercase_sha256(model_tree, digest):
    profile = resolved(model_tree)
    hashes = dict(profile["model_artifact_sha256"])
    hashes[next(iter(hashes))] = digest
    with pytest.raises(ValueError, match="artifact SHA-256"):
        profiles.resolve_profile(profile["profile_id"], hashes)


@pytest.mark.parametrize("change", [
    {"profile_overrides": []}, {"profile_overrides": None},
    {"profile_id": None}, {"model_artifact_sha256": None},
])
def test_profile_payload_shape_is_validated(model_tree, change):
    with pytest.raises(ValueError):
        profiles.validate_profile({**resolved(model_tree), **change})


def test_invalid_override_fails_before_reading_model_files(tmp_path):
    with patch.object(Path, "open", side_effect=AssertionError("Model files must not be opened")):
        with pytest.raises(ValueError, match="batch_size"):
            profiles.prepare_profile("rtdetr-r18-onnx-cpu", tmp_path, overrides={"batch_size": 3})


def test_model_hashing_uses_bounded_streaming_reads(model_tree):
    original_open = Path.open
    sizes = []

    @contextmanager
    def bounded_open(path, *args, **kwargs):
        with original_open(path, *args, **kwargs) as stream:
            reader = Mock()

            def read(size):
                assert type(size) is int and 0 < size <= 8 * 1024 * 1024
                sizes.append(size)
                return stream.read(size)

            reader.read.side_effect = read
            yield reader

    with patch.object(Path, "open", bounded_open):
        profile = resolved(model_tree)
    assert len(profile["model_artifact_sha256"]) == 4
    assert len(sizes) >= 8


def test_worker_uses_the_same_resolved_profile_and_rejects_drift(model_tree):
    profile = resolved(model_tree, "rtdetr-r50-onnx-cpu", batch_size=4, sample_fps=2.0)
    work = {**profile, "source_video": "video.mp4", "models_dir": str(model_tree)}
    config = _config_from_work(work)
    assert config.detector_model == "r50"
    assert config.batch_size == 4
    assert config.sample_fps == 2.0
    assert config.model_format == "onnx"
    assert config.models_dir == model_tree
    with patch("people_counter.local_spark._verified_video") as video, pytest.raises(
        ValueError, match="profile"
    ):
        _config_from_work({**work, "sample_fps": 9.0})
    video.assert_not_called()
    from people_counter.fabric_sjd_runtime import _model_artifact_paths

    assert _model_artifact_paths(work, validation_error=ValueError) == profiles.artifact_paths(
        profile["profile_id"]
    )
    with pytest.raises(ValueError, match="profile"):
        _model_artifact_paths({**work, "detector_model": "r18"}, validation_error=ValueError)


@pytest.mark.parametrize("field", ["runtime_key", "config_sha256"])
def test_registration_rejects_mismatched_computed_identities(model_tree, field):
    payload = {**resolved(model_tree), "line": [0, 100, 1919, 100]}
    record = {
        "work_id": "work", "payload": payload, "duration_seconds": 5,
        "runtime_key": payload["runtime_key"],
        "config_sha256": profiles.configuration_sha256(payload),
        "release_digest": "a" * 64,
    }
    assert normalize_registration_requests([record])[0]["config_sha256"] == record["config_sha256"]
    with pytest.raises(ValueError, match=field):
        normalize_registration_requests([{**record, field: "b" * 64}])


def test_legacy_work_remains_executable_without_a_managed_profile():
    config = _config_from_work({"source_video": "video.mp4", "sample_fps": 3.0})
    assert config.sample_fps == 3.0


def test_claim_validation_rechecks_managed_configuration_hash(model_tree, tmp_path):
    from people_counter.sjd_control import SQLiteControlStore
    from people_counter.sjd_process import verify_envelope

    payload = {**resolved(model_tree), "line": [0, 100, 1919, 100]}
    store = SQLiteControlStore(tmp_path / "control.sqlite", tmp_path / "content")
    store.register(
        "managed-work", payload, runtime_key=payload["runtime_key"],
        duration_seconds=5, config_sha256=profiles.configuration_sha256(payload),
        release_digest="a" * 64,
    )
    batch = store.claim(
        "owner", max_items=1, lease_seconds=1000,
        minimum_speed_x=1, safety_factor=1, margin_seconds=1,
    )
    assert batch is not None
    envelope = store.load_claim_envelope(batch.batch_id)
    verified = verify_envelope(
        envelope, batch_id=batch.batch_id, envelope_sha256=batch.envelope_sha256,
    )
    assert verified.items[0].config_sha256 == profiles.configuration_sha256(payload)
    envelope["items"][0]["config_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="config_sha256"):
        verify_envelope(
            envelope, batch_id=batch.batch_id, envelope_sha256=batch.envelope_sha256,
        )


@pytest.fixture
def binding_path(tmp_path):
    binding = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "environment_id": "33333333-3333-3333-3333-333333333333",
        "control_sjd_id": "44444444-4444-4444-4444-444444444444",
        "intake_mode": "explicit", "storage_account": "account",
        "filesystem": "footage", "shortcut_path": "Files/videos",
    }
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(json.dumps(binding))
    return binding_path


def test_prepare_catalog_creates_named_targets_without_manual_digests(model_tree, tmp_path, binding_path):
    from people_counter import backfill_operator as operator

    output = tmp_path / "catalog.json"
    assert operator.prepare_catalog_main([
        "--target", "production", "--target-config", str(binding_path),
        "--models-dir", str(model_tree),
        "--profile", "rtdetr-r18-onnx-cpu", "--profile", "rtdetr-r50-onnx-cpu",
        "--output", str(output),
    ]) == 0
    catalog = json.loads(output.read_text())
    assert set(catalog["targets"]) == {"production"}
    assert "config_sha256" not in catalog["targets"]["production"]["binding"]
    config = operator.load_target(output, "production", "rtdetr-r50-onnx-cpu")
    assert config.profile["detector_model"] == "r50"
    assert len(config.profile["model_artifact_sha256"]) == 4
    with pytest.raises(ValueError, match="profile"):
        operator.load_target(output, "production", "unknown")
    with pytest.raises(ValueError, match="target"):
        operator.load_target(output, "unknown", "rtdetr-r18-onnx-cpu")
    old = output.read_bytes()
    (model_tree / profiles.artifact_paths("rtdetr-r18-onnx-cpu")[0]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="overwrite"):
        operator.prepare_catalog_main([
            "--target", "production", "--target-config", str(binding_path),
            "--models-dir", str(model_tree), "--profile", "rtdetr-r18-onnx-cpu",
            "--output", str(output),
        ])
    assert output.read_bytes() == old


@pytest.mark.parametrize("flag", ["--target", "--target-config", "--models-dir", "--profile"])
def test_catalog_compiler_requires_administrator_inputs(model_tree, tmp_path, binding_path, flag):
    from people_counter import backfill_operator as operator

    args = [
        "--target", "production", "--target-config", str(binding_path),
        "--models-dir", str(model_tree), "--profile", "rtdetr-r18-onnx-cpu",
        "--output", str(tmp_path / "catalog.json"),
    ]
    position = args.index(flag)
    del args[position:position + 2]
    with pytest.raises(SystemExit) as error:
        operator.prepare_catalog_main(args)
    assert error.value.code == 2
    assert not (tmp_path / "catalog.json").exists()


def test_catalog_compiler_rejects_unsupported_model_in_cli(model_tree, tmp_path, binding_path):
    from people_counter import backfill_operator as operator

    with pytest.raises(SystemExit) as error:
        operator.prepare_catalog_main([
            "--target", "production", "--target-config", str(binding_path),
            "--models-dir", str(model_tree), "--profile", "supervision",
            "--output", str(tmp_path / "catalog.json"),
        ])
    assert error.value.code == 2


@pytest.mark.parametrize("raw", [None, [], {"profile": {"sample_fps": 9}}])
def test_catalog_compiler_rejects_nonbinding_configuration(model_tree, tmp_path, binding_path, raw):
    from people_counter import backfill_operator as operator

    document = {**json.loads(binding_path.read_text()), **raw} if isinstance(raw, dict) else raw
    binding_path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="deployment binding fields only"):
        operator.prepare_catalog_main([
            "--target", "production", "--target-config", str(binding_path),
            "--models-dir", str(model_tree), "--profile", "rtdetr-r18-onnx-cpu",
            "--output", str(tmp_path / "catalog.json"),
        ])
    assert not (tmp_path / "catalog.json").exists()


@pytest.mark.parametrize("output", [None, "deployment/catalogs/approved.json"])
def test_catalog_compiler_creates_output_directories_and_reports_output(
    model_tree, tmp_path, binding_path, monkeypatch, capsys, output,
):
    from people_counter import backfill_operator as operator

    monkeypatch.chdir(tmp_path)
    args = [
        "--target", "production", "--target-config", str(binding_path),
        "--models-dir", str(model_tree), "--profile", "rtdetr-r18-onnx-cpu",
    ]
    if output is not None:
        args += ["--output", output]
    assert operator.prepare_catalog_main(args) == 0
    expected = Path(output) if output is not None else operator.DEFAULT_CATALOG
    assert (tmp_path / expected).is_file()
    assert json.loads(capsys.readouterr().out) == {
        "catalog": str(expected), "target": "production",
        "profiles": ["rtdetr-r18-onnx-cpu"],
    }

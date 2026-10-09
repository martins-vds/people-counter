"""Direct unit tests for the private localization helpers extracted from
``_localize_process_inputs`` in :mod:`people_counter.fabric_candidate_a_jobs`.

These complement the end-to-end tests in ``test_fabric_candidate_a_phase1.py``
by exercising each helper's branches, defaults, and exact argument wiring in
isolation -- the level of detail needed to kill structural mutation gaps
(flipped conditionals, wrong defaults, dropped arguments, off-by-one slicing)
that an end-to-end-only test suite cannot reliably observe.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

import people_counter.fabric_candidate_a_jobs as candidate_jobs
from people_counter.fabric_candidate_a_jobs import (
    _candidate_a_model_paths,
    _check_synthetic_model_identity,
    _check_synthetic_video_identity,
    _fallback_localize,
    _load_runtime_release_evidence,
    _resolve_localized_models,
    _resolve_localized_video,
)
from people_counter.fabric_input_resolver import InputBackend, stream_sha256
from people_counter.fabric_source_cache import LocalizedSourceCache


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _stream_from(path: Path):
    def _stream(_spark: Any, _uri: str, destination: Path) -> None:
        with path.open("rb") as source, destination.open("xb") as target:
            while chunk := source.read(1024):
                target.write(chunk)

    return _stream


class _Boom(Exception):
    """Distinct sentinel validation_error type for assert-on-type tests."""


# --------------------------------------------------------------------------
# _check_synthetic_video_identity
# --------------------------------------------------------------------------


def test_check_synthetic_video_identity_is_noop_outside_shadow_synthetic() -> None:
    # Even with a flagrantly mismatched/missing route_identity, a non-SHADOW
    # route_mode must never raise -- this check is gated entirely on mode.
    _check_synthetic_video_identity(
        {"work_id": "a"},
        {"source_sha256": "x"},
        route_mode=None,
        route_identity=None,
        validation_error=_Boom,
    )
    _check_synthetic_video_identity(
        {"work_id": "a"},
        {"source_sha256": "x"},
        route_mode="LIVE",
        route_identity={"work_id": "b"},
        validation_error=_Boom,
    )


def test_check_synthetic_video_identity_accepts_exact_match() -> None:
    item = {"work_id": "work-1", "config_sha256": "c" * 64}
    payload = {"source_sha256": "s" * 64}
    identity = {
        "work_id": "work-1",
        "source_sha256": "s" * 64,
        "config_sha256": "c" * 64,
    }
    _check_synthetic_video_identity(
        item,
        payload,
        route_mode="SHADOW_SYNTHETIC",
        route_identity=identity,
        validation_error=_Boom,
    )


@pytest.mark.parametrize(
    "field_path",
    ["work_id", "source_sha256", "config_sha256"],
)
def test_check_synthetic_video_identity_rejects_each_mismatched_field(
    field_path: str,
) -> None:
    item = {"work_id": "work-1", "config_sha256": "c" * 64}
    payload = {"source_sha256": "s" * 64}
    identity = {
        "work_id": "work-1",
        "source_sha256": "s" * 64,
        "config_sha256": "c" * 64,
    }
    mismatched = {**identity, field_path: "mismatch"}
    with pytest.raises(_Boom) as exc_info:
        _check_synthetic_video_identity(
            item,
            payload,
            route_mode="SHADOW_SYNTHETIC",
            route_identity=mismatched,
            validation_error=_Boom,
        )
    assert str(exc_info.value) == "synthetic video/config identity differs"


def test_check_synthetic_video_identity_asserts_identity_present() -> None:
    with pytest.raises(AssertionError):
        _check_synthetic_video_identity(
            {"work_id": "work-1"},
            {"source_sha256": "s" * 64},
            route_mode="SHADOW_SYNTHETIC",
            route_identity=None,
            validation_error=_Boom,
        )


# --------------------------------------------------------------------------
# _resolve_localized_video
# --------------------------------------------------------------------------


def test_resolve_localized_video_rejects_non_default_lakehouse_source() -> None:
    with pytest.raises(_Boom) as exc_info:
        _resolve_localized_video(
            spark=SimpleNamespace(),
            payload={"source_video": "/production/video.mp4"},
            backend=InputBackend.FABRIC_DIRECT,
            mount_root="/lakehouse/default",
            cache=None,
            staged_sources={},
            broadcast_digests=set(),
            validation_error=_Boom,
        )
    assert str(exc_info.value) == (
        "Candidate A source_video must use the default Lakehouse"
    )


def test_resolve_localized_video_direct_backend_hashes_resolved_mount_path(
    tmp_path: Path,
) -> None:
    mount_root = tmp_path / "lakehouse"
    relative = "Files/_canary/candidate-a/v1/assets/synthetic.mp4"
    video_path = mount_root / relative
    video_path.parent.mkdir(parents=True)
    video_path.write_bytes(b"video-bytes")
    payload = {
        "source_video": f"/lakehouse/default/{relative}",
        "source_sha256": _sha256_bytes(b"video-bytes"),
    }

    relative_out, source_sha, localized_name = _resolve_localized_video(
        spark=SimpleNamespace(),
        payload=payload,
        backend=InputBackend.FABRIC_DIRECT,
        mount_root=str(mount_root),
        cache=None,
        staged_sources={},
        broadcast_digests=set(),
        validation_error=_Boom,
    )
    assert relative_out == relative
    assert source_sha == _sha256_bytes(b"video-bytes")
    assert localized_name is None


def test_resolve_localized_video_direct_backend_rejects_digest_mismatch(
    tmp_path: Path,
) -> None:
    mount_root = tmp_path / "lakehouse"
    relative = "Files/_canary/candidate-a/v1/assets/synthetic.mp4"
    video_path = mount_root / relative
    video_path.parent.mkdir(parents=True)
    video_path.write_bytes(b"video-bytes")
    payload = {
        "source_video": f"/lakehouse/default/{relative}",
        "source_sha256": "0" * 64,
    }
    with pytest.raises(_Boom) as exc_info:
        _resolve_localized_video(
            spark=SimpleNamespace(),
            payload=payload,
            backend=InputBackend.FABRIC_DIRECT,
            mount_root=str(mount_root),
            cache=None,
            staged_sources={},
            broadcast_digests=set(),
            validation_error=_Boom,
        )
    assert str(exc_info.value) == (
        "localized video digest differs from registered input"
    )


def test_resolve_localized_video_fallback_backend_uses_exact_abfss_uri_and_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "synthetic.mp4"
    source.write_bytes(b"video-bytes")
    monkeypatch.setattr(
        candidate_jobs,
        "_stream_hadoop_uri_to_local",
        _stream_from(source),
    )
    relative = "Files/_canary/candidate-a/v1/assets/synthetic.mp4"
    payload = {
        "source_video": f"/lakehouse/default/{relative}",
        "source_sha256": _sha256_bytes(b"video-bytes"),
    }
    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    cache = LocalizedSourceCache(
        tmp_path / "cache", max_entries=10, max_total_bytes=10_000
    )

    relative_out, source_sha, localized_name = _resolve_localized_video(
        spark=spark,
        payload=payload,
        backend=InputBackend.FABRIC_FALLBACK,
        mount_root=None,
        cache=cache,
        staged_sources={},
        broadcast_digests=set(),
        validation_error=_Boom,
    )
    assert relative_out == relative
    assert source_sha == _sha256_bytes(b"video-bytes")
    assert localized_name == _sha256_bytes(b"video-bytes")
    assert spark.sparkContext.addFile.call_count == 1
    assert spark.sparkContext.addFile.call_args.args == (
        str(cache.root / _sha256_bytes(b"video-bytes")),
    )


# --------------------------------------------------------------------------
# _candidate_a_model_paths
# --------------------------------------------------------------------------


def test_candidate_a_model_paths_rejects_non_rtdetr_osnet_pipeline() -> None:
    with pytest.raises(_Boom) as exc_info:
        _candidate_a_model_paths(
            {"pipeline": "rfdetr-botsort"}, validation_error=_Boom
        )
    assert str(exc_info.value) == (
        "Candidate A v1 localizer supports RT-DETR/OSNet only"
    )


def test_candidate_a_model_paths_defaults_pipeline_when_key_is_absent() -> None:
    # Omitting "pipeline" entirely must fall back to the "rtdetr-osnet"
    # default and succeed -- not raise, and not silently default to
    # ``None``/something else that would always reject.
    assert _candidate_a_model_paths({}, validation_error=_Boom) == (
        "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
        "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
        "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
    )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {},
            (
                "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
                "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
                "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
                "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
            ),
        ),
        (
            {"detector_model": "r18", "model_format": "pytorch"},
            (
                "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
                "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
                "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
                "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
            ),
        ),
        (
            {"detector_model": "r50", "model_format": "pytorch"},
            (
                "rtdetr_osnet/rtdetr_v2_r50vd/config.json",
                "rtdetr_osnet/rtdetr_v2_r50vd/preprocessor_config.json",
                "rtdetr_osnet/rtdetr_v2_r50vd/model.safetensors",
                "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
            ),
        ),
        (
            {"detector_model": "r18", "model_format": "onnx"},
            (
                "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
                "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
                "rtdetr_osnet/rtdetr_v2_r18vd/model.onnx",
                "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx",
            ),
        ),
        (
            {"detector_model": "r50", "model_format": "onnx"},
            (
                "rtdetr_osnet/rtdetr_v2_r50vd/config.json",
                "rtdetr_osnet/rtdetr_v2_r50vd/preprocessor_config.json",
                "rtdetr_osnet/rtdetr_v2_r50vd/model.onnx",
                "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx",
            ),
        ),
    ],
)
def test_candidate_a_model_paths_covers_every_detector_format_combination(
    payload: dict[str, str], expected: tuple[str, ...]
) -> None:
    full_payload = {"pipeline": "rtdetr-osnet", **payload}
    assert _candidate_a_model_paths(full_payload, validation_error=_Boom) == expected


# --------------------------------------------------------------------------
# _resolve_localized_models
# --------------------------------------------------------------------------


_MODEL_PATHS = (
    "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
    "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
)


def test_resolve_localized_models_rejects_non_mapping_expected_artifacts() -> None:
    with pytest.raises(_Boom) as exc_info:
        _resolve_localized_models(
            spark=SimpleNamespace(),
            payload={"model_artifact_sha256": ["not", "a", "mapping"]},
            model_paths=_MODEL_PATHS,
            backend=InputBackend.FABRIC_DIRECT,
            mount_root="/lakehouse/default",
            cache=None,
            staged_sources={},
            broadcast_digests=set(),
            validation_error=_Boom,
        )
    assert str(exc_info.value) == "model_artifact_sha256 must be an object"


def test_resolve_localized_models_direct_backend_hashes_every_path(
    tmp_path: Path,
) -> None:
    mount_root = tmp_path / "lakehouse"
    contents = {
        _MODEL_PATHS[0]: b"config-bytes",
        _MODEL_PATHS[1]: b"detector-bytes",
    }
    for relative, content in contents.items():
        path = mount_root / "Files/models" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    localized = _resolve_localized_models(
        spark=SimpleNamespace(),
        payload={
            "model_artifact_sha256": {
                relative: _sha256_bytes(content)
                for relative, content in contents.items()
            }
        },
        model_paths=_MODEL_PATHS,
        backend=InputBackend.FABRIC_DIRECT,
        mount_root=str(mount_root),
        cache=None,
        staged_sources={},
        broadcast_digests=set(),
        validation_error=_Boom,
    )
    assert len(localized) == 2
    for relative, content in contents.items():
        assert localized[relative] == {"sha256": _sha256_bytes(content)}


def test_resolve_localized_models_rejects_digest_mismatch_against_cpu_profile(
    tmp_path: Path,
) -> None:
    mount_root = tmp_path / "lakehouse"
    path = mount_root / "Files/models" / _MODEL_PATHS[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"config-bytes")

    with pytest.raises(_Boom) as exc_info:
        _resolve_localized_models(
            spark=SimpleNamespace(),
            payload={
                "model_artifact_sha256": {_MODEL_PATHS[0]: "0" * 64}
            },
            model_paths=(_MODEL_PATHS[0],),
            backend=InputBackend.FABRIC_DIRECT,
            mount_root=str(mount_root),
            cache=None,
            staged_sources={},
            broadcast_digests=set(),
            validation_error=_Boom,
        )
    assert str(exc_info.value) == "localized model digest differs from CPU profile"


def test_resolve_localized_models_accepts_matching_expected_digest(
    tmp_path: Path,
) -> None:
    mount_root = tmp_path / "lakehouse"
    path = mount_root / "Files/models" / _MODEL_PATHS[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"config-bytes")

    localized = _resolve_localized_models(
        spark=SimpleNamespace(),
        payload={
            "model_artifact_sha256": {
                _MODEL_PATHS[0]: _sha256_bytes(b"config-bytes")
            }
        },
        model_paths=(_MODEL_PATHS[0],),
        backend=InputBackend.FABRIC_DIRECT,
        mount_root=str(mount_root),
        cache=None,
        staged_sources={},
        broadcast_digests=set(),
        validation_error=_Boom,
    )
    assert localized[_MODEL_PATHS[0]]["sha256"] == _sha256_bytes(b"config-bytes")


def test_resolve_localized_models_fallback_backend_populates_localized_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "config.json"
    source.write_bytes(b"config-bytes")
    monkeypatch.setattr(
        candidate_jobs,
        "_stream_hadoop_uri_to_local",
        _stream_from(source),
    )
    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    cache = LocalizedSourceCache(
        tmp_path / "cache", max_entries=10, max_total_bytes=10_000
    )

    localized = _resolve_localized_models(
        spark=spark,
        payload={
            "model_artifact_sha256": {
                _MODEL_PATHS[0]: _sha256_bytes(b"config-bytes")
            }
        },
        model_paths=(_MODEL_PATHS[0],),
        backend=InputBackend.FABRIC_FALLBACK,
        mount_root=None,
        cache=cache,
        staged_sources={},
        broadcast_digests=set(),
        validation_error=_Boom,
    )
    assert localized[_MODEL_PATHS[0]] == {
        "localized_name": _sha256_bytes(b"config-bytes"),
        "sha256": _sha256_bytes(b"config-bytes"),
    }
    assert spark.sparkContext.addFile.call_args.args == (
        str(cache.root / _sha256_bytes(b"config-bytes")),
    )


# --------------------------------------------------------------------------
# _check_synthetic_model_identity
# --------------------------------------------------------------------------


def test_check_synthetic_model_identity_is_noop_outside_shadow_synthetic() -> None:
    _check_synthetic_model_identity(
        {"a": {"sha256": "x"}},
        route_mode=None,
        route_identity=None,
        validation_error=_Boom,
    )


def test_check_synthetic_model_identity_excludes_config_files_and_matches() -> None:
    from people_counter.fabric_production_routing import sha256_json

    localized_models = {
        "rtdetr_osnet/rtdetr_v2_r18vd/config.json": {"sha256": "config-sha"},
        "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json": {
            "sha256": "preproc-sha"
        },
        "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": {"sha256": "det-sha"},
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt": {"sha256": "reid-sha"},
    }
    model_sha256 = sha256_json(
        {
            "schema": "people-counter-fixed-model-artifacts-v1",
            "artifacts": {
                "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors":
                    "det-sha",
                "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt":
                    "reid-sha",
            },
        }
    )
    _check_synthetic_model_identity(
        localized_models,
        route_mode="SHADOW_SYNTHETIC",
        route_identity={"model_sha256": model_sha256},
        validation_error=_Boom,
    )
    # Changing only a config digest must NOT affect the pinned identity --
    # config/preprocessor files are intentionally excluded from the hash.
    localized_models_changed_config = {
        **localized_models,
        "rtdetr_osnet/rtdetr_v2_r18vd/config.json": {"sha256": "different-config"},
    }
    _check_synthetic_model_identity(
        localized_models_changed_config,
        route_mode="SHADOW_SYNTHETIC",
        route_identity={"model_sha256": model_sha256},
        validation_error=_Boom,
    )
    # The preprocessor config must be excluded too -- not just the model
    # config -- so changing *only* it must also leave the identity intact.
    localized_models_changed_preprocessor = {
        **localized_models,
        "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json": {
            "sha256": "different-preprocessor"
        },
    }
    _check_synthetic_model_identity(
        localized_models_changed_preprocessor,
        route_mode="SHADOW_SYNTHETIC",
        route_identity={"model_sha256": model_sha256},
        validation_error=_Boom,
    )


def test_check_synthetic_model_identity_rejects_mismatch() -> None:
    localized_models = {
        "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": {"sha256": "det-sha"},
    }
    with pytest.raises(_Boom) as exc_info:
        _check_synthetic_model_identity(
            localized_models,
            route_mode="SHADOW_SYNTHETIC",
            route_identity={"model_sha256": "0" * 64},
            validation_error=_Boom,
        )
    assert str(exc_info.value) == "synthetic model identity differs"


def test_check_synthetic_model_identity_asserts_identity_present() -> None:
    with pytest.raises(AssertionError):
        _check_synthetic_model_identity(
            {},
            route_mode="SHADOW_SYNTHETIC",
            route_identity=None,
            validation_error=_Boom,
        )


# --------------------------------------------------------------------------
# _fallback_localize
# --------------------------------------------------------------------------


def test_fallback_localize_returns_content_addressed_name_and_digest(
    tmp_path: Path,
) -> None:
    source = tmp_path / "config.json"
    source.write_bytes(b"config-bytes")
    digest = _sha256_bytes(b"config-bytes")
    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    cache = LocalizedSourceCache(
        tmp_path / "cache", max_entries=10, max_total_bytes=10_000
    )

    localized_name, digest = _fallback_localize(
        spark,
        cache,
        abfss_uri="abfss://ws@onelake/lh/Files/models/config.json",
        original_basename="config.json",
        expected_sha256=digest,
        staged_sources={},
        broadcast_digests=set(),
        stream_remote=_stream_from(source),
    )
    assert localized_name == digest
    assert spark.sparkContext.addFile.call_count == 1
    assert spark.sparkContext.addFile.call_args.args == (str(cache.root / digest),)


def test_fallback_localize_stages_each_distinct_uri_exactly_once(
    tmp_path: Path,
) -> None:
    source = tmp_path / "config.json"
    source.write_bytes(b"config-bytes")
    digest = _sha256_bytes(b"config-bytes")
    stream_calls: list[str] = []

    def _stream(_spark: Any, uri: str, destination: Path) -> None:
        stream_calls.append(uri)
        _stream_from(source)(_spark, uri, destination)

    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    cache = LocalizedSourceCache(
        tmp_path / "cache", max_entries=10, max_total_bytes=10_000
    )
    staged_sources: dict[str, Any] = {}
    broadcast_digests: set[str] = set()

    for _ in range(3):
        _fallback_localize(
            spark,
            cache,
            abfss_uri="abfss://ws@onelake/lh/Files/models/config.json",
            original_basename="config.json",
            expected_sha256=digest,
            staged_sources=staged_sources,
            broadcast_digests=broadcast_digests,
            stream_remote=_stream,
        )
    assert stream_calls == [
        "abfss://ws@onelake/lh/Files/models/config.json"
    ]
    assert spark.sparkContext.addFile.call_count == 1


def test_fallback_localize_distinguishes_same_basename_different_content(
    tmp_path: Path,
) -> None:
    source_a = tmp_path / "a"
    source_b = tmp_path / "b"
    source_a.mkdir()
    source_b.mkdir()
    (source_a / "config.json").write_bytes(b"content-a")
    (source_b / "config.json").write_bytes(b"content-b")

    added: list[str] = []
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=lambda path: added.append(path))
    )
    cache = LocalizedSourceCache(
        tmp_path / "cache", max_entries=10, max_total_bytes=10_000
    )
    staged_sources: dict[str, Any] = {}
    broadcast_digests: set[str] = set()

    name_a, digest_a = _fallback_localize(
        spark,
        cache,
        abfss_uri="abfss://ws@onelake/lh/a/config.json",
        original_basename="config.json",
        expected_sha256=_sha256_bytes(b"content-a"),
        staged_sources=staged_sources,
        broadcast_digests=broadcast_digests,
        stream_remote=_stream_from(source_a / "config.json"),
    )
    name_b, digest_b = _fallback_localize(
        spark,
        cache,
        abfss_uri="abfss://ws@onelake/lh/b/config.json",
        original_basename="config.json",
        expected_sha256=_sha256_bytes(b"content-b"),
        staged_sources=staged_sources,
        broadcast_digests=broadcast_digests,
        stream_remote=_stream_from(source_b / "config.json"),
    )
    assert digest_a != digest_b
    assert name_a != name_b
    assert digest_a == _sha256_bytes(b"content-a")
    assert digest_b == _sha256_bytes(b"content-b")
    assert {Path(path).name for path in added} == {digest_a, digest_b}


def test_fallback_localize_rejects_corrupt_stream_before_addfile(
    tmp_path: Path,
) -> None:
    source = tmp_path / "corrupt.bin"
    source.write_bytes(b"corrupt")
    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    cache = LocalizedSourceCache(
        tmp_path / "cache", max_entries=10, max_total_bytes=10_000
    )
    with pytest.raises(Exception, match="SHA-256 mismatch"):
        _fallback_localize(
            spark,
            cache,
            abfss_uri="abfss://ws@onelake/lh/video.mp4",
            original_basename="video.mp4",
            expected_sha256="0" * 64,
            staged_sources={},
            broadcast_digests=set(),
            stream_remote=_stream_from(source),
        )
    spark.sparkContext.addFile.assert_not_called()


def test_real_local_spark_fallback_is_collision_safe_before_staging(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyspark.sql")
    from pyspark import SparkFiles
    from people_counter.local_spark import create_local_spark_session

    spark = create_local_spark_session(
        master="local[2]",
        app_name="candidate-a-collision-proof",
        correlation_id="candidate-a-collision-proof",
    )
    try:
        sources: list[Path] = []
        for directory, name, content in (
            ("model-a", "config.json", b"model-config-a"),
            ("model-b", "config.json", b"model-config-b"),
            ("model-c-identical", "config.json", b"model-config-a"),
            ("video-a", "same.mp4", b"video-a"),
            ("video-b", "same.mp4", b"video-b" * (512 * 1024 + 1)),
        ):
            path = tmp_path / directory / name
            path.parent.mkdir()
            path.write_bytes(content)
            sources.append(path)
        cache = LocalizedSourceCache(
            tmp_path / "cache",
            max_entries=10,
            max_total_bytes=16 * 1024 * 1024,
        )
        staged_sources: dict[str, Any] = {}
        broadcast_digests: set[str] = set()
        alias_root = (tmp_path / "distributed-aliases").as_uri()
        localized: list[tuple[str, str]] = []
        for source in sources:
            digest, _ = stream_sha256(source)
            localized.append(
                _fallback_localize(
                    spark,
                    cache,
                    abfss_uri=source.as_uri(),
                    original_basename=source.name,
                    expected_sha256=digest,
                    staged_sources=staged_sources,
                    broadcast_digests=broadcast_digests,
                    distributed_alias_root_uri=alias_root,
                )
            )
        repeated = _fallback_localize(
            spark,
            cache,
            abfss_uri=sources[0].as_uri(),
            original_basename=sources[0].name,
            expected_sha256=localized[0][1],
            staged_sources=staged_sources,
            broadcast_digests=broadcast_digests,
            distributed_alias_root_uri=alias_root,
        )
        assert repeated == localized[0]
        assert len({name for name, _ in localized}) == 4
        assert localized[0] == localized[2]
        assert len(broadcast_digests) == 4

        corrupt = tmp_path / "corrupt" / "same.mp4"
        corrupt.parent.mkdir()
        corrupt.write_bytes(b"corrupt-before-add-file")
        with pytest.raises(Exception, match="SHA-256 mismatch"):
            _fallback_localize(
                spark,
                cache,
                abfss_uri=corrupt.as_uri(),
                original_basename=corrupt.name,
                expected_sha256="0" * 64,
                staged_sources=staged_sources,
                broadcast_digests=broadcast_digests,
                distributed_alias_root_uri=alias_root,
            )
        assert len(broadcast_digests) == 4

        names = [name for name, _ in localized]
        observed = spark.sparkContext.parallelize(names, 2).map(
            lambda name: (
                name,
                hashlib.sha256(
                    Path(SparkFiles.get(name)).read_bytes()
                ).hexdigest(),
            )
        ).collect()
        assert dict(observed) == dict(localized)
    finally:
        spark.stop()


def test_release_evidence_rejects_nonfixed_paths_before_reading() -> None:
    files = MagicMock()
    with pytest.raises(ValueError, match="fixed release identity"):
        _load_runtime_release_evidence(
            files,
            manifest_path="Files/arbitrary/manifest.json",
            manifest_sha256="a" * 64,
            receipt_path="Files/arbitrary/receipt.json",
            receipt_sha256="b" * 64,
        )
    files.read_text.assert_not_called()

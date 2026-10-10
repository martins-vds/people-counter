"""Installed-wheel entry points for Candidate A Fabric Spark jobs."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, NamedTuple, Sequence

from people_counter.fabric_candidate_a import (
    LAKEHOUSE_ID,
    WORKSPACE_ID,
    FabricCandidateAConfig,
)
from people_counter.fabric_input_resolver import (
    InputBackend,
    content_addressed_name,
    link_or_stream_copy,
    resolve_direct_mounted_path,
    select_input_backend,
    stream_sha256,
    verify_sha256,
)
from people_counter.fabric_source_cache import LocalizedSourceCache


def _spark() -> Any:
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def _consumer_probe_profile(
    envelope: Mapping[str, Any],
    *,
    planned_concurrent_tasks: int,
) -> Any:
    """Build the exact consumer/artifact topology required by one claim."""
    from people_counter.fabric_capability_probe import (
        ConsumerArtifact,
        ConsumerProbeProfile,
    )

    artifacts: dict[str, ConsumerArtifact] = {}
    video: ConsumerArtifact | None = None
    pytorch_model: ConsumerArtifact | None = None
    onnx_model: ConsumerArtifact | None = None
    for item in envelope["items"]:
        payload = item["payload"]
        source = str(payload["source_video"])
        prefix = "/lakehouse/default/"
        if not source.startswith(prefix):
            raise ValueError(
                "consumer probe requires default-Lakehouse source identities"
            )
        current_video = ConsumerArtifact(
            source[len(prefix) :],
            str(payload["source_sha256"]),
        )
        artifacts[current_video.relative_path] = current_video
        if video is None:
            video = current_video
        model_hashes = payload.get("model_artifact_sha256")
        if not isinstance(model_hashes, Mapping):
            raise ValueError("consumer probe requires model_artifact_sha256")
        for model_path in _candidate_a_model_paths(
            payload, validation_error=ValueError
        ):
            digest = model_hashes.get(model_path)
            if not isinstance(digest, str):
                raise ValueError(
                    f"consumer probe is missing model digest {model_path!r}"
                )
            artifact = ConsumerArtifact(f"Files/models/{model_path}", digest)
            artifacts[artifact.relative_path] = artifact
            if model_path.endswith((".pt", ".safetensors")):
                pytorch_model = pytorch_model or artifact
            if model_path.endswith(".onnx"):
                onnx_model = onnx_model or artifact
    if video is None:
        raise ValueError("consumer probe requires at least one claimed video")
    return ConsumerProbeProfile(
        hash_targets=tuple(
            artifacts[path] for path in sorted(artifacts)
        ),
        video=video,
        safetensors_or_pytorch_model=pytorch_model,
        onnx_model=onnx_model,
        concurrent_read_target=video,
        planned_concurrent_tasks=planned_concurrent_tasks,
    )


def _probe_and_persist_consumer_decision(
    spark: Any,
    config: FabricCandidateAConfig,
    batch_id: str,
    envelope: Mapping[str, Any],
    *,
    discover_executors: Any = None,
    probe_consumers: Any = None,
    files: Any = None,
) -> tuple[Any, dict[str, Any]]:
    """Run one workload-specific probe and persist its immutable decision."""
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
        probe_direct_mount_consumer_capability,
    )
    from people_counter.fabric_candidate_a_control import (
        NotebookUtilsOneLakeFiles,
    )
    from people_counter.fabric_executor_inventory import discover_active_executors

    discover = discover_executors or (
        lambda session: discover_active_executors(
            session, minimum_executors=1
        )
    )
    executors = tuple(discover(spark))
    task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    planned_tasks = sum(
        max(1, executor.total_cores // task_cpus) for executor in executors
    )
    profile = _consumer_probe_profile(
        envelope, planned_concurrent_tasks=planned_tasks
    )
    probe = probe_consumers or probe_direct_mount_consumer_capability
    result = probe(spark, executors, profile=profile)
    profile_payload = {
        "hash_targets": [
            asdict(artifact) for artifact in profile.hash_targets
        ],
        "video": asdict(profile.video) if profile.video else None,
        "safetensors_or_pytorch_model": (
            asdict(profile.safetensors_or_pytorch_model)
            if profile.safetensors_or_pytorch_model
            else None
        ),
        "onnx_model": (
            asdict(profile.onnx_model) if profile.onnx_model else None
        ),
        "concurrent_read_target": (
            asdict(profile.concurrent_read_target)
            if profile.concurrent_read_target
            else None
        ),
        "planned_concurrent_tasks": profile.planned_concurrent_tasks,
    }
    profile_sha256 = hashlib.sha256(
        json.dumps(
            profile_payload,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    decision = {
        "schema": "people-counter-consumer-probe-decision-v1",
        "batch_id": batch_id,
        "namespace_mode": config.mode.value,
        "profile_sha256": profile_sha256,
        "profile": profile_payload,
        "executor_ids": sorted(executor.executor_id for executor in executors),
        "status": result.status.value,
        "selected_backend": (
            InputBackend.FABRIC_DIRECT.value
            if result.status is CapabilityStatus.AVAILABLE
            else InputBackend.FABRIC_FALLBACK.value
        ),
        "capability": result.capability,
        "evidence": result.evidence,
    }
    content = json.dumps(
        decision,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    path = config.file_path(
        f"consumer-decisions/{batch_id}/{profile_sha256}.json"
    )
    storage = files or NotebookUtilsOneLakeFiles()
    if storage.exists(path):
        persisted_content = storage.read_text(path)
        if persisted_content != content:
            try:
                persisted = json.loads(persisted_content)
            except json.JSONDecodeError as error:
                raise RuntimeError(
                    "persisted consumer-probe decision is invalid JSON"
                ) from error
            if not isinstance(persisted, dict) or set(persisted) != set(decision):
                raise RuntimeError(
                    "persisted consumer-probe decision has an invalid shape"
                )
            invariant_keys = (
                "schema",
                "batch_id",
                "namespace_mode",
                "profile_sha256",
                "profile",
            )
            if any(persisted[key] != decision[key] for key in invariant_keys):
                raise RuntimeError(
                    "persisted consumer-probe decision conflicts with this workload"
                )
            persisted_executor_ids = persisted.get("executor_ids")
            if (
                not isinstance(persisted_executor_ids, list)
                or not persisted_executor_ids
                or any(
                    not isinstance(executor_id, str) or not executor_id
                    for executor_id in persisted_executor_ids
                )
                or len(set(persisted_executor_ids)) != len(persisted_executor_ids)
            ):
                raise RuntimeError(
                    "persisted consumer-probe decision has invalid executor IDs"
                )
            if persisted_executor_ids == decision["executor_ids"]:
                raise RuntimeError(
                    "persisted consumer-probe evidence drifted for the same executors"
                )
            canonical_persisted = json.dumps(
                persisted,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            if canonical_persisted != persisted_content:
                raise RuntimeError(
                    "persisted consumer-probe decision is not canonical JSON"
                )
            decision = persisted
            result = CapabilityProbeResult(
                capability="direct_mount_consumer_capability",
                status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
                evidence=(
                    "executor inventory changed after the persisted workload "
                    "probe; verified fallback is required"
                ),
            )
    else:
        storage.create_text(path, content)
        if storage.read_text(path) != content:
            raise RuntimeError("consumer-probe decision readback differs")
    persisted_content = json.dumps(
        decision,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    decision["path"] = path
    decision["sha256"] = hashlib.sha256(
        persisted_content.encode("utf-8")
    ).hexdigest()
    return result, decision


def _bind_consumer_probe_to_executor_inventory(
    result: Any,
    expected_executor_ids: Sequence[str],
) -> Any:
    """Return a probe callable that invalidates direct access on drift."""
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    expected = frozenset(expected_executor_ids)

    def _probe(_spark: Any, executors: Any) -> Any:
        observed = frozenset(executor.executor_id for executor in executors)
        if observed != expected:
            return CapabilityProbeResult(
                capability="direct_mount_consumer_capability",
                status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
                evidence=(
                    "executor inventory changed after the persisted workload "
                    "probe; verified fallback is required"
                ),
            )
        return result

    return _probe


def _executor_warm_works(
    envelope: Mapping[str, Any],
    enrichment: Mapping[str, Mapping[str, Any]],
    *,
    executor_cores: int,
    task_cpus: int,
    package_version: str,
    manifest_sha256: str,
) -> tuple[dict[str, Any], ...]:
    """Build one representative warm-up per exact model/runtime identity."""
    from people_counter.sjd_process import _model_identity

    works: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in envelope["items"]:
        payload = item["payload"]
        model_identity = _model_identity(payload)
        runtime_identity = str(payload.get("runtime_key", item.get("runtime_key", "")))
        identity = (model_identity, runtime_identity)
        if identity in seen:
            continue
        seen.add(identity)
        work_id = str(item["work_id"])
        works.append(
            {
                **payload,
                **enrichment[work_id],
                "model_identity": model_identity,
                "executor_cores": executor_cores,
                "task_cpus": task_cpus,
                "planned_concurrency": max(1, executor_cores // task_cpus),
                "_expected_release_package_version": package_version,
                "_expected_release_manifest_sha256": manifest_sha256,
            }
        )
    if not works:
        raise RuntimeError("Candidate A requires at least one runtime warm-up")
    return tuple(works)


def _fabric_process_profile(
    spark: Any,
    *,
    warm_up: Any = None,
    discover_executors: Any = None,
    probe_peak_rss_bytes: Any = None,
) -> Any:
    """Derive the reviewed Runtime 2.0 live-pool profile from real executors.

    Executor count is proven from the live Spark status store (mirroring
    the benchmark's ``_benchmark_execution_profile``) rather than assumed
    to be exactly one; the physical task count is the sum of each
    discovered executor's own placement-safe slot count, not the bare
    executor count. Before admitting any concurrency above one task per
    executor, this attempts a real warm-executor RSS measurement
    (:func:`people_counter.fabric_capability_probe.probe_executor_peak_rss_bytes`,
    executed inside the discovered executors' own tasks); if Fabric cannot
    expose that measurement, the reviewed unknown-RSS policy is enforced by
    capping the total physical task count at exactly one per executor
    (``operator_cap=len(executors)``) rather than silently admitting more
    concurrency than has been proven safe.
    """
    from people_counter.fabric_capability_probe import (
        CapabilityStatus,
        probe_executor_peak_rss_bytes,
    )
    from people_counter.fabric_executor_inventory import discover_active_executors
    from people_counter.sjd_process import GIB, build_profile_from_inventory

    if sys.version_info[:2] != (3, 13):
        raise RuntimeError(f"Candidate A requires Python 3.13, got {sys.version}")
    if not str(spark.version).startswith("4.1.1"):
        raise RuntimeError(f"Candidate A requires Spark 4.1.1, got {spark.version}")
    java = str(
        spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    )
    if not java.startswith("21"):
        raise RuntimeError(f"Candidate A requires Java 21, got {java}")
    if str(spark.conf.get("spark.dynamicAllocation.enabled", "")).lower() != "false":
        raise RuntimeError("Candidate A requires the reviewed fixed live pool")
    if str(spark.conf.get("spark.speculation", "false")).lower() != "false":
        raise RuntimeError("Candidate A requires Spark speculation disabled")
    expected_executor_cores = int(spark.conf.get("spark.executor.cores"))
    if expected_executor_cores < 1:
        raise RuntimeError("Candidate A requires at least one executor core")
    memory = str(spark.conf.get("spark.executor.memory")).lower()
    match = re.fullmatch(r"(\d+)([gmk])", memory)
    if match is None:
        raise RuntimeError(f"unsupported Fabric executor memory {memory!r}")
    scale = {"k": 1024, "m": 1024**2, "g": GIB}[match.group(2)]
    memory_bytes = int(match.group(1)) * scale
    task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    minimum_executors = max(
        1, int(spark.conf.get("spark.dynamicAllocation.minExecutors", "1"))
    )

    discover = discover_executors or (
        lambda session, minimum: discover_active_executors(
            session, minimum_executors=minimum
        )
    )
    executors = tuple(discover(spark, minimum_executors))

    if probe_peak_rss_bytes is None:
        rss_result = probe_executor_peak_rss_bytes(
            spark, executors, warm_up=warm_up
        )
    else:
        rss_result = probe_peak_rss_bytes(spark, executors)
    if rss_result.status is not CapabilityStatus.AVAILABLE:
        raise RuntimeError(
            "Candidate A profile promotion requires complete warmed executor "
            f"RSS evidence: {rss_result.evidence}"
        )
    peak_rss_bytes = rss_result.value

    return build_profile_from_inventory(
        "candidate-a-fabric-runtime2-v1",
        executors,
        task_cpus=task_cpus,
        expected_executor_cores=expected_executor_cores,
        executor_memory_bytes=memory_bytes,
        memory_reserve_bytes=min(4 * GIB, memory_bytes // 4),
        heartbeat_seconds=10.0,
        minimum_speed_x=0.05,
        lease_safety_factor=1.25,
        lease_margin_seconds=60.0,
        fixed_allocation=False,
        peak_rss_bytes=peak_rss_bytes,
        rss_headroom_fraction=0.20,
    )


def select_process_execution_harness(
    spark: Any,
    config: FabricCandidateAConfig,
    batch_id: str,
    *,
    row_enrichment: Mapping[str, Mapping[str, Any]],
    discover_executors: Any = None,
    probe_mounted_path: Any = None,
    consumer_probe_profile: Any = None,
) -> tuple[Any, dict[str, Any]]:
    """Select the live execution harness from proven direct-mount capability.

    Prefers :class:`~people_counter.sjd_process.StreamingSparkExecutionHarness`
    staged directly under the mounted Lakehouse ``Files`` path -- the
    "direct Lakehouse path" optimization, avoiding the per-row Spark
    collect boundary entirely -- but only once the mount is *proven*
    read/write/atomic-rename/fsync-capable inside every discovered
    executor's own task
    (:func:`people_counter.fabric_capability_probe.probe_direct_mounted_lakehouse_path`).
    If the mount cannot be proven (the probe reports
    ``FABRIC_PLATFORM_BLOCKED`` for any reason, including a partially
    observed executor inventory), this falls back to the existing
    collecting :class:`~people_counter.sjd_process.SparkExecutionHarness`
    and returns the exact capability evidence rather than silently
    hardwiring an unproven mount.

    ``probe_mounted_path``, when given, takes precedence over
    ``consumer_probe_profile`` (test/call-site injection always wins).
    Otherwise, if ``consumer_probe_profile`` is given, the bare POSIX
    prerequisite probe is upgraded to
    :func:`people_counter.fabric_capability_probe.probe_direct_mount_consumer_capability`
    bound to that profile, so the direct-mounted streaming harness is only
    ever selected once every real consumer artifact the profile requires
    has *also* been proven -- not merely the bare POSIX round-trip. Neither
    argument changes the default (bare POSIX-only) behavior for existing
    callers that pass neither.
    """
    from people_counter.fabric_capability_probe import (
        CapabilityStatus,
        ConsumerCapabilityReport,
        probe_direct_mount_consumer_capability,
        probe_direct_mounted_lakehouse_path,
    )
    from people_counter.fabric_executor_inventory import discover_active_executors
    from people_counter.sjd_process import StreamingSparkExecutionHarness

    discover = discover_executors or (
        lambda session: discover_active_executors(session, minimum_executors=1)
    )
    executors = tuple(discover(spark))
    if probe_mounted_path is not None:
        probe = probe_mounted_path
    elif consumer_probe_profile is not None:
        import functools

        probe = functools.partial(
            probe_direct_mount_consumer_capability, profile=consumer_probe_profile
        )
    else:
        probe = probe_direct_mounted_lakehouse_path
    result = probe(spark, executors)
    evidence: dict[str, Any] = {
        "capability": result.capability,
        "status": result.status.value,
        "evidence": result.evidence,
    }
    if result.status is CapabilityStatus.AVAILABLE:
        mount_root = (
            result.value.mount_root
            if isinstance(result.value, ConsumerCapabilityReport)
            else result.value
        )
        staging_root = f"{mount_root}/{config.file_path(f'process-streaming/{batch_id}')}"
        harness: Any = StreamingSparkExecutionHarness(
            spark,
            staging_root,
            verify_settings=False,
            row_enrichment=row_enrichment,
        )
        evidence["backend"] = "direct_mounted_streaming"
        evidence["staging_root"] = staging_root
    else:
        staging_root = config.file_path(f"process-streaming/{batch_id}")
        harness = StreamingSparkExecutionHarness(
            spark,
            staging_root,
            verify_settings=False,
            row_enrichment=row_enrichment,
            staging_backend="spark_delta",
        )
        evidence["backend"] = "fallback_spark_delta_receipts"
        evidence["staging_root"] = staging_root
    return harness, evidence


def _fallback_localize(
    spark: Any,
    cache: LocalizedSourceCache,
    *,
    abfss_uri: str,
    original_basename: str,
    expected_sha256: str,
    staged_sources: dict[str, Any],
    broadcast_digests: set[str],
    stream_remote: Any = None,
    distributed_alias_root_uri: str | None = None,
) -> tuple[str, str]:
    """Stage one artifact through the content-addressed fallback backend.

    Spark's own ``SparkFiles`` directory is flat and keyed by basename, so
    two different sources that happen to share a filename (for example two
    detector variants that both ship ``config.json``) must never enter that
    namespace under their original basename. The canonical URI is streamed
    through Hadoop directly to a driver-local name qualified by the expected
    digest, verified there, and only then passed to ``addFile`` under the
    cache's digest-only basename. Repeated identical content is staged and
    broadcast once.

    ``stream_remote`` is a test seam with the same ``(spark, uri,
    destination)`` signature as :func:`_stream_hadoop_uri_to_local`; the
    production path always uses the Hadoop stream and never a whole-file
    read.

    Returns ``(content_addressed_localized_name, content_sha256)``.
    """
    digest = expected_sha256.lower()
    qualified_name = content_addressed_name(original_basename, digest)
    staged = staged_sources.get(abfss_uri)
    if staged is None:
        stage_root = cache.root / "driver-staged"
        staged = stage_root / qualified_name
        if not staged.exists():
            stage_root.mkdir(parents=True, exist_ok=True)
            temporary = staged.with_name(
                f".{qualified_name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
            )
            streamer = stream_remote or _stream_hadoop_uri_to_local
            try:
                streamer(spark, abfss_uri, temporary)
                verify_sha256(temporary, digest, label=abfss_uri)
                os.replace(temporary, staged)
            finally:
                temporary.unlink(missing_ok=True)
        observed, size_bytes = verify_sha256(staged, digest, label=abfss_uri)
        staged_sources[abfss_uri] = (staged, observed, size_bytes)
    else:
        staged, observed, size_bytes = staged
        if observed != digest:
            raise ValueError(
                f"canonical URI {abfss_uri!r} was already staged with a "
                "different content identity"
            )
    entry = cache.get_or_stage(
        digest,
        size_bytes=size_bytes,
        stager=lambda destination: link_or_stream_copy(staged, destination),
    )
    if digest not in broadcast_digests:
        distributed_uri = str(entry.path)
        if distributed_alias_root_uri is not None:
            distributed_uri = (
                f"{distributed_alias_root_uri.rstrip('/')}/{entry.path.name}"
            )
            _stage_hadoop_alias(
                spark,
                entry.path,
                distributed_uri,
                expected_sha256=digest,
            )
        spark.sparkContext.addFile(distributed_uri)
        broadcast_digests.add(digest)
    return entry.path.name, digest


def _stage_hadoop_alias(
    spark: Any,
    source: Path,
    destination_uri: str,
    *,
    expected_sha256: str,
) -> None:
    """Create and verify one digest-qualified distributed Hadoop alias."""
    context = spark.sparkContext
    jvm = context._jvm
    configuration = context._jsc.hadoopConfiguration()
    destination = jvm.org.apache.hadoop.fs.Path(destination_uri)
    filesystem = destination.getFileSystem(configuration)
    filesystem.mkdirs(destination.getParent())
    if not filesystem.exists(destination):
        source_stream = jvm.java.io.FileInputStream(str(source))
        destination_stream = filesystem.create(destination, False)
        try:
            jvm.org.apache.hadoop.io.IOUtils.copyBytes(
                source_stream,
                destination_stream,
                configuration,
                False,
            )
        finally:
            try:
                source_stream.close()
            finally:
                destination_stream.close()
    temporary = source.with_name(
        f".{source.name}.alias-verify-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        _stream_hadoop_uri_to_local(spark, destination_uri, temporary)
        verify_sha256(temporary, expected_sha256, label=destination_uri)
    finally:
        temporary.unlink(missing_ok=True)


def _stream_hadoop_uri_to_local(
    spark: Any,
    source_uri: str,
    destination: Path,
) -> None:
    """Stream one Hadoop-supported URI to a create-only local file.

    Hadoop's ``IOUtils.copyBytes`` performs bounded-buffer streaming inside
    the JVM, avoiding Py4J byte-array materialization and avoiding any
    basename-keyed Spark staging before the caller verifies the digest.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"staging destination already exists: {destination}")
    context = spark.sparkContext
    jvm = context._jvm
    configuration = context._jsc.hadoopConfiguration()
    source = jvm.org.apache.hadoop.fs.Path(source_uri)
    source_stream = source.getFileSystem(configuration).open(source)
    destination_stream = jvm.java.io.FileOutputStream(str(destination))
    try:
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(
            source_stream,
            destination_stream,
            configuration,
            False,
        )
    finally:
        try:
            source_stream.close()
        finally:
            destination_stream.close()


def _check_synthetic_video_identity(
    item: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    route_mode: str | None,
    route_identity: Mapping[str, Any] | None,
    validation_error: type[Exception],
) -> None:
    """Pin a SHADOW_SYNTHETIC item's video/config identity before any I/O."""
    if route_mode != "SHADOW_SYNTHETIC":
        return
    assert route_identity is not None
    if (
        item.get("work_id") != route_identity.get("work_id")
        or payload.get("source_sha256") != route_identity.get("source_sha256")
        or item.get("config_sha256") != route_identity.get("config_sha256")
    ):
        raise validation_error("synthetic video/config identity differs")


def _resolve_localized_video(
    spark: Any,
    payload: Mapping[str, Any],
    *,
    backend: InputBackend,
    mount_root: str | None,
    cache: LocalizedSourceCache | None,
    staged_sources: dict[str, Any],
    broadcast_digests: set[str],
    distributed_alias_root_uri: str | None = None,
    validation_error: type[Exception],
) -> tuple[str, str, str | None]:
    """Resolve and hash-verify one item's video; never a whole-file read.

    Returns ``(lakehouse_relative_path, video_localized_name_or_none)`` only
    once the resolved content's digest matches the registered
    ``source_sha256`` -- otherwise raises ``validation_error`` so a tampered
    or drifted source can never silently reach the executor.
    """
    source = str(payload["source_video"])
    mount_prefix = "/lakehouse/default/"
    if not source.startswith(mount_prefix):
        raise validation_error(
            "Candidate A source_video must use the default Lakehouse"
        )
    relative = source[len(mount_prefix) :]

    video_localized_name: str | None = None
    if backend is InputBackend.FABRIC_DIRECT:
        assert mount_root is not None
        source_sha, _ = stream_sha256(
            resolve_direct_mounted_path(mount_root, relative)
        )
    else:
        assert cache is not None
        video_localized_name, source_sha = _fallback_localize(
            spark,
            cache,
            abfss_uri=(
                f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
                f"{LAKEHOUSE_ID}/{relative}"
            ),
            original_basename=source.rsplit("/", 1)[-1],
            expected_sha256=str(payload.get("source_sha256", "")),
            staged_sources=staged_sources,
            broadcast_digests=broadcast_digests,
            distributed_alias_root_uri=distributed_alias_root_uri,
        )
    if source_sha != payload.get("source_sha256"):
        raise validation_error(
            "localized video digest differs from registered input"
        )
    return relative, source_sha, video_localized_name


def _candidate_a_model_paths(
    payload: Mapping[str, Any], *, validation_error: type[Exception]
) -> tuple[str, ...]:
    """Build the four fixed RT-DETR/OSNet artifact paths for one item."""
    if payload.get("pipeline", "rtdetr-osnet") != "rtdetr-osnet":
        raise validation_error(
            "Candidate A v1 localizer supports RT-DETR/OSNet only"
        )
    detector = str(payload.get("detector_model", "r18"))
    model_format = str(payload.get("model_format", "pytorch"))
    detector_dir = "rtdetr_v2_r18vd" if detector == "r18" else "rtdetr_v2_r50vd"
    detector_file = (
        "model.safetensors" if model_format == "pytorch" else "model.onnx"
    )
    reid_file = (
        "osnet_ain_x0_25.pt" if model_format == "pytorch" else "osnet_ain_x0_25.onnx"
    )
    return (
        f"rtdetr_osnet/{detector_dir}/config.json",
        f"rtdetr_osnet/{detector_dir}/preprocessor_config.json",
        f"rtdetr_osnet/{detector_dir}/{detector_file}",
        f"rtdetr_osnet/libre_reid_osnet/{reid_file}",
    )


def _resolve_localized_models(
    spark: Any,
    payload: Mapping[str, Any],
    model_paths: tuple[str, ...],
    *,
    backend: InputBackend,
    mount_root: str | None,
    cache: LocalizedSourceCache | None,
    staged_sources: dict[str, Any],
    broadcast_digests: set[str],
    distributed_alias_root_uri: str | None = None,
    validation_error: type[Exception],
) -> dict[str, dict[str, str]]:
    """Resolve and hash-verify every fixed model artifact for one item."""
    expected_artifacts = payload.get("model_artifact_sha256", {})
    if not isinstance(expected_artifacts, Mapping):
        raise validation_error("model_artifact_sha256 must be an object")

    localized_models: dict[str, dict[str, str]] = {}
    for model_path in model_paths:
        full_model_path = f"Files/models/{model_path}"
        expected_digest = expected_artifacts.get(model_path)
        if not isinstance(expected_digest, str):
            raise validation_error(
                f"model_artifact_sha256 is missing {model_path!r}"
            )
        if backend is InputBackend.FABRIC_DIRECT:
            assert mount_root is not None
            digest, _ = stream_sha256(
                resolve_direct_mounted_path(mount_root, full_model_path)
            )
            localized_models[model_path] = {"sha256": digest}
        else:
            assert cache is not None
            localized_name, digest = _fallback_localize(
                spark,
                cache,
                abfss_uri=(
                    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
                    f"{LAKEHOUSE_ID}/{full_model_path}"
                ),
                original_basename=model_path.rsplit("/", 1)[-1],
                expected_sha256=expected_digest,
                staged_sources=staged_sources,
                broadcast_digests=broadcast_digests,
                distributed_alias_root_uri=distributed_alias_root_uri,
            )
            localized_models[model_path] = {
                "localized_name": localized_name,
                "sha256": digest,
            }
        if localized_models[model_path]["sha256"] != expected_digest:
            raise validation_error(
                "localized model digest differs from CPU profile"
            )
    return localized_models


def _check_synthetic_model_identity(
    localized_models: Mapping[str, Mapping[str, str]],
    *,
    route_mode: str | None,
    route_identity: Mapping[str, Any] | None,
    validation_error: type[Exception],
) -> None:
    """Pin a SHADOW_SYNTHETIC item's resolved model identity."""
    if route_mode != "SHADOW_SYNTHETIC":
        return
    from people_counter.fabric_production_routing import sha256_json

    assert route_identity is not None
    observed_model = sha256_json(
        {
            "schema": "people-counter-fixed-model-artifacts-v1",
            "artifacts": {
                f"Files/models/{path}": value["sha256"]
                for path, value in localized_models.items()
                # "preprocessor_config.json" is itself a suffix-match for
                # "config.json" (``str.endswith`` checks a suffix, not an
                # exact name), so it is already excluded by this single
                # check -- listing it separately would only add an
                # unkillable equivalent mutant, never different behavior.
                if not path.endswith("config.json")
            },
        }
    )
    if observed_model != route_identity.get("model_sha256"):
        raise validation_error("synthetic model identity differs")


class LocalizedProcessInputs(NamedTuple):
    """Typed ``(row_enrichment, resolver_evidence)`` contract.

    Named (rather than a bare 2-tuple) so a caller that accidentally binds
    the *whole* result to one name -- the exact regression this guards
    against, see ``fabric_benchmark_jobs._process_dispatch`` history --
    gets a value with no ``.items()``/dict semantics instead of something
    that can be silently misused as the enrichment mapping. Still supports
    positional indexing/unpacking (``enrichment, evidence = result`` or
    ``result[0]``) so every existing call site keeps working unchanged.
    """

    enrichment: dict[str, dict[str, Any]]
    resolver_evidence: dict[str, Any]


def _localize_process_inputs(
    spark: Any,
    store: Any,
    batch_id: str,
    *,
    route_mode: str | None = None,
    route_identity: Mapping[str, Any] | None = None,
    discover_executors: Any = None,
    probe_mounted_path: Any = None,
    consumer_probe_profile: Any = None,
) -> LocalizedProcessInputs:
    """Resolve immutable OneLake video/model inputs for executors.

    Selects the input-resolution backend once per batch from *proven*
    direct-mount capability
    (:func:`people_counter.fabric_input_resolver.select_input_backend`,
    the same contract as :func:`select_process_execution_harness`): when
    the mount is proven usable inside executor tasks, inputs resolve
    straight to the mounted path with no distribution step and no copy at
    all; otherwise the existing ``SparkFiles``/``addFile`` broadcast is
    used, but keyed by content digest (never basename) so distinct sources
    that happen to share a filename can never collide, and every hash is
    computed incrementally -- never by reading a whole video or model file
    into memory at once.

    Per-item resolution and verification is delegated to
    :func:`_resolve_localized_video`/:func:`_resolve_localized_models` (and
    their SHADOW_SYNTHETIC identity-pinning siblings) so this function
    itself only sequences the batch-level backend selection and the
    per-item enrichment shape -- kept deliberately low-complexity rather
    than folding every branch inline.

    ``consumer_probe_profile``, when given (and ``probe_mounted_path`` is
    not), upgrades the bare POSIX mount probe to the full real-consumer
    capability probe exactly as :func:`select_process_execution_harness`
    does -- see its docstring for the exact precedence contract.

    Returns a :class:`LocalizedProcessInputs` ``(row_enrichment,
    resolver_evidence)`` named tuple -- mirroring
    :func:`select_process_execution_harness`'s own
    ``(harness, evidence)`` contract -- so the exact capability evidence
    used to pick the backend, and (for the fallback backend) the content
    cache's hit/miss/eviction/corruption metrics, are never silently
    dropped on the floor.
    """
    envelope, _ = store.load_claim_envelope_with_digest(batch_id)
    validation_error: type[Exception] = ValueError
    if route_mode == "SHADOW_SYNTHETIC":
        from people_counter.sjd_process import ProcessValidationError

        validation_error = ProcessValidationError
    if route_mode == "SHADOW_SYNTHETIC" and not isinstance(
        route_identity, Mapping
    ):
        raise validation_error("synthetic route identity is required")

    backend, backend_evidence = select_input_backend(
        spark,
        discover_executors=discover_executors,
        probe_mounted_path=probe_mounted_path,
        consumer_probe_profile=consumer_probe_profile,
    )
    mount_root = backend_evidence.get("mount_root")
    cache: LocalizedSourceCache | None = None
    staged_sources: dict[str, Any] = {}
    broadcast_digests: set[str] = set()
    if backend is InputBackend.FABRIC_FALLBACK:
        from pyspark import SparkFiles

        cache = LocalizedSourceCache(
            Path(SparkFiles.getRootDirectory()) / "content-addressed",
            max_entries=256,
            max_total_bytes=16 * 1024**3,
        )
    store_config = getattr(store, "config", None)
    if not isinstance(store_config, FabricCandidateAConfig):
        store_config = FabricCandidateAConfig()
    distributed_alias_root_uri = (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/{store_config.file_path('distributed-cache')}"
    )

    enrichment: dict[str, dict[str, Any]] = {}
    for item in envelope["items"]:
        payload = item["payload"]
        _check_synthetic_video_identity(
            item,
            payload,
            route_mode=route_mode,
            route_identity=route_identity,
            validation_error=validation_error,
        )
        relative, _, video_localized_name = _resolve_localized_video(
            spark,
            payload,
            backend=backend,
            mount_root=mount_root,
            cache=cache,
            staged_sources=staged_sources,
            broadcast_digests=broadcast_digests,
            distributed_alias_root_uri=distributed_alias_root_uri,
            validation_error=validation_error,
        )
        model_paths = _candidate_a_model_paths(
            payload, validation_error=validation_error
        )
        localized_models = _resolve_localized_models(
            spark,
            payload,
            model_paths,
            backend=backend,
            mount_root=mount_root,
            cache=cache,
            staged_sources=staged_sources,
            broadcast_digests=broadcast_digests,
            distributed_alias_root_uri=distributed_alias_root_uri,
            validation_error=validation_error,
        )
        _check_synthetic_model_identity(
            localized_models,
            route_mode=route_mode,
            route_identity=route_identity,
            validation_error=validation_error,
        )
        if backend is InputBackend.FABRIC_DIRECT:
            enrichment[str(item["work_id"])] = {
                "resolver_backend": InputBackend.FABRIC_DIRECT.value,
                "lakehouse_mount_root": mount_root,
                "lakehouse_relative_video_path": relative,
                "lakehouse_relative_models_root": "Files/models",
            }
        else:
            enrichment[str(item["work_id"])] = {
                "resolver_backend": InputBackend.FABRIC_FALLBACK.value,
                "spark_localized_video_name": video_localized_name,
                "spark_localized_models": localized_models,
            }
    resolver_evidence = dict(backend_evidence)
    resolver_evidence["cache_metrics"] = cache.metrics() if cache is not None else None
    return LocalizedProcessInputs(enrichment, resolver_evidence)


def _control_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-fabric-control-sjd")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap")
    register = commands.add_parser("register")
    register.add_argument("--work-id", required=True)
    payload = register.add_mutually_exclusive_group(required=True)
    payload.add_argument("--payload-json")
    payload.add_argument("--payload-base64")
    register.add_argument("--runtime-key", required=True)
    register.add_argument("--duration-seconds", required=True, type=float)
    register.add_argument("--config-sha256", required=True)
    register.add_argument("--release-digest", required=True)
    register.add_argument("--max-attempts", type=int, default=3)
    bulk_register = commands.add_parser("bulk-register")
    bulk_register.add_argument("--partition-path", required=True)
    bulk_register.add_argument("--partition-sha256", required=True)
    bulk_register.add_argument("--max-items", required=True, type=int)
    claim = commands.add_parser("claim")
    claim.add_argument("--owner", required=True)
    claim.add_argument("--work-id", action="append")
    claim.add_argument("--max-items", required=True, type=int)
    claim.add_argument("--lease-seconds", required=True, type=float)
    claim.add_argument("--minimum-speed-x", type=float, default=1.0)
    claim.add_argument("--safety-factor", type=float, default=1.25)
    claim.add_argument("--margin-seconds", type=float, default=30.0)
    claim.add_argument("--minimum-items", type=int, default=1)
    replay = commands.add_parser("replay")
    replay.add_argument("--work-id", required=True)
    replay.add_argument("--operator", required=True)
    replay.add_argument("--reason", required=True)
    replay.add_argument("--additional-attempts", type=int, default=1)
    quarantine = commands.add_parser("quarantine")
    quarantine.add_argument("--work-evidence", action="append", required=True)
    quarantine.add_argument("--operator", required=True)
    quarantine.add_argument("--reason", required=True)
    recover = commands.add_parser("recover")
    recover.add_argument("--now", type=float)
    clear_lock = commands.add_parser("clear-stale-lock")
    clear_lock.add_argument("--expected-owner-id", required=True)
    commands.add_parser("reconcile")
    return parser


def _clear_stale_lock(
    spark: Any,
    expected_owner_id: str,
    *,
    config: FabricCandidateAConfig | None = None,
) -> dict[str, Any]:
    """Manually clear one investigated canary writer token by exact CAS."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions

    selected = config or FabricCandidateAConfig()
    selected.require_write_enabled()
    table = selected.table("locks")
    rows = spark.table(table).select(
        "lock_name", "owner_id", "acquired_at"
    ).limit(2).collect()
    if (
        len(rows) != 1
        or rows[0]["lock_name"] != "global"
        or rows[0]["owner_id"] != expected_owner_id
        or rows[0]["acquired_at"] is None
    ):
        raise RuntimeError(
            "refusing stale-lock clear: exact owner/readback precondition failed"
        )
    acquired_at = rows[0]["acquired_at"]
    DeltaTable.forName(spark, table).update(
        condition=(
            (functions.col("lock_name") == "global")
            & (functions.col("owner_id") == expected_owner_id)
        ),
        set={
            "owner_id": functions.lit(None).cast("string"),
            "acquired_at": functions.lit(None).cast("timestamp"),
        },
    )
    spark.catalog.refreshTable(table)
    observed = spark.table(table).select(
        "lock_name", "owner_id", "acquired_at"
    ).limit(2).collect()
    if (
        len(observed) != 1
        or observed[0]["lock_name"] != "global"
        or observed[0]["owner_id"] is not None
        or observed[0]["acquired_at"] is not None
    ):
        raise RuntimeError("stale-lock clear exact readback failed")
    return {
        "cleared_owner_id": expected_owner_id,
        "acquired_at": str(acquired_at),
    }


def _write_control_diagnostic(
    config: FabricCandidateAConfig,
    command: str,
    error: BaseException,
    *,
    prefix: str = "control",
) -> str | None:
    """Best-effort persistence of an uncaught job-dispatch traceback.

    Fabric's standard Spark driver log-fetch API returns a 404 for apps that
    die before full YARN log-aggregation registration completes, so a crash
    inside a control/process/gold command can otherwise leave zero
    diagnosable evidence. This side channel writes the full traceback
    directly to OneLake using plain overwrite semantics (not the
    create-only/verified protocol used by correctness-critical artifacts) so
    it never competes with, masks, or replaces the original exception; any
    failure here is swallowed and the caller still re-raises the real error.
    ``prefix`` namespaces the diagnostics directory per job family (for
    example ``"process"`` or ``"gold"``) so benchmark process/gold crashes do
    not collide with control-dispatch diagnostics.
    """
    try:
        import notebookutils

        path = config.file_path(
            f"{prefix}/diagnostics/"
            f"{command}-{int(time.time())}-{uuid.uuid4().hex}.json"
        )
        payload = json.dumps(
            {
                "command": command,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": traceback.format_exception(
                    type(error), error, error.__traceback__
                ),
                "captured_at": time.time(),
            },
            allow_nan=False,
            default=str,
            sort_keys=True,
        )
        notebookutils.fs.put(path, payload, True)
        return path
    except Exception:  # pragma: no cover - diagnostics must never mask errors
        return None


def control_main(
    argv: Sequence[str] | None = None,
    *,
    config: FabricCandidateAConfig | None = None,
) -> int:
    """Run bounded control maintenance against the fixed canary tables."""
    arguments = _control_parser().parse_args(argv)
    from people_counter.sjd_control import FabricControlStore

    selected = config or FabricCandidateAConfig()
    selected.require_write_enabled()
    store = FabricControlStore(_spark(), config=selected)
    try:
        return _control_dispatch(arguments, store, selected)
    except Exception as error:
        _write_control_diagnostic(selected, arguments.command, error)
        raise


def _control_dispatch(
    arguments: argparse.Namespace,
    store: Any,
    selected: FabricCandidateAConfig,
) -> int:
    """Execute one parsed control command; isolated so the diagnostic
    wrapper in ``control_main`` always sees command failures."""
    if arguments.command == "bootstrap":
        result: object = {"bootstrapped": True}
    elif arguments.command == "register":
        encoded = arguments.payload_json
        if arguments.payload_base64 is not None:
            try:
                encoded = base64.urlsafe_b64decode(
                    arguments.payload_base64.encode("ascii")
                ).decode("utf-8")
            except (UnicodeError, ValueError, binascii.Error) as error:
                raise ValueError("--payload-base64 must be canonical UTF-8 JSON") from error
        payload = json.loads(encoded)
        if not isinstance(payload, dict):
            raise ValueError("--payload-json must decode to an object")
        result = asdict(
            store.register(
                arguments.work_id,
                payload,
                runtime_key=arguments.runtime_key,
                duration_seconds=arguments.duration_seconds,
                config_sha256=arguments.config_sha256,
                release_digest=arguments.release_digest,
                max_attempts=arguments.max_attempts,
            )
        )
    elif arguments.command == "bulk-register":
        from people_counter.fabric_candidate_a_control import (
            NotebookUtilsOneLakeFiles,
        )
        from people_counter.sjd_control import parse_registration_manifest

        if not 1 <= arguments.max_items <= 10_000:
            raise ValueError("--max-items must be between 1 and 10000")
        expected_prefix = selected.file_path("intake/backfill") + "/"
        partition_path = selected.validate_files_path(arguments.partition_path)
        if not partition_path.startswith(expected_prefix):
            raise ValueError(
                "--partition-path must be under the stable backfill intake root"
            )
        partition_sha256 = arguments.partition_sha256
        if re.fullmatch(r"[0-9a-f]{64}", partition_sha256) is None:
            raise ValueError(
                "--partition-sha256 must be 64 lowercase hexadecimal characters"
            )
        content = NotebookUtilsOneLakeFiles().read_text(partition_path)
        actual_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if actual_sha256 != partition_sha256:
            raise ValueError(
                "backfill partition digest mismatch: "
                f"expected {partition_sha256}, got {actual_sha256}"
            )
        requests = parse_registration_manifest(
            content,
            source=partition_path,
        )
        if len(requests) > arguments.max_items:
            raise ValueError(
                "backfill partition contains "
                f"{len(requests)} items; maximum is {arguments.max_items}"
            )
        registered = store.register_many(requests)
        identities = "\n".join(item.work_id for item in registered)
        result = {
            "partition_path": partition_path,
            "partition_sha256": partition_sha256,
            "item_count": len(registered),
            "work_ids_sha256": hashlib.sha256(
                identities.encode("utf-8")
            ).hexdigest(),
        }
    elif arguments.command == "claim":
        claimed = store.claim(
            arguments.owner,
            max_items=arguments.max_items,
            lease_seconds=arguments.lease_seconds,
            minimum_speed_x=arguments.minimum_speed_x,
            safety_factor=arguments.safety_factor,
            margin_seconds=arguments.margin_seconds,
            minimum_items=arguments.minimum_items,
            allowed_work_ids=arguments.work_id,
        )
        result = None if claimed is None else asdict(claimed)
    elif arguments.command == "replay":
        result = asdict(
            store.replay(
                arguments.work_id,
                operator=arguments.operator,
                reason=arguments.reason,
                additional_attempts=arguments.additional_attempts,
            )
        )
    elif arguments.command == "quarantine":
        evidence: dict[str, str] = {}
        for value in arguments.work_evidence:
            work_id, separator, digest = value.partition("=")
            if not separator or work_id in evidence:
                raise ValueError(
                    "--work-evidence must contain unique WORK_ID=SHA256 values"
                )
            evidence[work_id] = digest
        result = asdict(
            store.quarantine(
                evidence,
                operator=arguments.operator,
                reason=arguments.reason,
            )
        )
    elif arguments.command == "recover":
        result = asdict(store.recover(now=arguments.now))
    elif arguments.command == "clear-stale-lock":
        result = _clear_stale_lock(
            _spark(), arguments.expected_owner_id, config=selected
        )
    else:
        result = [asdict(item) for item in store.reconcile()]
    print(json.dumps(result, allow_nan=False, default=str, sort_keys=True))
    return 0


def _process_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-fabric-process-sjd")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--mode", choices=("probe", "sdk"), default="sdk")
    parser.add_argument(
        "--peak-rss-mib",
        type=int,
        help=(
            "Override the executor-measured peak RSS (MiB) used for batch "
            "concurrency; defaults to the profile's own measured value."
        ),
    )
    parser.add_argument("--release-manifest-path", required=True)
    parser.add_argument("--release-manifest-sha256", required=True)
    parser.add_argument("--release-receipt-path", required=True)
    parser.add_argument("--release-receipt-sha256", required=True)
    parser.add_argument(
        "--inject-failure-after-stage-before-receipt",
        action="store_true",
    )
    return parser


def _dispatcher_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-production-dispatcher-sjd")
    parser.add_argument("--owner")
    parser.add_argument("--max-items", type=int, default=64)
    parser.add_argument("--maximum-active-batches", type=int, default=1)
    parser.add_argument("--lease-seconds", type=float, default=14_400.0)
    parser.add_argument("--minimum-speed-x", type=float, default=1.0)
    parser.add_argument("--safety-factor", type=float, default=1.25)
    parser.add_argument("--margin-seconds", type=float, default=60.0)
    parser.add_argument("--minimum-items", type=int, default=1)
    parser.add_argument("--mode", choices=("probe", "sdk"), default="sdk")
    parser.add_argument("--peak-rss-mib", type=int)
    parser.add_argument("--release-manifest-path", required=True)
    parser.add_argument("--release-manifest-sha256", required=True)
    parser.add_argument("--release-receipt-path", required=True)
    parser.add_argument("--release-receipt-sha256", required=True)
    return parser


def dispatcher_main(
    argv: Sequence[str] | None = None,
    *,
    config: FabricCandidateAConfig | None = None,
    route_mode: str | None = None,
) -> int:
    """Atomically claim at most one bounded batch and process it."""
    arguments = _dispatcher_parser().parse_args(argv)
    from people_counter.sjd_control import FabricControlStore

    selected = config or FabricCandidateAConfig()
    selected.require_write_enabled()
    owner = arguments.owner or f"pc-production-dispatcher-{uuid.uuid4().hex}"
    store = FabricControlStore(_spark(), config=selected)
    try:
        claimed = store.claim(
            owner,
            max_items=arguments.max_items,
            maximum_active_batches=arguments.maximum_active_batches,
            lease_seconds=arguments.lease_seconds,
            minimum_speed_x=arguments.minimum_speed_x,
            safety_factor=arguments.safety_factor,
            margin_seconds=arguments.margin_seconds,
            minimum_items=arguments.minimum_items,
        )
        if claimed is None:
            print(
                json.dumps(
                    {
                        "maximum_active_batches": (
                            arguments.maximum_active_batches
                        ),
                        "owner": owner,
                        "status": "IDLE",
                    },
                    sort_keys=True,
                )
            )
            return 0
        process_arguments = [
            "--batch-id",
            claimed.batch_id,
            "--mode",
            arguments.mode,
            "--release-manifest-path",
            arguments.release_manifest_path,
            "--release-manifest-sha256",
            arguments.release_manifest_sha256,
            "--release-receipt-path",
            arguments.release_receipt_path,
            "--release-receipt-sha256",
            arguments.release_receipt_sha256,
        ]
        if arguments.peak_rss_mib is not None:
            process_arguments.extend(
                ["--peak-rss-mib", str(arguments.peak_rss_mib)]
            )
        result = process_main(
            process_arguments,
            config=selected,
            route_mode=route_mode,
        )
        if result != 0:
            raise RuntimeError(
                f"stable process returned unexpected exit code {result}"
            )
        print(
            json.dumps(
                {
                    "batch_id": claimed.batch_id,
                    "item_count": len(claimed.items),
                    "owner": owner,
                    "status": "PROCESSED",
                },
                sort_keys=True,
            )
        )
        return 0
    except Exception as error:
        _write_control_diagnostic(
            selected,
            "dispatch",
            error,
            prefix="dispatcher",
        )
        raise


def _refresh_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-production-refresh-sjd")
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument(
        "--semantic-model-id",
        action="append",
        required=True,
    )
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument("--timeout-seconds", type=float, default=7200.0)
    return parser


def _power_bi_json(
    method: str,
    url: str,
    token: str,
    payload: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], Mapping[str, str]]:
    body = (
        None
        if payload is None
        else json.dumps(
            payload,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            content = response.read()
            parsed = json.loads(content) if content else {}
            if not isinstance(parsed, dict):
                raise RuntimeError(
                    f"{method} {url} returned non-object JSON"
                )
            return parsed, dict(response.headers)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {url} returned {error.code}: {detail}"
        ) from error


def _refresh_semantic_model(
    workspace_id: str,
    semantic_model_id: str,
    *,
    token: str,
    poll_seconds: float,
    timeout_seconds: float,
    request_json: Any = _power_bi_json,
    clock: Any = time.monotonic,
    sleep: Any = time.sleep,
) -> dict[str, Any]:
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise ValueError("poll_seconds must be finite and positive")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and positive")
    base = (
        "https://api.powerbi.com/v1.0/myorg/groups/"
        f"{workspace_id}/datasets/{semantic_model_id}/refreshes"
    )
    _, headers = request_json(
        "POST",
        base,
        token,
        {
            "applyRefreshPolicy": False,
            "commitMode": "transactional",
            "retryCount": 2,
            "type": "Full",
        },
    )
    location = headers.get("Location") or headers.get("location")
    if not isinstance(location, str) or not location:
        raise RuntimeError("Power BI refresh response omitted Location")
    deadline = clock() + timeout_seconds
    while True:
        status, _ = request_json("GET", location, token)
        state = str(status.get("status", ""))
        if state == "Completed":
            return {
                "location": location,
                "request_id": status.get("requestId"),
                "status": state,
            }
        if state in {"Failed", "Cancelled", "Disabled"}:
            raise RuntimeError(
                "Power BI semantic refresh failed: "
                + json.dumps(status, allow_nan=False, sort_keys=True)
            )
        if state not in {"", "Unknown", "NotStarted", "InProgress"}:
            raise RuntimeError(
                f"Power BI semantic refresh returned unknown status {state!r}"
            )
        if clock() >= deadline:
            raise TimeoutError(
                f"Power BI semantic refresh exceeded {timeout_seconds} seconds"
            )
        sleep(poll_seconds)


def refresh_main(
    argv: Sequence[str] | None = None,
    *,
    config: FabricCandidateAConfig | None = None,
) -> int:
    """Refresh the stable semantic model and acknowledge its durable outbox."""
    arguments = _refresh_parser().parse_args(argv)
    from people_counter.fabric_candidate_a_gold import FabricGoldState
    import notebookutils

    selected = config or FabricCandidateAConfig()
    selected.require_write_enabled()
    state = FabricGoldState(_spark(), config=selected)
    pending = state.pending_refreshes()
    if not pending:
        print(json.dumps({"status": "IDLE"}, sort_keys=True))
        return 0
    try:
        token = str(notebookutils.credentials.getToken("pbi"))
        if not token:
            raise RuntimeError("NotebookUtils returned an empty Power BI token")
        refreshes = [
            _refresh_semantic_model(
                arguments.workspace_id,
                semantic_model_id,
                token=token,
                poll_seconds=arguments.poll_seconds,
                timeout_seconds=arguments.timeout_seconds,
            )
            | {"semantic_model_id": semantic_model_id}
            for semantic_model_id in arguments.semantic_model_id
        ]
        acknowledged = []
        for item in pending:
            outbox_id = int(item["outbox_id"])
            dedupe_key = str(item["dedupe_key"])
            if not state.acknowledge_refresh(
                outbox_id,
                actor="pc-production-refresh-sjd",
                expected_dedupe_key=dedupe_key,
            ):
                raise RuntimeError(
                    f"semantic refresh outbox {outbox_id} was not acknowledged"
                )
            acknowledged.append(outbox_id)
        print(
            json.dumps(
                {
                    "acknowledged_outbox_ids": acknowledged,
                    **(
                        {"refresh": refreshes[0]}
                        if len(refreshes) == 1
                        else {"refreshes": refreshes}
                    ),
                    "status": "REFRESHED",
                },
                allow_nan=False,
                sort_keys=True,
            )
        )
        return 0
    except Exception as error:
        _write_control_diagnostic(
            selected,
            "refresh",
            error,
            prefix="refresh",
        )
        raise


def _configure_after_stage_failure(harness: Any, enabled: bool) -> None:
    if not enabled:
        return
    if getattr(harness, "staging_backend", None) != "spark_delta":
        raise RuntimeError(
            "stage-before-receipt injection requires Spark Delta staging"
        )

    def fail_after_stage() -> None:
        raise RuntimeError(
            "injected failure after Delta stage before receipt"
        )

    harness.after_stage_hook = fail_after_stage


def _load_runtime_release_evidence(
    files: Any,
    *,
    manifest_path: str,
    manifest_sha256: str,
    receipt_path: str,
    receipt_sha256: str,
) -> Any:
    """Load detached OneLake objects through independent expected hashes."""
    from people_counter.fabric_release_provenance import (
        detached_manifest_evidence_path,
        load_runtime_release_evidence,
        postpublish_receipt_evidence_path,
    )

    expected_manifest_path = detached_manifest_evidence_path()
    expected_receipt_path = postpublish_receipt_evidence_path()
    if manifest_path != expected_manifest_path:
        raise ValueError(
            "release manifest path differs from the fixed release identity: "
            f"{manifest_path!r} != {expected_manifest_path!r}"
        )
    if receipt_path != expected_receipt_path:
        raise ValueError(
            "release receipt path differs from the fixed release identity: "
            f"{receipt_path!r} != {expected_receipt_path!r}"
        )
    manifest_text = files.read_text(manifest_path)
    receipt_text = files.read_text(receipt_path)
    with tempfile.TemporaryDirectory(prefix="pc-release-evidence-") as root:
        local_manifest = Path(root) / "manifest.json"
        local_receipt = Path(root) / "receipt.json"
        local_manifest.write_text(manifest_text, encoding="utf-8")
        local_receipt.write_text(receipt_text, encoding="utf-8")
        return load_runtime_release_evidence(
            local_manifest,
            local_receipt,
            expected_manifest_sha256=manifest_sha256,
            expected_receipt_sha256=receipt_sha256,
        )


def _process_output(
    result: Any,
    release_evidence: Any,
    *,
    harness_capability: Any,
    resolver_capability: Any,
    consumer_decision: Any,
) -> dict[str, Any]:
    payload = asdict(result)
    payload["staging_path"] = str(payload["staging_path"])
    payload["harness_capability"] = harness_capability
    payload["resolver_capability"] = resolver_capability
    payload["consumer_probe_decision"] = consumer_decision
    payload["release_evidence"] = {
        "identity_sha256": release_evidence.identity_sha256,
        "manifest_sha256": release_evidence.manifest_sha256,
        "receipt_sha256": release_evidence.receipt_sha256,
        "environment_target_version": (
            release_evidence.receipt.environment_target_version
        ),
    }
    return payload


def process_main(
    argv: Sequence[str] | None = None,
    *,
    config: FabricCandidateAConfig | None = None,
    route_mode: str | None = None,
    route_identity: Mapping[str, Any] | None = None,
) -> int:
    """Run one claimed batch using only fixed Candidate A Fabric backends."""
    arguments = _process_parser().parse_args(argv)
    from people_counter.sjd_control import FabricControlStore
    from people_counter.sjd_process import (
        MIB,
        OneLakeDeltaAttemptAdapter,
        resume_committed_process_batch,
        run_process_batch,
        warm_executor_for_works,
    )
    import functools
    from people_counter.fabric_candidate_a_control import (
        NotebookUtilsOneLakeFiles,
    )

    spark = _spark()
    config = config or FabricCandidateAConfig()
    config.require_write_enabled()
    store = FabricControlStore(spark, config=config)
    release_evidence = _load_runtime_release_evidence(
        NotebookUtilsOneLakeFiles(),
        manifest_path=arguments.release_manifest_path,
        manifest_sha256=arguments.release_manifest_sha256,
        receipt_path=arguments.release_receipt_path,
        receipt_sha256=arguments.release_receipt_sha256,
    )
    attempts = OneLakeDeltaAttemptAdapter(
        config.file_path("attempts"),
        spark,
        config=config,
        route_mode=route_mode,
    )
    committed = resume_committed_process_batch(
        store,
        arguments.batch_id,
        attempts,
        release_evidence=release_evidence,
    )
    if committed is not None:
        skipped = {
            "status": "SKIPPED_COMMITTED_RESUME",
            "reason": "batch was already committed and sealed staging was verified",
        }
        print(
            json.dumps(
                _process_output(
                    committed,
                    release_evidence,
                    harness_capability=skipped,
                    resolver_capability=skipped,
                    consumer_decision=skipped,
                ),
                allow_nan=False,
                sort_keys=True,
            )
        )
        return 0
    envelope, _ = store.load_claim_envelope_with_digest(arguments.batch_id)
    consumer_result, consumer_decision = _probe_and_persist_consumer_decision(
        spark,
        config,
        arguments.batch_id,
        envelope,
    )
    fixed_probe = _bind_consumer_probe_to_executor_inventory(
        consumer_result,
        consumer_decision["executor_ids"],
    )
    enrichment, resolver_capability = _localize_process_inputs(
        spark,
        store,
        arguments.batch_id,
        route_mode=route_mode,
        route_identity=route_identity,
        probe_mounted_path=fixed_probe,
    )
    task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    executor_cores = int(spark.conf.get("spark.executor.cores"))
    warm_works = _executor_warm_works(
        envelope,
        enrichment,
        executor_cores=executor_cores,
        task_cpus=task_cpus,
        package_version=release_evidence.manifest.package_version,
        manifest_sha256=release_evidence.manifest_sha256,
    )
    for values in enrichment.values():
        values["_expected_release_package_version"] = (
            release_evidence.manifest.package_version
        )
        values["_expected_release_manifest_sha256"] = (
            release_evidence.manifest_sha256
        )
    profile = _fabric_process_profile(
        spark,
        warm_up=functools.partial(warm_executor_for_works, warm_works),
    )
    harness, harness_capability = select_process_execution_harness(
        spark,
        config,
        arguments.batch_id,
        row_enrichment=enrichment,
        probe_mounted_path=fixed_probe,
    )
    _configure_after_stage_failure(
        harness,
        arguments.inject_failure_after_stage_before_receipt,
    )
    result = run_process_batch(
        store,
        arguments.batch_id,
        profile,
        arguments.mode,
        harness,
        attempts,
        release_evidence=release_evidence,
        peak_rss_bytes=(
            arguments.peak_rss_mib * MIB
            if arguments.peak_rss_mib is not None
            else profile.peak_rss_bytes
        ),
    )
    payload = _process_output(
        result,
        release_evidence,
        harness_capability=harness_capability,
        resolver_capability=resolver_capability,
        consumer_decision=consumer_decision,
    )
    print(json.dumps(payload, allow_nan=False, sort_keys=True))
    return 0


def _gold_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-fabric-gold-sjd")
    parser.add_argument(
        "mode",
        choices=("plan", "validate", "run"),
    )
    parser.add_argument("--lookback-hours", type=int, default=48)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--full-rebuild", action="store_true")
    return parser


def gold_main(
    argv: Sequence[str] | None = None,
    *,
    config: FabricCandidateAConfig | None = None,
) -> int:
    """Build gold through committed pointers and typed Delta tables."""
    arguments = _gold_parser().parse_args(argv)
    from people_counter.fabric_candidate_a_gold import (
        FabricCommittedSource,
        FabricGoldJob,
        FabricGoldState,
        FabricGoldStoreImpl,
    )
    from people_counter.sjd_process import OneLakeDeltaAttemptAdapter

    spark = _spark()
    config = config or FabricCandidateAConfig()
    config.require_write_enabled()
    source = FabricCommittedSource(
        spark,
        OneLakeDeltaAttemptAdapter(
            config.file_path("attempts"), spark, config=config
        ),
        config=config,
    )
    job = FabricGoldJob(
        source,
        FabricGoldStoreImpl(spark, config=config),
        FabricGoldState(spark, config=config),
    )
    if arguments.mode == "plan":
        result: object = job.plan(
            lookback_hours=arguments.lookback_hours,
            full_rebuild=arguments.full_rebuild,
            reset=arguments.force,
        ).to_dict()
    elif arguments.mode == "validate":
        result = job.validate()
    else:
        result = job.run(
            lookback_hours=arguments.lookback_hours,
            force=arguments.force,
            full_rebuild=arguments.full_rebuild,
        )
    print(json.dumps(result, allow_nan=False, sort_keys=True))
    return 0

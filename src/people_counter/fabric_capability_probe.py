"""Fail-closed capability probes for Fabric-platform-dependent features.

Some optimizations in the attached report depend on Fabric Runtime/Spark
API surface that may or may not be exposed in the current environment (for
example, a Fabric-native GPU Spark runtime profile, or an OS-level RSS
high-water-mark reading). Rather than silently labeling such items as
"not applicable", every such item is implemented as an explicit, fail-closed
capability probe: it either proves the capability is available with a
concrete measured value, or records ``FABRIC_PLATFORM_BLOCKED`` with exact
evidence of why it could not be proven. No probe ever recommends or
provisions non-Fabric (external) compute; probes only ever read Fabric's
own exposed Spark configuration and OS-level process telemetry.

``probe_direct_mounted_lakehouse_path`` proves only that the mount is a
*usable POSIX filesystem* (create/write/fsync/rename/read/delete a throwaway
marker). That is a necessary prerequisite for relying on
:data:`people_counter.fabric_input_resolver.InputBackend.FABRIC_DIRECT`, but
it is not sufficient: a mount can be a perfectly normal POSIX filesystem
while still being unusable by the actual consumer libraries the pipeline
depends on (OpenCV's bundled FFmpeg build, safetensors' mmap-based reader,
ONNX Runtime's session loader) for reasons the bare POSIX probe can never
see -- codec support, mmap restrictions on some network filesystems,
provider availability, and so on. ``probe_direct_mount_consumer_capability``
extends the POSIX probe with real-consumer proofs against the exact fixed,
immutable, non-sensitive model/source artifacts the live pipeline actually
reads, so a passing result is evidence about the real consumption path --
never merely about raw byte I/O.
"""

from __future__ import annotations

import resource
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class CapabilityStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    FABRIC_PLATFORM_BLOCKED = "FABRIC_PLATFORM_BLOCKED"


class CapabilityProbeError(ValueError):
    """A capability probe result is internally inconsistent."""


@dataclass(frozen=True)
class CapabilityProbeResult:
    """One capability's proven availability (with evidence) or blocked status."""

    capability: str
    status: CapabilityStatus
    evidence: str
    value: Any | None = None

    def __post_init__(self) -> None:
        if not self.capability:
            raise CapabilityProbeError("capability name is required")
        if not self.evidence:
            raise CapabilityProbeError(
                "evidence is required for every probe result, pass or fail"
            )
        if self.status is CapabilityStatus.AVAILABLE and self.value is None:
            raise CapabilityProbeError(
                "AVAILABLE probes must record a non-null measured value"
            )


def probe_capability(
    capability: str,
    check: Callable[[], tuple[Any, str]],
) -> CapabilityProbeResult:
    """Run one capability check, normalizing any failure to blocked evidence.

    ``check`` must return ``(value, evidence)`` on success. Any exception is
    captured verbatim as the blocked evidence string; the capability is
    never silently marked "not applicable" and never fabricated -- every
    probe either proves availability with a concrete value or records exact
    blocked evidence explaining why it could not.
    """
    try:
        value, evidence = check()
    except Exception as error:  # noqa: BLE001 - deliberately broad fail-closed probe
        return CapabilityProbeResult(
            capability=capability,
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence=f"{type(error).__name__}: {error}",
        )
    return CapabilityProbeResult(
        capability=capability,
        status=CapabilityStatus.AVAILABLE,
        evidence=evidence,
        value=value,
    )


def _host_to_executor_id_map(executors: Any) -> dict[str, str]:
    """Build an authoritative ``host`` -> ``executor_id`` lookup.

    Per-task probes identify themselves by ``SPARK_EXECUTOR_ID``, falling
    back to the observed hostname when that environment variable is not
    set. Live verification against the deployed Fabric Spark 4.1.1 runtime
    showed ``SPARK_EXECUTOR_ID`` is *never* set inside PySpark task
    processes there (confirmed by dumping the full task environment), so
    every per-task reading always falls back to the bare hostname -- which
    never equals ``discover_active_executors``'s numeric Spark executor id
    (``"1"``, ``"2"``, ...). Resolving the observed hostname back to the
    driver-discovered ``ExecutorRecord.executor_id`` through this map
    (``ExecutorRecord`` already carries both fields) is what lets every
    per-executor probe below correctly recognize "one reading per
    discovered executor" instead of spuriously reporting every discovered
    executor as unobserved. Raises if two discovered executors report the
    same host with different ids: that would make host-based resolution
    ambiguous, so it must fail closed rather than silently pick one.
    """
    mapping: dict[str, str] = {}
    for record in executors:
        existing = mapping.get(record.host)
        if existing is not None and existing != record.executor_id:
            raise RuntimeError(
                f"executor host {record.host!r} is reported by multiple "
                f"executor ids ({existing!r} and {record.executor_id!r}); "
                "cannot reliably resolve per-task executor identity by host"
            )
        mapping[record.host] = record.executor_id
    return mapping


def _resolve_executor_id(
    host_to_executor_id: Mapping[str, str], host: str, reported_executor_id: str
) -> str:
    """Prefer the authoritative host-resolved id; fall back to the raw report.

    The fallback preserves compatibility with any platform where
    ``SPARK_EXECUTOR_ID`` genuinely is set and already correct, or where the
    observed host simply is not one of the discovered executors' hosts.
    """
    return host_to_executor_id.get(host, reported_executor_id)


def probe_gpu_spark_runtime(spark_session: Any) -> CapabilityProbeResult:
    """Probe whether the live Spark session exposes a Fabric-native GPU profile.

    Reads only Fabric/Spark-exposed configuration surface
    (``spark_session.conf.get``); never provisions, assumes, or infers GPU
    availability, and never recommends external (non-Fabric) compute.

    Verified against official Microsoft documentation (Microsoft Learn,
    "Apache Spark compute in Microsoft Fabric", and the Fabric Updates Blog
    "Introducing Capacity Pools for Data Engineering and Data Science", both
    checked during this implementation): as of this probe's implementation,
    Fabric Spark compute pools (starter pools and custom pools) are
    documented only in generic CPU/node-size terms (Small/Medium/.../XXL,
    autoscale, CPU/memory), with no documented GPU-accelerated Spark pool
    or GPU resource-profile Spark configuration key. Microsoft Research has
    published GPU-accelerated *query processing* work ("CoddSpeed") for the
    Fabric Data Warehouse engine, but that is a different compute surface
    (not Spark) and is not exposed as an end-user Spark configuration
    option. Consequently ``spark.fabric.resourceProfile.gpu.enabled`` is a
    placeholder key name (no GA Fabric Spark GPU config key exists to
    probe), and this probe is expected to report
    ``FABRIC_PLATFORM_BLOCKED`` on every current Fabric Spark runtime. The
    probe exists so that, if Fabric ever exposes a real GPU Spark profile
    config key, this capability is automatically detected rather than
    requiring a code change to notice -- the key name should be revisited
    against Fabric release notes before being relied upon as a true
    negative.
    """

    def _check() -> tuple[Any, str]:
        runtime = str(
            spark_session.conf.get("spark.fabric.pool.runtimeType", "unknown")
        )
        raw = spark_session.conf.get(
            "spark.fabric.resourceProfile.gpu.enabled", "false"
        )
        gpu_enabled = str(raw).strip().lower() == "true"
        if not gpu_enabled:
            raise RuntimeError(
                "spark.fabric.resourceProfile.gpu.enabled is not 'true' "
                f"(observed {raw!r}, runtime={runtime!r})"
            )
        return (
            gpu_enabled,
            f"spark.fabric.resourceProfile.gpu.enabled=true (runtime={runtime!r})",
        )

    return probe_capability("fabric_gpu_spark_runtime", _check)


def probe_rss_high_water_mark() -> CapabilityProbeResult:
    """Probe whether the executor OS exposes a process RSS high-water mark.

    Uses ``resource.getrusage(RUSAGE_SELF).ru_maxrss``, which is populated
    on Linux/macOS but not meaningfully on platforms lacking that syscall.
    """

    def _check() -> tuple[Any, str]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        peak_kib = usage.ru_maxrss
        if peak_kib <= 0:
            raise RuntimeError(
                f"ru_maxrss reported a non-positive value: {peak_kib}"
            )
        peak_rss_bytes = peak_kib * 1024
        return (
            peak_rss_bytes,
            f"resource.getrusage(RUSAGE_SELF).ru_maxrss={peak_kib}KiB",
        )

    return probe_capability("rss_high_water_mark", _check)


def probe_executor_peak_rss_bytes(
    spark_session: Any,
    executors: Any,
    *,
    warm_up: Callable[[], None] | None = None,
    oversample_per_executor: int = 4,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Measure each discovered executor's own post-warm-up RSS high-water mark.

    ``probe_rss_high_water_mark`` reads ``ru_maxrss`` wherever it happens to
    run; called from the driver (as the benchmark resource-inventory job
    did) it measures the *driver's* memory, not the executor memory that
    actually matters for task-placement safety. This probe instead runs
    ``warm_up`` once per task inside a Spark job spread across the
    discovered executors (oversampled so every executor is likely to run at
    least one task even without barrier scheduling) and reads
    ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` *inside that executor
    task*, tagging each reading with the executor identity
    (``SPARK_EXECUTOR_ID``) the task itself observes. Every executor in
    ``executors`` must be observed with a positive reading or the whole
    probe fails closed (``FABRIC_PLATFORM_BLOCKED``): a partially-observed
    inventory must never be silently treated as a true negative for the
    unmeasured executors.
    """

    def _check() -> tuple[Any, str]:
        expected_ids = {record.executor_id for record in executors}
        if not expected_ids:
            raise RuntimeError("executors must not be empty")
        if warm_up is None:
            raise RuntimeError(
                "executor RSS promotion requires an exact runtime warm-up"
            )
        host_to_executor_id = _host_to_executor_id_map(executors)

        def _probe_partition(_index: int) -> Any:
            import os as _os
            import resource as _resource
            import socket as _socket

            warm_up()
            peak_kib = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            return [(executor_id, host, int(peak_kib) * 1024)]

        run = probe_partitions or _default_probe_partitions
        rows = run(spark_session, len(expected_ids) * oversample_per_executor, _probe_partition)
        observed: dict[str, tuple[str, int]] = {}
        for executor_id, host, peak_rss_bytes in rows:
            executor_id = _resolve_executor_id(host_to_executor_id, host, executor_id)
            if peak_rss_bytes <= 0:
                raise RuntimeError(
                    f"executor {executor_id!r} ({host!r}) reported non-positive RSS"
                )
            previous = observed.get(executor_id)
            if previous is None or peak_rss_bytes > previous[1]:
                observed[executor_id] = (host, peak_rss_bytes)
        missing = expected_ids - observed.keys()
        if missing:
            raise RuntimeError(
                "no RSS reading observed for discovered executors "
                f"{sorted(missing)!r} (observed {sorted(observed)!r})"
            )
        peak_rss_bytes = max(value for _, value in observed.values())
        per_executor = {
            executor_id: value for executor_id, (_, value) in sorted(observed.items())
        }
        return (
            peak_rss_bytes,
            f"max warm RSS across {len(observed)} executors = "
            f"{peak_rss_bytes} bytes (per-executor: {per_executor!r})",
        )

    return probe_capability("executor_peak_rss_bytes", _check)


def probe_direct_mounted_lakehouse_path(
    spark_session: Any,
    executors: Any,
    *,
    mount_root: str = "/lakehouse/default",
    relative_probe_dir: str = "_capability_probe/direct_mount",
    oversample_per_executor: int = 4,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Prove the Fabric mounted Lakehouse ``Files`` path is usable from executors.

    The "direct Lakehouse path" optimization (resolving inputs/outputs via
    ``/lakehouse/default/Files/...`` instead of ``addFile``/``SparkFiles``
    indirection) is only safe to rely on if the mount is *actually visible
    as a real POSIX filesystem inside executor tasks* -- not merely on the
    driver, and not merely assumed because the path exists somewhere.  This
    probe runs, inside a real Spark job spread across every discovered
    executor (oversampled so each executor is very likely to run at least
    one task even without barrier scheduling), a create-only write of a
    small unique marker file under ``mount_root/relative_probe_dir``,
    fsyncs it, atomically renames it into place, reads it back, verifies
    the content round-trips exactly, and removes it again. An executor is
    only counted as proven if *every* reading it produced succeeded; any
    executor with zero observed readings, or any reading that failed for
    any reason, fails the whole probe closed
    (``FABRIC_PLATFORM_BLOCKED``) with the exact per-executor evidence --
    never silently narrowed to a partial "looks fine" success.
    """

    def _check() -> tuple[Any, str]:
        expected_ids = {record.executor_id for record in executors}
        if not expected_ids:
            raise RuntimeError("executors must not be empty")
        host_to_executor_id = _host_to_executor_id_map(executors)

        def _probe_partition(_index: int) -> Any:
            import os as _os
            import socket as _socket
            import uuid as _uuid
            from pathlib import Path as _Path

            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            root = _Path(mount_root) / relative_probe_dir
            try:
                root.mkdir(parents=True, exist_ok=True)
                token = _uuid.uuid4().hex
                marker = root / f"probe-{executor_id}-{_os.getpid()}-{token}.txt"
                temporary = marker.with_name(f"{marker.name}.tmp")
                payload = f"{executor_id}:{token}"
                with temporary.open("x", encoding="utf-8") as stream:
                    stream.write(payload)
                    stream.flush()
                    _os.fsync(stream.fileno())
                _os.replace(temporary, marker)
                read_back = marker.read_text(encoding="utf-8")
                marker.unlink(missing_ok=True)
                if read_back != payload:
                    return [
                        (
                            executor_id,
                            host,
                            False,
                            f"read-back mismatch at {marker}: wrote {payload!r}, "
                            f"read {read_back!r}",
                        )
                    ]
                return [(executor_id, host, True, f"wrote/read/removed {marker}")]
            except Exception as error:  # noqa: BLE001 - evidence, not a crash
                return [
                    (
                        executor_id,
                        host,
                        False,
                        f"{type(error).__name__}: {error} (root={root})",
                    )
                ]

        run = probe_partitions or _default_probe_partitions
        rows = run(
            spark_session, len(expected_ids) * oversample_per_executor, _probe_partition
        )
        observed: dict[str, tuple[str, list[bool], list[str]]] = {}
        for executor_id, host, ok, evidence in rows:
            executor_id = _resolve_executor_id(host_to_executor_id, host, executor_id)
            previous = observed.get(executor_id)
            if previous is None:
                observed[executor_id] = (host, [ok], [evidence])
            else:
                previous[1].append(ok)
                previous[2].append(evidence)
        missing = expected_ids - observed.keys()
        if missing:
            raise RuntimeError(
                "no direct-mount reading observed for discovered executors "
                f"{sorted(missing)!r} (observed {sorted(observed)!r})"
            )
        failures = {
            executor_id: evidence
            for executor_id, (_, oks, evidence) in observed.items()
            if not all(oks)
        }
        if failures:
            raise RuntimeError(
                f"mount {mount_root!r} is not reliably usable on every "
                f"discovered executor: {failures!r}"
            )
        per_executor = {
            executor_id: evidence[-1] for executor_id, (_, _, evidence) in sorted(observed.items())
        }
        return (
            mount_root,
            f"{mount_root!r} proven read/write/atomic-rename/fsync-capable on "
            f"{len(observed)} executors (per-executor evidence: {per_executor!r})",
        )

    return probe_capability("direct_mounted_lakehouse_path", _check)


def _default_probe_partitions(
    spark_session: Any, partitions: int, probe: Callable[[int], Any]
) -> list[Any]:
    """Run ``probe`` across ``partitions`` Spark tasks and flatten the results."""
    rdd = spark_session.sparkContext.parallelize(range(partitions), partitions)
    return rdd.mapPartitionsWithIndex(
        lambda index, _rows: probe(index)
    ).collect()


def probe_spark_event_log_accessible(event_log_dir: Path | str) -> CapabilityProbeResult:
    """Probe whether the configured Spark event-log directory is readable."""

    def _check() -> tuple[Any, str]:
        path = Path(event_log_dir)
        if not path.is_dir():
            raise RuntimeError(f"event log directory does not exist: {path}")
        entries = sorted(entry.name for entry in path.iterdir())
        if not entries:
            raise RuntimeError(f"event log directory is empty: {path}")
        return entries, f"{len(entries)} entries under {path}"

    return probe_capability("spark_event_log_accessible", _check)


# --------------------------------------------------------------------------
# Direct-mount *consumer* proofs.
#
# The POSIX probe above proves the mount is a usable filesystem; everything
# below proves the specific consumer libraries the live pipeline actually
# depends on can really read real (fixed, immutable, non-sensitive) model
# and source artifacts through it -- streamed hashing, OpenCV video
# decoding, safetensors/PyTorch model loading, and ONNX Runtime session
# creation plus one bounded inference, followed by a concurrent-read check
# on one executor. Every probe here is strictly read-only: artifacts are
# only ever opened for reading, never written or deleted.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsumerArtifact:
    """One fixed, immutable, non-sensitive artifact to probe, by mount-root-
    relative path. ``expected_sha256`` is required for the stream-hash
    proof and optional (purely informational) for the others.
    """

    relative_path: str
    expected_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.relative_path:
            raise CapabilityProbeError("relative_path is required")


@dataclass(frozen=True)
class ConsumerProbeProfile:
    """Which real-consumer checks apply for one deployed pipeline profile.

    Every field is optional: a profile only probes the consumer surfaces
    its own real artifacts exercise (for example, an ONNX-only deployment
    profile has no ``safetensors_or_pytorch_model`` to probe), so
    "all artifacts required by the chosen profile pass" is enforced by
    simply never invoking a check for an absent artifact, not by faking a
    pass for it.
    """

    hash_targets: tuple[ConsumerArtifact, ...] = field(default_factory=tuple)
    video: ConsumerArtifact | None = None
    safetensors_or_pytorch_model: ConsumerArtifact | None = None
    onnx_model: ConsumerArtifact | None = None
    concurrent_read_target: ConsumerArtifact | None = None
    planned_concurrent_tasks: int = 4


_STREAM_CHUNK_BYTES = 1024 * 1024


def _stream_sha256(path: Path) -> str:
    """Hash ``path`` by streaming fixed-size chunks -- never a whole-file read."""
    import hashlib

    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_STREAM_CHUNK_BYTES):
            hasher.update(chunk)
    return hasher.hexdigest()


def _run_per_executor(
    spark_session: Any,
    executors: Any,
    *,
    oversample_per_executor: int,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None,
    probe_partition: Callable[[int], Any],
) -> dict[str, tuple[str, list[bool], list[str]]]:
    """Shared fan-out/aggregation for every per-executor consumer probe below.

    Every row produced by ``probe_partition`` must be
    ``(executor_id, host, ok, evidence)``; this aggregates multiple
    (oversampled) readings per executor exactly like
    ``probe_direct_mounted_lakehouse_path`` does, and raises if any
    discovered executor produced zero readings.
    """
    expected_ids = {record.executor_id for record in executors}
    if not expected_ids:
        raise RuntimeError("executors must not be empty")
    host_to_executor_id = _host_to_executor_id_map(executors)
    run = probe_partitions or _default_probe_partitions
    rows = run(
        spark_session, len(expected_ids) * oversample_per_executor, probe_partition
    )
    observed: dict[str, tuple[str, list[bool], list[str]]] = {}
    for executor_id, host, ok, evidence in rows:
        executor_id = _resolve_executor_id(host_to_executor_id, host, executor_id)
        previous = observed.get(executor_id)
        if previous is None:
            observed[executor_id] = (host, [ok], [evidence])
        else:
            previous[1].append(ok)
            previous[2].append(evidence)
    missing = expected_ids - observed.keys()
    if missing:
        raise RuntimeError(
            f"no reading observed for discovered executors {sorted(missing)!r} "
            f"(observed {sorted(observed)!r})"
        )
    return observed


def _require_all_ok(
    observed: dict[str, tuple[str, list[bool], list[str]]], *, label: str
) -> dict[str, str]:
    failures = {
        executor_id: evidence
        for executor_id, (_, oks, evidence) in observed.items()
        if not all(oks)
    }
    if failures:
        raise RuntimeError(f"{label} is not reliable on every executor: {failures!r}")
    return {
        executor_id: evidence[-1]
        for executor_id, (_, _, evidence) in sorted(observed.items())
    }


def probe_stream_hash_artifacts(
    spark_session: Any,
    executors: Any,
    *,
    artifacts: tuple[ConsumerArtifact, ...],
    mount_root: str = "/lakehouse/default",
    oversample_per_executor: int = 4,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Prove every fixed artifact streams-hashes to its reviewed digest.

    Reads each artifact via :func:`_stream_sha256` (chunked, never a whole-
    file ``read_bytes()``) inside a real Spark task on every discovered
    executor. Never writes or deletes the artifact.
    """

    def _check() -> tuple[Any, str]:
        if not artifacts:
            raise RuntimeError("artifacts must not be empty")

        def _probe_partition(_index: int) -> Any:
            import os as _os
            import socket as _socket

            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            rows = []
            for artifact in artifacts:
                path = Path(mount_root) / artifact.relative_path
                try:
                    digest = _stream_sha256(path)
                    if (
                        artifact.expected_sha256 is not None
                        and digest != artifact.expected_sha256
                    ):
                        rows.append(
                            (
                                executor_id,
                                host,
                                False,
                                f"{artifact.relative_path}: sha256 {digest} != "
                                f"expected {artifact.expected_sha256}",
                            )
                        )
                        continue
                    rows.append(
                        (
                            executor_id,
                            host,
                            True,
                            f"{artifact.relative_path}: streamed sha256={digest}",
                        )
                    )
                except Exception as error:  # noqa: BLE001 - evidence, not a crash
                    rows.append(
                        (
                            executor_id,
                            host,
                            False,
                            f"{artifact.relative_path}: {type(error).__name__}: {error}",
                        )
                    )
            return rows

        observed = _run_per_executor(
            spark_session,
            executors,
            oversample_per_executor=oversample_per_executor,
            probe_partitions=probe_partitions,
            probe_partition=_probe_partition,
        )
        per_executor = _require_all_ok(observed, label="stream-hash of fixed artifacts")
        return (
            tuple(artifact.relative_path for artifact in artifacts),
            f"stream-hashed {len(artifacts)} artifact(s) on {len(observed)} "
            f"executors (per-executor evidence: {per_executor!r})",
        )

    return probe_capability("direct_mount_stream_hash", _check)


def probe_opencv_video_consumer(
    spark_session: Any,
    executors: Any,
    *,
    video: ConsumerArtifact,
    mount_root: str = "/lakehouse/default",
    oversample_per_executor: int = 4,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Prove OpenCV can really open/decode the fixed video through the mount.

    Opens ``cv2.VideoCapture`` against the mounted path, verifies it
    reports itself open, reads back frame-count/fps/width/height metadata,
    and seeks to and decodes the first, middle, and last frames (clamped to
    whatever the stream actually reports), verifying each decoded frame is
    non-empty and shaped consistently with the reported metadata. Never
    writes to the video file; releases the capture when done.
    """

    def _check() -> tuple[Any, str]:
        def _probe_partition(_index: int) -> Any:
            import os as _os
            import socket as _socket

            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            path = Path(mount_root) / video.relative_path
            try:
                import cv2

                capture = cv2.VideoCapture(str(path))
                try:
                    if not capture.isOpened():
                        return [
                            (
                                executor_id,
                                host,
                                False,
                                f"cv2.VideoCapture could not open {path}",
                            )
                        ]
                    frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
                    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
                    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    fps = float(capture.get(cv2.CAP_PROP_FPS))
                    if frame_count <= 0 or width <= 0 or height <= 0:
                        return [
                            (
                                executor_id,
                                host,
                                False,
                                f"{path}: non-positive metadata "
                                f"(frames={frame_count}, {width}x{height})",
                            )
                        ]
                    indices = sorted(
                        {0, frame_count // 2, frame_count - 1}
                    )
                    for frame_index in indices:
                        capture.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
                        ok, frame = capture.read()
                        if not ok or frame is None:
                            return [
                                (
                                    executor_id,
                                    host,
                                    False,
                                    f"{path}: failed to decode frame {frame_index}",
                                )
                            ]
                        if frame.shape[0] <= 0 or frame.shape[1] <= 0:
                            return [
                                (
                                    executor_id,
                                    host,
                                    False,
                                    f"{path}: frame {frame_index} has empty shape "
                                    f"{frame.shape!r}",
                                )
                            ]
                    return [
                        (
                            executor_id,
                            host,
                            True,
                            f"{path}: opened, {frame_count} frames, "
                            f"{width}x{height}@{fps:.2f}fps, decoded frames "
                            f"{indices!r}",
                        )
                    ]
                finally:
                    capture.release()
            except Exception as error:  # noqa: BLE001 - evidence, not a crash
                return [
                    (executor_id, host, False, f"{type(error).__name__}: {error}")
                ]

        observed = _run_per_executor(
            spark_session,
            executors,
            oversample_per_executor=oversample_per_executor,
            probe_partitions=probe_partitions,
            probe_partition=_probe_partition,
        )
        per_executor = _require_all_ok(observed, label="OpenCV video consumer proof")
        return (
            video.relative_path,
            f"OpenCV opened/decoded {video.relative_path!r} on {len(observed)} "
            f"executors (per-executor evidence: {per_executor!r})",
        )

    return probe_capability("direct_mount_opencv_video_consumer", _check)


def probe_model_file_consumer(
    spark_session: Any,
    executors: Any,
    *,
    model: ConsumerArtifact,
    mount_root: str = "/lakehouse/default",
    oversample_per_executor: int = 4,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Prove safetensors/PyTorch can really parse the fixed model file.

    Dispatches on the file extension: a ``.safetensors`` file is opened
    with ``safetensors.safe_open`` and exactly one tensor is materialized
    by key -- proving the mount supports safetensors' real (partial, non
    sequential) access pattern without loading the whole file into memory.
    A legacy ``.pt``/``.pth`` checkpoint has no partial-read API in the
    format itself, so ``torch.load`` is used directly; this is recorded
    honestly as a known capability-proof limitation (full-file read
    required by the format, not by this probe's choice) rather than
    silently pretended to be a partial read. Never performs a forward
    pass/inference -- only proves the file parses and its state dict is
    accessible.
    """

    def _check() -> tuple[Any, str]:
        def _probe_partition(_index: int) -> Any:
            import os as _os
            import socket as _socket

            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            path = Path(mount_root) / model.relative_path
            try:
                if path.suffix == ".safetensors":
                    from safetensors import safe_open

                    with safe_open(str(path), framework="pt") as handle:
                        keys = list(handle.keys())
                        if not keys:
                            return [
                                (
                                    executor_id,
                                    host,
                                    False,
                                    f"{path}: safetensors file has no tensors",
                                )
                            ]
                        sample_key = keys[0]
                        tensor = handle.get_tensor(sample_key)
                        evidence = (
                            f"{path}: safetensors, {len(keys)} tensors, sampled "
                            f"{sample_key!r} shape={tuple(tensor.shape)!r} "
                            "(partial read, not whole-file)"
                        )
                else:
                    import torch

                    state = torch.load(
                        str(path), map_location="cpu", weights_only=True
                    )
                    key_count = len(state) if hasattr(state, "__len__") else 1
                    evidence = (
                        f"{path}: torch.load succeeded, {key_count} top-level "
                        "entries (legacy .pt/.pth format has no partial-read "
                        "API, so this is a known full-file-read limitation of "
                        "the format, not of this probe)"
                    )
                return [(executor_id, host, True, evidence)]
            except Exception as error:  # noqa: BLE001 - evidence, not a crash
                return [
                    (executor_id, host, False, f"{type(error).__name__}: {error}")
                ]

        observed = _run_per_executor(
            spark_session,
            executors,
            oversample_per_executor=oversample_per_executor,
            probe_partitions=probe_partitions,
            probe_partition=_probe_partition,
        )
        per_executor = _require_all_ok(observed, label="model file consumer proof")
        return (
            model.relative_path,
            f"loaded {model.relative_path!r} on {len(observed)} executors "
            f"(per-executor evidence: {per_executor!r})",
        )

    return probe_capability("direct_mount_model_file_consumer", _check)


def probe_onnx_runtime_consumer(
    spark_session: Any,
    executors: Any,
    *,
    model: ConsumerArtifact,
    mount_root: str = "/lakehouse/default",
    oversample_per_executor: int = 4,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Prove ONNX Runtime can create a session and, where safe, run inference.

    Creates an ``onnxruntime.InferenceSession`` (CPU execution provider
    only) against the mounted path. If every model input has a fully
    static shape (no symbolic/dynamic dimensions), constructs a bounded
    all-zero input of the exact declared shape/dtype and runs exactly one
    inference -- proving genuine read/execute access rather than only
    session construction. If any input is dynamically shaped, a bounded
    input cannot be safely constructed without guessing a batch/sequence
    size, so this honestly records session-creation-only success instead
    of fabricating an input shape.
    """

    def _check() -> tuple[Any, str]:
        def _probe_partition(_index: int) -> Any:
            import os as _os
            import socket as _socket

            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            path = Path(mount_root) / model.relative_path
            try:
                import numpy as np
                import onnxruntime as ort

                session = ort.InferenceSession(
                    str(path), providers=["CPUExecutionProvider"]
                )
                inputs = session.get_inputs()
                static = all(
                    all(isinstance(dim, int) and dim > 0 for dim in spec.shape)
                    for spec in inputs
                )
                if not static:
                    return [
                        (
                            executor_id,
                            host,
                            True,
                            f"{path}: session created, {len(inputs)} input(s), "
                            "dynamic shape(s) present so bounded inference was "
                            "not attempted (cannot be safely constructed)",
                        )
                    ]
                _NUMPY_DTYPES = {
                    "tensor(float)": np.float32,
                    "tensor(double)": np.float64,
                    "tensor(int64)": np.int64,
                    "tensor(int32)": np.int32,
                }
                feed = {}
                for spec in inputs:
                    dtype = _NUMPY_DTYPES.get(spec.type, np.float32)
                    feed[spec.name] = np.zeros(spec.shape, dtype=dtype)
                outputs = session.run(None, feed)
                evidence = (
                    f"{path}: session created, bounded zero-input inference "
                    f"ran, {len(outputs)} output tensor(s)"
                )
                return [(executor_id, host, True, evidence)]
            except Exception as error:  # noqa: BLE001 - evidence, not a crash
                return [
                    (executor_id, host, False, f"{type(error).__name__}: {error}")
                ]

        observed = _run_per_executor(
            spark_session,
            executors,
            oversample_per_executor=oversample_per_executor,
            probe_partitions=probe_partitions,
            probe_partition=_probe_partition,
        )
        per_executor = _require_all_ok(observed, label="ONNX Runtime consumer proof")
        return (
            model.relative_path,
            f"ONNX Runtime session proven for {model.relative_path!r} on "
            f"{len(observed)} executors (per-executor evidence: {per_executor!r})",
        )

    return probe_capability("direct_mount_onnx_runtime_consumer", _check)


def probe_concurrent_executor_reads(
    spark_session: Any,
    executors: Any,
    *,
    target: ConsumerArtifact,
    concurrent_tasks: int,
    mount_root: str = "/lakehouse/default",
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Prove ``concurrent_tasks`` simultaneous readers on one executor succeed.

    Runs a single Spark task (so all concurrency happens inside one real
    executor process) that spawns ``concurrent_tasks`` threads, each
    independently streaming the same fixed artifact and hashing it; every
    thread must report the identical digest or the probe fails closed.
    This proves the planned concurrent-task count can really share read
    access to the mount within one executor, not merely that one
    single-threaded read works.
    """

    def _check() -> tuple[Any, str]:
        if concurrent_tasks <= 0:
            raise RuntimeError("concurrent_tasks must be positive")

        def _probe_partition(_index: int) -> Any:
            import os as _os
            import socket as _socket

            host = _socket.gethostname()
            executor_id = _os.environ.get("SPARK_EXECUTOR_ID", host)
            path = Path(mount_root) / target.relative_path
            try:
                with ThreadPoolExecutor(max_workers=concurrent_tasks) as pool:
                    digests = list(
                        pool.map(
                            lambda _n: _stream_sha256(path), range(concurrent_tasks)
                        )
                    )
                unique = set(digests)
                if len(unique) != 1:
                    return [
                        (
                            executor_id,
                            host,
                            False,
                            f"{path}: {concurrent_tasks} concurrent readers "
                            f"disagreed on content: {sorted(unique)!r}",
                        )
                    ]
                return [
                    (
                        executor_id,
                        host,
                        True,
                        f"{path}: {concurrent_tasks} concurrent readers agreed "
                        f"on sha256={digests[0]}",
                    )
                ]
            except Exception as error:  # noqa: BLE001 - evidence, not a crash
                return [
                    (executor_id, host, False, f"{type(error).__name__}: {error}")
                ]

        # Deliberately not oversampled: concurrency must be proven within
        # one single executor task/process, so exactly one task is run
        # rather than spread thinly across many.
        run = probe_partitions or _default_probe_partitions
        rows = run(spark_session, 1, _probe_partition)
        if not rows:
            raise RuntimeError("no reading observed for the concurrent-read probe")
        executor_id, host, ok, evidence = rows[0]
        if not ok:
            raise RuntimeError(f"executor {executor_id!r} ({host!r}): {evidence}")
        return (executor_id, f"executor {executor_id!r}: {evidence}")

    return probe_capability("direct_mount_concurrent_reads", _check)


@dataclass(frozen=True)
class ConsumerCapabilityReport:
    """The composed result of every real-consumer proof for one profile."""

    posix: CapabilityProbeResult
    stream_hash: CapabilityProbeResult | None
    video: CapabilityProbeResult | None
    model_file: CapabilityProbeResult | None
    onnx: CapabilityProbeResult | None
    concurrent_reads: CapabilityProbeResult | None

    @property
    def results(self) -> tuple[CapabilityProbeResult, ...]:
        return tuple(
            result
            for result in (
                self.posix,
                self.stream_hash,
                self.video,
                self.model_file,
                self.onnx,
                self.concurrent_reads,
            )
            if result is not None
        )

    @property
    def all_available(self) -> bool:
        return all(
            result.status is CapabilityStatus.AVAILABLE for result in self.results
        )

    @property
    def mount_root(self) -> Any:
        """The underlying POSIX mount root, for drop-in use where callers
        (e.g. :func:`people_counter.fabric_input_resolver.select_input_backend`)
        expect a plain mount-root value from ``CapabilityProbeResult.value``.
        """
        return self.posix.value


def probe_direct_mount_consumer_capability(
    spark_session: Any,
    executors: Any,
    *,
    profile: ConsumerProbeProfile,
    mount_root: str = "/lakehouse/default",
    posix_probe: Callable[..., CapabilityProbeResult] | None = None,
    probe_partitions: Callable[[Any, int, Callable[[int], Any]], Any] | None = None,
) -> CapabilityProbeResult:
    """Compose the POSIX mount probe with every real-consumer proof ``profile`` needs.

    Returns one :class:`CapabilityProbeResult` whose ``value`` (on success)
    is a :class:`ConsumerCapabilityReport` -- this is a drop-in replacement
    for ``probe_mounted_path`` in
    :func:`people_counter.fabric_input_resolver.select_input_backend`, so
    :data:`~people_counter.fabric_input_resolver.InputBackend.FABRIC_DIRECT`
    is only ever selected once every artifact the chosen profile actually
    needs has been proven, not merely the bare POSIX round-trip. A profile
    with no checks configured still requires the POSIX prerequisite to
    pass, but never fabricates a pass for a consumer surface it was not
    asked to prove.
    """

    probe_posix = posix_probe or probe_direct_mounted_lakehouse_path

    def _check() -> tuple[Any, str]:
        posix = probe_posix(
            spark_session,
            executors,
            mount_root=mount_root,
            probe_partitions=probe_partitions,
        )
        if posix.status is not CapabilityStatus.AVAILABLE:
            raise RuntimeError(f"POSIX mount prerequisite failed: {posix.evidence}")

        stream_hash = None
        if profile.hash_targets:
            stream_hash = probe_stream_hash_artifacts(
                spark_session,
                executors,
                artifacts=profile.hash_targets,
                mount_root=mount_root,
                probe_partitions=probe_partitions,
            )
            if stream_hash.status is not CapabilityStatus.AVAILABLE:
                raise RuntimeError(f"stream-hash proof failed: {stream_hash.evidence}")

        video = None
        if profile.video is not None:
            video = probe_opencv_video_consumer(
                spark_session,
                executors,
                video=profile.video,
                mount_root=mount_root,
                probe_partitions=probe_partitions,
            )
            if video.status is not CapabilityStatus.AVAILABLE:
                raise RuntimeError(f"OpenCV video consumer proof failed: {video.evidence}")

        model_file = None
        if profile.safetensors_or_pytorch_model is not None:
            model_file = probe_model_file_consumer(
                spark_session,
                executors,
                model=profile.safetensors_or_pytorch_model,
                mount_root=mount_root,
                probe_partitions=probe_partitions,
            )
            if model_file.status is not CapabilityStatus.AVAILABLE:
                raise RuntimeError(
                    f"model file consumer proof failed: {model_file.evidence}"
                )

        onnx = None
        if profile.onnx_model is not None:
            onnx = probe_onnx_runtime_consumer(
                spark_session,
                executors,
                model=profile.onnx_model,
                mount_root=mount_root,
                probe_partitions=probe_partitions,
            )
            if onnx.status is not CapabilityStatus.AVAILABLE:
                raise RuntimeError(f"ONNX Runtime consumer proof failed: {onnx.evidence}")

        concurrent_reads = None
        if profile.concurrent_read_target is not None:
            concurrent_reads = probe_concurrent_executor_reads(
                spark_session,
                executors,
                target=profile.concurrent_read_target,
                concurrent_tasks=profile.planned_concurrent_tasks,
                mount_root=mount_root,
                probe_partitions=probe_partitions,
            )
            if concurrent_reads.status is not CapabilityStatus.AVAILABLE:
                raise RuntimeError(
                    f"concurrent-read proof failed: {concurrent_reads.evidence}"
                )

        report = ConsumerCapabilityReport(
            posix=posix,
            stream_hash=stream_hash,
            video=video,
            model_file=model_file,
            onnx=onnx,
            concurrent_reads=concurrent_reads,
        )
        summary = "; ".join(
            f"{result.capability}=PASS" for result in report.results
        )
        return (report, f"all {len(report.results)} consumer proof(s) passed: {summary}")

    return probe_capability("direct_mount_consumer_capability", _check)


def redacted_consumer_capability_evidence(
    report: ConsumerCapabilityReport,
    *,
    backend: str,
    library_versions: Mapping[str, str],
) -> dict[str, Any]:
    """Build the persisted evidence payload for one consumer-capability report.

    "Redacted" means: no raw artifact bytes, no environment variables, and
    no absolute host filesystem paths beyond the fixed mount-root-relative
    artifact names already baked into each probe's evidence string (those
    are themselves non-sensitive, fixed, checked-in-style fixture/model
    paths, never user data). Only capability name/status/evidence/value,
    the selected backend, and caller-supplied library version strings are
    persisted.
    """

    return {
        "backend": backend,
        "library_versions": dict(library_versions),
        "results": [
            {
                "capability": result.capability,
                "status": result.status.value,
                "evidence": result.evidence,
            }
            for result in report.results
        ],
        "all_available": report.all_available,
    }


def _canonical_json(value: Any) -> str:
    import json

    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def persist_consumer_capability_evidence(
    files: Any,
    path: str,
    report: ConsumerCapabilityReport,
    *,
    backend: str,
    library_versions: Mapping[str, str],
) -> str:
    """Create-only persist redacted consumer-capability evidence to OneLake.

    Uses the same create-only-write-then-readback-verify contract as the
    rest of the codebase's immutable evidence (see
    :meth:`people_counter.sjd_process.FabricOneLakeAttemptStore.create_success`):
    writing the same ``path`` twice with identical content is tolerated (an
    idempotent retry), but any content conflict at an already-written path
    is a hard failure, and every write is verified by reading the content
    back rather than trusted blindly. ``files`` must implement the
    :class:`people_counter.fabric_candidate_a_control.OneLakeFiles`
    protocol (``exists``/``read_text``/``create_text``).
    """

    payload = redacted_consumer_capability_evidence(
        report, backend=backend, library_versions=library_versions
    )
    content = _canonical_json(payload)
    try:
        files.create_text(path, content)
    except FileExistsError:
        if files.read_text(path) != content:
            raise CapabilityProbeError(
                f"consumer capability evidence at {path!r} conflicts with "
                "a previously persisted (different) result"
            ) from None
    if files.read_text(path) != content:
        raise CapabilityProbeError(
            f"consumer capability evidence readback at {path!r} did not "
            "match what was written"
        )
    return path

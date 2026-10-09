"""Tests for the backend-neutral Candidate A input resolver."""

from __future__ import annotations

import hashlib
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from people_counter.fabric_input_resolver import (
    InputBackend,
    InputIntegrityError,
    InputResolverError,
    confined_relative_path,
    content_addressed_name,
    link_or_stream_copy,
    resolve_direct_mounted_path,
    select_input_backend,
    stream_sha256,
    verify_sha256,
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def test_link_or_stream_copy_is_create_only_under_concurrency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "cache" / "artifact.bin"
    content = b"immutable-content" * 1024
    source.write_bytes(content)
    real_link = os.link
    barrier = threading.Barrier(16)

    def synchronized_link(src: object, dst: object) -> None:
        barrier.wait(timeout=5)
        real_link(src, dst)

    monkeypatch.setattr(os, "link", synchronized_link)
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [
            executor.submit(link_or_stream_copy, source, destination)
            for _ in range(16)
        ]
        for future in futures:
            future.result()

    assert destination.read_bytes() == content


def test_stream_sha256_matches_whole_file_hash_and_reports_size(tmp_path: Path) -> None:
    path = tmp_path / "video.bin"
    content = b"x" * (3 * 1024 * 1024 + 7)
    path.write_bytes(content)
    digest, size_bytes = stream_sha256(path)
    assert digest == _sha256(content)
    assert size_bytes == len(content)


def test_stream_sha256_reads_in_bounded_chunks_never_the_whole_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from people_counter import fabric_input_resolver

    path = tmp_path / "video.bin"
    content = b"z" * 55
    path.write_bytes(content)
    monkeypatch.setattr(fabric_input_resolver, "_CHUNK_BYTES", 10)

    read_sizes: list[object] = []
    real_open = Path.open

    def recording_open(self: Path, *args: object, **kwargs: object):
        handle = real_open(self, *args, **kwargs)
        original_read = handle.read

        def recording_read(size: object = -1) -> bytes:
            read_sizes.append(size)
            return original_read(size)

        handle.read = recording_read
        return handle

    monkeypatch.setattr(Path, "open", recording_open)
    digest, size_bytes = stream_sha256(path)
    assert digest == _sha256(content)
    assert size_bytes == len(content)
    # Every read must request the bounded chunk size explicitly -- never a
    # single unbounded ``read()``/``read(None)`` of the whole file.
    assert read_sizes
    assert all(size == 10 for size in read_sizes)
    assert len(read_sizes) > 1


def test_verify_sha256_succeeds_and_rejects_mismatch_and_malformed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"artifact-bytes")
    expected = _sha256(b"artifact-bytes")

    actual, size_bytes = verify_sha256(path, expected, label="artifact")
    assert actual == expected
    assert size_bytes == len(b"artifact-bytes")

    with pytest.raises(InputIntegrityError, match="artifact SHA-256 mismatch"):
        verify_sha256(path, "0" * 64, label="artifact")
    with pytest.raises(InputResolverError, match="64-character hex digest"):
        verify_sha256(path, "not-a-digest", label="artifact")
    # A short string that is *entirely* valid hex must still be rejected --
    # the length check and the hex-alphabet check are both required, never
    # just one or the other.
    with pytest.raises(InputResolverError, match="64-character hex digest"):
        verify_sha256(path, "abc123", label="artifact")


def test_content_addressed_name_is_collision_free_across_basenames() -> None:
    digest_a = _sha256(b"r18-config")
    digest_b = _sha256(b"r50-config")
    name_a = content_addressed_name("config.json", digest_a)
    name_b = content_addressed_name("config.json", digest_b)
    assert name_a != name_b
    assert name_a == f"{digest_a}-config.json"
    # Identical content under different source paths still resolves to the
    # exact same content-addressed name -- natural dedup, never a collision.
    assert content_addressed_name(
        "other/path/config.json", digest_a
    ) == name_a


def test_content_addressed_name_rejects_bad_digest_and_missing_basename() -> None:
    with pytest.raises(InputResolverError, match="64-character hex digest"):
        content_addressed_name("config.json", "short")
    with pytest.raises(InputResolverError, match="must have a basename"):
        content_addressed_name("", _sha256(b"x"))


def test_confined_relative_path_rejects_escapes_and_absolutes() -> None:
    assert confined_relative_path("a/b/c.json") == "a/b/c.json"
    for unsafe in (
        "/abs/path",
        "a/../b",
        "a/./b",
        "",
        "a\\b",
        "a//b",
        "a/b/",
    ):
        with pytest.raises(InputResolverError, match="confined relative path"):
            confined_relative_path(unsafe)
    # The default label must actually appear in the error message when the
    # caller doesn't override it.
    with pytest.raises(InputResolverError, match=r"^path must be a confined relative path"):
        confined_relative_path("")
    # A non-string, truthy value must raise our own error type -- never an
    # unrelated AttributeError from a stray ``.startswith()`` call on it.
    with pytest.raises(InputResolverError, match="confined relative path"):
        confined_relative_path(42)  # type: ignore[arg-type]


def test_resolve_direct_mounted_path_joins_confined_relative_path() -> None:
    assert resolve_direct_mounted_path(
        "/lakehouse/default", "Files/models/config.json"
    ) == Path("/lakehouse/default/Files/models/config.json")
    with pytest.raises(
        InputResolverError,
        match=r"^lakehouse_relative_path must be a confined relative path",
    ):
        resolve_direct_mounted_path("/lakehouse/default", "../escape")


def test_link_or_stream_copy_hardlinks_when_possible_and_is_idempotent(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    # Two missing parent levels: only ``parents=True`` can create both.
    destination = tmp_path / "nested" / "deeper" / "destination.bin"

    link_or_stream_copy(source, destination)
    assert destination.read_bytes() == b"payload"
    assert destination.stat().st_ino == source.stat().st_ino

    # A second call must not fail or re-copy once the destination exists.
    destination_mtime = destination.stat().st_mtime_ns
    link_or_stream_copy(source, destination)
    assert destination.stat().st_mtime_ns == destination_mtime


def test_link_or_stream_copy_falls_back_to_streaming_copy_when_hardlink_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from people_counter import fabric_input_resolver

    source = tmp_path / "source.bin"
    content = b"streamed-payload" * 1024
    source.write_bytes(content)
    destination = tmp_path / "destination.bin"

    real_link = os.link

    def _raise_source_link_error(src: object, dst: object) -> None:
        if Path(src) == source:
            raise OSError("cross-device link not permitted")
        real_link(src, dst)

    monkeypatch.setattr(os, "link", _raise_source_link_error)
    monkeypatch.setattr(fabric_input_resolver, "_CHUNK_BYTES", 1024)

    read_sizes: list[object] = []
    real_open = Path.open

    def recording_open(self: Path, *args: object, **kwargs: object):
        handle = real_open(self, *args, **kwargs)
        if "r" in (args[0] if args else kwargs.get("mode", "r")):
            original_read = handle.read

            def recording_read(size: object = -1) -> bytes:
                read_sizes.append(size)
                return original_read(size)

            handle.read = recording_read
        return handle

    monkeypatch.setattr(Path, "open", recording_open)
    link_or_stream_copy(source, destination)
    assert read_sizes
    assert all(size == 1024 for size in read_sizes)
    monkeypatch.undo()
    assert destination.read_bytes() == content
    assert destination.stat().st_ino != source.stat().st_ino


def test_select_input_backend_prefers_direct_when_mount_is_proven() -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )
    from people_counter.fabric_executor_inventory import ExecutorRecord

    expected_executors = (
        ExecutorRecord(executor_id="0", host="h0", total_cores=4, max_memory_bytes=1),
    )
    received: dict[str, object] = {}

    def fake_discover(session: object) -> tuple[ExecutorRecord, ...]:
        received["discover_session"] = session
        return expected_executors

    def fake_probe(session: object, executors: object) -> CapabilityProbeResult:
        received["probe_session"] = session
        received["probe_executors"] = executors
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.AVAILABLE,
            evidence="proven on 2 executors",
            value="/lakehouse/default",
        )

    backend, evidence = select_input_backend(
        "spark-session",
        discover_executors=fake_discover,
        probe_mounted_path=fake_probe,
    )
    assert backend is InputBackend.FABRIC_DIRECT
    assert evidence == {
        "capability": "direct_mounted_lakehouse_path",
        "status": "AVAILABLE",
        "evidence": "proven on 2 executors",
        "backend": "FABRIC_DIRECT",
        "mount_root": "/lakehouse/default",
    }
    # The discovered executor tuple -- not a stray ``None`` -- must flow
    # through to the probe call verbatim, and both callables must receive
    # the exact Spark session passed in, never a dropped/``None`` one.
    assert received["discover_session"] == "spark-session"
    assert received["probe_session"] == "spark-session"
    assert received["probe_executors"] == expected_executors


def test_select_input_backend_uses_bare_posix_probe_by_default() -> None:
    """With neither ``probe_mounted_path`` nor ``consumer_probe_profile``
    given, the real bare POSIX probe (``probe_direct_mounted_lakehouse_path``)
    must be the one actually invoked -- not ``None``/a silently-dropped
    callable."""
    from unittest.mock import patch

    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    fake_result = CapabilityProbeResult(
        capability="direct_mounted_lakehouse_path",
        status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
        evidence="default probe used",
    )
    with patch(
        "people_counter.fabric_capability_probe.probe_direct_mounted_lakehouse_path",
        return_value=fake_result,
    ) as patched:
        backend, evidence = select_input_backend(
            "spark-session",
            discover_executors=lambda session: (),
        )
    patched.assert_called_once_with("spark-session", ())
    assert backend is InputBackend.FABRIC_FALLBACK
    assert evidence["evidence"] == "default probe used"


def test_select_input_backend_uses_default_discover_active_executors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from people_counter import fabric_executor_inventory, fabric_input_resolver
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    received: dict[str, object] = {}

    def fake_discover_active_executors(
        session: object, *, minimum_executors: object
    ) -> tuple[object, ...]:
        received["session"] = session
        received["minimum_executors"] = minimum_executors
        return ("executor-record",)

    monkeypatch.setattr(
        fabric_executor_inventory,
        "discover_active_executors",
        fake_discover_active_executors,
    )

    def fake_probe(session: object, executors: object) -> CapabilityProbeResult:
        assert executors == ("executor-record",)
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="not relevant",
        )

    backend, _evidence = fabric_input_resolver.select_input_backend(
        "spark-session", probe_mounted_path=fake_probe
    )
    assert backend is InputBackend.FABRIC_FALLBACK
    # Omitting ``discover_executors`` must fall back to exactly
    # ``discover_active_executors(spark, minimum_executors=1)``.
    assert received == {"session": "spark-session", "minimum_executors": 1}


def test_select_input_backend_falls_back_when_mount_is_unproven() -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    def fake_probe(session: object, executors: object) -> CapabilityProbeResult:
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="mount not visible on executor '1'",
        )

    backend, evidence = select_input_backend(
        "spark-session",
        discover_executors=lambda session: (),
        probe_mounted_path=fake_probe,
    )
    assert backend is InputBackend.FABRIC_FALLBACK
    assert evidence["backend"] == "FABRIC_FALLBACK"
    assert "mount_root" not in evidence
    assert evidence["status"] == "FABRIC_PLATFORM_BLOCKED"


def test_select_input_backend_upgrades_to_consumer_probe_when_given() -> None:
    """When ``consumer_probe_profile`` is given and no explicit
    ``probe_mounted_path`` override is supplied, the real-consumer composed
    probe (not the bare POSIX probe) must be the one actually invoked, and
    a :class:`ConsumerCapabilityReport` value must unwrap to its
    ``mount_root`` for the evidence dict."""
    from unittest.mock import patch

    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
        ConsumerCapabilityReport,
        ConsumerProbeProfile,
    )

    profile = ConsumerProbeProfile()
    posix = CapabilityProbeResult(
        capability="direct_mounted_lakehouse_path",
        status=CapabilityStatus.AVAILABLE,
        evidence="posix ok",
        value="/lakehouse/default",
    )
    report = ConsumerCapabilityReport(
        posix=posix,
        stream_hash=None,
        video=None,
        model_file=None,
        onnx=None,
        concurrent_reads=None,
    )
    recorded = {}

    def fake_composed_probe(session, executors, *, profile, **kwargs):
        recorded["profile"] = profile
        return CapabilityProbeResult(
            capability="direct_mount_consumer_capability",
            status=CapabilityStatus.AVAILABLE,
            evidence="simulated for this test",
            value=report,
        )

    with patch(
        "people_counter.fabric_capability_probe.probe_direct_mount_consumer_capability",
        side_effect=fake_composed_probe,
    ) as patched:
        backend, evidence = select_input_backend(
            "spark-session",
            discover_executors=lambda session: (),
            consumer_probe_profile=profile,
        )
    patched.assert_called_once()
    assert recorded["profile"] is profile
    assert backend is InputBackend.FABRIC_DIRECT
    assert evidence["mount_root"] == "/lakehouse/default"


def test_select_input_backend_probe_mounted_path_wins_over_profile() -> None:
    """An explicit ``probe_mounted_path`` override must always win over
    ``consumer_probe_profile``."""
    from unittest.mock import patch

    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
        ConsumerProbeProfile,
    )

    calls = []

    def fake_probe(session: object, executors: object) -> CapabilityProbeResult:
        calls.append((session, executors))
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="explicit override used",
        )

    with patch(
        "people_counter.fabric_capability_probe.probe_direct_mount_consumer_capability",
    ) as patched:
        select_input_backend(
            "spark-session",
            discover_executors=lambda session: (),
            probe_mounted_path=fake_probe,
            consumer_probe_profile=ConsumerProbeProfile(),
        )
    patched.assert_not_called()
    assert len(calls) == 1

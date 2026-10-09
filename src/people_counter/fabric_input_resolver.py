"""Backend-neutral input resolution for Candidate A's live Fabric call path.

Three backends are recognized, selected once per batch from *proven*
capability evidence -- never assumed and never silently hardwired:

* ``FABRIC_DIRECT`` -- the canonical registered identity (an
  OneLake/Lakehouse-relative path plus a content SHA-256) is resolved
  straight to the mounted path (for example
  ``/lakehouse/default/Files/...``) with no distribution step at all. This
  is only used once
  :func:`people_counter.fabric_capability_probe.probe_direct_mounted_lakehouse_path`
  has proven the mount is visible and read/write/atomic-rename/fsync
  capable *inside executor tasks*, not merely on the driver.
* ``FABRIC_FALLBACK`` -- the existing ``SparkFiles``/``addFile`` broadcast
  mechanism, but keyed by a content-addressed, never-basename-only
  localization name so two different sources that happen to share a
  filename (for example two detector variants that both ship a
  ``config.json``) can never collide, and a single source re-localized
  many times is never redundantly re-copied.
* ``LOCAL`` -- the local/standalone development path: inputs already live
  on a local filesystem and are resolved through the same content-hash
  cache without any Spark distribution step.

This module never decides, by itself, which backend is safe for a batch;
callers must run (or be given the result of)
:func:`~people_counter.fabric_capability_probe.probe_direct_mounted_lakehouse_path`
before selecting :data:`InputBackend.FABRIC_DIRECT`. It only supplies the
mechanical, hash-verified, never-whole-file-read primitives every backend
needs: incremental hashing, content-addressed naming, hardlink-first
materialization, and confined path resolution.
"""

from __future__ import annotations

import hashlib
import uuid
from enum import Enum
from pathlib import Path
from typing import Any

_CHUNK_BYTES = 1024 * 1024
_HEX_DIGITS = frozenset("0123456789abcdef")


class InputBackend(str, Enum):
    FABRIC_DIRECT = "FABRIC_DIRECT"
    FABRIC_FALLBACK = "FABRIC_FALLBACK"
    LOCAL = "LOCAL"


class InputResolverError(ValueError):
    """An artifact's recorded localization data is missing or malformed."""


class InputIntegrityError(ValueError):
    """A resolved artifact's content does not match its registered identity."""


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _HEX_DIGITS for character in value.lower())
    )


def stream_sha256(path: Path) -> tuple[str, int]:
    """Hash ``path`` incrementally; never loads the whole file into memory.

    Returns ``(hex_digest, size_bytes)`` so callers that also need the
    byte count (for example to feed
    :meth:`~people_counter.fabric_source_cache.LocalizedSourceCache.get_or_stage`)
    never need a second pass over the file.
    """
    hasher = hashlib.sha256()
    size_bytes = 0
    with Path(path).open("rb") as stream:
        while chunk := stream.read(_CHUNK_BYTES):
            hasher.update(chunk)
            size_bytes += len(chunk)
    return hasher.hexdigest(), size_bytes


def verify_sha256(path: Path, expected: str, *, label: str) -> tuple[str, int]:
    """Stream-hash ``path`` and raise :class:`InputIntegrityError` on mismatch."""
    if not _is_sha256(expected):
        raise InputResolverError(f"{label} sha256 must be a 64-character hex digest, got {expected!r}")
    actual, size_bytes = stream_sha256(path)
    if actual != expected.lower():
        raise InputIntegrityError(
            f"{label} SHA-256 mismatch for {path}: expected {expected}, got {actual}"
        )
    return actual, size_bytes


def content_addressed_name(original_name: str, content_sha256: str) -> str:
    """Build a localization name keyed by content, never by basename alone.

    Two sources that happen to share a basename (for example ``config.json``
    shipped by two different detector variants) always localize to distinct
    names because the digest -- not the basename -- is the collision
    boundary; a single source re-localized many times always maps back to
    the same name, so repeated identical content naturally dedupes instead
    of re-copying.
    """
    if not _is_sha256(content_sha256):
        raise InputResolverError(
            f"content_sha256 must be a 64-character hex digest, got {content_sha256!r}"
        )
    suffix = Path(original_name).name
    if not suffix:
        raise InputResolverError(f"original_name must have a basename, got {original_name!r}")
    return f"{content_sha256.lower()}-{suffix}"


def confined_relative_path(value: str, *, label: str = "path") -> str:
    """Return ``value`` unchanged once proven to be a confined relative path.

    Note: an explicit ``not value.startswith("/")`` guard is intentionally
    not needed here -- splitting any string that starts with ``"/"`` on
    ``"/"`` always yields a leading empty segment, which the trailing
    ``any(part in {"", ".", ".."} ...)`` check below already rejects. Adding
    a redundant guard would only create an unkillable equivalent mutant.
    """
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise InputResolverError(f"{label} must be a confined relative path, got {value!r}")
    return value


def resolve_direct_mounted_path(mount_root: str, lakehouse_relative_path: str) -> Path:
    """Resolve a canonical Lakehouse-relative identity to its proven mount path.

    ``lakehouse_relative_path`` is validated as a confined relative path
    (no absolute paths, no ``..`` traversal) before being joined onto
    ``mount_root``; this function performs no I/O itself.
    """
    relative = confined_relative_path(
        lakehouse_relative_path, label="lakehouse_relative_path"
    )
    return Path(mount_root) / relative


def link_or_stream_copy(source: Path, destination: Path) -> None:
    """Materialize ``destination`` from ``source`` without a blind whole-file copy.

    Prefers a hardlink -- instant, zero extra disk bytes, safe because every
    resolved input here is treated as immutable, content-addressed data --
    and falls back to an incremental streaming copy (never a single
    unverified ``shutil.copyfile`` call) when hardlinking is not possible,
    for example across filesystem/mount boundaries.
    """
    import os

    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return
    try:
        os.link(source, destination)
        return
    except FileExistsError:
        return
    except OSError:
        pass
    temporary = destination.with_name(
        f".{destination.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        with source.open("rb") as read_stream, temporary.open("xb") as write_stream:
            while chunk := read_stream.read(_CHUNK_BYTES):
                write_stream.write(chunk)
        try:
            os.link(temporary, destination)
        except FileExistsError:
            return
    finally:
        temporary.unlink(missing_ok=True)


def select_input_backend(
    spark: Any,
    *,
    discover_executors: Any = None,
    probe_mounted_path: Any = None,
    consumer_probe_profile: Any = None,
) -> tuple[InputBackend, dict[str, Any]]:
    """Select the input-resolution backend from proven direct-mount capability.

    Mirrors
    :func:`people_counter.fabric_candidate_a_jobs.select_process_execution_harness`'s
    contract exactly: prefers :data:`InputBackend.FABRIC_DIRECT` only once
    the mount is *proven* usable inside every discovered executor's own
    task; otherwise falls back to :data:`InputBackend.FABRIC_FALLBACK` and
    returns the exact capability evidence rather than silently hardwiring
    an unproven mount.

    ``probe_mounted_path``, when given, takes precedence over
    ``consumer_probe_profile`` (test/call-site injection always wins).
    Otherwise, if ``consumer_probe_profile`` is given, the bare POSIX
    prerequisite probe is upgraded to
    :func:`people_counter.fabric_capability_probe.probe_direct_mount_consumer_capability`
    bound to that profile, so :data:`InputBackend.FABRIC_DIRECT` is only
    ever selected once every real consumer artifact the profile requires
    has *also* been proven. Neither argument changes the default
    (bare POSIX-only) behavior for existing callers that pass neither.
    """
    from people_counter.fabric_capability_probe import (
        CapabilityStatus,
        ConsumerCapabilityReport,
        probe_direct_mount_consumer_capability,
        probe_direct_mounted_lakehouse_path,
    )
    from people_counter.fabric_executor_inventory import discover_active_executors

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
        evidence["backend"] = InputBackend.FABRIC_DIRECT.value
        evidence["mount_root"] = mount_root
        return InputBackend.FABRIC_DIRECT, evidence
    evidence["backend"] = InputBackend.FABRIC_FALLBACK.value
    return InputBackend.FABRIC_FALLBACK, evidence

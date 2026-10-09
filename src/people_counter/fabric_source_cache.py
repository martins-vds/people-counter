"""Executor-local, content-hash-keyed cache for localized source/model bytes.

Bounds local disk usage for repeatedly-staged immutable content (for
example, a shared model artifact or a source video reused across many
bucket videos within one executor process) with explicit hit/miss telemetry
and deterministic least-recently-used eviction. One cache instance is
confined to a single process's memory and local disk; it is never designed
to be shared, pickled, or passed across the Spark driver/executor boundary,
so mutable cache state is never shared between executors.
"""

from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


class SourceCacheError(RuntimeError):
    """A content-hash-keyed local cache entry, size, or bound is invalid."""


@dataclass(frozen=True)
class CacheEntry:
    """One immutable, content-addressed localized byte blob."""

    content_sha256: str
    path: Path
    size_bytes: int


def _normalized_digest(value: object) -> str:
    text = str(value).lower() if value is not None else ""
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise SourceCacheError("content_sha256 must be 64 lowercase hexadecimal characters")
    return text


def _hashed_file(path: Path) -> tuple[str, int]:
    hasher = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            hasher.update(chunk)
            size_bytes += len(chunk)
    return hasher.hexdigest(), size_bytes


class LocalizedSourceCache:
    """Bounded local cache of immutable, content-hash-identified byte blobs.

    Entries are keyed strictly by verified content SHA-256, never by
    filename or mutable location, so two differently-named sources with
    identical bytes share one cache slot and a single source can never
    silently serve stale bytes under its hash. Eviction is least-recently-used
    and bounded by both entry count and total bytes.
    """

    def __init__(
        self,
        root: Path,
        *,
        max_entries: int,
        max_total_bytes: int,
    ) -> None:
        if type(max_entries) is not int or max_entries < 1:
            raise SourceCacheError("max_entries must be a positive integer")
        if type(max_total_bytes) is not int or max_total_bytes < 1:
            raise SourceCacheError("max_total_bytes must be a positive integer")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_entries = max_entries
        self.max_total_bytes = max_total_bytes
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.corruptions = 0

    @property
    def total_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self._entries.values())

    def get_or_stage(
        self,
        content_sha256: str,
        *,
        size_bytes: int,
        stager: Callable[[Path], None],
    ) -> CacheEntry:
        """Return the cached entry for ``content_sha256``, staging it once.

        ``stager`` is called with a destination path exactly once per
        distinct digest and must create that path containing exactly
        ``size_bytes`` bytes. The cache always re-hashes the bytes it is
        about to trust, both for an existing on-disk candidate and for
        newly staged content, and it additionally enforces the claimed
        size as a cheap tamper/truncation check.
        """
        digest = _normalized_digest(content_sha256)
        if type(size_bytes) is not int or size_bytes < 1:
            raise SourceCacheError("size_bytes must be a positive integer")
        if size_bytes > self.max_total_bytes:
            raise SourceCacheError(
                f"size_bytes {size_bytes} exceeds cache byte budget {self.max_total_bytes}"
            )
        with self._lock:
            hit = self._existing_entry_hit(digest, size_bytes)
            if hit is not None:
                return hit
            self.misses += 1
            reused = self._reused_disk_entry(digest, size_bytes)
            if reused is not None:
                return reused
            return self._stage_entry(digest, size_bytes, stager)

    def _existing_entry_hit(self, digest: str, size_bytes: int) -> CacheEntry | None:
        entry = self._entries.get(digest)
        if entry is None:
            return None
        if not entry.path.is_file():
            raise SourceCacheError(f"cache entry {digest} is recorded but missing on disk")
        actual_digest, actual_size = _hashed_file(entry.path)
        if actual_digest != entry.content_sha256:
            self._mark_corruption(digest, entry.path)
            return None
        self._raise_on_unexpected_size(digest, actual_size, size_bytes)
        self._entries.move_to_end(digest)
        self.hits += 1
        return entry

    def _reused_disk_entry(self, digest: str, size_bytes: int) -> CacheEntry | None:
        destination = self.root / digest
        if not destination.exists():
            return None
        if not destination.is_file():
            raise SourceCacheError(f"cache entry {digest} is recorded but missing on disk")
        actual_digest, actual_size = _hashed_file(destination)
        if actual_digest != digest:
            self._mark_corruption(digest, destination)
            return None
        self._raise_on_unexpected_size(digest, actual_size, size_bytes)
        return self._remember_entry(digest, destination, actual_size)

    def _stage_entry(
        self,
        digest: str,
        size_bytes: int,
        stager: Callable[[Path], None],
    ) -> CacheEntry:
        destination = self.root / digest
        stager(destination)
        if not destination.is_file():
            raise SourceCacheError(
                f"stager did not create a file for {digest} at {destination}"
            )
        actual_digest, actual_size = _hashed_file(destination)
        if actual_size != size_bytes:
            destination.unlink(missing_ok=True)
            raise SourceCacheError(
                f"staged content for {digest} has size {actual_size}, "
                f"expected {size_bytes}"
            )
        if actual_digest != digest:
            destination.unlink(missing_ok=True)
            raise SourceCacheError(
                f"staged content for {digest} hashed to {actual_digest}"
            )
        return self._remember_entry(digest, destination, actual_size)

    def _remember_entry(self, digest: str, path: Path, size_bytes: int) -> CacheEntry:
        entry = CacheEntry(content_sha256=digest, path=path, size_bytes=size_bytes)
        self._entries[digest] = entry
        self._evict_if_needed()
        return entry

    def _mark_corruption(self, digest: str, path: Path) -> None:
        self._entries.pop(digest, None)
        path.unlink(missing_ok=True)
        self.corruptions += 1

    def _raise_on_unexpected_size(
        self,
        digest: str,
        actual_size: int,
        expected_size: int,
    ) -> None:
        if actual_size != expected_size:
            raise SourceCacheError(
                f"cache entry {digest} has size {actual_size}, "
                f"expected {expected_size}"
            )

    def _evict_if_needed(self) -> None:
        # New and recently-hit entries are always moved/inserted at the end,
        # so the least-recently-used (first) entry can only be the one just
        # requested when it is the sole entry in the cache.
        while (
            len(self._entries) > self.max_entries
            or self.total_bytes > self.max_total_bytes
        ):
            oldest_key = next(iter(self._entries))
            oldest = self._entries.pop(oldest_key)
            oldest.path.unlink(missing_ok=True)
            self.evictions += 1

    def metrics(self) -> dict[str, int]:
        """Return an explicit, never-fabricated snapshot of cache counters."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "corruptions": self.corruptions,
            "entries": len(self._entries),
            "total_bytes": self.total_bytes,
        }

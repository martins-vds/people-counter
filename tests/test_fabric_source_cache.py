"""Tests for the bounded, content-hash-keyed localized source cache."""

from __future__ import annotations

import hashlib
import os
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from people_counter.fabric_source_cache import (
    CacheEntry,
    LocalizedSourceCache,
    SourceCacheError,
    _normalized_digest,
)


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _writer(content: bytes):
    def _stage(destination: Path) -> None:
        destination.write_bytes(content)

    return _stage


class LocalizedSourceCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = (
            Path.cwd()
            / ".test-artifacts"
            / "fabric-source-cache"
            / self.id().replace(".", "_")
        )
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def cache(self, *, max_entries: int = 10, max_total_bytes: int = 10_000) -> LocalizedSourceCache:
        return LocalizedSourceCache(
            self.root,
            max_entries=max_entries,
            max_total_bytes=max_total_bytes,
        )

    def test_constructor_rejects_non_positive_bounds(self) -> None:
        with self.assertRaises(SourceCacheError) as entries_error:
            LocalizedSourceCache(self.root, max_entries=0, max_total_bytes=10)
        self.assertEqual(
            str(entries_error.exception), "max_entries must be a positive integer"
        )
        with self.assertRaises(SourceCacheError) as bytes_error:
            LocalizedSourceCache(self.root, max_entries=10, max_total_bytes=0)
        self.assertEqual(
            str(bytes_error.exception), "max_total_bytes must be a positive integer"
        )

    def test_constructor_creates_missing_nested_root_directories(self) -> None:
        nested = self.root / "a" / "b" / "c"
        self.assertFalse(nested.exists())
        LocalizedSourceCache(nested, max_entries=1, max_total_bytes=1)
        self.assertTrue(nested.is_dir())
        # Constructing again over the same (now-existing) root must not raise.
        LocalizedSourceCache(nested, max_entries=1, max_total_bytes=1)

    def test_first_request_is_a_miss_and_stages_content(self) -> None:
        cache = self.cache()
        content = b"source-bytes"
        digest = _digest(content)
        entry = cache.get_or_stage(
            digest, size_bytes=len(content), stager=_writer(content)
        )
        self.assertIsInstance(entry, CacheEntry)
        self.assertEqual(entry.content_sha256, digest)
        self.assertEqual(entry.path.read_bytes(), content)
        self.assertEqual(
            cache.metrics(),
            {
                "hits": 0,
                "misses": 1,
                "evictions": 0,
                "corruptions": 0,
                "entries": 1,
                "total_bytes": len(content),
            },
        )

    def test_second_request_for_the_same_digest_is_a_hit_and_does_not_restage(
        self,
    ) -> None:
        cache = self.cache()
        content = b"source-bytes"
        digest = _digest(content)
        calls = []

        def stager(destination: Path) -> None:
            calls.append(destination)
            destination.write_bytes(content)

        first = cache.get_or_stage(digest, size_bytes=len(content), stager=stager)
        second = cache.get_or_stage(digest, size_bytes=len(content), stager=stager)
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        self.assertEqual(cache.metrics()["hits"], 1)
        self.assertEqual(cache.metrics()["misses"], 1)

        # A second hit on the same digest must accumulate, not overwrite,
        # the hit counter.
        cache.get_or_stage(digest, size_bytes=len(content), stager=stager)
        self.assertEqual(cache.metrics()["hits"], 2)
        self.assertEqual(cache.metrics()["misses"], 1)
        self.assertEqual(cache.metrics()["corruptions"], 0)

    def test_misses_accumulate_across_distinct_digests(self) -> None:
        cache = self.cache()
        cache.get_or_stage(
            _digest(b"first"), size_bytes=len(b"first"), stager=_writer(b"first")
        )
        cache.get_or_stage(
            _digest(b"second"), size_bytes=len(b"second"), stager=_writer(b"second")
        )
        self.assertEqual(cache.metrics()["misses"], 2)
        self.assertEqual(cache.metrics()["hits"], 0)

    def test_differently_named_identical_bytes_share_one_cache_slot(self) -> None:
        cache = self.cache()
        content = b"identical-bytes"
        digest = _digest(content)
        cache.get_or_stage(digest, size_bytes=len(content), stager=_writer(content))
        cache.get_or_stage(digest, size_bytes=len(content), stager=_writer(content))
        self.assertEqual(cache.metrics()["entries"], 1)

    def test_rejects_invalid_content_sha256(self) -> None:
        cache = self.cache()
        with self.assertRaises(SourceCacheError):
            cache.get_or_stage("not-a-digest", size_bytes=1, stager=_writer(b"x"))

    def test_rejects_a_well_formed_length_digest_containing_invalid_characters(
        self,
    ) -> None:
        """64 characters alone is not sufficient; every character must
        actually be a lowercase hex digit."""
        cache = self.cache()
        not_hex = "g" * 64
        with self.assertRaises(SourceCacheError):
            cache.get_or_stage(not_hex, size_bytes=1, stager=_writer(b"x"))

    def test_normalized_digest_exact_validation_message(self) -> None:
        with self.assertRaises(SourceCacheError) as error:
            _normalized_digest("not-a-digest")
        self.assertEqual(
            str(error.exception),
            "content_sha256 must be 64 lowercase hexadecimal characters",
        )

    def test_rejects_non_positive_size(self) -> None:
        cache = self.cache()
        digest = _digest(b"x")
        with self.assertRaises(SourceCacheError) as error:
            cache.get_or_stage(digest, size_bytes=0, stager=_writer(b"x"))
        self.assertEqual(
            str(error.exception), "size_bytes must be a positive integer"
        )

    def test_accepts_the_minimum_positive_size_of_one(self) -> None:
        cache = self.cache()
        digest = _digest(b"x")
        entry = cache.get_or_stage(digest, size_bytes=1, stager=_writer(b"x"))
        self.assertEqual(entry.size_bytes, 1)

    def test_rejects_a_non_integer_size_even_when_numerically_non_negative(
        self,
    ) -> None:
        """A float size_bytes must always be rejected, regardless of its
        numeric magnitude -- the type check and magnitude check are both
        independently required (an "and" would let a bad-typed-but-large
        value slip through)."""
        cache = self.cache()
        digest = _digest(b"x")
        with self.assertRaises(SourceCacheError):
            cache.get_or_stage(digest, size_bytes=1.5, stager=_writer(b"x"))

    def test_fails_closed_when_staged_size_does_not_match_and_removes_partial_file(
        self,
    ) -> None:
        cache = self.cache()
        content = b"actual-content"
        digest = _digest(content)
        with self.assertRaises(SourceCacheError) as error:
            cache.get_or_stage(
                digest, size_bytes=len(content) + 1, stager=_writer(content)
            )
        self.assertFalse((self.root / digest).exists())
        self.assertEqual(
            str(error.exception),
            f"staged content for {digest} has size {len(content)}, "
            f"expected {len(content) + 1}",
        )

    def test_rejects_staged_same_sized_bytes_with_a_mismatched_hash(self) -> None:
        cache = self.cache()
        claimed_digest = _digest(b"expected-content")
        actual_content = b"actual-contents!"
        self.assertEqual(len(actual_content), len(b"expected-content"))
        with self.assertRaises(SourceCacheError) as error:
            cache.get_or_stage(
                claimed_digest,
                size_bytes=len(actual_content),
                stager=_writer(actual_content),
            )
        self.assertEqual(
            str(error.exception),
            f"staged content for {claimed_digest} hashed to {_digest(actual_content)}",
        )
        self.assertFalse((self.root / claimed_digest).exists())
        self.assertEqual(cache.metrics()["misses"], 1)
        self.assertEqual(cache.metrics()["corruptions"], 0)

    def test_fails_closed_when_stager_does_not_create_a_file(self) -> None:
        cache = self.cache()
        digest = _digest(b"missing")
        with self.assertRaises(SourceCacheError) as error:
            cache.get_or_stage(digest, size_bytes=1, stager=lambda destination: None)
        destination = self.root / digest
        self.assertEqual(
            str(error.exception),
            f"stager did not create a file for {digest} at {destination}",
        )

    def test_size_mismatch_cleanup_tolerates_the_file_already_being_removed(
        self,
    ) -> None:
        """The post-mismatch cleanup unlink must swallow an already-missing
        file (a theoretical concurrent-removal race) rather than letting an
        unrelated FileNotFoundError mask the real SourceCacheError."""
        cache = self.cache()
        content = b"actual-content"
        digest = _digest(content)
        destination = self.root / digest
        original_unlink = Path.unlink

        def racing_unlink(self_path: Path, *args: object, **kwargs: object) -> None:
            if self_path == destination and destination.exists():
                os.remove(destination)
            return original_unlink(self_path, *args, **kwargs)

        with patch.object(Path, "unlink", racing_unlink):
            with self.assertRaises(SourceCacheError):
                cache.get_or_stage(
                    digest, size_bytes=len(content) + 1, stager=_writer(content)
                )

    def test_fails_closed_when_recorded_entry_is_missing_on_disk(self) -> None:
        cache = self.cache()
        content = b"will-be-deleted"
        digest = _digest(content)
        entry = cache.get_or_stage(
            digest, size_bytes=len(content), stager=_writer(content)
        )
        entry.path.unlink()
        with self.assertRaises(SourceCacheError) as error:
            cache.get_or_stage(digest, size_bytes=len(content), stager=_writer(content))
        self.assertEqual(
            str(error.exception),
            f"cache entry {digest} is recorded but missing on disk",
        )

    def test_corrupt_equal_sized_cached_content_is_rehashed_counted_and_replaced(
        self,
    ) -> None:
        cache = self.cache()
        original = b"source-bytes-1"
        replacement = b"source-bytes-2"
        self.assertEqual(len(original), len(replacement))
        digest = _digest(original)
        staged_calls: list[Path] = []

        def replacement_stager(destination: Path) -> None:
            staged_calls.append(destination)
            destination.write_bytes(original)

        entry = cache.get_or_stage(
            digest,
            size_bytes=len(original),
            stager=_writer(original),
        )
        entry.path.write_bytes(replacement)

        refreshed = cache.get_or_stage(
            digest,
            size_bytes=len(original),
            stager=replacement_stager,
        )

        self.assertEqual(len(staged_calls), 1)
        self.assertEqual(refreshed.path.read_bytes(), original)
        self.assertEqual(refreshed.content_sha256, digest)
        self.assertEqual(
            cache.metrics(),
            {
                "hits": 0,
                "misses": 2,
                "evictions": 0,
                "corruptions": 1,
                "entries": 1,
                "total_bytes": len(original),
            },
        )

    def test_evicts_least_recently_used_entry_when_entry_count_exceeds_bound(
        self,
    ) -> None:
        cache = self.cache(max_entries=2, max_total_bytes=10_000)
        first = _digest(b"first")
        second = _digest(b"second")
        third = _digest(b"third")
        cache.get_or_stage(first, size_bytes=len(b"first"), stager=_writer(b"first"))
        cache.get_or_stage(
            second, size_bytes=len(b"second"), stager=_writer(b"second")
        )
        cache.get_or_stage(third, size_bytes=len(b"third"), stager=_writer(b"third"))
        metrics = cache.metrics()
        self.assertEqual(metrics["entries"], 2)
        self.assertEqual(metrics["evictions"], 1)
        self.assertFalse((self.root / first).exists())
        self.assertTrue((self.root / second).exists())
        self.assertTrue((self.root / third).exists())

    def test_recently_hit_entry_is_protected_from_eviction(self) -> None:
        cache = self.cache(max_entries=2, max_total_bytes=10_000)
        first = _digest(b"first")
        second = _digest(b"second")
        third = _digest(b"third")
        cache.get_or_stage(first, size_bytes=len(b"first"), stager=_writer(b"first"))
        cache.get_or_stage(
            second, size_bytes=len(b"second"), stager=_writer(b"second")
        )
        # Re-access "first" so "second" becomes the least-recently-used entry.
        cache.get_or_stage(first, size_bytes=len(b"first"), stager=_writer(b"first"))
        cache.get_or_stage(third, size_bytes=len(b"third"), stager=_writer(b"third"))
        self.assertTrue((self.root / first).exists())
        self.assertFalse((self.root / second).exists())
        self.assertTrue((self.root / third).exists())

    def test_evicts_when_total_bytes_exceed_bound_even_under_entry_limit(self) -> None:
        cache = self.cache(max_entries=10, max_total_bytes=10)
        first = _digest(b"1234567")
        second = _digest(b"abcdefg")
        cache.get_or_stage(first, size_bytes=7, stager=_writer(b"1234567"))
        cache.get_or_stage(second, size_bytes=7, stager=_writer(b"abcdefg"))
        metrics = cache.metrics()
        self.assertLessEqual(metrics["total_bytes"], 10)
        self.assertEqual(metrics["evictions"], 1)
        self.assertFalse((self.root / first).exists())
        self.assertTrue((self.root / second).exists())

    def test_total_bytes_exactly_at_the_bound_does_not_trigger_eviction(self) -> None:
        """total_bytes == max_total_bytes must be tolerated (strictly-greater
        is the eviction trigger, not greater-or-equal). Two entries are used
        so the single-entry eviction guard does not mask the boundary
        comparison itself."""
        cache = self.cache(max_entries=10, max_total_bytes=10)
        first = _digest(b"12345")
        second = _digest(b"67890")
        cache.get_or_stage(first, size_bytes=5, stager=_writer(b"12345"))
        cache.get_or_stage(second, size_bytes=5, stager=_writer(b"67890"))
        metrics = cache.metrics()
        self.assertEqual(metrics["total_bytes"], 10)
        self.assertEqual(metrics["evictions"], 0)
        self.assertTrue((self.root / first).exists())
        self.assertTrue((self.root / second).exists())

    def test_evictions_counter_accumulates_across_multiple_eviction_events(
        self,
    ) -> None:
        cache = self.cache(max_entries=1, max_total_bytes=10_000)
        first = _digest(b"first")
        second = _digest(b"second")
        third = _digest(b"third")
        cache.get_or_stage(first, size_bytes=len(b"first"), stager=_writer(b"first"))
        cache.get_or_stage(
            second, size_bytes=len(b"second"), stager=_writer(b"second")
        )
        cache.get_or_stage(third, size_bytes=len(b"third"), stager=_writer(b"third"))
        self.assertEqual(cache.metrics()["evictions"], 2)

    def test_eviction_tolerates_an_already_missing_on_disk_entry(self) -> None:
        """The eviction cleanup unlink must not raise even when the oldest
        entry's file has already been removed out-of-band on disk."""
        cache = self.cache(max_entries=1, max_total_bytes=10_000)
        first = _digest(b"first")
        second = _digest(b"second")
        cache.get_or_stage(first, size_bytes=len(b"first"), stager=_writer(b"first"))
        (self.root / first).unlink()
        # Must not raise despite the about-to-be-evicted file already gone.
        cache.get_or_stage(
            second, size_bytes=len(b"second"), stager=_writer(b"second")
        )
        self.assertEqual(cache.metrics()["evictions"], 1)

    def test_single_entry_exceeding_byte_budget_is_rejected_even_when_cache_is_empty(
        self,
    ) -> None:
        cache = self.cache(max_entries=1, max_total_bytes=1)
        content = b"too-big-for-the-bound"
        digest = _digest(content)
        with self.assertRaises(SourceCacheError) as error:
            cache.get_or_stage(
                digest,
                size_bytes=len(content),
                stager=_writer(content),
            )
        self.assertEqual(
            str(error.exception),
            f"size_bytes {len(content)} exceeds cache byte budget 1",
        )
        self.assertFalse((self.root / digest).exists())
        self.assertEqual(
            cache.metrics(),
            {
                "hits": 0,
                "misses": 0,
                "evictions": 0,
                "corruptions": 0,
                "entries": 0,
                "total_bytes": 0,
            },
        )


if __name__ == "__main__":
    unittest.main()

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from people_counter.local_storage import (
    ContentAddressedStore,
    ContentHashMismatchError,
    ImmutableContentConflictError,
    PathConfinementError,
)


class ContentAddressedStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "objects"
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_put_is_content_addressed_verified_and_idempotent(self):
        content = b"immutable"
        expected = hashlib.sha256(content).hexdigest()

        first = self.store.put_bytes(content, expected_sha256=expected)
        second = self.store.put_bytes(content, expected_sha256=expected)

        self.assertEqual(first, second)
        self.assertEqual(first.path.read_bytes(), content)
        self.assertEqual(self.store.read_bytes(expected), content)

    def test_put_rejects_expected_hash_mismatch(self):
        with self.assertRaises(ContentHashMismatchError):
            self.store.put_bytes(b"wrong", expected_sha256="0" * 64)

    def test_existing_different_content_is_an_immutable_conflict(self):
        stored = self.store.put_bytes(b"original")
        stored.path.write_bytes(b"tampered")

        with self.assertRaises(ImmutableContentConflictError):
            self.store.put_bytes(b"original")
        with self.assertRaises(ContentHashMismatchError):
            self.store.read_bytes(stored.sha256)

    def test_confinement_rejects_absolute_traversal_and_symlink_escape(self):
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (self.root / "escape").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(
            PathConfinementError,
            "storage paths must be relative",
        ):
            self.store.confined_path("/tmp/object")
        for path in ("../object", "escape/object"):
            with self.subTest(path=path):
                with self.assertRaisesRegex(
                    PathConfinementError,
                    "storage path escapes root",
                ):
                    self.store.confined_path(path)

    def test_json_has_deterministic_content_identity(self):
        first = self.store.put_json({"b": 2, "a": 1})
        second = self.store.put_json({"a": 1, "b": 2})

        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(self.store.read_json(first.sha256), {"a": 1, "b": 2})

    def test_digest_hierarchy_fsyncs_each_parent_after_child_creation(self):
        with patch.object(
            self.store,
            "_fsync_directory",
            wraps=self.store._fsync_directory,
        ) as fsync_directory:
            stored = self.store.put_bytes(b"durable")

        digest_root = self.root / "sha256"
        self.assertEqual(
            [call.args[0] for call in fsync_directory.call_args_list],
            [self.root, digest_root, stored.path.parent],
        )


if __name__ == "__main__":
    unittest.main()

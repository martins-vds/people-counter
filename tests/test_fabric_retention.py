from __future__ import annotations

import copy

import pytest

from people_counter.fabric_control_store import NotebookUtilsOneLakeFiles
from people_counter.fabric_retention import (
    DEFAULT_RETENTION_DAYS,
    MAX_COMPACTION_BATCHES,
    MIN_RETENTION_DAYS,
    FabricRetentionPolicy,
)
from people_counter import fabric_sjd_runtime
from tests.test_fabric_sjd_runtime import MemoryControlStore, MemoryFiles

pytestmark = pytest.mark.usefixtures("fabric_runtime_context")


class RetentionFiles(MemoryFiles):
    def __init__(self) -> None:
        super().__init__()
        self.deleted: list[str] = []

    def delete_tree(self, path: str) -> None:
        self.deleted.append(path)
        prefix = path.rstrip("/") + "/"
        self.content = {
            key: value
            for key, value in self.content.items()
            if key != path and not key.startswith(prefix)
        }
        self.paths = {
            value for value in self.paths if value != path and not value.startswith(prefix)
        }

    def list_children(self, path: str) -> tuple[str, ...]:
        prefix = path.rstrip("/") + "/"
        return tuple(
            sorted(
                {
                    value
                    for value in self.paths
                    if value.startswith(prefix)
                    and "/" not in value[len(prefix):]
                }
            )
        )


class FakeAttemptStore:
    def __init__(self, files: RetentionFiles, root: str) -> None:
        self.files = files
        self.root = root.rstrip("/")
        self.deleted: list[str] = []

    def delete_batch(self, batch_id: str) -> None:
        path = f"{self.root}/process/batch={batch_id}"
        self.deleted.append(batch_id)
        self.files.delete_tree(path)


def _row(table: str, **values: object) -> dict[str, object]:
    return {
        key: values.get(key)
        for key in {
            "attempts": (
                "attempt_id", "work_id", "batch_id", "status", "output_path",
                "created_at", "sealed_at",
            ),
            "batches": (
                "batch_id", "status", "created_at", "sealed_at", "committed_at",
                "lease_expires_at", "envelope_path",
            ),
            "batch_members": ("batch_id", "ordinal", "work_id", "attempt_id"),
            "publications": (
                "publication_sequence", "work_id", "attempt_id", "batch_id",
                "output_path", "output_sha256", "published_at",
            ),
            "reconciliation_findings": (
                "finding_id", "resolved_at", "last_seen_at",
            ),
        }[table]
    }


def test_retention_policy_has_bounded_explicit_defaults() -> None:
    policy = FabricRetentionPolicy()

    assert DEFAULT_RETENTION_DAYS == 30
    assert MIN_RETENTION_DAYS == 7
    assert MAX_COMPACTION_BATCHES == 100
    assert policy.retention_days == DEFAULT_RETENTION_DAYS
    assert policy.max_batches == MAX_COMPACTION_BATCHES
    with pytest.raises(ValueError, match="at least 7"):
        FabricRetentionPolicy(retention_days=6)
    with pytest.raises(ValueError, match="positive"):
        FabricRetentionPolicy(max_batches=0)


def test_retention_dry_run_preserves_active_and_published_provenance() -> None:
    now = 1_000_000.0
    old = now - 31 * 24 * 60 * 60
    recent = now - 2 * 24 * 60 * 60
    files = RetentionFiles()
    store = MemoryControlStore(files)
    store._clock_value = now
    root = store.config.file_path("attempts")
    attempts = FakeAttemptStore(files, root)

    store.data["work"] = [
        {
            "work_id": "published",
            "committed_attempt_id": "attempt-published",
            "lease_attempt_id": None,
            "release_digest": "r" * 64,
        },
        {
            "work_id": "active",
            "committed_attempt_id": None,
            "lease_attempt_id": "attempt-active",
            "release_digest": "s" * 64,
        },
        {
            "work_id": "failed",
            "committed_attempt_id": None,
            "lease_attempt_id": None,
            "release_digest": "t" * 64,
        },
    ]
    for batch_id, status, terminal_at in (
        ("published", "COMMITTED", old),
        ("active", "EXPIRED", old),
        ("obsolete", "COMMITTED", old),
        ("recent", "COMMITTED", recent),
    ):
        store.data["batches"].append(
            _row(
                "batches",
                batch_id=batch_id,
                status=status,
                created_at=terminal_at - 1,
                sealed_at=terminal_at,
                committed_at=terminal_at if status == "COMMITTED" else None,
                lease_expires_at=terminal_at,
                envelope_path=store.config.file_path(
                    f"control/claims/batch={batch_id}/envelope.json"
                ),
            )
        )
        store.data["attempts"].append(
            _row(
                "attempts",
                attempt_id=f"attempt-{batch_id}",
                work_id=batch_id,
                batch_id=batch_id,
                status="FAILED",
                output_path=f"{root}/process/batch={batch_id}/attempt=process",
                created_at=terminal_at - 1,
                sealed_at=terminal_at,
            )
        )
        store.data["batch_members"].append(
            _row(
                "batch_members",
                batch_id=batch_id,
                ordinal=0,
                work_id=batch_id,
                attempt_id=f"attempt-{batch_id}",
            )
        )
        files.paths.add(f"{root}/process/batch={batch_id}")
        files.content[
            store.config.file_path(
                f"control/claims/batch={batch_id}/envelope.json"
            )
        ] = "{}"
        files.paths.add(
            store.config.file_path(f"consumer-decisions/{batch_id}")
        )
    store.data["publications"].append(
        _row(
            "publications",
            publication_sequence=1,
            work_id="published",
            attempt_id="attempt-published",
            batch_id="published",
            output_path=f"{root}/process/batch=published/attempt=process",
            output_sha256="a" * 64,
            published_at=old,
        )
    )
    before = copy.deepcopy(store.data)

    plan = store.compact_history(
        attempt_store=attempts,
        policy=FabricRetentionPolicy(),
        apply=False,
    )

    assert plan["control_batch_ids"] == ["obsolete"]
    assert plan["consumer_batch_ids"] == ["obsolete", "published"]
    assert store.data == before
    assert attempts.deleted == []
    assert files.deleted == []


def test_retention_apply_deletes_only_obsolete_batch_evidence() -> None:
    now = 2_000_000.0
    old = now - 31 * 24 * 60 * 60
    files = RetentionFiles()
    store = MemoryControlStore(files)
    store._clock_value = now
    root = store.config.file_path("attempts")
    attempts = FakeAttemptStore(files, root)
    store.data["work"] = [{
        "work_id": "failed",
        "committed_attempt_id": None,
        "lease_attempt_id": None,
        "release_digest": "r" * 64,
        "status": "DEAD",
    }]
    store.data["batches"] = [
        _row(
            "batches", batch_id="obsolete", status="COMMITTED",
            created_at=old - 1, sealed_at=old, committed_at=old,
            lease_expires_at=old,
            envelope_path=store.config.file_path(
                "control/claims/batch=obsolete/envelope.json"
            ),
        )
    ]
    store.data["attempts"] = [
        _row(
            "attempts", attempt_id="attempt-obsolete", work_id="failed",
            batch_id="obsolete", status="FAILED",
            output_path=f"{root}/process/batch=obsolete/attempt=process",
            created_at=old - 1, sealed_at=old,
        )
    ]
    store.data["batch_members"] = [
        _row(
            "batch_members", batch_id="obsolete", ordinal=0,
            work_id="failed", attempt_id="attempt-obsolete",
        )
    ]
    store.data["reconciliation_findings"] = [
        _row(
            "reconciliation_findings", finding_id="resolved",
            resolved_at=old, last_seen_at=old,
        ),
        _row(
            "reconciliation_findings", finding_id="active",
            resolved_at=None, last_seen_at=old,
        ),
    ]
    files.paths.update({
        f"{root}/process/batch=obsolete",
        store.config.file_path("consumer-decisions/obsolete"),
    })
    files.content[
        store.config.file_path("control/claims/batch=obsolete/envelope.json")
    ] = "{}"

    report = store.compact_history(
        attempt_store=attempts,
        policy=FabricRetentionPolicy(),
        apply=True,
    )

    assert report["deleted"]["batches"] == 1
    assert report["deleted"]["attempts"] == 1
    assert report["deleted"]["batch_members"] == 1
    assert report["deleted"]["reconciliation_findings"] == 1
    assert [item["finding_id"] for item in store.data["reconciliation_findings"]] == [
        "active"
    ]
    assert store.data["work"][0]["release_digest"] == "r" * 64
    assert attempts.deleted == ["obsolete"]
    assert store.compact_history(
        attempt_store=attempts,
        policy=FabricRetentionPolicy(),
        apply=False,
    )["consumer_batch_ids"] == []


def test_notebook_files_delete_tree_uses_recursive_notebookutils(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        "people_counter.fabric_control_store.importlib.import_module",
        lambda _name: type(
            "NotebookUtils",
            (),
            {"fs": type(
                "Fs",
                (),
                {
                    "exists": staticmethod(lambda _path: True),
                    "rm": staticmethod(
                        lambda path, recursive: calls.append((path, recursive))
                    ),
                },
            )()},
        )(),
    )

    NotebookUtilsOneLakeFiles().delete_tree("Files/exact/path")

    assert calls == [("Files/exact/path", True)]


def test_retention_command_is_dry_run_unless_apply_is_explicit(
    monkeypatch, capsys,
) -> None:
    store = type(
        "Store",
        (),
        {
            "spark": object(),
            "compact_history": lambda self, **kwargs: kwargs,
        },
    )()
    monkeypatch.setattr(
        "people_counter.sjd_process.OneLakeDeltaAttemptAdapter",
        lambda *_args, **_kwargs: object(),
    )
    config = MemoryControlStore(RetentionFiles()).config
    arguments = fabric_sjd_runtime._control_parser().parse_args(
        ["compact-retention"]
    )

    assert fabric_sjd_runtime._control_dispatch(arguments, store, config) == 0

    report = capsys.readouterr().out
    assert '"apply": false' in report
    assert "retention_days=30.0" in report


def test_onelake_attempt_retention_uses_exact_batch_boundary() -> None:
    from people_counter.sjd_process import OneLakeDeltaAttemptAdapter

    files = RetentionFiles()
    files.paths.add("root/process/batch=batch-1/attempt=process")
    adapter = object.__new__(OneLakeDeltaAttemptAdapter)
    adapter.root = "root"
    adapter.files = files

    adapter.delete_batch("batch-1")

    assert files.deleted == ["root/process/batch=batch-1"]
    with pytest.raises(RuntimeError, match="path segment"):
        adapter.delete_batch("../other")

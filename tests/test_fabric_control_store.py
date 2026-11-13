import copy
import hashlib
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from people_counter.fabric_control_store import (
    BatchValidationError,
    FabricControlStoreImpl,
    NotebookUtilsOneLakeFiles,
    OneLakeEnvelopeWriter,
)
from people_counter.fabric_control import ControlWriter
from people_counter.sjd_control import LeaseLostError
from people_counter.sjd_process import verify_envelope
from tests.test_fabric_sjd_runtime import MemoryControlStore, MemoryFiles


pytestmark = pytest.mark.usefixtures("fabric_runtime_context")
pytest_plugins = ["tests.test_fabric_gold"]


@pytest.fixture
def claimed_control():
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work", {"camera_id": "camera"}, runtime_key="cpu", duration_seconds=1,
        config_sha256="c" * 64, release_digest="r" * 64, max_attempts=1,
    )
    claim = store.claim(
        "owner", max_items=1, lease_seconds=60,
        minimum_speed_x=1, safety_factor=1, margin_seconds=0.1,
    )
    document, digest = store.load_claim_envelope_with_digest(claim.batch_id)
    envelope = verify_envelope(document, batch_id=claim.batch_id, envelope_sha256=digest)
    return store, claim, envelope


@pytest.mark.parametrize("changes,message", [
    ({"owner": ""}, "owner"),
    ({"max_items": 0}, "max_items"),
    ({"max_items": True}, "max_items"),
    ({"minimum_items": 2}, "minimum_items"),
    ({"maximum_active_batches": 0}, "maximum_active_batches"),
    ({"lease_seconds": float("inf")}, "finite"),
    ({"lease_seconds": 0}, "greater than zero"),
    ({"minimum_speed_x": float("nan")}, "finite"),
    ({"safety_factor": 0.5}, "admission"),
    ({"margin_seconds": -1}, "greater than zero"),
    ({"allowed_work_ids": [""]}, "allowed_work_id"),
])
def test_invalid_claim_does_not_change_work(claimed_control, changes, message):
    store, _, _ = claimed_control
    before = copy.deepcopy(store.data)
    arguments = dict(
        owner="owner", max_items=1, lease_seconds=60,
        minimum_speed_x=1, safety_factor=1, margin_seconds=0.1,
    )
    with pytest.raises(ValueError, match=message):
        store.claim(**(arguments | changes))
    assert store.data == before


def test_future_and_out_of_scope_work_cannot_be_claimed():
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "future", {}, runtime_key="cpu", duration_seconds=1,
        config_sha256="c", release_digest="r", available_at=200,
    )
    arguments = dict(
        max_items=2, lease_seconds=60, minimum_speed_x=1,
        safety_factor=1, margin_seconds=0.1,
    )
    assert store.claim("owner", **arguments) is None
    store._clock_value = 200
    assert store.claim("owner", **arguments, allowed_work_ids=[]) is None
    assert store.claim("owner", **arguments, minimum_items=2) is None
    observed = store.register(
        "future", {}, runtime_key="cpu", duration_seconds=1,
        config_sha256="c", release_digest="r", available_at=200,
    )
    assert observed.attempt_count == 0


@pytest.mark.parametrize("value", [True, "100", None, float("nan"), float("inf")])
def test_fabric_clock_rejects_non_builtin_or_nonfinite_numbers(claimed_control, value):
    store, _, _ = claimed_control
    store._clock = lambda: value
    with pytest.raises(ValueError, match="clock must be finite"):
        store.now()


@pytest.mark.parametrize("number_type", [int, float])
def test_fabric_clock_rejects_numeric_subclasses(claimed_control, number_type):
    class NumericSubclass(number_type):
        pass

    store, _, _ = claimed_control
    store._clock = lambda: NumericSubclass(100)
    with pytest.raises(ValueError, match="clock must be finite"):
        store.now()


def test_fabric_failed_attempt_is_terminal_and_replay_adds_budget(claimed_control):
    store, claim, envelope = claimed_control
    item = claim.items[0]
    output = {
        "work_id": item.work_id, "attempt_id": item.attempt_id, "succeeded": False,
        "output_path": "Files/output", "output_sha256": "a" * 64,
        "records": [{
            "record_sequence": 0, "error": "processing failed",
            "executor_identity": "executor", "partition_id": 0, "task_attempt_id": 0,
        }],
    }
    arguments = dict(
        envelope_sha256=claim.envelope_sha256,
        membership_sha256=envelope.membership_sha256,
    )
    store.seal_batch(claim.batch_id, [output], **arguments)
    store.seal_batch(claim.batch_id, [output], **arguments)
    with pytest.raises(LeaseLostError, match="heartbeat"):
        store.heartbeat_process(envelope, 1)
    assert store.commit_batch(claim.batch_id) == ()
    arguments = dict(
        runtime_key="cpu", duration_seconds=1, config_sha256="c" * 64,
        release_digest="r" * 64, max_attempts=1,
    )
    assert store.register("work", {"camera_id": "camera"}, **arguments).status == "DEAD"
    store.assert_process_fence(envelope)
    replay = store.replay("work", operator="operator", reason="repair", additional_attempts=2)
    assert replay.generation == 1
    observed = store.register("work", {"camera_id": "camera"}, **arguments)
    assert observed.max_attempts == 3
    assert observed.status == "READY"


@pytest.mark.parametrize("operation", ["heartbeat", "seal", "commit", "fence"])
def test_expired_fabric_lease_cannot_advance_state(claimed_control, operation):
    store, claim, envelope = claimed_control
    store._clock_value = claim.lease_expires_at
    before = copy.deepcopy(store.data)
    with pytest.raises(LeaseLostError):
        if operation == "heartbeat":
            store.heartbeat_process(envelope, 60)
        elif operation == "seal":
            store.seal_batch(
                claim.batch_id, [], envelope_sha256=claim.envelope_sha256,
                membership_sha256=envelope.membership_sha256,
            )
        elif operation == "commit":
            store.commit_batch(claim.batch_id)
        else:
            store.assert_process_fence(envelope)
    assert store.data == before


def test_leased_work_cannot_be_replayed_and_missing_work_is_explicit(claimed_control):
    store, _, _ = claimed_control
    with pytest.raises(LeaseLostError, match="leased"):
        store.replay("work", operator="operator", reason="repair")
    with pytest.raises(ValueError, match="positive"):
        store.replay("work", operator="operator", reason="repair", additional_attempts=0)
    with pytest.raises(KeyError):
        store.replay("absent", operator="operator", reason="repair")
    with pytest.raises(ValueError, match="non-empty mapping"):
        store.quarantine({}, operator="operator", reason="repair")
    with pytest.raises(KeyError):
        store.quarantine({"absent": "a" * 64}, operator="operator", reason="repair")


def test_fabric_reconciliation_tracks_repeated_and_resolved_invalid_pointers(claimed_control):
    store, _, _ = claimed_control
    store.data["work"][0]["committed_attempt_id"] = "missing"
    findings = store.reconcile(now=101)
    assert [finding.finding_type for finding in findings] == ["INVALID_COMMITTED_POINTER"]
    repeated = store.reconcile(now=102)
    assert repeated[0].finding_id == findings[0].finding_id
    assert len(store.data["reconciliation_findings"]) == 1
    store.data["work"][0]["committed_attempt_id"] = None
    store.reconcile(now=103)
    assert store.data["reconciliation_findings"][0]["resolved_at"] == 103


@pytest.mark.parametrize("content,message", [
    ("{", "not valid JSON"), ("[]", "must be an object"),
])
def test_envelope_rejects_bad_json_even_with_matching_digest(content, message):
    files = MemoryFiles()
    store = MemoryControlStore(files)
    files.content["bad"] = content
    with pytest.raises(BatchValidationError, match=message):
        store.envelopes.read("bad", hashlib.sha256(content.encode()).hexdigest())


def test_envelope_rejects_wrong_digest_and_unsafe_batch_names():
    files = MemoryFiles()
    store = MemoryControlStore(files)
    path, _ = store.envelopes.write("batch", {"batch_id": "batch"})
    with pytest.raises(BatchValidationError, match="digest mismatch"):
        store.envelopes.read(path, "0" * 64)
    for batch_id in ("../escape", ".", ""):
        with pytest.raises(ValueError):
            store.envelopes.write(batch_id, {})
    with pytest.raises(ValueError, match="fixed Fabric"):
        OneLakeEnvelopeWriter("wrong-root", files, config=store.config)


def test_claim_envelope_batch_identity_is_checked_after_digest(claimed_control):
    store, claim, _ = claimed_control
    batch = store.data["batches"][0]
    content = json.dumps({"batch_id": "another"})
    store.envelopes.files.content[batch["envelope_path"]] = content
    batch["envelope_sha256"] = hashlib.sha256(content.encode()).hexdigest()
    with pytest.raises(BatchValidationError, match="batch mismatch"):
        store.load_claim_envelope_with_digest(claim.batch_id)


def test_notebook_files_adapter_reports_create_conflicts_and_io_failures(monkeypatch):
    fs = Mock()
    fs.exists.return_value = False
    fs.head.return_value = "content"
    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=fs))
    files = NotebookUtilsOneLakeFiles()
    assert files.read_text("path") == "content"
    fs.head.assert_called_once_with("path", 100 * 1024 * 1024)
    files.create_text("path", "content")
    fs.put.assert_called_once_with("path", "content", False)
    fs.put.return_value = False
    with pytest.raises(OSError, match="create failed"):
        files.create_text("path", "content")
    fs.exists.return_value = True
    with pytest.raises(FileExistsError):
        files.create_text("path", "content")


@pytest.mark.parametrize("table,field,value", [
    ("work", "fence", None),
    ("attempts", "fence", "invalid"),
    ("batches", "lease_expires_at", None),
    ("attempts", "lease_expires_at", "invalid"),
    ("work", "fence", "missing"),
    ("batches", "envelope_sha256", "missing"),
])
def test_exact_recovery_rejects_stale_rows_before_malformed_identity(
    claimed_control, table, field, value,
):
    store, claim, envelope = claimed_control
    store.data["work"][0]["status"] = "READY"
    if value == "missing":
        del store.data[table][0][field]
    else:
        store.data[table][0][field] = value
    before = copy.deepcopy(store.data)
    item = claim.items[0]
    with pytest.raises(LeaseLostError, match="identity or fence differs"):
        store.recover_exact(
            work_id=item.work_id, batch_id=claim.batch_id,
            attempt_id=item.attempt_id, owner="owner", fence=item.fence,
            lease_expires_at=claim.lease_expires_at,
            envelope_sha256=claim.envelope_sha256,
            membership_sha256=envelope.membership_sha256,
            now=claim.lease_expires_at + 1, safe_skew_seconds=1,
        )
    assert store.data == before


def test_exact_recovery_stale_readback_short_circuits_missing_later_fields(
    claimed_control, monkeypatch,
):
    store, claim, envelope = claimed_control
    replace_many = store._replace_many

    def stale_readback(**tables):
        replace_many(**tables)
        store.data["work"][0]["status"] = "READY"
        del store.data["batches"][0]["status"]

    monkeypatch.setattr(store, "_replace_many", stale_readback)
    item = claim.items[0]
    with pytest.raises(LeaseLostError, match="readback differs"):
        store.recover_exact(
            work_id=item.work_id, batch_id=claim.batch_id,
            attempt_id=item.attempt_id, owner="owner", fence=item.fence,
            lease_expires_at=claim.lease_expires_at,
            envelope_sha256=claim.envelope_sha256,
            membership_sha256=envelope.membership_sha256,
            now=claim.lease_expires_at + 1, safe_skew_seconds=1,
        )


@pytest.fixture
def delta_control(gold_spark, tmp_path):
    database = f"control_test_{uuid4().hex}"
    gold_spark.sql(f"CREATE DATABASE {database} LOCATION '{tmp_path.as_uri()}'")
    try:
        store = FabricControlStoreImpl(
            gold_spark, files=MemoryFiles(), clock=lambda: 100.0,
            auto_bootstrap=False,
        )
        store.tables = {
            suffix: f"{database}.{suffix}" for suffix in store.tables
        }
        store.writer = ControlWriter(gold_spark, store.tables["locks"])
        store.bootstrap()
        yield store
    finally:
        gold_spark.sql(f"DROP DATABASE {database} CASCADE")


def test_delta_compatibility_migrates_existing_work_and_processes_a_mixed_queue(delta_control):
    from people_counter.fabric_control_store import _SCHEMAS

    store = delta_control
    for work_id, payload in (("blocked", {"work_schema_version": 2}), ("supported", {})):
        store.register(
            work_id, payload, runtime_key="cpu", duration_seconds=1,
            config_sha256="config", release_digest="old-release",
        )
    legacy_schema = _SCHEMAS["work"].replace(
        "submitted_sdk_version string, compatibility_json string, ", "",
    )
    old_rows = [
        {key: value for key, value in row.items() if key not in ("submitted_sdk_version", "compatibility_json")}
        for row in store._rows("work")
    ]
    store.spark.createDataFrame(old_rows, schema=legacy_schema).write.format("delta").mode(
        "overwrite",
    ).option("overwriteSchema", "true").saveAsTable(store.tables["work"])
    store.bootstrap()
    store.bootstrap()
    assert {"submitted_sdk_version", "compatibility_json"} <= set(store.spark.table(store.tables["work"]).columns)
    assert [
        {key: value for key, value in row.items() if key not in ("submitted_sdk_version", "compatibility_json")}
        for row in store._rows("work")
    ] == old_rows
    batch = store.claim(
        "driver", max_items=2, lease_seconds=60,
        minimum_speed_x=1, safety_factor=1, margin_seconds=1,
    )
    assert [item.work_id for item in batch.items] == ["supported"]
    assert store.get_work("blocked").status == "BLOCKED_INCOMPATIBLE"
    assert store.get_work("blocked").attempt_count == 0
    assert store.recheck_compatibility(
        ["blocked"], operator="ops", reason="new SDK",
    )[0].status == "BLOCKED_INCOMPATIBLE"
    assert store.spark.table(store.tables["attempts"]).count() == 1
    assert store.spark.table(store.tables["locks"]).first().owner_id is None


def test_delta_exact_recovery_and_replacement_fail_closed(delta_control, monkeypatch):
    store = delta_control
    registration = {
        "runtime_key": "cpu", "duration_seconds": 1, "config_sha256": "c" * 64,
        "release_digest": "r" * 64, "max_attempts": 1,
    }
    assert store.register("work", {}, **registration).status == "READY"
    claim = store.claim(
        "owner", max_items=1, lease_seconds=10, minimum_speed_x=1,
        safety_factor=1, margin_seconds=0.1,
    )
    assert claim is not None
    document, digest = store.load_claim_envelope_with_digest(claim.batch_id)
    envelope = verify_envelope(document, batch_id=claim.batch_id, envelope_sha256=digest)
    item = claim.items[0]
    recovery = {
        "work_id": item.work_id, "batch_id": claim.batch_id, "attempt_id": item.attempt_id,
        "owner": "owner", "fence": item.fence, "lease_expires_at": claim.lease_expires_at,
        "envelope_sha256": digest, "membership_sha256": envelope.membership_sha256,
        "now": claim.lease_expires_at + 1, "safe_skew_seconds": 1,
    }
    assert store.recover_exact(**recovery) == {
        "work_id": "work", "batch_id": claim.batch_id,
        "attempt_id": item.attempt_id, "fence": item.fence,
        "outcome": "DEAD", "idempotent": False,
    }
    assert store.recover_exact(**recovery)["idempotent"] is True
    assert store.register("work", {}, **registration).status == "DEAD"
    assert store.claim(
        "next-owner", max_items=1, lease_seconds=10, minimum_speed_x=1,
        safety_factor=1, margin_seconds=0.1,
    ) is None
    assert store.spark.table(store.tables["attempts"]).first().recovery_outcome == "DEAD"
    assert store.spark.table(store.tables["batches"]).first().status == "EXPIRED"

    rows = store._rows("work")
    with pytest.raises(BatchValidationError, match="duplicate work table identity"):
        store._replace("work", rows + rows)
    assert store._rows("work") == rows

    read_rows = store._rows
    work_reads = 0

    def stale_readback(suffix):
        nonlocal work_reads
        observed = read_rows(suffix)
        if suffix == "work":
            work_reads += 1
            if work_reads == 2:
                return rows
        return observed

    with monkeypatch.context() as patch:
        patch.setattr(store, "_rows", stale_readback)
        with pytest.raises(BatchValidationError, match="exact readback/cardinality"):
            store.register("unverified", {}, **registration)
    assert {row.work_id for row in store.spark.table(store.tables["work"]).collect()} == {
        "work", "unverified",
    }
    lock = store.spark.table(store.tables["locks"]).first()
    assert lock.owner_id is not None
    assert lock.acquired_at is not None

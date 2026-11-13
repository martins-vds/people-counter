import json
import sqlite3
import hashlib

import pytest

from people_counter.config import DETECTOR_FLOOR
from people_counter.work_compatibility import compatibility_evaluation, evaluate_compatibility
from people_counter.sjd_control import ControlError, SQLiteControlStore
from tests.test_fabric_sjd_runtime import MemoryControlStore, MemoryFiles


@pytest.fixture(params=["sqlite", "delta"])
def store(request, tmp_path, fabric_runtime_context):
    if request.param == "sqlite":
        return SQLiteControlStore(tmp_path / "control.sqlite3", clock=lambda: 100)
    return MemoryControlStore(MemoryFiles())


def register(store, work_id, payload, **kwargs):
    return store.register(
        work_id, payload, runtime_key="runtime", duration_seconds=1,
        config_sha256="config", release_digest="old-release", **kwargs,
    )


def claim(store, **kwargs):
    return store.claim(
        "dispatcher", max_items=10, lease_seconds=100, minimum_speed_x=1,
        safety_factor=1, margin_seconds=1, **kwargs,
    )


def stored_work(store, work_id):
    if isinstance(store, SQLiteControlStore):
        with store._connect() as connection:
            return dict(connection.execute("SELECT * FROM work WHERE work_id=?", (work_id,)).fetchone())
    return next(row for row in store._rows("work") if row["work_id"] == work_id)


def test_mixed_queue_blocks_one_item_without_starving_supported_work(store):
    register(store, "unsupported", {"work_schema_version": 2}, submitted_sdk_version="0.9.0")
    register(store, "supported", {})
    batch = claim(store)
    assert [item.work_id for item in batch.items] == ["supported"]
    blocked = store.get_work("unsupported")
    assert blocked.status == "BLOCKED_INCOMPATIBLE"
    assert blocked.attempt_count == 0
    assert blocked.compatibility["reason_code"] == "UNSUPPORTED_WORK_SCHEMA"
    assert blocked.compatibility["required_contract"]["work_schema_version"] == 2
    assert blocked.compatibility["evaluating_sdk_version"]
    assert blocked.compatibility["evaluated_at"] == 100
    assert blocked.compatibility["reason"] == "Unsupported work_schema_version: 2"
    store.recover(now=100000)
    assert store.get_work("unsupported").status == "BLOCKED_INCOMPATIBLE"
    with pytest.raises(ControlError, match="blocked|BLOCKED"):
        store.replay("unsupported", operator="ops", reason="upgrade")


@pytest.mark.parametrize("scope", [[], "", [""]])
def test_recheck_requires_explicit_nonempty_ids(store, scope):
    messages = {
        "[]": "recheck requires selected work IDs",
        "''": "allowed_work_ids must be a collection",
        "['']": "allowed work_id must be a non-empty string",
    }
    with pytest.raises(ValueError) as error:
        store.recheck_compatibility(scope, operator="ops", reason="upgrade")
    assert str(error.value) == messages[repr(scope)]


@pytest.mark.parametrize("field", ["operator", "reason"])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_recheck_rejects_invalid_attribution_without_changing_work(store, field, value):
    register(store, "blocked", {"work_schema_version": 2})
    claim(store)
    before = stored_work(store, "blocked")
    arguments = {"operator": "ops", "reason": "upgrade", field: value}
    with pytest.raises(ValueError) as error:
        store.recheck_compatibility(["blocked"], **arguments)
    assert str(error.value) == f"{field} must be a non-empty string"
    assert stored_work(store, "blocked") == before


def test_recheck_identifies_missing_work_without_changing_selected_work(store):
    register(store, "blocked", {"work_schema_version": 2})
    claim(store)
    before = stored_work(store, "blocked")
    with pytest.raises(KeyError) as error:
        store.recheck_compatibility(["blocked", "missing"], operator="ops", reason="upgrade")
    assert error.value.args == ("missing",)
    assert stored_work(store, "blocked") == before


def test_blocking_persists_when_supported_items_cannot_form_a_batch(store):
    register(store, "unsupported", {"payload_schema_version": 2})
    register(store, "supported", {})
    assert claim(store, minimum_items=2) is None
    assert store.get_work("unsupported").status == "BLOCKED_INCOMPATIBLE"
    assert store.get_work("supported").attempt_count == 0


def test_explicit_recheck_keeps_unsupported_then_releases_after_upgrade(store, monkeypatch):
    import people_counter.work_compatibility as compatibility

    register(store, "unsupported", {"work_schema_version": 2})
    assert claim(store) is None
    before = stored_work(store, "unsupported")
    monkeypatch.setattr(store, "_clock", lambda: 125)
    report = store.recheck_compatibility(
        ["unsupported"], operator="ops", reason="new SDK",
    )
    assert report[0].status == "BLOCKED_INCOMPATIBLE"
    assert report[0].compatibility["operator"] == "ops"
    assert report[0].compatibility["supported"] is False
    assert report[0].compatibility["evaluated_at"] == 125
    rejected = stored_work(store, "unsupported")
    assert rejected["updated_at"] == 125
    assert json.loads(rejected["last_error"]) == report[0].compatibility
    monkeypatch.setattr(
        compatibility, "evaluate_compatibility",
        lambda payload, **kwargs: compatibility.CompatibilityDecision(True, dict(payload)),
    )
    monkeypatch.setattr(compatibility, "resolve_installed_package_version", lambda: "0.10.2")
    monkeypatch.setattr(store, "_clock", lambda: 150)
    report = store.recheck_compatibility(
        ["unsupported"], operator="ops", reason="supported now",
    )
    assert report[0].status == "READY"
    assert report[0].attempt_count == 0
    assert report[0].max_attempts == 3
    assert report[0].compatibility["operator_reason"] == "supported now"
    assert report[0].compatibility["evaluating_sdk_version"] == "0.10.2"
    assert report[0].compatibility["supported"] is True
    assert report[0].compatibility["evaluated_at"] == 150
    accepted = stored_work(store, "unsupported")
    assert accepted["updated_at"] == 150
    assert accepted["last_error"] is None
    mutable = {"status", "compatibility_json", "last_error", "updated_at"}
    assert {key: value for key, value in accepted.items() if key not in mutable} == {
        key: value for key, value in before.items() if key not in mutable
    }
    assert claim(store) is not None


def test_recheck_invalid_scope_does_not_partially_unblock(store):
    register(store, "unsupported", {"work_schema_version": 2})
    register(store, "ready", {})
    claim(store, allowed_work_ids=["unsupported"])
    with pytest.raises(ControlError, match="recheck"):
        store.recheck_compatibility(
            ["unsupported", "ready"], operator="ops", reason="upgrade",
        )
    assert store.get_work("unsupported").status == "BLOCKED_INCOMPATIBLE"


def test_submission_provenance_is_readable_and_preserved(store):
    register(store, "old", {}, submitted_sdk_version="0.9.0")
    assert store.get_work("old").submitted_sdk_version == "0.9.0"
    batch = claim(store)
    envelope, _ = store.load_claim_envelope_with_digest(batch.batch_id)
    assert envelope["items"][0]["submitted_sdk_version"] == "0.9.0"
    assert envelope["items"][0]["release_digest"] == "old-release"


def test_control_parsers_require_explicit_recheck_attribution():
    from people_counter.sjd_control import _parser
    from people_counter.fabric_sjd_runtime import _control_parser

    for parser, prefix in ((_parser(), ["--database", "control.sqlite3"]), (_control_parser(), [])):
        args = parser.parse_args(prefix + [
            "recheck-compatibility", "--work-id", "one", "--work-id", "two",
            "--operator", "ops", "--reason", "upgrade",
        ])
        assert args.work_id == ["one", "two"]
        assert args.operator == "ops"
        with pytest.raises(SystemExit):
            parser.parse_args(prefix + ["recheck-compatibility", "--work-id", "one"])


def test_local_cli_reads_blocked_diagnostics_and_rechecks(tmp_path, capsys):
    from people_counter.sjd_control import main

    database = tmp_path / "control.sqlite3"
    store = SQLiteControlStore(database, clock=lambda: 100)
    register(store, "blocked", {"work_schema_version": 2})
    claim(store)
    assert main(["--database", str(database), "status", "--work-id", "blocked"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["compatibility"]["reason_code"] == "UNSUPPORTED_WORK_SCHEMA"
    assert main([
        "--database", str(database), "recheck-compatibility", "--work-id", "blocked",
        "--operator", "ops", "--reason", "upgrade",
    ]) == 0
    result = json.loads(capsys.readouterr().out)[0]
    assert result["status"] == "BLOCKED_INCOMPATIBLE"
    assert result["compatibility"]["operator"] == "ops"


def test_fabric_cli_reads_blocked_diagnostics_and_rechecks(fabric_runtime_context, capsys):
    from people_counter.fabric_sjd_runtime import _control_dispatch, _control_parser

    store = MemoryControlStore(MemoryFiles())
    register(store, "blocked", {"work_schema_version": 2})
    claim(store)
    parser = _control_parser()
    assert _control_dispatch(parser.parse_args([
        "status", "--work-id", "blocked",
    ]), store, store.config) == 0
    assert json.loads(capsys.readouterr().out)[0]["compatibility"]["reason_code"] == "UNSUPPORTED_WORK_SCHEMA"
    assert _control_dispatch(parser.parse_args([
        "recheck-compatibility", "--work-id", "blocked", "--operator", "ops", "--reason", "upgrade",
    ]), store, store.config) == 0
    assert json.loads(capsys.readouterr().out)[0]["compatibility"]["operator"] == "ops"


def test_sqlite_v1_migration_preserves_live_claims_and_indexes(tmp_path):
    store = SQLiteControlStore(tmp_path / "control.sqlite3", clock=lambda: 100)
    register(store, "leased", {})
    batch = claim(store)
    register(store, "ready", {"work_schema_version": 2})
    with sqlite3.connect(store.database) as connection:
        connection.row_factory = sqlite3.Row
        original = [dict(row) for row in connection.execute("SELECT * FROM work ORDER BY work_id")]
        names = [name for name in original[0] if name not in ("submitted_sdk_version", "compatibility_json")]
        connection.execute("""
            CREATE TABLE work_v1 (
                work_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL, runtime_key TEXT NOT NULL,
                duration_seconds REAL NOT NULL CHECK (duration_seconds > 0),
                config_sha256 TEXT NOT NULL, release_digest TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('READY','LEASED','SUCCEEDED','DEAD')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                original_max_attempts INTEGER NOT NULL CHECK (original_max_attempts > 0),
                fence INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL,
                lease_owner TEXT, lease_attempt_id TEXT, lease_expires_at REAL,
                committed_attempt_id TEXT, publication_sequence INTEGER,
                replay_generation INTEGER NOT NULL DEFAULT 0,
                last_replay_id TEXT, last_error TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                CHECK (
                    (status='LEASED' AND lease_owner IS NOT NULL AND lease_attempt_id IS NOT NULL AND lease_expires_at IS NOT NULL)
                    OR (status!='LEASED' AND lease_owner IS NULL AND lease_attempt_id IS NULL AND lease_expires_at IS NULL)
                )
            )
        """)
        columns = ",".join(names)
        connection.execute(f"INSERT INTO work_v1 ({columns}) SELECT {columns} FROM work")
        connection.execute("DROP TABLE work")
        connection.execute("ALTER TABLE work_v1 RENAME TO work")
        connection.execute("CREATE INDEX custom_work_index ON work(runtime_key, work_id)")
        connection.execute("UPDATE control_metadata SET schema_version=1")
        connection.execute("PRAGMA user_version=1")
    store.bootstrap()
    store.bootstrap()
    with store._connect() as connection:
        migrated = [dict(row) for row in connection.execute("SELECT * FROM work ORDER BY work_id")]
        assert [{name: row[name] for name in names} for row in migrated] == [
            {name: row[name] for name in names} for row in original
        ]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        assert "custom_work_index" in {
            row["name"] for row in connection.execute("PRAGMA index_list(work)")
        }
        assert connection.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 1
    assert store.load_claim_envelope_with_digest(batch.batch_id)[0]["items"][0]["work_id"] == "leased"
    assert claim(store) is None
    assert store.get_work("ready").status == "BLOCKED_INCOMPATIBLE"


def test_existing_claim_with_unsupported_payload_is_rejected_before_execution(tmp_path):
    import hashlib
    from people_counter.sjd_process import ProcessValidationError, verify_envelope

    store = SQLiteControlStore(tmp_path / "control.sqlite3", clock=lambda: 100)
    register(store, "old-claim", {})
    batch = claim(store)
    envelope, digest = store.load_claim_envelope_with_digest(batch.batch_id)
    item = envelope["items"][0]
    item["payload"]["payload_schema_version"] = 2
    item["payload_sha256"] = hashlib.sha256(
        json.dumps(item["payload"], sort_keys=True, separators=(",", ":")).encode(),
    ).hexdigest()
    with pytest.raises(ProcessValidationError, match="payload_schema_version"):
        verify_envelope(envelope, batch_id=batch.batch_id, envelope_sha256=digest)


@pytest.mark.parametrize("version", ["0.9.0", "0.10.2", None])
def test_submission_version_is_not_an_eligibility_rule(version):
    decision = evaluate_compatibility(
        {"source_video": "video.mp4", "submitted_sdk_version": version},
        fabric=True,
    )
    assert decision.supported
    assert decision.required_contract["work_schema_version"] == 1
    assert decision.required_contract["payload_schema_version"] == 1
    assert decision.required_contract["sample_fps"] == 3.0


@pytest.mark.parametrize(
    "payload,code",
    [
        ({"work_schema_version": 2}, "UNSUPPORTED_WORK_SCHEMA"),
        ({"payload_schema_version": 2}, "UNSUPPORTED_PAYLOAD_SCHEMA"),
        ({"profile_schema": "future-v2"}, "UNSUPPORTED_PROFILE_SCHEMA"),
        ({"pipeline": "rfdetr-botsort"}, "UNSUPPORTED_PIPELINE"),
        ({"detector_model": "r101"}, "UNSUPPORTED_DETECTOR"),
        ({"model_format": "openvino"}, "UNSUPPORTED_MODEL_FORMAT"),
        ({"device": "cuda"}, "UNSUPPORTED_DEVICE"),
        ({"device_variant": "gpu"}, "UNSUPPORTED_DEVICE"),
        ({"batch_size": 3}, "UNSUPPORTED_INFERENCE_SETTINGS"),
        ({"sample_fps": -1}, "UNSUPPORTED_INFERENCE_SETTINGS"),
        ({"detection_threshold": 0.01}, "UNSUPPORTED_INFERENCE_SETTINGS"),
        ({"use_fp16": True}, "UNSUPPORTED_INFERENCE_SETTINGS"),
        ({"profile_schema": "people-counter-inference-profile-v1", "profile_id": "future-model"}, "UNSUPPORTED_INFERENCE_PROFILE"),
        ({
            "profile_schema": "people-counter-inference-profile-v1",
            "profile_id": "rtdetr-r18-pytorch-cpu",
            "profile_overrides": {"future_setting": True},
        }, "UNSUPPORTED_INFERENCE_SETTINGS"),
        ({"model_artifact_sha256": {"future-model.onnx": "a" * 64}}, "UNSUPPORTED_MODEL_REQUIREMENTS"),
    ],
)
def test_unsupported_contract_reports_the_actual_requirement(payload, code):
    decision = evaluate_compatibility(payload, fabric=True)
    assert decision.supported is False
    assert decision.reason_code == code
    fields = {
        "UNSUPPORTED_WORK_SCHEMA": "work_schema_version",
        "UNSUPPORTED_PAYLOAD_SCHEMA": "payload_schema_version",
        "UNSUPPORTED_PIPELINE": "pipeline",
        "UNSUPPORTED_DETECTOR": "detector_model",
        "UNSUPPORTED_MODEL_FORMAT": "model_format",
        "UNSUPPORTED_DEVICE": "device_variant" if "device_variant" in payload else "device",
    }
    messages = {
        "UNSUPPORTED_PROFILE_SCHEMA": "Unsupported profile schema: 'future-v2'",
        "UNSUPPORTED_INFERENCE_PROFILE": "Unsupported inference profile: 'future-model'",
        "UNSUPPORTED_INFERENCE_SETTINGS": (
            "Inference profile requires unsupported overrides"
            if "profile_overrides" in payload
            else "Inference settings are not supported by the CPU worker"
        ),
        "UNSUPPORTED_MODEL_REQUIREMENTS": "Model artifact declarations differ from the required pipeline artifacts",
    }
    expected = (
        f"Unsupported {fields[code]}: {payload[fields[code]]!r}"
        if code in fields else messages[code]
    )
    assert decision.reason == expected
    assert isinstance(decision.required_contract, dict)
    assert json.loads(json.dumps(decision.required_contract)) == decision.required_contract
    for field, value in payload.items():
        assert decision.required_contract[field] == value


def test_legacy_contract_defaults_match_the_worker_configuration(tmp_path):
    from people_counter.local_spark import _config_from_work

    video = tmp_path / "legacy.mp4"
    content = b"legacy-video-input"
    video.write_bytes(content)
    payload = {"source_video": str(video), "source_sha256": hashlib.sha256(content).hexdigest()}
    configuration = _config_from_work(payload)
    decision = evaluate_compatibility(payload)
    assert decision.supported is True
    for field in (
        "detector_model", "model_format", "device_variant", "device", "batch_size",
        "sample_fps", "detection_threshold", "use_fp16",
    ):
        assert decision.required_contract[field] == getattr(configuration, field)


@pytest.mark.parametrize("payload", [
    {"sample_fps": 0}, {"sample_fps": float("inf")},
    {"detection_threshold": True}, {"detection_threshold": 1.01},
])
def test_invalid_inference_numbers_are_not_supported(payload):
    decision = evaluate_compatibility(payload)
    assert decision.supported is False
    assert decision.reason_code == "UNSUPPORTED_INFERENCE_SETTINGS"
    assert decision.reason == "Inference settings are not supported by the CPU worker"


@pytest.mark.parametrize("payload", [
    {"detection_threshold": DETECTOR_FLOOR},
    {"detection_threshold": 1.0},
    {"sample_fps": None},
])
def test_supported_inference_boundaries_are_retained(payload):
    decision = evaluate_compatibility(payload)
    assert decision.supported is True
    for field, value in payload.items():
        assert decision.required_contract[field] == value


@pytest.mark.parametrize("value", [None, [], "model"])
def test_malformed_artifact_declarations_have_an_explicit_error(value):
    with pytest.raises(ValueError) as error:
        evaluate_compatibility({"model_artifact_sha256": value})
    assert str(error.value) == "model_artifact_sha256 must be an object"


@pytest.mark.parametrize("fabric", [False, True])
def test_compatibility_evaluation_preserves_backend_policy_and_audit_schema(monkeypatch, fabric):
    import people_counter.work_compatibility as compatibility

    payload = {"pipeline": "rfdetr-botsort"}
    decision = evaluate_compatibility(payload, fabric=fabric)
    monkeypatch.setattr(compatibility, "resolve_installed_package_version", lambda: "0.10.2")
    evaluation = compatibility_evaluation(
        payload, fabric=fabric, now=175.25, operator="ops", reason="upgrade",
    )
    assert evaluation == {
        "supported": not fabric,
        "reason_code": "UNSUPPORTED_PIPELINE" if fabric else None,
        "reason": "Unsupported pipeline: 'rfdetr-botsort'" if fabric else None,
        "required_contract": decision.required_contract,
        "evaluating_sdk_version": "0.10.2",
        "evaluated_at": 175.25,
        "operator": "ops",
        "operator_reason": "upgrade",
    }


def test_fabric_recheck_does_not_use_the_broader_local_sdk_policy(fabric_runtime_context):
    store = MemoryControlStore(MemoryFiles())
    register(store, "unsupported-pipeline", {"pipeline": "rfdetr-botsort"})
    assert claim(store) is None
    report = store.recheck_compatibility(["unsupported-pipeline"], operator="ops", reason="upgrade")
    assert report[0].status == "BLOCKED_INCOMPATIBLE"
    assert report[0].compatibility["supported"] is False
    assert report[0].compatibility["reason_code"] == "UNSUPPORTED_PIPELINE"


def test_local_sdk_retains_rfdetr_support():
    assert evaluate_compatibility({"pipeline": "rfdetr-botsort"}).supported


def test_future_and_out_of_scope_work_remain_ready(store):
    register(store, "future", {"work_schema_version": 2}, available_at=1000)
    register(store, "unselected", {"work_schema_version": 2})
    register(store, "selected", {})
    assert [item.work_id for item in claim(store, allowed_work_ids=["selected"]).items] == ["selected"]
    assert store.get_work("future").status == "READY"
    assert store.get_work("unselected").status == "READY"


def test_blocking_survives_a_compatible_batch_admission_failure(store):
    from people_counter.sjd_control import LeaseBudgetError

    register(store, "blocked", {"work_schema_version": 2})
    store.register(
        "too-long", {}, runtime_key="runtime", duration_seconds=1000,
        config_sha256="config", release_digest="old-release",
    )
    with pytest.raises(LeaseBudgetError):
        claim(store)
    assert store.get_work("blocked").status == "BLOCKED_INCOMPATIBLE"
    assert store.get_work("blocked").attempt_count == 0
    assert store.get_work("too-long").attempt_count == 0


def test_blocked_registration_retry_and_reconciliation_do_not_unblock(store):
    register(store, "blocked", {"work_schema_version": 2})
    assert claim(store) is None
    diagnostic = store.get_work("blocked").compatibility
    register(store, "blocked", {"work_schema_version": 2})
    store.reconcile(now=1000)
    assert claim(store) is None
    assert store.get_work("blocked").status == "BLOCKED_INCOMPATIBLE"
    assert store.get_work("blocked").compatibility == diagnostic


def test_sqlite_does_not_claim_work_registered_after_classification(tmp_path, monkeypatch):
    store = SQLiteControlStore(tmp_path / "control.sqlite3", clock=lambda: 100)
    register(store, "supported", {})
    classify = store._classify_ready

    def concurrent_registration(scope, now):
        selected = classify(scope, now)
        register(store, "new-unsupported", {"work_schema_version": 2})
        return selected

    monkeypatch.setattr(store, "_classify_ready", concurrent_registration)
    assert [item.work_id for item in claim(store).items] == ["supported"]
    assert store.get_work("new-unsupported").attempt_count == 0


def test_compatibility_does_not_read_or_substitute_model_bytes():
    payload = {"model_artifact_sha256": {"model.onnx": "a" * 64}}
    decision = evaluate_compatibility(payload)
    assert decision.supported
    assert decision.required_contract["model_artifact_sha256"] == payload["model_artifact_sha256"]

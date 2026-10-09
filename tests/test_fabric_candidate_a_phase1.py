from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    FILES_ROOT,
    LAKEHOUSE_ID,
    TABLE_PREFIX,
    WORKSPACE_ID,
    FabricCandidateAConfig,
    build_sjd_v2_definition,
    validate_environment_library_policy,
)
from people_counter.fabric_candidate_a_control import (
    BatchValidationError,
    FabricControlStoreImpl,
    ImmutableConflictError,
    OneLakeEnvelopeWriter,
    _SCHEMAS,
)
from people_counter.fabric_candidate_a_gold import (
    FabricCommittedSource,
    FabricGoldJob,
    FabricGoldState,
    _normalize_spark_row_timestamp,
    _sha256,
    _spark_timestamp,
)
from people_counter.fabric_candidate_a_jobs import (
    _bind_consumer_probe_to_executor_inventory,
    _clear_stale_lock,
    _control_parser,
    _executor_warm_works,
    _fabric_process_profile,
    _localize_process_inputs,
    _probe_and_persist_consumer_decision,
    _write_control_diagnostic,
    control_main,
    process_main,
)
from people_counter.fabric_capability_probe import CapabilityStatus
from people_counter.sjd_gold import (
    FACT_TABLES,
    GoldSourceError,
    SourceCheckpoint,
    _normal_incremental_facts_noop,
    _required_datetime,
)
from people_counter.sjd_process import (
    OneLakeDeltaAttemptAdapter,
    ProcessResult,
    ProcessRouteMode,
    ProcessValidationError,
)


class MemoryFiles:
    def __init__(self) -> None:
        self.content: dict[str, str] = {}
        self.paths: set[str] = set()
        self.read_calls = 0

    def exists(self, path: str) -> bool:
        return path in self.content or path in self.paths

    def read_text(self, path: str) -> str:
        self.read_calls += 1
        return self.content[path]

    def create_text(self, path: str, content: str) -> None:
        if path in self.content:
            raise FileExistsError(path)
        self.content[path] = content


class ImmediateWriter:
    @staticmethod
    def run(operation):
        return operation()


def test_process_main_resumes_committed_batch_before_live_probing(
    monkeypatch,
    capsys,
) -> None:
    import people_counter.fabric_candidate_a_control as control_module
    import people_counter.sjd_control as sjd_control_module
    import people_counter.sjd_process as process_module

    runtime = sys.modules["people_counter.fabric_candidate_a_jobs"]
    spark = object()
    store = object()
    attempts = object()
    release_evidence = SimpleNamespace(
        identity_sha256="a" * 64,
        manifest_sha256="b" * 64,
        receipt_sha256="c" * 64,
        manifest=SimpleNamespace(package_version="0.9.48"),
        receipt=SimpleNamespace(environment_target_version="target-version"),
    )
    result = ProcessResult(
        batch_id="batch-1",
        process_attempt_id="process-attempt-1",
        staging_path="/lakehouse/default/Files/staging/process-attempt-1",
        record_count=2,
        failed_work_ids=(),
        publication_sequences=(41,),
        resumed=True,
        driver_package_version="0.9.48",
    )
    config = MagicMock()
    config.file_path.return_value = "/lakehouse/default/Tables/attempts"

    monkeypatch.setattr(runtime, "_spark", lambda: spark)
    monkeypatch.setattr(
        runtime,
        "_load_runtime_release_evidence",
        lambda *_args, **_kwargs: release_evidence,
    )
    monkeypatch.setattr(
        runtime,
        "_probe_and_persist_consumer_decision",
        lambda *_args, **_kwargs: pytest.fail("committed resume probed consumers"),
    )
    monkeypatch.setattr(
        control_module,
        "NotebookUtilsOneLakeFiles",
        lambda: object(),
    )
    monkeypatch.setattr(
        sjd_control_module,
        "FabricControlStore",
        lambda *_args, **_kwargs: store,
    )
    monkeypatch.setattr(
        process_module,
        "OneLakeDeltaAttemptAdapter",
        lambda *_args, **_kwargs: attempts,
    )
    resume = MagicMock(return_value=result)
    monkeypatch.setattr(
        process_module,
        "resume_committed_process_batch",
        resume,
    )

    exit_code = process_main(
        [
            "--batch-id",
            "batch-1",
            "--release-manifest-path",
            "manifest.json",
            "--release-manifest-sha256",
            "b" * 64,
            "--release-receipt-path",
            "receipt.json",
            "--release-receipt-sha256",
            "c" * 64,
        ],
        config=config,
    )

    assert exit_code == 0
    resume.assert_called_once_with(
        store,
        "batch-1",
        attempts,
        release_evidence=release_evidence,
    )
    config.require_write_enabled.assert_called_once_with()
    payload = json.loads(capsys.readouterr().out)
    assert payload["resumed"] is True
    assert payload["consumer_probe_decision"]["status"] == (
        "SKIPPED_COMMITTED_RESUME"
    )
    assert payload["resolver_capability"]["status"] == (
        "SKIPPED_COMMITTED_RESUME"
    )
    assert payload["harness_capability"]["status"] == (
        "SKIPPED_COMMITTED_RESUME"
    )


def test_workload_probe_binding_invalidates_direct_access_on_executor_drift() -> None:
    proven_result = object()
    bound = _bind_consumer_probe_to_executor_inventory(
        proven_result,
        ("executor-1", "executor-2"),
    )
    unchanged = [
        SimpleNamespace(executor_id="executor-2"),
        SimpleNamespace(executor_id="executor-1"),
    ]
    assert bound(object(), unchanged) is proven_result

    drifted = [SimpleNamespace(executor_id="executor-1")]
    result = bound(object(), drifted)
    assert result.status is CapabilityStatus.FABRIC_PLATFORM_BLOCKED
    assert result.value is None
    assert result.capability == "direct_mount_consumer_capability"
    assert result.evidence == (
        "executor inventory changed after the persisted workload probe; "
        "verified fallback is required"
    )


def test_consumer_decision_is_workload_specific_immutable_and_read_back() -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )
    from people_counter.fabric_executor_inventory import ExecutorRecord

    digest = "a" * 64
    envelope = {
        "items": [
            {
                "work_id": "work-1",
                "payload": {
                    "source_video": "/lakehouse/default/Files/video/same.mp4",
                    "source_sha256": digest,
                    "pipeline": "rtdetr-osnet",
                    "detector_model": "r18",
                    "model_format": "pytorch",
                    "model_artifact_sha256": {
                        "rtdetr_osnet/rtdetr_v2_r18vd/config.json": digest,
                        (
                            "rtdetr_osnet/rtdetr_v2_r18vd/"
                            "preprocessor_config.json"
                        ): digest,
                        (
                            "rtdetr_osnet/rtdetr_v2_r18vd/"
                            "model.safetensors"
                        ): digest,
                        (
                            "rtdetr_osnet/libre_reid_osnet/"
                            "osnet_ain_x0_25.pt"
                        ): digest,
                    },
                },
            }
        ]
    }
    spark = SimpleNamespace(conf=SimpleNamespace(get=lambda _key, _default: "1"))
    executors = (
        ExecutorRecord("2", "host-b", 1, 1024),
        ExecutorRecord("1", "host-a", 1, 1024),
    )
    result = CapabilityProbeResult(
        "direct consumers",
        CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
        "consumer open failed on every executor",
    )
    files = MemoryFiles()
    config = FabricCandidateAConfig.benchmark()
    observed_profiles: list[object] = []

    def probe(_spark, _executors, *, profile):
        observed_profiles.append(profile)
        return result

    observed, first = _probe_and_persist_consumer_decision(
        spark,
        config,
        "batch-1",
        envelope,
        discover_executors=lambda _spark: executors,
        probe_consumers=probe,
        files=files,
    )
    _, repeated = _probe_and_persist_consumer_decision(
        spark,
        config,
        "batch-1",
        envelope,
        discover_executors=lambda _spark: executors,
        probe_consumers=probe,
        files=files,
    )

    assert observed is result
    assert first == repeated
    assert first["selected_backend"] == "FABRIC_FALLBACK"
    assert first["executor_ids"] == ["1", "2"]
    assert first["profile"]["planned_concurrent_tasks"] == 2
    assert first["profile"]["hash_targets"]
    assert files.read_calls == 2
    assert len(observed_profiles) == 2
    assert len(files.content) == 1
    profile = first["profile"]
    expected_profile_sha256 = hashlib.sha256(
        json.dumps(
            profile,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    expected_path = config.file_path(
        f"consumer-decisions/batch-1/{expected_profile_sha256}.json"
    )
    persisted = {
        "schema": "people-counter-consumer-probe-decision-v1",
        "batch_id": "batch-1",
        "namespace_mode": config.mode.value,
        "profile_sha256": expected_profile_sha256,
        "profile": profile,
        "executor_ids": ["1", "2"],
        "status": "FABRIC_PLATFORM_BLOCKED",
        "selected_backend": "FABRIC_FALLBACK",
        "capability": "direct consumers",
        "evidence": "consumer open failed on every executor",
    }
    persisted_text = json.dumps(
        persisted,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert files.content == {expected_path: persisted_text}
    assert first == {
        **persisted,
        "path": expected_path,
        "sha256": hashlib.sha256(persisted_text.encode("utf-8")).hexdigest(),
    }


def test_consumer_decision_reuses_immutable_probe_after_executor_drift() -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    digest = "a" * 64
    envelope = {
        "items": [
            {
                "work_id": "work-1",
                "payload": {
                    "source_video": "/lakehouse/default/Files/video/same.mp4",
                    "source_sha256": digest,
                    "pipeline": "rtdetr-osnet",
                    "detector_model": "r18",
                    "model_format": "pytorch",
                    "model_artifact_sha256": {
                        path: digest
                        for path in (
                            "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
                            "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
                            "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
                            "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
                        )
                    },
                },
            }
        ]
    }
    spark = SimpleNamespace(conf=SimpleNamespace(get=lambda _key, _default: "1"))
    discoveries = iter(
        (
            _fake_candidate_executors(2, cores=1),
            tuple(
                replace(
                    executor,
                    executor_id=str(index + 3),
                    host=f"host-{index + 3}",
                )
                for index, executor in enumerate(
                    _fake_candidate_executors(2, cores=1)
                )
            ),
        )
    )
    direct = CapabilityProbeResult(
        "direct consumers",
        CapabilityStatus.AVAILABLE,
        "all consumers opened the mounted files",
        {"opened": True},
    )
    files = MemoryFiles()

    _, first = _probe_and_persist_consumer_decision(
        spark,
        FabricCandidateAConfig.benchmark(),
        "batch-drift",
        envelope,
        discover_executors=lambda _spark: next(discoveries),
        probe_consumers=lambda *_args, **_kwargs: direct,
        files=files,
    )
    drifted, repeated = _probe_and_persist_consumer_decision(
        spark,
        FabricCandidateAConfig.benchmark(),
        "batch-drift",
        envelope,
        discover_executors=lambda _spark: next(discoveries),
        probe_consumers=lambda *_args, **_kwargs: direct,
        files=files,
    )

    assert repeated == first
    assert drifted.status is CapabilityStatus.FABRIC_PLATFORM_BLOCKED
    assert drifted.value is None
    assert drifted.capability == "direct_mount_consumer_capability"
    assert drifted.evidence == (
        "executor inventory changed after the persisted workload probe; "
        "verified fallback is required"
    )
    assert len(files.content) == 1


@pytest.mark.parametrize(
    ("corruption", "expected_error"),
    (
        ("invalid-json", "persisted consumer-probe decision is invalid JSON"),
        ("invalid-shape", "persisted consumer-probe decision has an invalid shape"),
        (
            "workload-conflict",
            "persisted consumer-probe decision conflicts with this workload",
        ),
        (
            "empty-executor-ids",
            "persisted consumer-probe decision has invalid executor IDs",
        ),
        (
            "duplicate-executor-ids",
            "persisted consumer-probe decision has invalid executor IDs",
        ),
        (
            "non-string-executor-id",
            "persisted consumer-probe decision has invalid executor IDs",
        ),
        (
            "same-executor-evidence-drift",
            "persisted consumer-probe evidence drifted for the same executors",
        ),
        (
            "noncanonical-drift",
            "persisted consumer-probe decision is not canonical JSON",
        ),
    ),
)
def test_consumer_decision_rejects_corrupt_persistence(
    corruption: str,
    expected_error: str,
) -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    digest = "a" * 64
    envelope = {
        "items": [
            {
                "work_id": "work-1",
                "payload": {
                    "source_video": "/lakehouse/default/Files/video/same.mp4",
                    "source_sha256": digest,
                    "pipeline": "rtdetr-osnet",
                    "detector_model": "r18",
                    "model_format": "pytorch",
                    "model_artifact_sha256": {
                        path: digest
                        for path in (
                            "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
                            "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
                            "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
                            "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
                        )
                    },
                },
            }
        ]
    }
    spark = SimpleNamespace(conf=SimpleNamespace(get=lambda _key, _default: "1"))
    original_executors = _fake_candidate_executors(2, cores=1)
    current_executors = original_executors
    direct = CapabilityProbeResult(
        "direct consumers",
        CapabilityStatus.AVAILABLE,
        "all consumers opened the mounted files",
        {"opened": True},
    )
    files = MemoryFiles()
    config = FabricCandidateAConfig.benchmark()

    _probe_and_persist_consumer_decision(
        spark,
        config,
        "batch-corrupt",
        envelope,
        discover_executors=lambda _spark: original_executors,
        probe_consumers=lambda *_args, **_kwargs: direct,
        files=files,
    )
    path = next(iter(files.content))
    persisted = json.loads(files.content[path])

    if corruption == "invalid-json":
        files.content[path] = "{"
    elif corruption == "invalid-shape":
        persisted["unexpected"] = True
        files.content[path] = json.dumps(
            persisted, separators=(",", ":"), sort_keys=True
        )
    elif corruption == "workload-conflict":
        persisted["namespace_mode"] = "unexpected"
        files.content[path] = json.dumps(
            persisted, separators=(",", ":"), sort_keys=True
        )
    elif corruption == "empty-executor-ids":
        persisted["executor_ids"] = []
        files.content[path] = json.dumps(
            persisted, separators=(",", ":"), sort_keys=True
        )
    elif corruption == "duplicate-executor-ids":
        persisted["executor_ids"] = ["1", "1"]
        files.content[path] = json.dumps(
            persisted, separators=(",", ":"), sort_keys=True
        )
    elif corruption == "non-string-executor-id":
        persisted["executor_ids"] = [1]
        files.content[path] = json.dumps(
            persisted, separators=(",", ":"), sort_keys=True
        )
    elif corruption == "same-executor-evidence-drift":
        persisted["evidence"] = "changed"
        files.content[path] = json.dumps(
            persisted, separators=(",", ":"), sort_keys=True
        )
    elif corruption == "noncanonical-drift":
        current_executors = tuple(
            replace(executor, executor_id=str(index + 3))
            for index, executor in enumerate(original_executors)
        )
        files.content[path] = json.dumps(persisted, indent=2, sort_keys=True)
    else:
        raise AssertionError(f"unexpected corruption case: {corruption}")

    with pytest.raises(RuntimeError, match=expected_error):
        _probe_and_persist_consumer_decision(
            spark,
            config,
            "batch-corrupt",
            envelope,
            discover_executors=lambda _spark: current_executors,
            probe_consumers=lambda *_args, **_kwargs: direct,
            files=files,
        )


def test_consumer_decision_uses_default_discovery_and_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    spark = SimpleNamespace(conf=SimpleNamespace(get=lambda key, default: default))
    executors = _fake_candidate_executors(1, cores=4)
    discovered: list[tuple[object, int]] = []
    probed: list[tuple[object, object, object]] = []

    def discover(session, *, minimum_executors):
        discovered.append((session, minimum_executors))
        return executors

    result = CapabilityProbeResult(
        "full consumer profile",
        CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
        "mount unavailable",
    )

    def probe(session, observed, *, profile):
        probed.append((session, observed, profile))
        return result

    monkeypatch.setattr(
        "people_counter.fabric_executor_inventory.discover_active_executors",
        discover,
    )
    monkeypatch.setattr(
        "people_counter.fabric_capability_probe."
        "probe_direct_mount_consumer_capability",
        probe,
    )
    digest = "a" * 64
    envelope = {
        "items": [
            {
                "work_id": "work",
                "payload": {
                    "source_video": "/lakehouse/default/Files/video.mp4",
                    "source_sha256": digest,
                    "pipeline": "rtdetr-osnet",
                    "detector_model": "r18",
                    "model_format": "pytorch",
                    "model_artifact_sha256": {
                        path: digest
                        for path in (
                            "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
                            "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
                            "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
                            "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
                        )
                    },
                },
            }
        ]
    }
    _probe_and_persist_consumer_decision(
        spark,
        FabricCandidateAConfig.benchmark(),
        "default-probe",
        envelope,
        files=MemoryFiles(),
    )
    assert discovered == [(spark, 1)]
    assert len(probed) == 1
    assert probed[0][0] is spark
    assert probed[0][1] == executors
    assert probed[0][2].planned_concurrent_tasks == 4


class MemoryControlStore(FabricControlStoreImpl):
    def __init__(self, files: MemoryFiles) -> None:
        self.config = FabricCandidateAConfig()
        self.data = {name: [] for name in _SCHEMAS}
        self.writer = ImmediateWriter()
        self.envelopes = OneLakeEnvelopeWriter(
            FabricCandidateAConfig().file_path("control"), files
        )
        self._clock_value = 100.0
        self._clock = lambda: self._clock_value
        identifiers = iter(
            [
                "batch-1",
                "attempt-1",
                "attempt-2",
                "replay-1",
                "batch-2",
                "attempt-3",
            ]
        )
        self._id_factory = lambda: next(identifiers)

    def _rows(self, suffix: str):
        return copy.deepcopy(self.data[suffix])

    def _replace(self, suffix: str, rows):
        self.data[suffix] = copy.deepcopy(rows)


class FakeFrame:
    def __init__(self, spark, rows) -> None:
        self.spark = spark
        self.rows = copy.deepcopy(rows)
        self.write = self

    def format(self, _value):
        return self

    def mode(self, _value):
        return self

    def option(self, _key, _value):
        return self

    def save(self, path):
        self.spark.rows[path] = copy.deepcopy(self.rows)
        relative = path.split(
            f"/{FabricCandidateAConfig().lakehouse_id}/", 1
        )[-1]
        self.spark.files.paths.add(f"{relative}/_delta_log")


class FakeRead:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.path = ""

    def format(self, _value):
        return self

    def load(self, path):
        self.path = path
        return self

    def select(self, *_columns):
        return self

    def collect(self):
        return copy.deepcopy(self.spark.rows[self.path])


class FakeSpark:
    def __init__(self, files: MemoryFiles) -> None:
        self.files = files
        self.rows = {}
        self.read = FakeRead(self)

    def createDataFrame(self, rows, schema):
        assert "record_json string" in schema
        return FakeFrame(self, rows)


def _decode_parts(payload):
    return {
        part["path"]: base64.b64decode(part["payload"])
        for part in payload["definition"]["parts"]
    }


def test_fixed_config_and_sjd_v2_mappings() -> None:
    config = FabricCandidateAConfig()
    assert (
        config.workspace_id,
        config.lakehouse_id,
        config.environment_id,
        config.table("work"),
        config.file_path("claims/a.json"),
        config.runtime,
    ) == (
        WORKSPACE_ID,
        LAKEHOUSE_ID,
        ENVIRONMENT_ID,
        TABLE_PREFIX + "work",
        FILES_ROOT + "claims/a.json",
        "2.0",
    )
    with pytest.raises(ValueError, match="fixed"):
        FabricCandidateAConfig(runtime="1.3")
    assert config.abfss_path(config.file_path("attempts")).endswith(
        f"/{LAKEHOUSE_ID}/{FILES_ROOT}attempts"
    )
    with pytest.raises(ValueError, match="fixed Candidate A root"):
        config.abfss_path("Files/production")

    shadow = FabricCandidateAConfig.production_shadow()
    adapter = OneLakeDeltaAttemptAdapter(
        shadow.file_path("attempts"),
        object(),
        MemoryFiles(),
        config=shadow,
        route_mode=ProcessRouteMode.SHADOW_SYNTHETIC,
    )
    assert adapter.config is shadow
    assert adapter.route_mode is ProcessRouteMode.SHADOW_SYNTHETIC
    default_shadow = OneLakeDeltaAttemptAdapter(
        shadow.file_path("attempts") + "/",
        object(),
        MemoryFiles(),
        config=shadow,
    )
    assert default_shadow.root == shadow.file_path("attempts").rstrip("/")
    assert default_shadow.route_mode is ProcessRouteMode.PRODUCTION_SHADOW
    for root, spark in (
        (None, object()),
        (shadow.file_path("attempts"), None),
    ):
        with pytest.raises(ProcessValidationError, match="unsupported"):
            OneLakeDeltaAttemptAdapter(
                root,
                spark,
                MemoryFiles(),
                config=shadow,
            )
    with pytest.raises(ProcessValidationError, match="route mode"):
        OneLakeDeltaAttemptAdapter(
            shadow.file_path("attempts"),
            object(),
            MemoryFiles(),
            config=shadow,
            route_mode=ProcessRouteMode.BENCHMARK,
        )
    with pytest.raises(ProcessValidationError, match="fixed Candidate A root"):
        OneLakeDeltaAttemptAdapter(
            config.file_path("attempts"),
            object(),
            MemoryFiles(),
            config=shadow,
            route_mode=ProcessRouteMode.SHADOW_SYNTHETIC,
        )
    with pytest.raises(ProcessValidationError, match="production"):
        production = FabricCandidateAConfig.production()
        OneLakeDeltaAttemptAdapter(
            production.file_path("attempts"),
            object(),
            MemoryFiles(),
            config=production,
        )

    for job in ("control", "process", "gold"):
        definition = build_sjd_v2_definition(job)
        assert definition["definition"]["format"] == "SparkJobDefinitionV2"
        parts = _decode_parts(definition)
        metadata = json.loads(parts["SparkJobDefinitionV1.json"])
        assert metadata["defaultLakehouseArtifactId"] == LAKEHOUSE_ID
        assert metadata["environmentArtifactId"] == ENVIRONMENT_ID
        assert metadata["additionalLibraryUris"] == []
        source = parts["Main/main.py"].decode()
        assert "people_counter.fabric_candidate_a_jobs import" in source
        assert "sys.path" not in source and "source" not in source.lower()


def test_fabric_committed_source_converts_staged_records_to_gold_document() -> None:
    records = [
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "video_result",
            "record_sequence": 0,
            "status": "SUCCEEDED",
            "payload_json": json.dumps(
                {"captured_at_utc": "2026-10-04T03:15:00Z"}
            ),
            "processed_frames": 4,
            "processing_seconds": 1.5,
        },
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "line_count",
            "record_sequence": 1,
            "status": "SUCCEEDED",
            "payload_json": json.dumps({"frame": 30}),
        },
    ]
    attempts = type(
        "Attempts",
        (),
        {"read_records": lambda self, batch, process: records},
    )()
    source = FabricCommittedSource(None, attempts)
    source._visible_rows = lambda: [
        {
            "work": {
                "work_id": "work-1",
                "payload_json": "{}",
            },
            "attempt": {"attempt_id": "attempt-1"},
            "publication": {
                "publication_sequence": 1,
                "output_path": (
                    "Files/_canary/people-counter/candidate-a/v1/attempts/"
                    "process/batch=batch-1/attempt=process-1"
                ),
                "output_sha256": _sha256(records),
                "published_at": 1.0,
            },
            "batch": {
                "batch_id": "batch-1",
                "status": "COMMITTED",
                "committed_at": 1.0,
            },
        }
    ]
    outputs = source.committed_outputs()
    assert outputs[0].run["processed_frames"] == 4
    assert outputs[0].line_counts[0]["frame"] == 30


def test_fabric_gold_optional_publication_time_uses_only_committed_batch_time() -> None:
    records = [
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "video_result",
            "record_sequence": 0,
            "status": "SUCCEEDED",
            "payload_json": json.dumps(
                {"captured_at_utc": "2026-10-05T00:00:00Z"}
            ),
        }
    ]
    attempts = type(
        "Attempts",
        (),
        {"read_records": lambda self, batch, process: records},
    )()
    source = FabricCommittedSource(None, attempts)
    pointer = {
        "work": {"work_id": "work-1", "payload_json": "{}"},
        "attempt": {"attempt_id": "attempt-1", "batch_id": "batch-1"},
        "publication": {
            "publication_sequence": 1,
            "output_path": (
                "Files/_benchmark/people-counter/candidate-a/v1/attempts/"
                "process/batch=batch-1/attempt=process-1"
            ),
            "output_sha256": _sha256(records),
            "published_at": None,
        },
        "batch": {
            "batch_id": "batch-1",
            "status": "COMMITTED",
            "committed_at": 1.0,
        },
    }
    source._visible_rows = lambda: [pointer]

    output = source.committed_outputs()[0]

    assert output.published_at == datetime.fromtimestamp(1, timezone.utc)
    assert output.attempt["batch_committed_at"] == 1.0
    pointer["batch"]["committed_at"] = None
    assert source.committed_outputs()[0].published_at is None


def test_fabric_gold_timestamp_matches_spark_naive_utc_readback() -> None:
    eastern = timezone(-timedelta(hours=4))
    assert _spark_timestamp(
        datetime(2026, 10, 4, 1, 2, 3, tzinfo=eastern)
    ) == datetime(2026, 10, 4, 5, 2, 3)
    naive = datetime(2026, 10, 4, 5, 2, 3)
    assert _spark_timestamp(naive) is naive
    with pytest.raises(TypeError, match="datetime"):
        _spark_timestamp("2026-10-04")  # type: ignore[arg-type]


def test_fabric_gold_normalizes_only_spark_datetime_values() -> None:
    naive = datetime(2026, 10, 4, 4, 28, 3, 545631)
    assert _normalize_spark_row_timestamp(naive) == naive.replace(
        tzinfo=timezone.utc
    )

    eastern = timezone(-timedelta(hours=4))
    aware = datetime(2026, 10, 4, 1, 2, 3, tzinfo=eastern)
    assert _normalize_spark_row_timestamp(aware) == datetime(
        2026, 10, 4, 5, 2, 3, tzinfo=timezone.utc
    )

    utc_text = "2026-10-04T05:02:03Z"
    assert _normalize_spark_row_timestamp(utc_text) == utc_text
    assert _required_datetime(utc_text) == datetime(
        2026, 10, 4, 5, 2, 3, tzinfo=timezone.utc
    )

    malformed = "2026-10-04T05:02:03"
    assert _normalize_spark_row_timestamp(malformed) == malformed
    with pytest.raises(GoldSourceError, match="invalid UTC timestamp"):
        _required_datetime(malformed)


def test_fabric_gold_normalizer_handles_tzinfo_without_an_offset() -> None:
    class MissingOffset(tzinfo):
        def utcoffset(self, _value):
            return None

    value = datetime(2026, 10, 4, 5, 2, 3, tzinfo=MissingOffset())

    assert _normalize_spark_row_timestamp(value) == value.replace(
        tzinfo=timezone.utc
    )


def test_fabric_gold_normalizer_does_not_use_local_timezone() -> None:
    previous = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Los_Angeles"
        time.tzset()
        value = datetime(
            2026,
            10,
            4,
            1,
            2,
            3,
            tzinfo=timezone(timedelta(hours=-4)),
        )

        normalized = _normalize_spark_row_timestamp(value)
        assert normalized == datetime(
            2026, 10, 4, 5, 2, 3, tzinfo=timezone.utc
        )
        assert normalized.tzinfo is timezone.utc
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


@pytest.mark.parametrize(
    "changes",
    [
        {"automatic": False},
        {"force": True},
        {"full_rebuild": True},
        {"checkpoint": None},
        {"checkpoint": {"source_key": "different"}},
        {"targets_current": False},
    ],
)
def test_normal_incremental_facts_noop_requires_every_gate(
    changes: dict[str, object],
) -> None:
    arguments = {
        "automatic": True,
        "force": False,
        "full_rebuild": False,
        "checkpoint": {"source_key": "same"},
        "source_key": "same",
        "targets_current": True,
    }
    assert _normal_incremental_facts_noop(**arguments) is True

    arguments.update(changes)

    assert _normal_incremental_facts_noop(**arguments) is False


class _CollectedRow:
    def __init__(self, value: dict[str, object]) -> None:
        self.value = value

    def asDict(self, *, recursive: bool) -> dict[str, object]:
        assert recursive is True
        return copy.deepcopy(self.value)


class _CheckpointSpark:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.catalog = SimpleNamespace(refreshTable=lambda _table: None)

    def table(self, _table: str) -> SimpleNamespace:
        return SimpleNamespace(
            collect=lambda: [_CollectedRow(row) for row in self.rows]
        )


def test_fabric_checkpoint_rows_normalize_naive_spark_timestamps() -> None:
    completed_at = datetime(2026, 10, 4, 4, 28, 3, 545631)
    state = FabricGoldState(
        _CheckpointSpark(
            [
                {
                    "stage": "facts",
                    "source_key": "source",
                    "publication_sequence": 1,
                    "source_versions_json": "{}",
                    "target_versions_json": "{}",
                    "completed_at": completed_at,
                }
            ]
        ),
        auto_bootstrap=False,
    )

    checkpoint = state.checkpoint("facts")

    assert checkpoint is not None
    assert checkpoint["completed_at"] == completed_at.replace(
        tzinfo=timezone.utc
    )


def test_normal_fabric_gold_rerun_accepts_checkpoint_and_is_a_noop() -> None:
    flow = SourceCheckpoint(1, {"publication": "same"})
    operations = SourceCheckpoint(1, {"control": "same"})
    combined = FabricGoldJob._fact_source(flow, operations)
    target_versions = {name: 7 for name in FACT_TABLES}
    checkpoint = {
        "stage": "facts",
        "source_key": combined.key,
        "publication_sequence": 1,
        "source_versions_json": json.dumps(combined.versions),
        "target_versions_json": json.dumps(target_versions, sort_keys=True),
        "completed_at": datetime(2026, 10, 4, 4, 28, 3, 545631),
    }
    state = FabricGoldState(
        _CheckpointSpark([checkpoint]),
        auto_bootstrap=False,
    )

    class UnchangedSource:
        def checkpoint(self) -> SourceCheckpoint:
            return flow

        def operations_checkpoint(self) -> SourceCheckpoint:
            return operations

        def committed_outputs(self):
            raise AssertionError("unchanged committed work must not be scheduled")

        def control_rows(self):
            raise AssertionError("unchanged control work must not be scheduled")

    class UnchangedStore:
        def versions(self, names):
            assert tuple(names) == FACT_TABLES
            return dict(target_versions)

    job = FabricGoldJob(
        UnchangedSource(),
        UnchangedStore(),
        state,
    )

    result = job.build_facts()

    assert result == {
        "stage": "facts",
        "skipped": True,
        "source_key": combined.key,
        "partition_count": 0,
    }
    assert state.checkpoint("facts")["completed_at"] == checkpoint[
        "completed_at"
    ].replace(tzinfo=timezone.utc)


def test_stale_lock_clear_requires_an_exact_expected_owner() -> None:
    parser = _control_parser()
    parsed = parser.parse_args(
        ["clear-stale-lock", "--expected-owner-id", "owner-1"]
    )
    assert parsed.expected_owner_id == "owner-1"
    with pytest.raises(SystemExit):
        parser.parse_args(["clear-stale-lock"])
    claim = parser.parse_args(
        [
            "claim",
            "--owner",
            "owner-1",
            "--work-id",
            "work-1",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
        ]
    )
    assert claim.minimum_items == 1
    assert claim.minimum_speed_x == 1.0
    assert claim.safety_factor == 1.25
    assert claim.margin_seconds == 30.0
    assert claim.work_id == ["work-1"]
    replay = parser.parse_args(
        [
            "replay",
            "--work-id",
            "work-1",
            "--operator",
            "operator",
            "--reason",
            "reason",
        ]
    )
    assert replay.additional_attempts == 1


def test_control_parser_rejects_missing_required_and_non_numeric_values() -> None:
    parser = _control_parser()
    invalid = (
        [],
        ["register"],
        [
            "register",
            "--work-id",
            "work-1",
            "--payload-json",
            "{}",
            "--runtime-key",
            "cpu",
            "--duration-seconds",
            "not-float",
            "--config-sha256",
            "c" * 64,
            "--release-digest",
            "release",
        ],
        [
            "register",
            "--work-id",
            "work-1",
            "--payload-json",
            "{}",
            "--runtime-key",
            "cpu",
            "--duration-seconds",
            "1",
            "--config-sha256",
            "c" * 64,
            "--release-digest",
            "release",
            "--max-attempts",
            "not-int",
        ],
        ["claim"],
        [
            "claim",
            "--owner",
            "owner",
            "--work-id",
            "work-1",
            "--max-items",
            "not-int",
            "--lease-seconds",
            "60",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--work-id",
            "work-1",
            "--max-items",
            "1",
            "--lease-seconds",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--minimum-speed-x",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--safety-factor",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--margin-seconds",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--minimum-items",
            "bad",
        ],
        ["replay"],
        [
            "replay",
            "--work-id",
            "work",
            "--operator",
            "operator",
            "--reason",
            "reason",
            "--additional-attempts",
            "bad",
        ],
        ["recover", "--now", "bad"],
    )
    for arguments in invalid:
        with pytest.raises(SystemExit):
            parser.parse_args(arguments)

    register_arguments = [
        "register",
        "--work-id",
        "work-1",
        "--payload-json",
        "{}",
        "--runtime-key",
        "cpu",
        "--duration-seconds",
        "1",
        "--config-sha256",
        "c" * 64,
        "--release-digest",
        "release",
    ]
    for option in (
        "--work-id",
        "--payload-json",
        "--runtime-key",
        "--duration-seconds",
        "--config-sha256",
        "--release-digest",
    ):
        omitted = list(register_arguments)
        index = omitted.index(option)
        del omitted[index : index + 2]
        with pytest.raises(SystemExit):
            parser.parse_args(omitted)
    for arguments, required_options in (
        (
            [
                "claim",
                "--owner",
                "owner",
                "--max-items",
                "1",
                "--lease-seconds",
                "60",
            ],
            ("--owner", "--max-items", "--lease-seconds"),
        ),
        (
            [
                "replay",
                "--work-id",
                "work",
                "--operator",
                "operator",
                "--reason",
                "reason",
            ],
            ("--work-id", "--operator", "--reason"),
        ),
    ):
        for option in required_options:
            omitted = list(arguments)
            index = omitted.index(option)
            del omitted[index : index + 2]
            with pytest.raises(SystemExit):
                parser.parse_args(omitted)

    register = parser.parse_args(register_arguments)
    assert register.max_attempts == 3


def test_environment_policy_is_published_full_mode_only() -> None:
    validate_environment_library_policy(
        {
            "environmentArtifactId": ENVIRONMENT_ID,
            "runtimeVersion": "2.0",
            "libraryMode": "Full",
            "published": True,
            "additionalLibraryUris": [],
        }
    )
    with pytest.raises(ValueError, match="inline"):
        validate_environment_library_policy(
            {
                "environment_id": ENVIRONMENT_ID,
                "runtime": "2.0",
                "library_mode": "full",
                "publishState": "Succeeded",
                "inline_libraries": ["Files/wheel.whl"],
            }
        )
    valid = {
        "environmentArtifactId": ENVIRONMENT_ID,
        "runtimeVersion": "2.0",
        "libraryMode": "Full",
        "published": True,
        "additionalLibraryUris": [],
    }
    for field, value, message in (
        ("environmentArtifactId", "other", "fixed Fabric Environment"),
        ("runtimeVersion", "1.3", "Runtime 2.0"),
        ("published", False, "published Environment"),
        ("additionalLibraryUris", ["Files/wheel.whl"], "inline"),
    ):
        with pytest.raises(ValueError, match=message):
            validate_environment_library_policy(valid | {field: value})
    validate_environment_library_policy(
        {
            "environment_id": ENVIRONMENT_ID,
            "runtime": "2.0",
            "library_mode": "full",
            "publishState": "Succeeded",
            "inline_libraries": (),
        }
    )
    with pytest.raises(ValueError, match="Full"):
        validate_environment_library_policy(
            {
                "environmentArtifactId": ENVIRONMENT_ID,
                "runtimeVersion": "2.0",
                "libraryMode": "Quick",
                "published": True,
                "additionalLibraryUris": [],
            }
        )


def test_default_candidate_a_consumer_profile_targets_real_model_paths() -> None:
    from people_counter.fabric_capability_probe import ConsumerProbeProfile
    from people_counter.fabric_candidate_a import default_candidate_a_consumer_profile

    profile = default_candidate_a_consumer_profile(
        reference_video_relative_path="Files/_benchmark/reference/sample.mp4",
    )
    assert isinstance(profile, ConsumerProbeProfile)
    hash_paths = {artifact.relative_path for artifact in profile.hash_targets}
    assert hash_paths == {
        "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/config.json",
        "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
        "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
        "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
    }
    assert profile.video.relative_path == "Files/_benchmark/reference/sample.mp4"
    assert (
        profile.safetensors_or_pytorch_model.relative_path
        == "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors"
    )
    assert (
        profile.onnx_model.relative_path
        == "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx"
    )
    assert (
        profile.concurrent_read_target.relative_path
        == "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors"
    )
    assert profile.planned_concurrent_tasks == 4


def test_default_candidate_a_consumer_profile_honors_overrides() -> None:
    from people_counter.fabric_candidate_a import default_candidate_a_consumer_profile

    profile = default_candidate_a_consumer_profile(
        models_dir="Files/other-models/",
        detector_model="r50",
        reference_video_relative_path="Files/_benchmark/reference/other.mp4",
        planned_concurrent_tasks=7,
    )
    assert (
        profile.safetensors_or_pytorch_model.relative_path
        == "Files/other-models/rtdetr_osnet/rtdetr_v2_r50vd/model.safetensors"
    )
    assert profile.planned_concurrent_tasks == 7


def test_default_candidate_a_consumer_profile_strips_only_trailing_slashes() -> None:
    """``models_dir`` normalization must strip trailing ``/`` characters
    only -- never any other trailing character (a looser ``rstrip`` charset
    would silently truncate a legitimate directory name ending in a
    coincidentally similar character)."""
    from people_counter.fabric_candidate_a import default_candidate_a_consumer_profile

    profile = default_candidate_a_consumer_profile(
        models_dir="Files/other-modelsX",
        reference_video_relative_path="Files/_benchmark/reference/sample.mp4",
    )
    assert (
        profile.safetensors_or_pytorch_model.relative_path
        == "Files/other-modelsX/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors"
    )


def test_control_transitions_are_homogeneous_fenced_and_monotonic() -> None:
    store = MemoryControlStore(MemoryFiles())
    for work_id, runtime in (("a", "cpu"), ("b", "cpu"), ("c", "gpu")):
        store.register(
            work_id,
            {"batch_size": 1, "asset": work_id},
            runtime_key=runtime,
            duration_seconds=1,
            config_sha256="c" * 64,
            release_digest="release",
        )
    claim = store.claim(
        "owner",
        max_items=2,
        lease_seconds=60,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    assert [item.work_id for item in claim.items] == ["a", "b"]
    assert {item.fence for item in claim.items} == {1}
    envelope, digest = store.load_claim_envelope_with_digest(claim.batch_id)

    from people_counter.sjd_process import verify_envelope

    verified = verify_envelope(
        envelope, batch_id=claim.batch_id, envelope_sha256=digest
    )
    before = store.process_batch_state(verified).lease_expires_at
    store.heartbeat_process(verified, 120)
    assert store.process_batch_state(verified).lease_expires_at > before
    store.seal_batch(
        claim.batch_id,
        [
            {
                "work_id": item.work_id,
                "attempt_id": item.attempt_id,
                "succeeded": True,
                "output_path": (
                    f"Files/root/process/batch={claim.batch_id}/"
                    "attempt=process-1"
                ),
                "output_sha256": ("a" if item.work_id == "a" else "b") * 64,
                "records": [
                    {
                        "executor_identity": "executor-1",
                        "partition_id": 0,
                        "task_attempt_id": 0,
                        "record_sequence": 0,
                    }
                ],
            }
            for item in claim.items
        ],
        envelope_sha256=digest,
        membership_sha256=verified.membership_sha256,
    )
    assert store.commit_batch(claim.batch_id) == (1, 2)
    assert store.commit_batch(claim.batch_id) == (1, 2)
    assert [
        row["publication_sequence"] for row in store.data["publications"]
    ] == [1, 2]
    replay = store.replay("a", operator="tester", reason="verification")
    assert replay.generation == 1
    assert next(row for row in store.data["work"] if row["work_id"] == "a")[
        "status"
    ] == "READY"


def test_onelake_envelope_and_delta_attempts_are_immutable() -> None:
    files = MemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=0.3,
    )
    path, digest = envelopes.write("batch-1", {"batch_id": "batch-1"})
    assert envelopes.read(path, digest) == {"batch_id": "batch-1"}
    assert envelopes.write("batch-1", {"batch_id": "batch-1"}) == (
        path,
        digest,
    )
    # No staleness anywhere here: write, read, and the FileExistsError-branch
    # re-write (which independently verifies via both the except-branch and
    # the unconditional final check) must each confirm the match with
    # exactly one read, not a comparison against the wrong expected content
    # that retries until the bounded timeout elapses.
    assert files.read_calls == 4

    spark = FakeSpark(files)
    adapter = OneLakeDeltaAttemptAdapter(
        FabricCandidateAConfig().file_path("attempts"), spark, files
    )
    records = [
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "video_result",
            "record_sequence": 0,
            "payload": {},
        }
    ]
    attempt_path = adapter.write_records("batch-1", "process-1", records)
    adapter.create_success("batch-1", "process-1", {"record_count": 1})
    loaded, marker = adapter.load_complete("batch-1", "process-1")
    assert loaded == records
    assert marker == {"record_count": 1}
    with pytest.raises(Exception, match="already exists"):
        adapter.write_records("batch-1", "process-1", records)
    assert attempt_path.endswith("batch=batch-1/attempt=process-1")


class FlakyReadMemoryFiles(MemoryFiles):
    """Returns stale content for a bounded number of reads, then the truth.

    Models live Fabric behavior confirmed during this engagement: an
    immediate ``notebookutils.fs.head`` read right after a successful
    ``put`` can momentarily return stale/short content even though the
    stored bytes already match the content-addressed digest (verified by
    fetching the live file directly via the OneLake DFS API and comparing
    its sha256 to the path digest).
    """

    def __init__(self, *, stale_reads: int) -> None:
        super().__init__()
        self._stale_reads_remaining = stale_reads

    def read_text(self, path: str) -> str:
        # Counts via self.read_calls (base class) without double-counting
        # by delegating to super() only through the shared counter field.
        self.read_calls += 1
        if self._stale_reads_remaining > 0:
            self._stale_reads_remaining -= 1
            return "stale"
        return self.content[path]


class AlwaysStaleMemoryFiles(MemoryFiles):
    """Always returns mismatched content, modeling genuine corruption."""

    def read_text(self, path: str) -> str:
        self.read_calls += 1
        return "corrupted"


def test_onelake_envelope_write_succeeds_without_retry_logs_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A clean first-try match must not retry or log a spurious lag warning.

    This pins the ``and`` (not ``or``) in the final
    ``observed == content and attempts > 1`` guard and the exact
    ``attempts > 1`` boundary (not ``>= 1``).
    """
    files = MemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"), files
    )
    with caplog.at_level(
        logging.WARNING, logger="people_counter.fabric_candidate_a_control"
    ):
        envelopes.write("batch-1", {"batch_id": "batch-1"})
    assert files.read_calls == 1
    assert caplog.records == []


def test_onelake_envelope_write_retries_transient_read_after_write_lag(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pins the exact retry-loop mechanics with a fully deterministic clock.

    Two stale reads followed by a correct one must: call ``read_text``
    exactly 3 times, sleep exactly twice with jitter drawn from
    ``random.uniform(0.1, 0.4)``, and log exactly one warning naming both
    the attempt count and the path.
    """
    files = FlakyReadMemoryFiles(stale_reads=2)
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=5.0,
    )
    clock_values = iter([0.0, 1.0, 2.0, 3.0, 4.0])
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.time.monotonic",
        lambda: next(clock_values, 5.0),
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.time.sleep", sleeps.append
    )
    bounds: list[tuple[float, float]] = []

    def _tracking_uniform(low: float, high: float) -> float:
        bounds.append((low, high))
        return 0.2

    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.random.uniform",
        _tracking_uniform,
    )
    with caplog.at_level(
        logging.WARNING, logger="people_counter.fabric_candidate_a_control"
    ):
        path, digest = envelopes.write("batch-1", {"batch_id": "batch-1"})
    assert files.read_calls == 3
    assert bounds == [(0.1, 0.4), (0.1, 0.4)]
    assert sleeps == [0.2, 0.2]
    assert caplog.messages == [
        f"claim envelope readback required 3 attempt(s) at {path} "
        "(OneLake read-after-write lag)"
    ]
    assert envelopes.read(path, digest) == {"batch_id": "batch-1"}


def test_onelake_envelope_write_raises_on_persistent_readback_mismatch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    files = AlwaysStaleMemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=0.05,
    )
    with caplog.at_level(
        logging.WARNING, logger="people_counter.fabric_candidate_a_control"
    ):
        with pytest.raises(BatchValidationError, match="readback differs"):
            envelopes.write("batch-1", {"batch_id": "batch-1"})
    # A permanent mismatch must never log the success-flavored lag warning,
    # even though it retried (attempts > 1) before giving up.
    assert caplog.records == []


def test_onelake_envelope_write_retries_before_false_immutable_conflict(
    caplog: pytest.LogCaptureFixture,
) -> None:
    files = FlakyReadMemoryFiles(stale_reads=1)
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=2.0,
    )
    path, digest = envelopes.write("batch-1", {"batch_id": "batch-1"})
    # A second writer racing identical content hits FileExistsError and must
    # tolerate the same transient staleness rather than raising a false
    # ImmutableConflictError.
    files._stale_reads_remaining = 1
    caplog.clear()
    with caplog.at_level(
        logging.WARNING, logger="people_counter.fabric_candidate_a_control"
    ):
        result = envelopes.write("batch-1", {"batch_id": "batch-1"})
    assert result == (path, digest)
    # Exactly 2 attempts (one stale, one real) must still cross the
    # ``attempts > 1`` logging threshold, pinning it against an ``> 2``
    # off-by-one mutation.
    assert caplog.messages == [
        f"claim envelope readback required 2 attempt(s) at {path} "
        "(OneLake read-after-write lag)"
    ]


def test_onelake_envelope_write_raises_immutable_conflict_on_genuine_mismatch() -> None:
    files = MemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=0.05,
    )
    path, _ = envelopes.write("batch-1", {"batch_id": "batch-1"})
    files.content[path] = "tampered"
    with pytest.raises(ImmutableConflictError, match="conflicts"):
        envelopes.write("batch-1", {"batch_id": "batch-1"})


@pytest.mark.parametrize("verify_timeout_seconds", [-1.0, -0.001, float("nan")])
def test_onelake_envelope_writer_rejects_invalid_verify_timeout(
    verify_timeout_seconds: float,
) -> None:
    with pytest.raises(
        ValueError,
        match=r"^verify_timeout_seconds must be finite and non-negative$",
    ):
        OneLakeEnvelopeWriter(
            FabricCandidateAConfig().file_path("control"),
            MemoryFiles(),
            verify_timeout_seconds=verify_timeout_seconds,
        )


def test_onelake_envelope_writer_accepts_zero_verify_timeout() -> None:
    # 0.0 is the boundary-valid case (pins ``< 0`` against an ``<= 0``
    # mutation) and the implicit default must be exactly 15.0 seconds.
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        MemoryFiles(),
        verify_timeout_seconds=0.0,
    )
    assert envelopes._verify_timeout_seconds == 0.0
    default_envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"), MemoryFiles()
    )
    assert default_envelopes._verify_timeout_seconds == 15.0


def test_onelake_envelope_verify_readback_stops_exactly_at_deadline_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At the exact deadline instant the loop must not attempt one more
    retry; pins the outer ``time.monotonic() < deadline`` as exclusive at
    equality (not ``<=``).
    """
    files = AlwaysStaleMemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=3.0,
    )
    clock_calls = {"count": 0}

    def _fake_monotonic() -> float:
        clock_calls["count"] += 1
        return 0.0 if clock_calls["count"] == 1 else 3.0

    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.time.monotonic",
        _fake_monotonic,
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.time.sleep",
        lambda _: pytest.fail("must not sleep once the deadline is reached"),
    )
    with pytest.raises(BatchValidationError, match="readback differs"):
        envelopes.write("batch-1", {"batch_id": "batch-1"})
    assert files.read_calls == 1
    assert clock_calls["count"] == 2


def test_onelake_envelope_verify_readback_breaks_when_remaining_hits_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once inside the loop, hitting the deadline mid-iteration must break
    immediately without sleeping or re-reading, returning the last real
    observed value (not a bare ``None``). Pins ``remaining = deadline -
    time.monotonic()`` (not ``+``), ``remaining <= 0`` (not ``< 0``), and
    ``break`` (not ``return``).
    """
    files = AlwaysStaleMemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=5.0,
    )
    # a0 -> deadline calc (0.0 + 5.0 = 5.0); a1 -> outer condition (1.0 < 5.0,
    # True); a2 -> "remaining" computation lands exactly on the deadline.
    clock_values = iter([0.0, 1.0, 5.0])
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.time.monotonic",
        lambda: next(clock_values, 5.0),
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_control.time.sleep",
        lambda _: pytest.fail("must not sleep when remaining has hit zero"),
    )
    observed = envelopes._verify_readback("some/path", "expected-content")
    assert observed == "corrupted"
    assert files.read_calls == 1


def test_onelake_envelope_verify_readback_returns_last_observed_on_timeout() -> None:
    """The bounded exit must return what was actually read, not a bare
    ``None``, so future callers can log/diagnose the real mismatch.
    """
    files = AlwaysStaleMemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"),
        files,
        verify_timeout_seconds=0.05,
    )
    observed = envelopes._verify_readback("some/path", "expected-content")
    assert observed == "corrupted"


def test_fabric_modules_are_lazy_and_checked_in_mains_are_thin() -> None:
    before = set(sys.modules)
    for module in (
        "people_counter.fabric_candidate_a_control",
        "people_counter.fabric_candidate_a_gold",
        "people_counter.fabric_candidate_a_jobs",
    ):
        importlib.reload(importlib.import_module(module))
    imported = set(sys.modules) - before
    assert "pyspark" not in imported
    assert "delta" not in imported

    root = Path(__file__).parents[1] / "fabric" / "candidate_a"
    for job in ("control", "process", "gold"):
        source = (root / job / "Main" / "main.py").read_text()
        assert "people_counter.fabric_candidate_a_jobs import" in source
        assert "sys.path" not in source
        assert "except ImportError" not in source


def _pointer_rows() -> dict[str, list[dict[str, object]]]:
    return {
        "work": [
            {
                "work_id": "work-1",
                "status": "SUCCEEDED",
                "committed_attempt_id": "attempt-1",
                "publication_sequence": 2,
                "payload_json": "{}",
                "updated_at": 10.0,
            },
            {
                "work_id": "work-0",
                "status": "SUCCEEDED",
                "committed_attempt_id": "attempt-0",
                "publication_sequence": 1,
                "payload_json": "{}",
                "updated_at": 9.0,
            },
            {
                "work_id": "ready",
                "status": "READY",
                "committed_attempt_id": None,
                "publication_sequence": None,
                "payload_json": "{}",
                "updated_at": 8.0,
            },
        ],
        "attempts": [
            {
                "attempt_id": "attempt-1",
                "work_id": "work-1",
                "batch_id": "batch-1",
                "status": "SUCCEEDED",
                "output_path": "path-1",
                "output_sha256": "1" * 64,
            },
            {
                "attempt_id": "attempt-0",
                "work_id": "work-0",
                "batch_id": "batch-0",
                "status": "SUCCEEDED",
                "output_path": "path-0",
                "output_sha256": "0" * 64,
            },
        ],
        "publications": [
            {
                "work_id": "work-1",
                "attempt_id": "attempt-1",
                "batch_id": "batch-1",
                "publication_sequence": 2,
                "output_path": "path-1",
                "output_sha256": "1" * 64,
                "published_at": 10.0,
            },
            {
                "work_id": "work-0",
                "attempt_id": "attempt-0",
                "batch_id": "batch-0",
                "publication_sequence": 1,
                "output_path": "path-0",
                "output_sha256": "0" * 64,
                "published_at": 9.0,
            },
        ],
        "batches": [
            {
                "batch_id": "batch-1",
                "status": "COMMITTED",
                "sealed_at": 9.0,
                "committed_at": 10.0,
                "lease_expires_at": 20.0,
            },
            {
                "batch_id": "batch-0",
                "status": "COMMITTED",
                "sealed_at": 8.0,
                "committed_at": 9.0,
                "lease_expires_at": 19.0,
            },
        ],
    }


def _source_with_rows(
    rows: dict[str, list[dict[str, object]]],
) -> FabricCommittedSource:
    source = FabricCommittedSource(None, None)
    source._rows = lambda suffix: copy.deepcopy(rows[suffix])
    return source


def test_fabric_committed_source_filters_and_orders_only_valid_pointers() -> None:
    rows = _pointer_rows()
    rows["work"].insert(0, rows["work"].pop())
    source = _source_with_rows(rows)

    visible = source._visible_rows()

    assert [row["work"]["work_id"] for row in visible] == [
        "work-0",
        "work-1",
    ]
    assert source.checkpoint().publication_sequence == 2
    assert source.operations_checkpoint().publication_sequence == 2
    control = source.control_rows()
    assert len(control["work"]) == 3
    assert control["attempts"][0]["published_at"] == 10.0
    assert control["attempts"][1]["batch_completed_at"] == 9.0


@pytest.mark.parametrize(
    ("section", "index", "field", "value"),
    [
        ("attempts", 0, "attempt_id", "missing-attempt"),
        ("publications", 0, "attempt_id", "missing-publication"),
        ("attempts", 0, "work_id", "other-work"),
        ("attempts", 0, "status", "FAILED"),
        ("work", 0, "status", "LEASED"),
        ("publications", 0, "publication_sequence", 99),
        ("publications", 0, "output_path", "other-path"),
        ("publications", 0, "output_sha256", "f" * 64),
    ],
)
def test_fabric_committed_source_rejects_every_broken_pointer_identity(
    section: str, index: int, field: str, value: object
) -> None:
    rows = _pointer_rows()
    rows[section][index][field] = value

    with pytest.raises(GoldSourceError, match="invalid committed pointer"):
        _source_with_rows(rows)._visible_rows()


def _fake_blocked_mount_probe(session: object, executors: object) -> object:
    """A reusable fallback-forcing probe for tests with fake Spark sessions.

    Mirrors the exact contract :func:`select_process_execution_harness` and
    :func:`select_input_backend` both rely on: an unproven mount reports
    ``FABRIC_PLATFORM_BLOCKED`` with concrete evidence rather than ever
    being silently hardwired.
    """
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    return CapabilityProbeResult(
        capability="direct_mounted_lakehouse_path",
        status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
        evidence="fallback forced for this test",
    )


def _fake_candidate_executors(
    count: int, *, cores: int = 8, memory_bytes: int = 16 * 1024**3
) -> tuple:
    from people_counter.fabric_executor_inventory import ExecutorRecord

    return tuple(
        ExecutorRecord(
            executor_id=str(index),
            host=f"host-{index}",
            total_cores=cores,
            max_memory_bytes=memory_bytes,
        )
        for index in range(count)
    )


def test_fabric_process_profile_enforces_runtime_and_computes_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Conf:
        values = {
            "spark.dynamicAllocation.enabled": "false",
            "spark.speculation": "false",
            "spark.executor.cores": "8",
            "spark.executor.memory": "16g",
        }

        def get(self, name: str, default: object = None) -> object:
            return self.values.get(name, default)

    spark = SimpleNamespace(
        version="4.1.1.5",
        conf=Conf(),
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(
                java=SimpleNamespace(
                    lang=SimpleNamespace(
                        System=SimpleNamespace(
                            getProperty=lambda _name: "21.0.4"
                        )
                    )
                )
            )
        ),
    )
    monkeypatch.setattr(sys, "version_info", (3, 13))

    def discover(session, minimum):
        return _fake_candidate_executors(5)

    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    def measured_rss(session, executors):
        return CapabilityProbeResult(
            capability="executor_peak_rss_bytes",
            status=CapabilityStatus.AVAILABLE,
            evidence="test warmed RSS",
            value=256 * 1024**2,
        )

    profile = _fabric_process_profile(
        spark,
        discover_executors=discover,
        probe_peak_rss_bytes=measured_rss,
    )

    assert profile.executor_memory_bytes == 16 * 1024**3
    assert profile.memory_reserve_bytes == 4 * 1024**3
    assert profile.fixed_allocation is False
    assert profile.executor_instances == 5
    assert profile.executor_cores == 8
    assert profile.peak_rss_bytes == 256 * 1024**2
    assert profile.planned_task_count == 40
    assert profile.rss_headroom_fraction == 0.20
    for key, bad_value, message in (
        ("spark.dynamicAllocation.enabled", "true", "fixed"),
        ("spark.speculation", "true", "speculation"),
        ("spark.executor.cores", "0", "executor core"),
        ("spark.executor.memory", "16x", "memory"),
    ):
        original = Conf.values[key]
        Conf.values[key] = bad_value
        try:
            with pytest.raises(RuntimeError, match=message):
                _fabric_process_profile(
                    spark,
                    discover_executors=discover,
                    probe_peak_rss_bytes=measured_rss,
                )
        finally:
            Conf.values[key] = original


def test_executor_warm_works_covers_mixed_variants_and_deduplicates() -> None:
    def item(
        work_id: str, detector: str, model_format: str, runtime_key: str
    ) -> dict[str, object]:
        return {
            "work_id": work_id,
            "runtime_key": runtime_key,
            "payload": {
                "pipeline": "rtdetr-osnet",
                "detector_model": detector,
                "model_format": model_format,
                "device_variant": "cpu",
            },
        }

    envelope = {
        "items": [
            item("r18-a", "r18", "pytorch", "runtime-r18"),
            item("r18-b", "r18", "pytorch", "runtime-r18"),
            item("r50", "r50", "onnx", "runtime-r50"),
        ]
    }
    enrichment = {
        "r18-a": {"spark_localized_video_name": "video-a"},
        "r18-b": {"spark_localized_video_name": "video-b"},
        "r50": {"spark_localized_video_name": "video-c"},
    }
    works = _executor_warm_works(
        envelope,
        enrichment,
        executor_cores=8,
        task_cpus=2,
        package_version="9.8.7",
        manifest_sha256="a" * 64,
    )
    assert len(works) == 2
    assert {
        (work["detector_model"], work["model_format"]) for work in works
    } == {("r18", "pytorch"), ("r50", "onnx")}
    assert {work["planned_concurrency"] for work in works} == {4}
    assert {
        work["_expected_release_package_version"] for work in works
    } == {"9.8.7"}
    with pytest.raises(RuntimeError, match="at least one runtime"):
        _executor_warm_works(
            {"items": []},
            {},
            executor_cores=8,
            task_cpus=1,
            package_version="9.8.7",
            manifest_sha256="a" * 64,
        )


def test_fabric_process_profile_uses_the_measured_executor_rss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Placement safety uses a real executor-measured RSS value, not a bare
    executor count: a reported five executors x eight cores should plan
    40 one-CPU-wide physical tasks once memory proves it safe, matching the
    exact headline scenario the rubber-duck review flagged."""
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    class Conf:
        values = {
            "spark.dynamicAllocation.enabled": "false",
            "spark.speculation": "false",
            "spark.executor.cores": "8",
            "spark.executor.memory": "16g",
        }

        def get(self, name: str, default: object = None) -> object:
            return self.values.get(name, default)

    spark = SimpleNamespace(
        version="4.1.1.5",
        conf=Conf(),
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(
                java=SimpleNamespace(
                    lang=SimpleNamespace(
                        System=SimpleNamespace(
                            getProperty=lambda _name: "21.0.4"
                        )
                    )
                )
            )
        ),
    )
    monkeypatch.setattr(sys, "version_info", (3, 13))

    def fake_probe(session, executors):
        return CapabilityProbeResult(
            capability="executor_peak_rss_bytes",
            status=CapabilityStatus.AVAILABLE,
            evidence="fake measured 256MiB",
            value=256 * 1024**2,
        )

    profile = _fabric_process_profile(
        spark,
        discover_executors=lambda session, minimum: _fake_candidate_executors(5),
        probe_peak_rss_bytes=fake_probe,
    )
    assert profile.peak_rss_bytes == 256 * 1024**2
    assert profile.planned_task_count == 40


def test_select_process_execution_harness_prefers_direct_mount_when_proven() -> None:
    """When the mount-capability probe proves the Lakehouse Files mount is
    usable inside every executor's own task, the streaming harness staged
    directly under that mount must be selected -- the direct Lakehouse path
    optimization -- not the collecting fallback harness."""
    from people_counter.fabric_candidate_a_jobs import select_process_execution_harness
    from people_counter.fabric_candidate_a import FabricCandidateAConfig
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )
    from people_counter.sjd_process import StreamingSparkExecutionHarness

    config = FabricCandidateAConfig.canary()
    spark = SimpleNamespace()

    def fake_probe(session, executors):
        assert session is spark
        assert len(executors) == 3
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.AVAILABLE,
            evidence="proven on 3 executors",
            value="/lakehouse/default",
        )

    harness, evidence = select_process_execution_harness(
        spark,
        config,
        "batch-123",
        row_enrichment={"work-0": {"flag": "present"}},
        discover_executors=lambda session: _fake_candidate_executors(3),
        probe_mounted_path=fake_probe,
    )
    assert isinstance(harness, StreamingSparkExecutionHarness)
    assert harness.spark_session is spark
    assert harness.verify_settings is False
    assert harness.row_enrichment == {"work-0": {"flag": "present"}}
    assert str(harness.staging_root) == (
        f"/lakehouse/default/{config.file_path('process-streaming/batch-123')}"
    )
    assert evidence["capability"] == "direct_mounted_lakehouse_path"
    assert evidence["backend"] == "direct_mounted_streaming"
    assert evidence["status"] == "AVAILABLE"
    assert evidence["staging_root"] == str(harness.staging_root)


def test_configure_after_stage_failure_is_explicit_and_delta_only() -> None:
    from people_counter.fabric_sjd_runtime import (
        _configure_after_stage_failure,
    )

    disabled = SimpleNamespace(staging_backend="local", after_stage_hook=None)
    _configure_after_stage_failure(disabled, False)
    assert disabled.after_stage_hook is None

    with pytest.raises(
        RuntimeError,
        match="^stage-before-receipt injection requires Spark Delta staging$",
    ):
        _configure_after_stage_failure(disabled, True)

    enabled = SimpleNamespace(
        staging_backend="spark_delta",
        after_stage_hook=None,
    )
    _configure_after_stage_failure(enabled, True)
    with pytest.raises(
        RuntimeError,
        match="^injected failure after Delta stage before receipt$",
    ):
        enabled.after_stage_hook()


def test_select_process_execution_harness_uses_default_executor_discovery() -> None:
    """When no ``discover_executors`` override is supplied (the real call
    path taken by ``process_main``/``_process_dispatch``), the function must
    fall back to discovering active executors for real via
    ``discover_active_executors(spark, minimum_executors=1)`` -- not silently
    skip discovery or use the wrong minimum/session."""
    from people_counter import fabric_executor_inventory
    from people_counter.fabric_candidate_a_jobs import select_process_execution_harness
    from people_counter.fabric_candidate_a import FabricCandidateAConfig
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    config = FabricCandidateAConfig.canary()
    spark = SimpleNamespace()
    recorded_calls = []

    def fake_discover_active_executors(session, *, minimum_executors):
        recorded_calls.append((session, minimum_executors))
        return _fake_candidate_executors(2)

    def fake_probe(session, executors):
        assert len(executors) == 2
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="probe not relevant to this test",
        )

    monkeypatch_target = fabric_executor_inventory.discover_active_executors
    fabric_executor_inventory.discover_active_executors = (
        fake_discover_active_executors
    )
    try:
        select_process_execution_harness(
            spark,
            config,
            "batch-123",
            row_enrichment={},
            probe_mounted_path=fake_probe,
        )
    finally:
        fabric_executor_inventory.discover_active_executors = monkeypatch_target

    assert recorded_calls == [(spark, 1)]


def test_select_process_execution_harness_falls_back_when_mount_unproven() -> None:
    """When the probe cannot prove the mount (any reason), the collecting
    harness must be used and the exact capability evidence reported -- an
    unproven mount must never be hardwired."""
    from people_counter.fabric_candidate_a_jobs import select_process_execution_harness
    from people_counter.fabric_candidate_a import FabricCandidateAConfig
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )
    from people_counter.sjd_process import StreamingSparkExecutionHarness

    config = FabricCandidateAConfig.canary()
    spark = SimpleNamespace()

    def fake_probe(session, executors):
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="OSError: mount not visible on executor '2'",
        )

    harness, evidence = select_process_execution_harness(
        spark,
        config,
        "batch-123",
        row_enrichment={"work-0": {"flag": "present"}},
        discover_executors=lambda session: _fake_candidate_executors(3),
        probe_mounted_path=fake_probe,
    )
    assert isinstance(harness, StreamingSparkExecutionHarness)
    assert harness.spark_session is spark
    assert harness.verify_settings is False
    assert harness.row_enrichment == {"work-0": {"flag": "present"}}
    assert evidence["capability"] == "direct_mounted_lakehouse_path"
    assert harness.staging_backend == "spark_delta"
    assert evidence["backend"] == "fallback_spark_delta_receipts"
    assert evidence["status"] == "FABRIC_PLATFORM_BLOCKED"
    assert "not visible" in evidence["evidence"]
    assert evidence["staging_root"] == config.file_path(
        "process-streaming/batch-123"
    )


def test_select_process_execution_harness_upgrades_to_consumer_probe_when_given() -> (
    None
):
    """When ``consumer_probe_profile`` is given and no explicit
    ``probe_mounted_path`` override is supplied, the real-consumer composed
    probe (not the bare POSIX probe) must be the one actually invoked, and
    an ``AVAILABLE`` result whose ``value`` is a ``ConsumerCapabilityReport``
    must unwrap to its ``mount_root`` for the staging root -- never the
    report object itself."""
    from people_counter.fabric_candidate_a_jobs import select_process_execution_harness
    from people_counter.fabric_candidate_a import FabricCandidateAConfig
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
        ConsumerCapabilityReport,
        ConsumerProbeProfile,
    )

    config = FabricCandidateAConfig.canary()
    spark = SimpleNamespace()
    profile = ConsumerProbeProfile()
    recorded = {}
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

    def fake_composed_probe(session, executors, *, profile, **kwargs):
        recorded["profile"] = profile
        recorded["kwargs"] = kwargs
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
        _harness, evidence = select_process_execution_harness(
            spark,
            config,
            "batch-123",
            row_enrichment={},
            discover_executors=lambda session: _fake_candidate_executors(1),
            consumer_probe_profile=profile,
        )
    patched.assert_called_once()
    assert recorded["profile"] is profile
    assert evidence["backend"] == "direct_mounted_streaming"
    assert evidence["staging_root"].startswith("/lakehouse/default/")


def test_select_process_execution_harness_uses_bare_posix_probe_by_default() -> None:
    """With neither ``probe_mounted_path`` nor ``consumer_probe_profile``
    given, the real bare POSIX probe (``probe_direct_mounted_lakehouse_path``)
    must be the one actually invoked -- not ``None``/a silently-dropped
    callable."""
    from people_counter.fabric_candidate_a_jobs import select_process_execution_harness
    from people_counter.fabric_candidate_a import FabricCandidateAConfig
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    config = FabricCandidateAConfig.canary()
    spark = SimpleNamespace()
    fake_result = CapabilityProbeResult(
        capability="direct_mounted_lakehouse_path",
        status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
        evidence="default probe used",
    )
    with patch(
        "people_counter.fabric_capability_probe.probe_direct_mounted_lakehouse_path",
        return_value=fake_result,
    ) as patched:
        _harness, evidence = select_process_execution_harness(
            spark,
            config,
            "batch-123",
            row_enrichment={},
            discover_executors=lambda session: _fake_candidate_executors(1),
        )
    patched.assert_called_once()
    assert evidence["backend"] == "fallback_spark_delta_receipts"
    assert evidence["evidence"] == "default probe used"


def test_select_process_execution_harness_probe_mounted_path_wins_over_profile() -> (
    None
):
    """An explicit ``probe_mounted_path`` override must always win over
    ``consumer_probe_profile`` -- test/call-site injection is never
    silently superseded by the new default-upgrade behavior."""
    from people_counter.fabric_candidate_a_jobs import select_process_execution_harness
    from people_counter.fabric_candidate_a import FabricCandidateAConfig
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
        ConsumerProbeProfile,
    )

    config = FabricCandidateAConfig.canary()
    spark = SimpleNamespace()
    calls = []

    def fake_probe(session, executors):
        calls.append((session, executors))
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="explicit override used",
        )

    with patch(
        "people_counter.fabric_capability_probe.probe_direct_mount_consumer_capability",
    ) as patched:
        select_process_execution_harness(
            spark,
            config,
            "batch-123",
            row_enrichment={},
            discover_executors=lambda session: _fake_candidate_executors(1),
            probe_mounted_path=fake_probe,
            consumer_probe_profile=ConsumerProbeProfile(),
        )
    patched.assert_not_called()
    assert len(calls) == 1


def test_process_input_localizer_distributes_and_hashes_immutable_inputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    files = {
        "synthetic.mp4": b"video",
        "config.json": b"config",
        "preprocessor_config.json": b"preprocessor",
        "model.safetensors": b"detector",
        "osnet_ain_x0_25.pt": b"reid",
    }
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    spark_files = SimpleNamespace(
        get=lambda name: str(tmp_path / name),
        getRootDirectory=lambda: str(tmp_path / "spark-root"),
    )
    monkeypatch.setitem(
        sys.modules, "pyspark", SimpleNamespace(SparkFiles=spark_files)
    )
    payload = {
        "source_video": (
            "/lakehouse/default/Files/_canary/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": _sha256_bytes(b"video"),
        "pipeline": "rtdetr-osnet",
        "detector_model": "r18",
        "model_format": "pytorch",
        "model_artifact_sha256": {
            "rtdetr_osnet/rtdetr_v2_r18vd/config.json": _sha256_bytes(
                b"config"
            ),
            "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json": (
                _sha256_bytes(b"preprocessor")
            ),
            "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": (
                _sha256_bytes(b"detector")
            ),
            "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt": (
                _sha256_bytes(b"reid")
            ),
        },
    }
    requested_batches: list[str] = []

    def load_envelope(batch: str) -> tuple[dict[str, object], str]:
        requested_batches.append(batch)
        return {"items": [{"work_id": "work-1", "payload": payload}]}, "digest"

    store = SimpleNamespace(load_claim_envelope_with_digest=load_envelope)
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=MagicMock())
    )
    current_jobs_module = sys.modules["people_counter.fabric_candidate_a_jobs"]

    def stream_remote(_spark, uri, destination):
        source = tmp_path / uri.rsplit("/", 1)[-1]
        with source.open("rb") as read_stream, destination.open("xb") as write_stream:
            while chunk := read_stream.read(1024):
                write_stream.write(chunk)

    monkeypatch.setattr(
        current_jobs_module,
        "_stream_hadoop_uri_to_local",
        stream_remote,
    )
    monkeypatch.setattr(
        current_jobs_module,
        "_stage_hadoop_alias",
        lambda *_args, **_kwargs: None,
    )

    localized = _localize_process_inputs(
        spark,
        store,
        "batch-1",
        discover_executors=lambda session: _fake_candidate_executors(2),
        probe_mounted_path=_fake_blocked_mount_probe,
    )
    # Pin the exact named-tuple shape so any future caller that binds the
    # whole result to one name (the ``fabric_benchmark_jobs`` regression
    # this type exists to prevent) fails a type/isinstance check instead
    # of silently drifting back to an untyped 2-tuple or bare dict. Look the
    # class up live from ``sys.modules`` rather than a top-level from-import:
    # an earlier test in this file (``test_fabric_modules_are_lazy_and_checked_in_mains_are_thin``)
    # reloads this exact module, which rebinds ``LocalizedProcessInputs`` to
    # a new class object in the module's own namespace -- a stale
    # from-import captured at collection time would then fail this
    # isinstance check even though construction and this assertion are both
    # using the one actually-current class.
    assert isinstance(localized, current_jobs_module.LocalizedProcessInputs)
    enrichment, resolver_capability = localized
    assert resolver_capability["backend"] == "FABRIC_FALLBACK"
    assert resolver_capability["cache_metrics"]["misses"] == 5

    assert enrichment["work-1"]["resolver_backend"] == "FABRIC_FALLBACK"
    assert enrichment["work-1"]["spark_localized_video_name"] == _sha256_bytes(
        b"video"
    )
    models = enrichment["work-1"]["spark_localized_models"]
    assert len(models) == 4
    assert models["rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors"][
        "sha256"
    ] == _sha256_bytes(b"detector")
    assert models["rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors"][
        "localized_name"
    ] == _sha256_bytes(b"detector")
    assert requested_batches == ["batch-1"]
    assert spark.sparkContext.addFile.call_count == 5
    assert all(
        value["localized_name"] == value["sha256"] for value in models.values()
    )
    broadcast_args = {
        call.args[0] for call in spark.sparkContext.addFile.call_args_list
    }
    assert all(arg.startswith("abfss://") for arg in broadcast_args)
    assert not any("/Files/models/" in arg for arg in broadcast_args)
    assert any(
        arg.endswith(
            models["rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt"][
                "sha256"
            ]
        )
        for arg in broadcast_args
    )

    from people_counter.fabric_production_routing import sha256_json

    model_sha256 = sha256_json(
        {
            "schema": "people-counter-fixed-model-artifacts-v1",
            "artifacts": {
                "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors":
                    _sha256_bytes(b"detector"),
                "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt":
                    _sha256_bytes(b"reid"),
            },
        }
    )
    synthetic_item = {
        "work_id": "work-1",
        "config_sha256": "c" * 64,
        "payload": payload,
    }
    store.load_claim_envelope_with_digest = lambda _batch: (
        {"items": [synthetic_item]},
        "digest",
    )
    synthetic_identity = {
        "work_id": "work-1",
        "source_sha256": payload["source_sha256"],
        "config_sha256": "c" * 64,
        "model_sha256": model_sha256,
    }
    assert _localize_process_inputs(
        spark,
        store,
        "batch-1",
        route_mode="SHADOW_SYNTHETIC",
        route_identity=synthetic_identity,
        discover_executors=lambda session: _fake_candidate_executors(2),
        probe_mounted_path=_fake_blocked_mount_probe,
    )[0]["work-1"]["spark_localized_models"] == models
    with pytest.raises(ProcessValidationError, match="identity is required"):
        _localize_process_inputs(
            spark,
            store,
            "batch-1",
            route_mode="SHADOW_SYNTHETIC",
            discover_executors=lambda session: _fake_candidate_executors(2),
            probe_mounted_path=_fake_blocked_mount_probe,
        )
    for changed, message in (
        ({**synthetic_identity, "source_sha256": "a" * 64}, "video/config"),
        ({**synthetic_identity, "config_sha256": "a" * 64}, "video/config"),
        ({**synthetic_identity, "model_sha256": "a" * 64}, "model identity"),
    ):
        with pytest.raises(ProcessValidationError, match=message):
            _localize_process_inputs(
                spark,
                store,
                "batch-1",
                route_mode="SHADOW_SYNTHETIC",
                route_identity=changed,
                discover_executors=lambda session: _fake_candidate_executors(2),
                probe_mounted_path=_fake_blocked_mount_probe,
            )

    for update, message in (
        ({"source_video": "/production/video.mp4"}, "default Lakehouse"),
        ({"source_sha256": "0" * 64}, "SHA-256 mismatch"),
        ({"pipeline": "rfdetr-botsort"}, "RT-DETR"),
        ({"model_artifact_sha256": ["not", "a", "mapping"]}, "must be an object"),
        (
            {
                "model_artifact_sha256": {
                    **payload["model_artifact_sha256"],
                    "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": "f" * 64
                }
            },
            "SHA-256 mismatch",
        ),
    ):
        changed = payload | update
        store.load_claim_envelope_with_digest = lambda _batch, item=changed: (
            {"items": [{"work_id": "work-1", "payload": item}]},
            "digest",
        )
        with pytest.raises(ValueError, match=message):
            _localize_process_inputs(
                spark,
                store,
                "batch-1",
                discover_executors=lambda session: _fake_candidate_executors(2),
                probe_mounted_path=_fake_blocked_mount_probe,
            )
    with pytest.raises(
        ProcessValidationError,
        match=r"^synthetic route identity is required$",
    ):
        _localize_process_inputs(
            spark,
            store,
            "batch-1",
            route_mode="SHADOW_SYNTHETIC",
            discover_executors=lambda session: _fake_candidate_executors(2),
            probe_mounted_path=_fake_blocked_mount_probe,
        )


def test_process_input_localizer_passes_exact_spark_session_and_cache_params(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "synthetic.mp4").write_bytes(b"video")
    (tmp_path / "config.json").write_bytes(b"config")
    (tmp_path / "preprocessor_config.json").write_bytes(b"preprocessor")
    (tmp_path / "model.safetensors").write_bytes(b"detector")
    (tmp_path / "osnet_ain_x0_25.pt").write_bytes(b"reid")
    spark_root = tmp_path / "spark-root"
    spark_files = SimpleNamespace(
        get=lambda name: str(tmp_path / name),
        getRootDirectory=lambda: str(spark_root),
    )
    monkeypatch.setitem(
        sys.modules, "pyspark", SimpleNamespace(SparkFiles=spark_files)
    )
    payload = {
        "source_video": (
            "/lakehouse/default/Files/_canary/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": _sha256_bytes(b"video"),
        "pipeline": "rtdetr-osnet",
        "detector_model": "r18",
        "model_format": "pytorch",
        "model_artifact_sha256": {
            "rtdetr_osnet/rtdetr_v2_r18vd/config.json": _sha256_bytes(
                b"config"
            ),
            "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json": (
                _sha256_bytes(b"preprocessor")
            ),
            "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": (
                _sha256_bytes(b"detector")
            ),
            "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt": (
                _sha256_bytes(b"reid")
            ),
        },
    }
    store = SimpleNamespace(
        load_claim_envelope_with_digest=lambda _batch: (
            {"items": [{"work_id": "work-1", "payload": payload}]},
            "digest",
        )
    )
    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    received: dict[str, object] = {}

    def recording_probe(session: object, executors: object) -> object:
        from people_counter.fabric_capability_probe import (
            CapabilityProbeResult,
            CapabilityStatus,
        )

        received["probe_session"] = session
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="fallback forced for this test",
        )

    def recording_discover(session: object) -> tuple:
        received["discover_session"] = session
        return _fake_candidate_executors(2)

    from people_counter import fabric_candidate_a_jobs
    from people_counter.fabric_source_cache import LocalizedSourceCache

    real_cache_cls = LocalizedSourceCache

    class _RecordingCache(real_cache_cls):  # type: ignore[misc]
        def __init__(self, root: Path, **kwargs: object) -> None:
            received["cache_root"] = root
            received["cache_kwargs"] = kwargs
            super().__init__(root, **kwargs)

    monkeypatch.setattr(fabric_candidate_a_jobs, "LocalizedSourceCache", _RecordingCache)

    def stream_remote(_spark, uri, destination):
        source = tmp_path / uri.rsplit("/", 1)[-1]
        with source.open("rb") as read_stream, destination.open("xb") as write_stream:
            while chunk := read_stream.read(1024):
                write_stream.write(chunk)

    monkeypatch.setattr(
        fabric_candidate_a_jobs,
        "_stream_hadoop_uri_to_local",
        stream_remote,
    )
    monkeypatch.setattr(
        fabric_candidate_a_jobs,
        "_stage_hadoop_alias",
        lambda *_args, **_kwargs: None,
    )

    _localize_process_inputs(
        spark,
        store,
        "batch-1",
        discover_executors=recording_discover,
        probe_mounted_path=recording_probe,
    )
    # The exact Spark session passed in must flow through unaltered to both
    # the executor-discovery and mount-probe callables -- never dropped or
    # replaced with ``None``.
    assert received["probe_session"] is spark
    assert received["discover_session"] is spark
    # The fallback content-addressed cache must be constructed under its own
    # "content-addressed" subdirectory with the reviewed 256-entry /
    # 16 GiB byte budget -- not some other silently-drifted value.
    assert received["cache_root"] == Path(str(spark_root)) / "content-addressed"
    assert received["cache_kwargs"] == {
        "max_entries": 256,
        "max_total_bytes": 16 * 1024**3,
    }


def test_process_input_localizer_direct_backend_resolves_mount_without_distribution(
    tmp_path: Path,
) -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    mount_root = tmp_path / "lakehouse"
    video_path = (
        mount_root
        / "Files/_canary/people-counter/candidate-a/v1/assets/synthetic.mp4"
    )
    video_path.parent.mkdir(parents=True)
    video_path.write_bytes(b"video")
    model_files = {
        "rtdetr_osnet/rtdetr_v2_r18vd/config.json": b"config",
        "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json": b"preprocessor",
        "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": b"detector",
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt": b"reid",
    }
    for relative, content in model_files.items():
        path = mount_root / "Files/models" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    payload = {
        "source_video": (
            "/lakehouse/default/Files/_canary/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": _sha256_bytes(b"video"),
        "pipeline": "rtdetr-osnet",
        "detector_model": "r18",
        "model_format": "pytorch",
        "model_artifact_sha256": {
            relative: _sha256_bytes(content)
            for relative, content in model_files.items()
        },
    }
    store = SimpleNamespace(
        load_claim_envelope_with_digest=lambda _batch: (
            {"items": [{"work_id": "work-1", "payload": payload}]},
            "digest",
        )
    )
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=MagicMock())
    )

    def available_probe(session: object, executors: object) -> object:
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.AVAILABLE,
            evidence="mount proven on 2 executors",
            value=str(mount_root),
        )

    enrichment, resolver_capability = _localize_process_inputs(
        spark,
        store,
        "batch-1",
        discover_executors=lambda session: _fake_candidate_executors(2),
        probe_mounted_path=available_probe,
    )

    assert resolver_capability["backend"] == "FABRIC_DIRECT"
    assert resolver_capability["mount_root"] == str(mount_root)
    assert resolver_capability["cache_metrics"] is None

    row = enrichment["work-1"]
    assert row["resolver_backend"] == "FABRIC_DIRECT"
    assert row["lakehouse_mount_root"] == str(mount_root)
    assert row["lakehouse_relative_video_path"] == (
        "Files/_canary/people-counter/candidate-a/v1/assets/synthetic.mp4"
    )
    assert row["lakehouse_relative_models_root"] == "Files/models"
    # The direct backend never distributes or copies: no addFile call at all.
    spark.sparkContext.addFile.assert_not_called()


def test_process_input_localizer_direct_backend_rejects_video_digest_mismatch(
    tmp_path: Path,
) -> None:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    mount_root = tmp_path / "lakehouse"
    video_path = (
        mount_root
        / "Files/_canary/people-counter/candidate-a/v1/assets/synthetic.mp4"
    )
    video_path.parent.mkdir(parents=True)
    video_path.write_bytes(b"video")

    payload = {
        "source_video": (
            "/lakehouse/default/Files/_canary/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": "0" * 64,
        "pipeline": "rtdetr-osnet",
        "detector_model": "r18",
        "model_format": "pytorch",
    }
    store = SimpleNamespace(
        load_claim_envelope_with_digest=lambda _batch: (
            {"items": [{"work_id": "work-1", "payload": payload}]},
            "digest",
        )
    )
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=MagicMock())
    )

    def available_probe(session: object, executors: object) -> object:
        return CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.AVAILABLE,
            evidence="mount proven on 2 executors",
            value=str(mount_root),
        )

    with pytest.raises(ValueError, match="video digest"):
        _localize_process_inputs(
            spark,
            store,
            "batch-1",
            discover_executors=lambda session: _fake_candidate_executors(2),
            probe_mounted_path=available_probe,
        )


def test_localize_process_inputs_forwards_consumer_probe_profile_unchanged() -> (
    None
):
    """``consumer_probe_profile`` must be forwarded to
    :func:`~people_counter.fabric_input_resolver.select_input_backend`
    exactly as given -- never dropped/replaced with ``None`` -- so the
    real-consumer composed probe is actually used when a profile is
    supplied."""
    from people_counter.fabric_capability_probe import ConsumerProbeProfile
    from people_counter.fabric_input_resolver import InputBackend

    store = SimpleNamespace(
        load_claim_envelope_with_digest=lambda _batch: (
            {"items": []},
            "digest",
        )
    )
    spark = SimpleNamespace(sparkContext=SimpleNamespace(addFile=MagicMock()))
    profile = ConsumerProbeProfile()
    recorded: dict[str, Any] = {}

    def fake_select_input_backend(session, *, discover_executors, **kwargs):
        recorded["consumer_probe_profile"] = kwargs.get("consumer_probe_profile")
        return InputBackend.FABRIC_DIRECT, {"mount_root": "/lakehouse/default"}

    with patch(
        "people_counter.fabric_candidate_a_jobs.select_input_backend",
        side_effect=fake_select_input_backend,
    ) as patched:
        _localize_process_inputs(
            spark,
            store,
            "batch-1",
            discover_executors=lambda session: _fake_candidate_executors(1),
            consumer_probe_profile=profile,
        )
    patched.assert_called_once()
    assert recorded["consumer_probe_profile"] is profile


def test_synthetic_localizer_pins_r50_onnx_artifacts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    files = {
        "synthetic.mp4": b"video-r50",
        "config.json": b"config-r50",
        "preprocessor_config.json": b"preprocessor-r50",
        "model.onnx": b"detector-r50",
        "osnet_ain_x0_25.onnx": b"reid-onnx",
    }
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    monkeypatch.setitem(
        sys.modules,
        "pyspark",
        SimpleNamespace(
            SparkFiles=SimpleNamespace(
                get=lambda name: str(tmp_path / name),
                getRootDirectory=lambda: str(tmp_path / "spark-root"),
            )
        ),
    )
    payload = {
        "source_video": (
            "/lakehouse/default/Files/_shadow/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": _sha256_bytes(files["synthetic.mp4"]),
        "pipeline": "rtdetr-osnet",
        "detector_model": "r50",
        "model_format": "onnx",
        "model_artifact_sha256": {
            "rtdetr_osnet/rtdetr_v2_r50vd/config.json": _sha256_bytes(
                files["config.json"]
            ),
            "rtdetr_osnet/rtdetr_v2_r50vd/preprocessor_config.json": (
                _sha256_bytes(files["preprocessor_config.json"])
            ),
            "rtdetr_osnet/rtdetr_v2_r50vd/model.onnx": _sha256_bytes(
                files["model.onnx"]
            ),
            "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx": (
                _sha256_bytes(files["osnet_ain_x0_25.onnx"])
            ),
        },
    }
    item = {
        "work_id": "work-r50",
        "config_sha256": "c" * 64,
        "payload": payload,
    }
    store = SimpleNamespace(
        load_claim_envelope_with_digest=lambda batch: (
            {"items": [item]} if batch == "batch-r50" else {},
            "digest",
        )
    )
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=MagicMock())
    )
    from people_counter.fabric_production_routing import sha256_json
    from people_counter import fabric_candidate_a_jobs

    def stream_remote(_spark, uri, destination):
        source = tmp_path / uri.rsplit("/", 1)[-1]
        with source.open("rb") as read_stream, destination.open("xb") as write_stream:
            while chunk := read_stream.read(1024):
                write_stream.write(chunk)

    monkeypatch.setattr(
        fabric_candidate_a_jobs,
        "_stream_hadoop_uri_to_local",
        stream_remote,
    )
    monkeypatch.setattr(
        fabric_candidate_a_jobs,
        "_stage_hadoop_alias",
        lambda *_args, **_kwargs: None,
    )

    identity = {
        "work_id": "work-r50",
        "source_sha256": payload["source_sha256"],
        "config_sha256": item["config_sha256"],
        "model_sha256": sha256_json(
            {
                "schema": "people-counter-fixed-model-artifacts-v1",
                "artifacts": {
                    "Files/models/rtdetr_osnet/rtdetr_v2_r50vd/model.onnx":
                        _sha256_bytes(files["model.onnx"]),
                    "Files/models/rtdetr_osnet/libre_reid_osnet/"
                    "osnet_ain_x0_25.onnx":
                        _sha256_bytes(files["osnet_ain_x0_25.onnx"]),
                },
            }
        ),
    }

    localized = _localize_process_inputs(
        spark,
        store,
        "batch-r50",
        route_mode="SHADOW_SYNTHETIC",
        route_identity=identity,
        discover_executors=lambda session: _fake_candidate_executors(2),
        probe_mounted_path=_fake_blocked_mount_probe,
    )[0]["work-r50"]

    models = localized["spark_localized_models"]
    assert set(models) == {
        "rtdetr_osnet/rtdetr_v2_r50vd/config.json",
        "rtdetr_osnet/rtdetr_v2_r50vd/preprocessor_config.json",
        "rtdetr_osnet/rtdetr_v2_r50vd/model.onnx",
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx",
    }
    assert localized["resolver_backend"] == "FABRIC_FALLBACK"
    assert localized["spark_localized_video_name"] == _sha256_bytes(
        files["synthetic.mp4"]
    )
    assert spark.sparkContext.addFile.call_count == 5
    broadcast_args = {
        call.args[0] for call in spark.sparkContext.addFile.call_args_list
    }
    assert all(arg.startswith("abfss://") for arg in broadcast_args)
    assert not any("/Files/models/" in arg for arg in broadcast_args)
    assert any(
        arg.endswith(
            models["rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx"][
                "sha256"
            ]
        )
        for arg in broadcast_args
    )


def _sha256_bytes(value: bytes) -> str:
    import hashlib

    return hashlib.sha256(value).hexdigest()


def test_control_main_dispatches_all_safe_commands(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    @dataclass
    class Result:
        value: object

    store = MagicMock()
    store.register.return_value = Result("work-1")
    store.claim.return_value = None
    store.replay.return_value = Result("replay-1")
    store.recover.return_value = Result(1)
    store.reconcile.return_value = []
    store_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _store_factory(*args: object, **kwargs: object) -> object:
        store_calls.append((args, kwargs))
        return store

    monkeypatch.setattr(
        "people_counter.sjd_control.FabricControlStore", _store_factory
    )
    spark_sentinel = object()
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: spark_sentinel
    )
    clear_lock_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _fake_clear_stale_lock(*args: object, **kwargs: object) -> dict[str, object]:
        clear_lock_calls.append((args, kwargs))
        return {"cleared_owner_id": "owner", "acquired_at": "never"}

    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._clear_stale_lock",
        _fake_clear_stale_lock,
    )
    shared_config = FabricCandidateAConfig.benchmark()
    commands = (
        ["bootstrap"],
        [
            "register",
            "--work-id",
            "work-1",
            "--payload-base64",
            base64.urlsafe_b64encode(b'{"asset":"safe"}').decode(),
            "--runtime-key",
            "cpu",
            "--duration-seconds",
            "1",
            "--config-sha256",
            "c" * 64,
            "--release-digest",
            "release",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--work-id",
            "work-1",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
        ],
        [
            "replay",
            "--work-id",
            "work-1",
            "--operator",
            "operator",
            "--reason",
            "test",
        ],
        ["recover"],
        ["clear-stale-lock", "--expected-owner-id", "owner"],
        ["reconcile"],
    )
    expected_outputs = [
        {"bootstrapped": True},
        {"value": "work-1"},
        None,
        {"value": "replay-1"},
        {"value": 1},
        {"cleared_owner_id": "owner", "acquired_at": "never"},
        [],
    ]
    for command, expected in zip(commands, expected_outputs, strict=True):
        assert control_main(command, config=shared_config) == 0
        printed = json.loads(capsys.readouterr().out)
        assert printed == expected
    assert store.claim.call_args.kwargs["allowed_work_ids"] == ["work-1"]

    # Every dispatch call must build the control store from the exact
    # resource/config pair control_main resolved, not a mutated/default one.
    assert len(store_calls) == len(commands)
    for args, kwargs in store_calls:
        assert args == (spark_sentinel,)
        assert kwargs == {"config": shared_config}

    # clear-stale-lock must thread the resolved config through unchanged,
    # not a silently-dropped kwarg or a freshly constructed default.
    assert len(clear_lock_calls) == 1
    clear_args, clear_kwargs = clear_lock_calls[0]
    assert clear_args == (spark_sentinel, "owner")
    assert clear_kwargs == {"config": shared_config}


def test_control_main_persists_diagnostic_traceback_and_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crashing control command must leave an evidence trail even when
    Fabric's standard Spark driver log-fetch API 404s for apps that die
    before full YARN log-aggregation registration completes."""

    store = MagicMock()
    store.claim.side_effect = RuntimeError("boom from the real bug")
    monkeypatch.setattr(
        "people_counter.sjd_control.FabricControlStore",
        lambda *_args, **_kwargs: store,
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: object()
    )
    written: dict[str, object] = {}

    class _FakeFs:
        @staticmethod
        def put(path: str, content: str, overwrite: bool) -> bool:
            written["path"] = path
            written["content"] = content
            written["overwrite"] = overwrite
            return True

    fake_notebookutils = SimpleNamespace(fs=_FakeFs())
    monkeypatch.setitem(sys.modules, "notebookutils", fake_notebookutils)

    with pytest.raises(RuntimeError, match="boom from the real bug"):
        control_main(
            [
                "claim",
                "--owner",
                "owner",
                "--max-items",
                "1",
                "--lease-seconds",
                "60",
            ]
        )

    assert written["overwrite"] is True
    assert "/control/diagnostics/claim-" in written["path"]
    payload = json.loads(written["content"])
    assert payload["command"] == "claim"
    assert payload["error_type"] == "RuntimeError"
    assert payload["error_message"] == "boom from the real bug"
    assert any("boom from the real bug" in line for line in payload["traceback"])
    # The traceback must retain real stack frames (not just the summary
    # line), otherwise ``error.__traceback__`` was dropped on the way in.
    assert len(payload["traceback"]) > 1
    assert any(
        "Traceback (most recent call last):" in line for line in payload["traceback"]
    )
    assert "captured_at" in payload
    assert isinstance(payload["captured_at"], float)
    # ``sort_keys=True`` is required for stable, deterministic diagnostics;
    # verify the raw (unparsed) JSON text actually preserves alphabetical
    # key order rather than relying on json.loads to hide it.
    raw = written["content"]
    key_positions = [
        raw.index(f'"{key}"')
        for key in ("captured_at", "command", "error_message", "error_type", "traceback")
    ]
    assert key_positions == sorted(key_positions)


def test_write_control_diagnostic_swallows_its_own_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Diagnostic capture must never mask or replace the real exception."""

    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
    config = FabricCandidateAConfig.benchmark()
    result = _write_control_diagnostic(config, "claim", RuntimeError("original"))
    assert result is None


def test_fabric_control_bootstrap_creates_tables_and_rejects_bad_lock() -> None:
    store = FabricControlStoreImpl.__new__(FabricControlStoreImpl)
    store.tables = {name: f"table_{name}" for name in _SCHEMAS}
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    frame = MagicMock()
    spark.createDataFrame.return_value = frame
    frame.write.format.return_value.mode.return_value.saveAsTable.return_value = None
    store.spark = spark
    rows = [{"lock_name": "global", "owner_id": None, "acquired_at": None}]
    store._rows = lambda _suffix: copy.deepcopy(rows)

    store.bootstrap()

    assert spark.createDataFrame.call_count == len(_SCHEMAS)
    assert frame.write.format.return_value.mode.return_value.saveAsTable.call_count == len(
        _SCHEMAS
    )
    rows[:] = []
    with pytest.raises(RuntimeError, match="exactly one"):
        store.bootstrap()
    rows[:] = [{"lock_name": "global", "owner_id": "owner", "acquired_at": None}]
    with pytest.raises(RuntimeError, match="inconsistent"):
        store.bootstrap()


def test_process_batch_verification_rejects_each_stale_identity_and_fence() -> None:
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work-1",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
    )
    claim = store.claim(
        "owner-1",
        max_items=1,
        lease_seconds=60,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    raw, digest = store.load_claim_envelope_with_digest(claim.batch_id)
    from people_counter.sjd_process import verify_envelope

    envelope = verify_envelope(
        raw, batch_id=claim.batch_id, envelope_sha256=digest
    )
    for changed in (
        replace(envelope, envelope_sha256="f" * 64),
        replace(envelope, membership_sha256="e" * 64),
        replace(
            envelope,
            items=(
                replace(envelope.items[0], fence=envelope.items[0].fence + 1),
            ),
        ),
    ):
        with pytest.raises(Exception):
            store.process_batch_state(changed)

    work = store.data["work"][0]
    original = copy.deepcopy(work)
    for field, value in (
        ("status", "READY"),
        ("lease_attempt_id", "other-attempt"),
        ("fence", int(work["fence"]) + 1),
    ):
        store.data["work"][0] = copy.deepcopy(original)
        store.data["work"][0][field] = value
        with pytest.raises(Exception, match="lost its work fence"):
            store.process_batch_state(envelope)
    store.data["work"][0] = original
    store.seal_batch(
        claim.batch_id,
        [
            {
                "work_id": envelope.items[0].work_id,
                "attempt_id": envelope.items[0].attempt_id,
                "succeeded": True,
                "output_path": "Files/safe",
                "output_sha256": "a" * 64,
                "records": [
                    {
                        "executor_identity": "executor-1",
                        "partition_id": 0,
                        "task_attempt_id": 0,
                        "record_sequence": 0,
                    }
                ],
            }
        ],
        envelope_sha256=digest,
        membership_sha256=envelope.membership_sha256,
    )
    store.data["work"][0]["fence"] = int(store.data["work"][0]["fence"]) + 1
    with pytest.raises(Exception, match="lost its work fence"):
        store.process_batch_state(envelope)
    store.data["work"][0]["fence"] = envelope.items[0].fence
    store.commit_batch(claim.batch_id)
    assert store.process_batch_state(envelope).status == "COMMITTED"


def test_recovery_retries_expired_lease_and_reconciliation_resolves_findings() -> None:
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work-1",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=2,
    )
    claim = store.claim(
        "owner-1",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None

    report = store.recover(now=111.0)

    assert report.recovered == 1
    assert report.retried == 1
    assert report.dead == 0
    assert store.data["batches"][0]["status"] == "EXPIRED"
    assert store.data["attempts"][0]["status"] == "EXPIRED"
    assert store.data["work"][0]["status"] == "READY"
    assert store.recover(now=112.0).recovered == 0
    assert store.reconcile() == []

    store.data["work"][0]["status"] = "SUCCEEDED"
    store.data["work"][0]["committed_attempt_id"] = "missing"
    findings = store.reconcile()
    assert findings
    assert any(item.severity == "ERROR" for item in findings)
    store.data["work"][0]["status"] = "READY"
    store.data["work"][0]["committed_attempt_id"] = None
    assert store.reconcile() == []
    assert all(
        row["resolved_at"] is not None
        for row in store.data["reconciliation_findings"]
    )


def test_exact_recovery_requires_expiry_identity_fence_and_no_output() -> None:
    files = MemoryFiles()
    store = MemoryControlStore(files)
    store.register(
        "work-exact",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=1,
    )
    claim = store.claim(
        "owner-exact",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    item = claim.items[0]
    arguments = {
        "work_id": item.work_id,
        "batch_id": claim.batch_id,
        "attempt_id": item.attempt_id,
        "owner": "owner-exact",
        "fence": item.fence,
        "lease_expires_at": claim.lease_expires_at,
        "envelope_sha256": claim.envelope_sha256,
        "membership_sha256": store.data["batches"][0]["membership_sha256"],
        "safe_skew_seconds": 1.0,
    }
    with pytest.raises(Exception, match="safe skew"):
        store.recover_exact(**arguments, now=claim.lease_expires_at)
    with pytest.raises(Exception, match="fence must be positive"):
        store.recover_exact(
            **{**arguments, "fence": 0},
            now=claim.lease_expires_at + 2,
        )
    for name, value in (
        ("work_id", "wrong-work"),
        ("batch_id", "wrong-batch"),
        ("owner", "wrong-owner"),
        ("fence", item.fence + 1),
        ("attempt_id", "wrong-attempt"),
        ("lease_expires_at", claim.lease_expires_at + 1),
        ("envelope_sha256", "e" * 64),
        ("membership_sha256", "m" * 64),
    ):
        with pytest.raises(Exception):
            store.recover_exact(
                **{**arguments, name: value},
                now=claim.lease_expires_at + 2,
            )

    row_mismatches = (
        ("work", "status", "READY"),
        ("batches", "status", "READY"),
        ("attempts", "status", "READY"),
        ("work", "lease_owner", "wrong-owner"),
        ("work", "lease_attempt_id", "wrong-attempt"),
        ("attempts", "work_id", "wrong-work"),
        ("attempts", "batch_id", "wrong-batch"),
        ("work", "fence", item.fence + 1),
        ("attempts", "fence", item.fence + 1),
        ("batch_members", "fence", item.fence + 1),
        ("batch_members", "payload_sha256", "p" * 64),
        ("work", "lease_expires_at", claim.lease_expires_at + 1),
        ("batches", "lease_expires_at", claim.lease_expires_at + 1),
        ("attempts", "lease_expires_at", claim.lease_expires_at + 1),
        ("batches", "envelope_sha256", "e" * 64),
        ("batches", "membership_sha256", "m" * 64),
        ("work", "attempt_count", 0),
        ("work", "committed_attempt_id", "committed-attempt"),
    )
    for table, field, value in row_mismatches:
        row = store.data[table][0]
        original = row[field]
        row[field] = value
        with pytest.raises(Exception):
            store.recover_exact(
                **arguments, now=claim.lease_expires_at + 2
            )
        row[field] = original

    duplicate_member = copy.deepcopy(store.data["batch_members"][0])
    store.data["batch_members"].append(duplicate_member)
    with pytest.raises(Exception, match="membership is not singular"):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["batch_members"].pop()
    store.data["publications"].append({"work_id": item.work_id})
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["publications"].clear()
    duplicate_attempt = copy.deepcopy(store.data["attempts"][0])
    duplicate_attempt["attempt_id"] = "newer-attempt"
    store.data["attempts"].append(duplicate_attempt)
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["attempts"].pop()
    duplicate_attempt["fence"] = item.fence + 1
    store.data["attempts"].append(duplicate_attempt)
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["attempts"].pop()

    from people_counter.sjd_process import verify_envelope

    raw, digest = store.load_claim_envelope_with_digest(claim.batch_id)
    envelope_path = store.data["batches"][0]["envelope_path"]
    original_envelope = files.content[envelope_path]
    changed_envelope = copy.deepcopy(raw)
    changed_envelope["owner"] = "wrong-envelope-owner"
    changed_encoded = json.dumps(
        changed_envelope,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    changed_digest = _sha256_bytes(changed_encoded.encode())
    files.content[envelope_path] = changed_encoded
    store.data["batches"][0]["envelope_sha256"] = changed_digest
    with pytest.raises(Exception, match="immutable envelope differs"):
        store.recover_exact(
            **{**arguments, "envelope_sha256": changed_digest},
            now=claim.lease_expires_at + 2,
        )
    files.content[envelope_path] = original_envelope
    store.data["batches"][0]["envelope_sha256"] = digest
    verified = verify_envelope(
        raw, batch_id=claim.batch_id, envelope_sha256=digest
    )
    staged = (
        f"{store.config.file_path('attempts')}/process/"
        f"batch={claim.batch_id}/attempt={verified.execution_attempt_id}/_SUCCESS"
    )
    files.content[staged] = "{}"
    with pytest.raises(Exception, match="staged or committed"):
        store.recover_exact(
            **arguments, now=claim.lease_expires_at + 2
        )
    del files.content[staged]
    delta_log = staged.removesuffix("_SUCCESS") + "_delta_log"
    files.content[delta_log] = "{}"
    with pytest.raises(Exception, match="staged or committed"):
        store.recover_exact(
            **arguments, now=claim.lease_expires_at + 2
        )
    del files.content[delta_log]

    result = store.recover_exact(
        **{**arguments, "safe_skew_seconds": 0.0},
        now=claim.lease_expires_at + 2,
    )
    assert result["outcome"] == "DEAD"
    assert result["idempotent"] is False
    assert store.data["work"][0]["status"] == "DEAD"
    assert store.data["batches"][0]["status"] == "EXPIRED"
    assert store.data["attempts"][0]["status"] == "EXPIRED"
    assert store.data["attempts"][0]["recovery_outcome"] == "DEAD"
    terminal_mismatches = (
        ("work", "status", "READY"),
        ("work", "lease_owner", "owner-exact"),
        ("work", "lease_attempt_id", item.attempt_id),
        ("work", "lease_expires_at", claim.lease_expires_at),
        ("work", "committed_attempt_id", "committed-attempt"),
        ("batches", "owner", "wrong-owner"),
        ("work", "fence", item.fence + 1),
        ("attempts", "fence", item.fence + 1),
        ("batch_members", "fence", item.fence + 1),
        ("batch_members", "payload_sha256", "p" * 64),
        ("batches", "lease_expires_at", claim.lease_expires_at + 1),
        ("attempts", "lease_expires_at", claim.lease_expires_at + 1),
        ("batches", "envelope_sha256", "e" * 64),
        ("batches", "membership_sha256", "m" * 64),
        ("batches", "status", "LEASED"),
        ("attempts", "status", "LEASED"),
        ("attempts", "recovery_outcome", None),
    )
    for table, field, value in terminal_mismatches:
        row = store.data[table][0]
        original = row[field]
        row[field] = value
        with pytest.raises(Exception):
            store.recover_exact(
                **arguments, now=claim.lease_expires_at + 3
            )
        row[field] = original
    store.data["publications"].append({"work_id": item.work_id})
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 3)
    store.data["publications"].clear()
    duplicate_attempt = copy.deepcopy(store.data["attempts"][0])
    duplicate_attempt["attempt_id"] = "terminal-extra-attempt"
    store.data["attempts"].append(duplicate_attempt)
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 3)
    store.data["attempts"].pop()
    assert store.recover_exact(
        **arguments, now=claim.lease_expires_at + 3
    )["idempotent"] is True


def test_exact_recovery_accepts_expiry_boundary_and_default_clock() -> None:
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work-boundary",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=1,
    )
    claim = store.claim(
        "owner-boundary",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    item = claim.items[0]
    store._clock_value = claim.lease_expires_at + 1.0

    result = store.recover_exact(
        work_id=item.work_id,
        batch_id=claim.batch_id,
        attempt_id=item.attempt_id,
        owner="owner-boundary",
        fence=item.fence,
        lease_expires_at=claim.lease_expires_at,
        envelope_sha256=claim.envelope_sha256,
        membership_sha256=store.data["batches"][0]["membership_sha256"],
        safe_skew_seconds=1.0,
    )

    assert result["outcome"] == "DEAD"
    assert result["idempotent"] is False


@pytest.mark.parametrize(
    ("table", "field", "value"),
    (
        ("work", "status", "READY"),
        ("work", "lease_owner", "still-owned"),
        ("batches", "status", "LEASED"),
        ("attempts", "status", "LEASED"),
        ("attempts", "recovery_outcome", None),
    ),
)
def test_exact_recovery_rejects_non_atomic_readback(
    table: str, field: str, value: object
) -> None:
    class CorruptingStore(MemoryControlStore):
        corrupt = False

        def _replace_many(self, **tables: list[dict[str, object]]) -> None:
            super()._replace_many(**tables)
            if self.corrupt and {"work", "batches", "attempts"} <= set(tables):
                self.data[table][0][field] = value

    store = CorruptingStore(MemoryFiles())
    store.register(
        "work-readback",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=1,
    )
    claim = store.claim(
        "owner-readback",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    item = claim.items[0]
    store.corrupt = True
    with pytest.raises(Exception, match="readback differs"):
        store.recover_exact(
            work_id=item.work_id,
            batch_id=claim.batch_id,
            attempt_id=item.attempt_id,
            owner="owner-readback",
            fence=item.fence,
            lease_expires_at=claim.lease_expires_at,
            envelope_sha256=claim.envelope_sha256,
            membership_sha256=store.data["batches"][0]["membership_sha256"],
            now=claim.lease_expires_at + 1,
            safe_skew_seconds=0,
        )


def test_clear_stale_lock_uses_exact_owner_cas_and_exact_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Expression:
        def __eq__(self, _other: object) -> "Expression":
            return self

        def __and__(self, _other: object) -> "Expression":
            return self

        def cast(self, _name: str) -> "Expression":
            return self

    functions = SimpleNamespace(
        col=lambda _name: Expression(), lit=lambda _value: Expression()
    )
    state = {
        "rows": [
            {
                "lock_name": "global",
                "owner_id": "owner-1",
                "acquired_at": datetime(2026, 10, 4),
            }
        ]
    }
    frame = MagicMock()
    frame.select.return_value.limit.return_value.collect.side_effect = (
        lambda: copy.deepcopy(state["rows"])
    )
    spark = MagicMock()
    spark.table.return_value = frame

    class Delta:
        def update(self, **_kwargs: object) -> None:
            state["rows"][0]["owner_id"] = None
            state["rows"][0]["acquired_at"] = None

    delta_table = SimpleNamespace(forName=lambda *_args: Delta())
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", SimpleNamespace(functions=functions)
    )
    monkeypatch.setitem(
        sys.modules, "delta.tables", SimpleNamespace(DeltaTable=delta_table)
    )

    result = _clear_stale_lock(spark, "owner-1")

    assert result["cleared_owner_id"] == "owner-1"
    with pytest.raises(RuntimeError, match="precondition"):
        _clear_stale_lock(spark, "other-owner")

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from people_counter.local_spark import (
    OptionalLocalSparkDependencyError,
    create_local_spark_session,
)
from people_counter.sjd_control import SQLiteControlStore
from people_counter.sjd_gold import LocalGoldJob
from people_counter.sjd_orchestrate import (
    LocalPipelineConfig,
    LocalPipelinePaths,
    main,
    run_local_pipeline,
)


def test_full_direct_probe_pipeline_publishes_pointer_and_gold(capsys):
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        paths = LocalPipelinePaths.resolve(Path(temporary))
        with patch(
            "people_counter.local_spark.create_local_spark_session",
            side_effect=AssertionError("direct harness must not initialize Spark"),
        ):
            result = run_local_pipeline(
                LocalPipelineConfig(paths, "probe", max_items=2),
                fixture_count=2,
            )

        assert result["status"]["work"] == {"SUCCEEDED": 2}
        assert len(result["process"]["publication_sequences"]) == 2
        assert len(result["executor_identities"]) == 2
        assert result["reconciliation_findings"] == []
        assert result["gold"]["validation"]["valid"] is True
        assert result["refresh"]["pending_before_ack"]
        assert result["refresh"]["acknowledged"] == result["refresh"][
            "pending_before_ack"
        ]

        control = SQLiteControlStore(paths.control_database, paths.content_root)
        assert control.status()["last_publication_sequence"] == 2
        assert LocalGoldJob(paths.control_database, paths.gold_root).state.pending_refreshes() == []

        assert main(
            [
                "pipeline",
                "--root",
                str(paths.root),
                "--fixture-count",
                "2",
                "--mode",
                "probe",
                "--harness",
                "direct",
            ]
        ) == 0
        rerun = json.loads(capsys.readouterr().out)
        assert rerun["status"]["work"]["SUCCEEDED"] == 2
        assert rerun["process"]["resumed"] is True
        assert rerun["process"]["publication_sequences"] == [1, 2]
        assert rerun["gold"]["facts"]["skipped"] is True
        assert rerun["gold"]["dimensions"]["skipped"] is True
        assert rerun["refresh"]["pending_before_ack"] == []


def test_manifest_jsonl_registration_is_idempotent():
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        root = Path(temporary)
        manifest = root / "work.jsonl"
        rows = [
            {
                "work_id": f"work-{index}",
                "payload": {
                    "source_video": f"/probe/{index}.mp4",
                    "pipeline": "rtdetr-osnet",
                    "batch_size": 1,
                    "captured_at_utc": "2026-01-01T00:00:00Z",
                },
                "runtime_key": "probe",
                "duration_seconds": 1,
                "config_sha256": "config",
                "release_digest": "release",
            }
            for index in range(2)
        ]
        manifest.write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in rows),
            encoding="utf-8",
        )
        store = SQLiteControlStore(root / "control.sqlite3", root / "content")

        first = store.register_manifest(manifest)
        second = store.register_manifest(manifest)

        assert [work.work_id for work in first] == ["work-0", "work-1"]
        assert first == second


def test_orchestrator_drains_more_work_than_one_claim():
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        paths = LocalPipelinePaths.resolve(Path(temporary))

        result = run_local_pipeline(
            LocalPipelineConfig(paths, "probe", max_items=2),
            fixture_count=3,
        )

        assert result["status"]["work"] == {"SUCCEEDED": 3}
        assert len(result["processes"]) == 2
        assert [
            len(process["publication_sequences"]) for process in result["processes"]
        ] == [2, 1]


def test_orchestrator_never_claims_unrelated_ready_work():
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        paths = LocalPipelinePaths.resolve(Path(temporary))
        store = SQLiteControlStore(paths.control_database, paths.content_root)
        unrelated = store.register(
            "unrelated",
            {"source_video": "/probe/unrelated.mp4"},
            runtime_key="probe:rtdetr-osnet:cpu",
            duration_seconds=1,
            config_sha256="unrelated-config",
            release_digest="unrelated-release",
        )

        result = run_local_pipeline(
            LocalPipelineConfig(paths, "probe", max_items=2),
            fixture_count=2,
        )

        assert result["registered_work_ids"] == ["probe-0000", "probe-0001"]
        assert store.get_work(unrelated.work_id).status == "READY"
        assert result["status"]["work"] == {"READY": 1, "SUCCEEDED": 2}


def test_missing_optional_spark_dependencies_are_actionable():
    real_import = __import__

    def without_pyspark(name, *args, **kwargs):
        if name == "pyspark.sql":
            raise ModuleNotFoundError("No module named 'pyspark'", name="pyspark")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=without_pyspark):
        with pytest.raises(
            OptionalLocalSparkDependencyError,
            match=r"people-counter\[local-spark\].*direct harness/JSON backend",
        ):
            create_local_spark_session(
                master="local[1]",
                app_name="missing-dependency",
                correlation_id="missing-dependency",
            )


def test_orchestrator_validation_boundaries_and_manifest_default():
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        paths = LocalPipelinePaths.resolve(Path(temporary))
        with pytest.raises(ValueError):
            run_local_pipeline(
                LocalPipelineConfig(paths, "probe", max_items=0),
                fixture_count=1,
            )
        with pytest.raises(ValueError):
            run_local_pipeline(
                LocalPipelineConfig(paths, "sdk"),
                fixture_count=1,
            )

        boundary = run_local_pipeline(
            LocalPipelineConfig(paths, "probe", max_items=100),
            fixture_count=1,
        )
        assert boundary["registered_work_ids"] == ["probe-0000"]

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        root = Path(temporary)
        manifest = root / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "work_id": "manifest-default",
                    "payload": {
                        "source_video": "/probe/default.mp4",
                        "captured_at_utc": "2026-01-01T00:00:00Z",
                        "camera_id": "camera-default",
                        "location_id": "location-default",
                        "camera_timezone": "UTC",
                        "asset_id": "manifest-default",
                        "asset_version": "v1",
                    },
                    "runtime_key": "probe:rtdetr-osnet:cpu",
                    "duration_seconds": 1,
                    "config_sha256": "config",
                    "release_digest": "release",
                }
            ),
            encoding="utf-8",
        )
        result = run_local_pipeline(
            LocalPipelineConfig(LocalPipelinePaths.resolve(root), "probe"),
            manifest=manifest,
        )
        assert result["registered_work_ids"] == ["manifest-default"]

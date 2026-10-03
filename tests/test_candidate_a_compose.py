import hashlib
import json
import os
import shutil
import socket
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest


pytestmark = [
    pytest.mark.compose,
    pytest.mark.skipif(
        os.environ.get("RUN_CANDIDATE_A_COMPOSE_E2E") != "1",
        reason=(
            "set RUN_CANDIDATE_A_COMPOSE_E2E=1 to run the Candidate A "
            "Docker Compose E2E"
        ),
    ),
]


class CandidateACompose:
    def __init__(self) -> None:
        identity = uuid4().hex
        self.project = f"pc-candidate-a-{identity[:12]}"
        self.release_digest = f"candidate-a-e2e-{identity}"
        self.image = f"people-counter-spark:{self.release_digest}"
        self.output_root = (
            Path("local-data/output") / self.project
        ).resolve()
        self.container_root = f"/data/output/{self.project}"
        self._started = False
        self._application_ids: set[str] = set()
        self.environment = {
            **os.environ,
            "COMPOSE_PROJECT_NAME": self.project,
            "PEOPLE_COUNTER_IMAGE": self.image,
            "PEOPLE_COUNTER_RELEASE_DIGEST": self.release_digest,
        }

    def start(self) -> None:
        self.output_root.mkdir(parents=True)
        reservations = []
        try:
            for variable in (
                "SPARK_MASTER_UI_PORT",
                "SPARK_WORKER_1_UI_PORT",
                "SPARK_WORKER_2_UI_PORT",
                "SPARK_DRIVER_UI_PORT",
            ):
                reservation = socket.socket()
                reservation.bind(("127.0.0.1", 0))
                reservations.append(reservation)
                self.environment[variable] = str(
                    reservation.getsockname()[1]
                )
            self.compose("build", "spark-client", timeout=1200)
            self._assert_image_revision()
            self._chmod_output()
        finally:
            for reservation in reservations:
                reservation.close()

        self.compose(
            "up",
            "-d",
            "--wait",
            "spark-master",
            "spark-worker-1",
            "spark-worker-2",
            "spark-client",
            timeout=300,
        )
        self._started = True

    def close(self) -> None:
        if self._started:
            try:
                self._application_ids.update(
                    self.master_application_ids()
                )
            except (OSError, subprocess.SubprocessError, ValueError):
                pass
        self.compose(
            "down",
            "--volumes",
            "--remove-orphans",
            check=False,
            timeout=180,
        )
        if self._image_exists():
            self._remove_event_logs()
            self._chmod_output(check=False)
        if self.output_root.exists():
            shutil.rmtree(self.output_root)
        subprocess.run(
            ["docker", "image", "rm", self.image],
            check=False,
            env=self.environment,
            timeout=180,
        )

    def compose(
        self,
        *arguments: str,
        check: bool = True,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "compose", "-p", self.project, *arguments],
            check=check,
            env=self.environment,
            text=True,
            timeout=timeout,
        )

    def exec_json(self, *arguments: str) -> object:
        completed = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "exec",
                "-T",
                "spark-client",
                *arguments,
            ],
            check=True,
            capture_output=True,
            env=self.environment,
            text=True,
            timeout=900,
        )
        lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if not lines:
            raise AssertionError(
                f"command produced no JSON output: {arguments!r}"
            )
        return json.loads(lines[-1])

    def pipeline(
        self, *, manifest: str | None = None
    ) -> dict[str, object]:
        source = (
            ["--manifest", manifest]
            if manifest is not None
            else ["--fixture-count", "2"]
        )
        result = self.exec_json(
            "pc-local-orchestrate",
            "pipeline",
            "--root",
            self.container_root,
            *source,
            "--mode",
            "probe",
            "--profile",
            "local-two-workers",
            "--harness",
            "spark",
            "--spark-master",
            "spark://spark-master:7077",
        )
        assert isinstance(result, dict)
        self._application_ids.update(self.master_application_ids())
        return result

    def control_json(self, *arguments: str) -> object:
        return self.exec_json(
            "pc-control-sjd",
            "--database",
            f"{self.container_root}/control/control.sqlite3",
            "--content-root",
            f"{self.container_root}/content",
            *arguments,
        )

    def expire_batch(self, batch_id: str, work_id: str) -> None:
        script = """
import sqlite3
import sys
import time

database, batch_id, work_id = sys.argv[1:]
expired = time.time() - 1
with sqlite3.connect(database) as connection:
    attempt_id = connection.execute(
        "SELECT lease_attempt_id FROM work WHERE work_id = ?",
        (work_id,),
    ).fetchone()[0]
    connection.execute(
        "UPDATE work SET lease_expires_at = ? WHERE work_id = ?",
        (expired, work_id),
    )
    connection.execute(
        "UPDATE attempts SET lease_expires_at = ? WHERE attempt_id = ?",
        (expired, attempt_id),
    )
    connection.execute(
        "UPDATE batches SET lease_expires_at = ? WHERE batch_id = ?",
        (expired, batch_id),
    )
"""
        self.exec_json(
            "python",
            "-c",
            script + "\nprint('null')",
            f"{self.container_root}/control/control.sqlite3",
            batch_id,
            work_id,
        )

    def control_snapshot(self) -> dict[str, object]:
        script = """
import json
import sqlite3
import sys

with sqlite3.connect(sys.argv[1]) as connection:
    result = {
        "release_digests": [
            row[0] for row in connection.execute(
                "SELECT DISTINCT release_digest FROM work ORDER BY release_digest"
            )
        ],
        "work": {
            row[0]: row[1] for row in connection.execute(
                "SELECT work_id, status FROM work ORDER BY work_id"
            )
        },
        "committed": connection.execute(
            "SELECT COUNT(*) FROM work WHERE committed_attempt_id IS NOT NULL"
        ).fetchone()[0],
        "publications": connection.execute(
            "SELECT COUNT(*) FROM publications"
        ).fetchone()[0],
        "expired_attempts": connection.execute(
            "SELECT COUNT(*) FROM attempts "
            "WHERE status = 'EXPIRED' AND recovery_outcome = 'READY'"
        ).fetchone()[0],
    }
print(json.dumps(result, sort_keys=True))
"""
        result = self.exec_json(
            "python",
            "-c",
            script,
            f"{self.container_root}/control/control.sqlite3",
        )
        assert isinstance(result, dict)
        return result

    def master_application_ids(self) -> set[str]:
        script = """
import json
import urllib.request

with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
    payload = json.load(response)
applications = payload.get("activeapps", []) + payload.get("completedapps", [])
print(json.dumps(sorted({item["id"] for item in applications})))
"""
        result = self.exec_json("python", "-c", script)
        assert isinstance(result, list)
        return {str(item) for item in result}

    def _assert_image_revision(self) -> None:
        revision = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                self.image,
                "--format",
                '{{index .Config.Labels "org.opencontainers.image.revision"}}',
            ],
            check=True,
            capture_output=True,
            env=self.environment,
            text=True,
            timeout=30,
        ).stdout.strip()
        assert revision == self.release_digest
        image_digest = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                self.image,
                "--format",
                "{{.Id}}",
            ],
            check=True,
            capture_output=True,
            env=self.environment,
            text=True,
            timeout=30,
        ).stdout.strip()
        self.environment["PEOPLE_COUNTER_IMAGE_DIGEST"] = image_digest

    def _chmod_output(self, *, check: bool = True) -> None:
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0",
                "--entrypoint",
                "chmod",
                "--volume",
                f"{self.output_root}:/data/output",
                self.image,
                "-R",
                "0777",
                "/data/output",
            ],
            check=check,
            env=self.environment,
            timeout=120,
        )

    def _image_exists(self) -> bool:
        return (
            subprocess.run(
                ["docker", "image", "inspect", self.image],
                check=False,
                capture_output=True,
                env=self.environment,
                timeout=30,
            ).returncode
            == 0
        )

    def _remove_event_logs(self) -> None:
        event_root = Path("local-data/output/spark-events").resolve()
        if not event_root.is_dir():
            return
        owned = [
            item.name
            for item in event_root.iterdir()
            if any(app_id in item.name for app_id in self._application_ids)
        ]
        if not owned:
            return
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0",
                "--entrypoint",
                "rm",
                "--volume",
                f"{event_root}:/events",
                self.image,
                "-rf",
                *(f"/events/{name}" for name in owned),
            ],
            check=False,
            env=self.environment,
            timeout=120,
        )


@pytest.fixture(scope="module")
def candidate_a_compose() -> CandidateACompose:
    cluster = CandidateACompose()
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.close()


def test_candidate_a_compose_current_source_e2e(
    candidate_a_compose: CandidateACompose,
) -> None:
    cluster = candidate_a_compose

    first = cluster.pipeline()
    first_process = first["process"]
    assert isinstance(first_process, dict)
    assert first_process["resumed"] is False
    assert first_process["publication_sequences"] == [1, 2]
    hosts = {
        str(identity).rsplit("@", 1)[1]
        for identity in first["executor_identities"]
    }
    assert hosts == {"spark-worker-1", "spark-worker-2"}
    assert first["gold"]["validation"]["valid"] is True
    assert first["gold"]["validation"]["table_rows"]["gold_video"] == 2
    assert first["status"]["work"] == {"SUCCEEDED": 2}
    assert list(cluster.output_root.glob("attempts/batch=*/attempt=*/_delta_log"))
    assert (
        cluster.output_root / "gold" / "gold_video" / "_delta_log"
    ).is_dir()
    assert (
        cluster.output_root / "gold" / "gold_operations_hour" / "_delta_log"
    ).is_dir()
    assert not (cluster.output_root / "gold" / "gold_video.json").exists()

    idempotent = cluster.pipeline()
    idempotent_process = idempotent["process"]
    assert isinstance(idempotent_process, dict)
    assert idempotent_process["resumed"] is True
    assert idempotent_process["process_attempt_id"] == first_process[
        "process_attempt_id"
    ]
    assert idempotent_process["publication_sequences"] == [1, 2]
    assert idempotent["gold"]["facts"]["skipped"] is True
    assert idempotent["gold"]["dimensions"]["skipped"] is True
    assert idempotent["refresh"]["pending_before_ack"] == []

    recovery_work_id = f"recover-{uuid4().hex}"
    manifest_path = cluster.output_root / "recovery-work.jsonl"
    manifest_path.write_text(
        json.dumps(
            {
                "work_id": recovery_work_id,
                "payload": {
                    "source_video": "/probe/recovery.mp4",
                    "pipeline": "rtdetr-osnet",
                    "batch_size": 1,
                    "probe_delay_seconds": 0.01,
                    "captured_at_utc": "2026-01-01T00:00:00Z",
                    "camera_id": "recovery-camera",
                    "location_id": "local-probe",
                    "camera_timezone": "UTC",
                    "asset_id": recovery_work_id,
                    "asset_version": "v1",
                },
                "runtime_key": "probe:rtdetr-osnet:cpu",
                "duration_seconds": 1.0,
                "config_sha256": hashlib.sha256(
                    b"probe-config-v1"
                ).hexdigest(),
                "release_digest": cluster.release_digest,
                "max_attempts": 3,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    container_manifest = f"{cluster.container_root}/{manifest_path.name}"
    registered = cluster.control_json(
        "register", "--manifest", container_manifest
    )
    assert [item["work_id"] for item in registered] == [recovery_work_id]
    claim = cluster.control_json(
        "claim",
        "--owner",
        "candidate-a-e2e-expiring-owner",
        "--max-items",
        "1",
        "--minimum-items",
        "1",
        "--lease-seconds",
        "120",
        "--minimum-speed-x",
        "1",
        "--safety-factor",
        "1.25",
        "--margin-seconds",
        "30",
    )
    assert claim["items"][0]["work_id"] == recovery_work_id

    pointer_check = cluster.pipeline()
    pointer_process = pointer_check["process"]
    assert isinstance(pointer_process, dict)
    assert pointer_process["resumed"] is True
    assert pointer_process["process_attempt_id"] == first_process[
        "process_attempt_id"
    ]
    assert pointer_process["publication_sequences"] == [1, 2]
    assert pointer_check["gold"]["validation"]["table_rows"]["gold_video"] == 2
    pointer_snapshot = cluster.control_snapshot()
    assert pointer_snapshot["work"][recovery_work_id] == "LEASED"
    assert pointer_snapshot["committed"] == 2
    assert pointer_snapshot["publications"] == 2

    cluster.expire_batch(str(claim["batch_id"]), recovery_work_id)
    recovery = cluster.control_json("recover")
    assert recovery == {"dead": 0, "recovered": 1, "retried": 1}

    recovered = cluster.pipeline(manifest=container_manifest)
    assert recovered["status"]["work"] == {"SUCCEEDED": 3}
    assert recovered["gold"]["validation"]["valid"] is True
    assert recovered["gold"]["validation"]["table_rows"]["gold_video"] == 3
    final_snapshot = cluster.control_snapshot()
    assert final_snapshot["release_digests"] == [cluster.release_digest]
    assert final_snapshot["work"][recovery_work_id] == "SUCCEEDED"
    assert final_snapshot["committed"] == 3
    assert final_snapshot["publications"] == 3
    assert final_snapshot["expired_attempts"] == 1

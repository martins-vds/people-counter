import json
import os
import subprocess
import unittest
from pathlib import Path
from uuid import uuid4

import pytest


pytestmark = pytest.mark.compose


@unittest.skipUnless(
    os.environ.get("RUN_COMPOSE_TESTS") == "1",
    "set RUN_COMPOSE_TESTS=1 to run the Docker Compose smoke",
)
class LocalComposeSmokeTests(unittest.TestCase):
    project = "people-counter-integration"

    @classmethod
    def setUpClass(cls):
        output_root = Path("local-data/output")
        output_root.mkdir(parents=True, exist_ok=True)
        cls.release_digest = f"compose-test-{uuid4()}"
        cls.image = f"people-counter-spark:{cls.release_digest}"
        cls.environment = {
            **os.environ,
            "COMPOSE_PROJECT_NAME": cls.project,
            "PEOPLE_COUNTER_IMAGE": cls.image,
            "PEOPLE_COUNTER_RELEASE_DIGEST": cls.release_digest,
        }
        subprocess.run(
            ["docker", "compose", "-p", cls.project, "build"],
            check=True,
            env=cls.environment,
        )
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
                f"{output_root.resolve()}:/data/output",
                cls.image,
                "0777",
                "/data/output",
            ],
            check=True,
        )
        image_digest = subprocess.run(
            ["docker", "image", "inspect", cls.image, "--format", "{{.Id}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        revision = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                cls.image,
                "--format",
                "{{index .Config.Labels \"org.opencontainers.image.revision\"}}",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if revision != cls.release_digest:
            raise AssertionError(
                f"built image revision {revision!r} does not match "
                f"{cls.release_digest!r}"
            )
        cls.environment["PEOPLE_COUNTER_IMAGE_DIGEST"] = image_digest

    @classmethod
    def tearDownClass(cls):
        subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                cls.project,
                "down",
                "--volumes",
                "--remove-orphans",
            ],
            check=False,
            env=getattr(cls, "environment", None),
        )

    def test_current_release_uses_exact_workers_before_and_after_idle_restart(self):
        subprocess.run(
            ["docker", "compose", "-p", self.project, "up", "-d", "--wait"],
            check=True,
            env=self.environment,
        )
        self._seed("first")
        first = self._submit()
        self._assert_current_two_worker_release(first)

        subprocess.run(
            ["docker", "compose", "-p", self.project, "restart", "spark-worker-2"],
            check=True,
            env=self.environment,
        )
        subprocess.run(
            ["docker", "compose", "-p", self.project, "up", "-d", "--wait"],
            check=True,
            env=self.environment,
        )
        self._seed("after-restart")
        second = self._submit()
        self._assert_current_two_worker_release(second)

    def _assert_current_two_worker_release(self, result):
        hosts = {
            identity.rsplit("@", 1)[1]
            for identity in result["executor_identities"]
        }
        self.assertEqual(hosts, {"spark-worker-1", "spark-worker-2"})
        self.assertEqual(result["release_digests"], [self.release_digest])

    def _seed(self, prefix):
        subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "exec",
                "-T",
                "spark-client",
                "people-counter-local",
                "seed",
                "--video",
                "/data/samples/three_people_walking.mp4",
                "--idempotency-prefix",
                prefix,
                "--copies",
                "2",
            ],
            check=True,
            env=self.environment,
        )

    def _submit(self):
        completed = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "exec",
                "-T",
                "spark-client",
                "people-counter-local",
                "submit",
                "--max-items",
                "2",
                "--minimum-workers",
                "2",
            ],
            check=True,
            capture_output=True,
            text=True,
            env=self.environment,
        )
        return json.loads(completed.stdout.splitlines()[-1])

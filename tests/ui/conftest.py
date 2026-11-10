import hashlib
import shlex
import subprocess
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

pytest.importorskip("flask")

from werkzeug.serving import make_server

from people_counter import backfill_operator as backfill
from people_counter import portal_service as portal_module
from people_counter.inference_profiles import artifact_paths
from people_counter.operator_portal import create_app
from people_counter.portal_service import PortalService
from people_counter.sjd_control import parse_registration_manifest
from tests.test_manifest_publisher import FakeStorage


@pytest.fixture
def portal(tmp_path, monkeypatch, request):
    source = tmp_path / "videos"
    (source / "lobby").mkdir(parents=True)

    def make_video(name, width=320, height=180):
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            "ffmpeg", "-v", "error", "-f", "lavfi", "-i", f"testsrc=size={width}x{height}:rate=5",
            "-t", "1", "-c:v", "mpeg4", "-pix_fmt", "yuv420p",
            "-metadata", "creation_time=2026-01-01T12:00:00Z",
            str(source / name),
        ], check=True, timeout=30)

    make_video("lobby/clip.mp4")
    models = tmp_path / "models"
    for relative in artifact_paths("rtdetr-r18-onnx-cpu"):
        path = models / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(relative.encode())
    cloud = request.node.get_closest_marker("portal_cloud") is not None
    service = PortalService(source, models, tmp_path / "state", allow_cloud=cloud)
    calls = []

    def forbidden(*args, **kwargs):
        raise AssertionError("UI tests must not construct live cloud clients")

    monkeypatch.setattr(backfill.OneLakeFiles, "__init__", forbidden)
    monkeypatch.setattr(portal_module.AzureDataLakeStorage, "__init__", forbidden)
    if cloud:
        storage = FakeStorage()
        partitions = {}
        registered = {}

        class Files:
            def __init__(self, config):
                assert config.workspace_id == "11111111-1111-1111-1111-111111111111"

            def read_text(self, path):
                raise AssertionError("Release evidence is provided by the test adapter")

            def read_bytes(self, path):
                return storage.read_bytes(path.removeprefix("Files/videos/"))

            def verify_video(self, path, size, digest):
                if path.startswith("Files/models/"):
                    content = (models / path.removeprefix("Files/models/")).read_bytes()
                else:
                    content = storage.read_bytes(path.removeprefix("Files/videos/"))
                assert hashlib.sha256(content).hexdigest() == digest
                assert size is None or size == len(content)

            def write_partition(self, path, content):
                partitions[path] = content

        class Fabric:
            def request_response(self, method, path, *, value, expected):
                assert method == "POST" and expected == (202,)
                arguments = shlex.split(value["executionData"]["commandLineArguments"])
                partition = arguments[arguments.index("--partition-path") + 1]
                digest = arguments[arguments.index("--partition-sha256") + 1]
                assert hashlib.sha256(partitions[partition]).hexdigest() == digest
                records = parse_registration_manifest(partitions[partition].decode())
                registered.update((record["work_id"], record) for record in records)
                calls.append(value)
                return 202, {"Location": "/jobs/fake-control-job"}, b""

            def request_json(self, method, location):
                assert method == "GET"
                return {"id": "fake-control-job", "status": "Completed"}

        @contextmanager
        def reader(config):
            yield None

        def rows(connection, prefix, suffix, columns, filters):
            return [
                {"work_id": identity, "status": "SUCCEEDED", "committed_attempt_id": "test-attempt", "last_error": None}
                for identity in filters[0][2] if identity in registered
            ]

        monkeypatch.setattr(backfill, "OneLakeFiles", Files)
        monkeypatch.setattr(backfill, "AzureDataLakeStorage", lambda *args, **kwargs: storage)
        monkeypatch.setattr(portal_module, "AzureDataLakeStorage", lambda *args, **kwargs: storage)
        monkeypatch.setattr(backfill, "FabricAutomationClient", lambda *args, **kwargs: Fabric())
        monkeypatch.setattr(backfill, "processing_reader", reader)
        monkeypatch.setattr(backfill, "read_demo_delta", rows)
        monkeypatch.setattr(backfill, "resolve_published_release", lambda read: SimpleNamespace(
            manifest_sha256="d" * 64,
            manifest=SimpleNamespace(environment_id="33333333-3333-3333-3333-333333333333"),
        ))
    app = create_app(service)
    server = make_server("127.0.0.1", 0, app, threaded=True)
    stop = threading.Event()
    worker = threading.Thread(target=service.work, args=(stop,), daemon=True)
    web = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    web.start()
    try:
        yield SimpleNamespace(
            url=f"http://127.0.0.1:{server.server_port}", service=service,
            calls=calls, source=source,
            make_video=make_video,
        )
    finally:
        server.shutdown()
        stop.set()
        worker.join(timeout=10)
        web.join(timeout=10)
        assert not worker.is_alive(), "Portal test worker did not stop"

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest

from scripts.deploy_sjd_operations import (
    FabricApi,
    ITEM_NAMES,
    OperationsDeploymentError,
    deploy_operations,
    schedule_payload,
    validate_definition_readback,
)


def test_fabric_api_uses_the_supplied_bearer_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Response:
        status = 200
        headers: dict[str, str] = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read() -> bytes:
            return b"{}"

    def open_request(request, *, timeout):
        observed["authorization"] = request.get_header("Authorization")
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr(
        "scripts.deploy_sjd_operations.urllib.request.urlopen",
        open_request,
    )
    api = FabricApi("workspace-1", token="token-1")

    status, _, body = api.request("GET", "/workspaces/workspace-1/items")

    assert status == 200
    assert body == {}
    assert observed == {
        "authorization": "Bearer token-1",
        "timeout": 120,
    }


class FakeApi:
    workspace_id = "workspace-1"

    def __init__(self) -> None:
        self.upserts: list[tuple[str, str]] = []
        self.schedules: list[tuple[str, dict[str, object]]] = []

    def upsert_sjd(self, kind: str, display_name: str) -> str:
        self.upserts.append((kind, display_name))
        return f"{kind}-id"

    @staticmethod
    def get_definition(item_id: str) -> dict[str, object]:
        from people_counter.fabric_sjd_definition import build_sjd_definition

        definition = build_sjd_definition(item_id.removesuffix("-id"))
        definition["definition"]["parts"].append(
            {
                "path": ".platform",
                "payload": base64.b64encode(b"{}").decode("ascii"),
                "payloadType": "InlineBase64",
            }
        )
        return definition

    def reconcile_schedule(
        self,
        item_id: str,
        *,
        payload: dict[str, object],
    ) -> dict[str, object]:
        self.schedules.append((item_id, payload))
        return {
            "configuration": payload["configuration"],
            "enabled": payload["enabled"],
            "id": f"{item_id}-schedule",
        }


def test_schedule_payload_is_bounded_deterministic_and_disabled_by_default() -> None:
    result = schedule_payload(
        interval_minutes=5,
        command_line_arguments="recover",
        enabled=False,
        now=datetime(2026, 10, 9, 20, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "configuration": {
            "endDateTime": "2036-10-06T20:05:00Z",
            "interval": 5,
            "localTimeZoneId": "UTC",
            "startDateTime": "2026-10-09T20:05:00Z",
            "type": "Cron",
        },
        "enabled": False,
        "executionData": {"commandLineArguments": "recover"},
    }
    with pytest.raises(ValueError, match="between 1 and 10080"):
        schedule_payload(
            interval_minutes=0,
            command_line_arguments="recover",
            enabled=False,
        )


def test_deploy_operations_reconciles_all_stable_jobs_and_schedules() -> None:
    api = FakeApi()
    result = deploy_operations(
        api,
        release_manifest_sha256="a" * 64,
        release_receipt_sha256="b" * 64,
        semantic_model_id="model-1",
        additional_semantic_model_ids=("model-2",),
        enable_schedules=False,
        now=datetime(2026, 10, 9, 20, 0, tzinfo=timezone.utc),
    )

    assert api.upserts == list(ITEM_NAMES.items())
    scheduled_ids = [item_id for item_id, _ in api.schedules]
    assert scheduled_ids == [
        "dispatcher-id",
        "6df10e00-517c-409a-8dd6-40ae9bc62003",
        "c94fc6fb-7397-4d11-9bd8-c4d3e778b501",
        "621391c9-ba0c-4965-8f1e-52fbb46b59eb",
        "refresh-id",
    ]
    assert all(
        payload["enabled"] is False for _, payload in api.schedules
    )
    dispatcher = api.schedules[0][1]["executionData"][
        "commandLineArguments"
    ]
    assert "--maximum-active-batches 1" in dispatcher
    assert "--release-manifest-sha256 " + "a" * 64 in dispatcher
    refresh = api.schedules[-1][1]["executionData"][
        "commandLineArguments"
    ]
    assert refresh == (
        "--workspace-id workspace-1 --semantic-model-id model-1 "
        "--semantic-model-id model-2"
    )
    assert result["enabled"] is False


def test_deploy_operations_fails_closed_on_definition_drift() -> None:
    api = FakeApi()
    api.get_definition = lambda _item_id: {"definition": {"format": "wrong"}}

    with pytest.raises(
        OperationsDeploymentError,
        match="parts readback is invalid",
    ):
        deploy_operations(
            api,
            release_manifest_sha256="a" * 64,
            release_receipt_sha256="b" * 64,
            semantic_model_id="model-1",
            enable_schedules=False,
        )


def test_definition_readback_accepts_fabric_json_formatting_only() -> None:
    from people_counter.fabric_sjd_definition import build_sjd_definition

    observed = build_sjd_definition("dispatcher")
    parts = observed["definition"]["parts"]
    metadata = next(
        part for part in parts if part["path"] == "SparkJobDefinitionV1.json"
    )
    value = json.loads(base64.b64decode(metadata["payload"]))
    metadata["payload"] = base64.b64encode(
        json.dumps(value, indent=2).encode("utf-8")
    ).decode("ascii")
    parts.append(
        {
            "path": ".platform",
            "payload": base64.b64encode(b"{}").decode("ascii"),
            "payloadType": "InlineBase64",
        }
    )
    parts.append(
        {
            "path": ".schedules",
            "payload": base64.b64encode(b"{}").decode("ascii"),
            "payloadType": "InlineBase64",
        }
    )

    validate_definition_readback("dispatcher", observed)

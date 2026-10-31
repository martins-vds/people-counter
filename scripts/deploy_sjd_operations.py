"""Deploy and schedule the stable production SJD control loop."""

from __future__ import annotations

import argparse
import ast
import base64
import copy
import json
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from people_counter.fabric_release_provenance import RELEASE_IDENTITY
from people_counter.fabric_sjd_definition import build_sjd_definition, reviewed_compute_settings, validate_reviewed_compute_settings


API_ROOT = "https://api.fabric.microsoft.com/v1"
ANALYTICS_MODEL_ID = "9e6b8041-44a2-4f46-9ad8-85b7ca87b71e"
JOB_TYPE = "sparkjob"
ITEM_NAMES = {
    "dispatcher": "pc-sjd-dispatcher",
    "refresh": "pc-sjd-refresh",
}


class OperationsDeploymentError(RuntimeError):
    """A Fabric operation deployment or readback failed."""


def validate_workspace_pool_policy(settings: Mapping[str, Any]) -> None:
    desired = reviewed_compute_settings({})
    count = int(next(row["value"] for row in desired["sparkProperties"] if row["key"] == "spark.executor.instances"))
    pool = settings.get("pool", {})
    limits = pool.get("starterPool", {})
    if (
        pool.get("customizeComputeEnabled") is not True
        or limits.get("maxExecutors") != count
        or limits.get("maxNodeCount") != count + 1
    ):
        raise OperationsDeploymentError(
            f"Starter Pool must allow {count + 1} nodes / {count} executors; "
            "Environment properties alone do not prove the live allocation"
        )


def validate_live_allocation(
    properties: Mapping[str, str], executors: Sequence[Mapping[str, Any]],
) -> None:
    desired = reviewed_compute_settings({})
    count = int(next(row["value"] for row in desired["sparkProperties"] if row["key"] == "spark.executor.instances"))
    if (
        properties.get("spark.dynamicAllocation.enabled", "").lower() != "false"
        or int(properties.get("spark.executor.instances", "0")) != count
        or properties.get("spark.executor.cores") != str(desired["executorCores"])
    ):
        raise OperationsDeploymentError(f"Canary runtime must request {count} fixed executors with reviewed cores")
    workers = [row for row in executors if row.get("id") != "driver"]
    if len(workers) != count or any(int(row["totalCores"]) != desired["executorCores"] for row in workers):
        raise OperationsDeploymentError(f"Canary must contain exactly {count} observed executors with reviewed cores")
    if properties.get("spark.executor.memory", "").lower() != desired["executorMemory"]:
        raise OperationsDeploymentError("Canary executor memory differs from the reviewed allocation")


def bind_intake_release(
    definition: Mapping[str, Any], release_digest: str,
) -> dict[str, Any]:
    if re.fullmatch(r"[0-9a-f]{64}", release_digest) is None:
        raise OperationsDeploymentError("release digest must be a SHA-256 value")
    updated = copy.deepcopy(dict(definition))
    parts = updated.get("definition", {}).get("parts", [])
    matches = [part for part in parts if part.get("path") == "pipeline-content.json"]
    if len(matches) != 1:
        raise OperationsDeploymentError("expected exactly one pipeline-content.json part")
    part = matches[0]
    if part.get("payloadType") != "InlineBase64":
        raise OperationsDeploymentError("intake definition must use InlineBase64")
    try:
        content = json.loads(base64.b64decode(part["payload"], validate=True))
    except (ValueError, KeyError) as error:
        raise OperationsDeploymentError("intake pipeline definition is invalid") from error
    activities = [
        activity for activity in content.get("properties", {}).get("activities", [])
        if activity.get("name") == "ValidateManifest"
    ]
    if len(activities) != 1:
        raise OperationsDeploymentError("expected exactly one ValidateManifest activity")
    parameters = activities[0].get("typeProperties", {}).get("parameters", {})
    parameter = parameters.get("RELEASE_MANIFEST_SHA256")
    if not isinstance(parameter, dict) or parameter.get("type") != "string":
        raise OperationsDeploymentError("intake release parameter must be a string")
    parameter["value"] = release_digest
    part["payload"] = base64.b64encode(
        json.dumps(content, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")
    return updated


def bind_registration_notebook(definition: Mapping[str, Any]) -> dict[str, Any]:
    updated = copy.deepcopy(dict(definition))
    parts = updated.get("definition", {}).get("parts", [])
    matches = [part for part in parts if part.get("path") == "notebook-content.py"]
    if len(matches) != 1 or matches[0].get("payloadType") != "InlineBase64":
        raise OperationsDeploymentError("expected exactly one notebook-content.py part")
    part = matches[0]
    try:
        source = base64.b64decode(part["payload"], validate=True).decode("utf-8")
        tree = ast.parse(source)
    except (ValueError, KeyError, SyntaxError) as error:
        raise OperationsDeploymentError("registration notebook source is invalid") from error
    assignments = [
        node for node in tree.body if isinstance(node, ast.Assign)
        and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "release_digest"
    ]
    if len(assignments) != 1:
        raise OperationsDeploymentError("expected exactly one release_digest assignment")
    assignment = assignments[0]
    expression = (
        "registration_release_digest(NotebookUtilsOneLakeFiles().read_text, "
        "expected_digest=RELEASE_MANIFEST_SHA256)"
    )
    if ast.dump(assignment.value) == ast.dump(ast.parse(expression, mode="eval").body):
        return updated
    replacement = (
        "from people_counter.fabric_release_provenance import registration_release_digest\n"
        "from people_counter.fabric_control_store import NotebookUtilsOneLakeFiles\n"
        f"release_digest = {expression}\n"
    )
    lines = source.splitlines(keepends=True)
    lines[assignment.lineno - 1:assignment.end_lineno] = [replacement]
    source = "".join(lines)
    ast.parse(source)
    part["payload"] = base64.b64encode(source.encode("utf-8")).decode("ascii")
    return updated


def _token() -> str:
    output = subprocess.check_output(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://api.fabric.microsoft.com",
            "--output",
            "json",
        ],
        text=True,
    )
    token = str(json.loads(output).get("accessToken", ""))
    if not token:
        raise OperationsDeploymentError("Azure CLI returned an empty Fabric token")
    return token


class FabricApi:
    def __init__(
        self,
        workspace_id: str,
        *,
        token: str,
        sleep: Any = time.sleep,
    ) -> None:
        self.workspace_id = workspace_id
        self.token = token
        self.sleep = sleep

    def request(
        self,
        method: str,
        path_or_url: str,
        payload: object | None = None,
    ) -> tuple[int, Mapping[str, str], dict[str, Any]]:
        url = (
            path_or_url
            if path_or_url.startswith("https://")
            else f"{API_ROOT}{path_or_url}"
        )
        body = (
            None
            if payload is None
            else json.dumps(
                payload,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                content = response.read()
                parsed = json.loads(content) if content else {}
                if parsed is None:
                    parsed = {}
                if not isinstance(parsed, dict):
                    raise OperationsDeploymentError(
                        f"{method} {url} returned non-object JSON"
                    )
                return response.status, dict(response.headers), parsed
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise OperationsDeploymentError(
                f"{method} {url} returned {error.code}: {detail}"
            ) from error

    def verify_compute(self) -> None:
        _, _, settings = self.request(
            "GET",
            f"/workspaces/{self.workspace_id}/environments/{RELEASE_IDENTITY.environment_id}/sparkcompute?beta=false",
        )
        validate_reviewed_compute_settings(settings)
        _, _, workspace = self.request("GET", f"/workspaces/{self.workspace_id}/spark/settings")
        validate_workspace_pool_policy(workspace)

    def wait(self, headers: Mapping[str, str]) -> str:
        location = headers.get("Location") or headers.get("location")
        if not location:
            raise OperationsDeploymentError(
                "Fabric accepted an operation without a Location header"
            )
        operation_id = (
            headers.get("x-ms-operation-id")
            or headers.get("X-Ms-Operation-Id")
            or location.rstrip("/").split("/")[-1]
        )
        if not operation_id:
            raise OperationsDeploymentError(
                "Fabric operation response omitted its operation ID"
            )
        for _ in range(120):
            _, operation_headers, operation = self.request("GET", location)
            state = str(operation.get("status", ""))
            if state == "Succeeded":
                return operation_id
            if state in {"Failed", "Cancelled"}:
                raise OperationsDeploymentError(
                    "Fabric operation failed: "
                    + json.dumps(operation, allow_nan=False, sort_keys=True)
                )
            retry = (
                operation_headers.get("Retry-After")
                or operation_headers.get("retry-after")
                or "5"
            )
            self.sleep(max(1.0, float(retry)))
        raise OperationsDeploymentError("Fabric operation polling timed out")

    def list_sjds(self) -> list[dict[str, Any]]:
        path = f"/workspaces/{self.workspace_id}/sparkJobDefinitions"
        _, _, response = self.request("GET", path)
        values = response.get("value", [])
        if not isinstance(values, list) or not all(
            isinstance(item, dict) for item in values
        ):
            raise OperationsDeploymentError("invalid Spark Job Definition listing")
        return values

    def upsert_sjd(self, kind: str, display_name: str) -> str:
        matches = [
            item
            for item in self.list_sjds()
            if item.get("displayName") == display_name
        ]
        if len(matches) > 1:
            raise OperationsDeploymentError(
                f"multiple Spark Job Definitions named {display_name!r}"
            )
        definition = build_sjd_definition(kind)
        if not matches:
            path = f"/workspaces/{self.workspace_id}/sparkJobDefinitions"
            status, headers, created = self.request(
                "POST",
                path,
                {
                    "definition": definition["definition"],
                    "description": (
                        "Stable people-counter production "
                        f"{kind} Spark Job Definition"
                    ),
                    "displayName": display_name,
                },
            )
            if status == 202:
                self.wait(headers)
                matches = [
                    item
                    for item in self.list_sjds()
                    if item.get("displayName") == display_name
                ]
                if len(matches) != 1:
                    raise OperationsDeploymentError(
                        f"created SJD {display_name!r} has no unique readback"
                    )
                return str(matches[0]["id"])
            item_id = created.get("id")
            if status != 201 or not item_id:
                raise OperationsDeploymentError(
                    f"create SJD {display_name!r} returned no item ID"
                )
            return str(item_id)
        item_id = str(matches[0]["id"])
        status, headers, _ = self.request(
            "POST",
            (
                f"/workspaces/{self.workspace_id}/items/{item_id}"
                "/updateDefinition"
            ),
            definition,
        )
        if status == 202:
            self.wait(headers)
        elif status not in {200, 204}:
            raise OperationsDeploymentError(
                f"update SJD {display_name!r} returned {status}"
            )
        return item_id

    def get_definition(
        self, item_id: str, *, format_name: str | None = "SparkJobDefinitionV2",
    ) -> dict[str, Any]:
        status, headers, definition = self.request(
            "POST",
            (
                f"/workspaces/{self.workspace_id}/items/{item_id}"
                "/getDefinition" + (f"?format={format_name}" if format_name else "")
            ),
        )
        if status == 202:
            operation_id = self.wait(headers)
            _, _, definition = self.request(
                "GET",
                f"/operations/{operation_id}/result",
            )
        return definition

    def reconcile_intake_release(self, item_id: str, release_digest: str) -> None:
        expected = bind_intake_release(self.get_definition(item_id, format_name=None), release_digest)
        status, headers, _ = self.request(
            "POST", f"/workspaces/{self.workspace_id}/items/{item_id}/updateDefinition",
            expected,
        )
        if status == 202:
            self.wait(headers)
        elif status not in {200, 204}:
            raise OperationsDeploymentError(f"intake definition update returned {status}")
        observed = self.get_definition(item_id, format_name=None)
        for definition in (expected, observed):
            part = next(
                part for part in definition["definition"]["parts"]
                if part["path"] == "pipeline-content.json"
            )
            definition["pipeline_content"] = json.loads(base64.b64decode(part["payload"]))
        if observed["pipeline_content"] != expected["pipeline_content"]:
            raise OperationsDeploymentError("intake release definition readback differs")

    def reconcile_registration_notebook(self, item_id: str) -> None:
        expected = bind_registration_notebook(self.get_definition(item_id, format_name=None))
        status, headers, _ = self.request(
            "POST", f"/workspaces/{self.workspace_id}/items/{item_id}/updateDefinition",
            expected,
        )
        if status == 202:
            self.wait(headers)
        elif status not in {200, 204}:
            raise OperationsDeploymentError(f"registration notebook update returned {status}")
        observed = self.get_definition(item_id, format_name=None)
        sources = []
        for definition in (expected, observed):
            part = next(
                part for part in definition["definition"]["parts"]
                if part["path"] == "notebook-content.py"
            )
            sources.append(base64.b64decode(part["payload"]).decode("utf-8").replace("\r\n", "\n"))
        if sources[0] != sources[1]:
            raise OperationsDeploymentError("registration notebook source readback differs")

    def reconcile_schedule(
        self,
        item_id: str,
        *,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        base = (
            f"/workspaces/{self.workspace_id}/items/{item_id}/jobs/"
            f"{JOB_TYPE}/schedules"
        )
        _, _, response = self.request("GET", base)
        schedules = response.get("value", [])
        if not isinstance(schedules, list):
            raise OperationsDeploymentError("invalid schedule listing")
        if len(schedules) > 1:
            raise OperationsDeploymentError(
                f"SJD {item_id} has multiple schedules; refusing ambiguous update"
            )
        if not schedules:
            _, _, schedule = self.request("POST", base, payload)
            return schedule
        schedule_id = str(schedules[0].get("id", ""))
        if not schedule_id:
            raise OperationsDeploymentError("schedule readback omitted its ID")
        _, _, schedule = self.request(
            "PATCH",
            f"{base}/{schedule_id}",
            payload,
        )
        return schedule


def _command(arguments: Sequence[str]) -> str:
    return shlex.join(arguments)


def schedule_payload(
    *,
    interval_minutes: int,
    command_line_arguments: str,
    enabled: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    if type(interval_minutes) is not int or not 1 <= interval_minutes <= 10_080:
        raise ValueError("interval_minutes must be between 1 and 10080")
    start = (now or datetime.now(timezone.utc)) + timedelta(minutes=5)
    end = start + timedelta(days=3650)
    return {
        "configuration": {
            "endDateTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": interval_minutes,
            "localTimeZoneId": "UTC",
            "startDateTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "type": "Cron",
        },
        "enabled": enabled,
        "executionData": {
            "commandLineArguments": command_line_arguments,
        },
    }


def _fixed_item_ids() -> dict[str, str]:
    required = {
        "control": "stable-control",
        "gold": "stable-gold",
        "reconciliation": "stable-reconciliation",
    }
    values = {}
    for kind, identity_name in required.items():
        item_id = RELEASE_IDENTITY.sjd_ids.get(identity_name)
        if not item_id:
            raise OperationsDeploymentError(
                f"release identity omitted {identity_name!r}"
            )
        values[kind] = item_id
    return values


def validate_definition_readback(
    kind: str,
    observed: Mapping[str, Any],
) -> None:
    expected_parts = {
        part["path"]: part
        for part in build_sjd_definition(kind)["definition"]["parts"]
    }
    definition = observed.get("definition")
    if not isinstance(definition, Mapping):
        raise OperationsDeploymentError(
            f"{kind} SJD definition readback is missing"
        )
    parts = definition.get("parts")
    if not isinstance(parts, list) or not all(
        isinstance(part, Mapping) for part in parts
    ):
        raise OperationsDeploymentError(
            f"{kind} SJD parts readback is invalid"
        )
    observed_parts = {str(part.get("path")): part for part in parts}
    observed_paths = set(observed_parts)
    required_paths = {*expected_parts, ".platform"}
    if (
        not required_paths.issubset(observed_paths)
        or not (observed_paths - required_paths).issubset({".schedules"})
    ):
        raise OperationsDeploymentError(
            f"{kind} SJD definition readback has unexpected parts"
        )
    for path, expected in expected_parts.items():
        actual = observed_parts[path]
        if actual.get("payloadType") != "InlineBase64":
            raise OperationsDeploymentError(
                f"{kind} SJD part {path!r} has an invalid payload type"
            )
        try:
            expected_bytes = base64.b64decode(expected["payload"])
            actual_bytes = base64.b64decode(str(actual["payload"]))
        except (KeyError, ValueError) as error:
            raise OperationsDeploymentError(
                f"{kind} SJD part {path!r} has an invalid payload"
            ) from error
        if path.endswith(".json"):
            if json.loads(actual_bytes) != json.loads(expected_bytes):
                raise OperationsDeploymentError(
                    f"{kind} SJD metadata readback differs from deployment"
                )
        elif actual_bytes != expected_bytes:
            raise OperationsDeploymentError(
                f"{kind} SJD source readback differs from deployment"
            )


def deploy_operations(
    api: FabricApi,
    *,
    release_manifest_sha256: str,
    release_receipt_sha256: str,
    semantic_model_id: str,
    additional_semantic_model_ids: Sequence[str] = (),
    enable_schedules: bool,
    intake_pipeline_id: str,
    registration_notebook_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    api.verify_compute()
    items = _fixed_item_ids()
    for kind, display_name in ITEM_NAMES.items():
        items[kind] = api.upsert_sjd(kind, display_name)
        observed = api.get_definition(items[kind])
        validate_definition_readback(kind, observed)
    api.reconcile_intake_release(intake_pipeline_id, release_manifest_sha256)
    api.reconcile_registration_notebook(registration_notebook_id)
    release_root = RELEASE_IDENTITY.release_evidence_root.rstrip("/")
    schedules = {
        "dispatcher": (
            5,
            _command(
                [
                    "--max-items",
                    "64",
                    "--maximum-active-batches",
                    "1",
                    "--lease-seconds",
                    "14400",
                    "--minimum-speed-x",
                    "1",
                    "--safety-factor",
                    "1.25",
                    "--margin-seconds",
                    "60",
                    "--release-manifest-path",
                    f"{release_root}/detached-manifest.json",
                    "--release-manifest-sha256",
                    release_manifest_sha256,
                    "--release-receipt-path",
                    f"{release_root}/postpublish-receipt.json",
                    "--release-receipt-sha256",
                    release_receipt_sha256,
                ]
            ),
        ),
        "control": (5, "recover"),
        "reconciliation": (15, ""),
        "gold": (15, "run --lookback-hours 48"),
        "refresh": (
            15,
            _command(
                [
                    "--workspace-id",
                    api.workspace_id,
                    "--semantic-model-id",
                    semantic_model_id,
                ]
                + [
                    value
                    for model_id in additional_semantic_model_ids
                    for value in ("--semantic-model-id", model_id)
                ]
            ),
        ),
    }
    readbacks = {}
    for kind, (interval, arguments) in schedules.items():
        expected = schedule_payload(
            interval_minutes=interval,
            command_line_arguments=arguments,
            enabled=enable_schedules,
            now=now,
        )
        observed = api.reconcile_schedule(items[kind], payload=expected)
        if observed:
            if observed.get("enabled") != enable_schedules:
                raise OperationsDeploymentError(
                    f"{kind} schedule enabled readback differs"
                )
            configuration = observed.get("configuration")
            if not isinstance(configuration, Mapping):
                raise OperationsDeploymentError(
                    f"{kind} schedule configuration readback is invalid"
                )
            if configuration.get("interval") != interval:
                raise OperationsDeploymentError(
                    f"{kind} schedule interval readback differs"
                )
        readbacks[kind] = observed
    return {
        "enabled": enable_schedules,
        "items": items,
        "schedules": readbacks,
        "intake_pipeline_id": intake_pipeline_id,
        "registration_notebook_id": registration_notebook_id,
        "release_manifest_sha256": release_manifest_sha256,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workspace-id",
        default=RELEASE_IDENTITY.workspace_id,
    )
    parser.add_argument(
        "--semantic-model-id",
        default=ANALYTICS_MODEL_ID,
    )
    parser.add_argument(
        "--additional-semantic-model-id",
        action="append",
        default=[],
    )
    parser.add_argument("--release-manifest-sha256", required=True)
    parser.add_argument("--release-receipt-sha256", required=True)
    parser.add_argument("--intake-pipeline-id", required=True)
    parser.add_argument("--registration-notebook-id", required=True)
    parser.add_argument("--enable-schedules", action="store_true")
    arguments = parser.parse_args(argv)
    result = deploy_operations(
        FabricApi(arguments.workspace_id, token=_token()),
        release_manifest_sha256=arguments.release_manifest_sha256,
        release_receipt_sha256=arguments.release_receipt_sha256,
        semantic_model_id=arguments.semantic_model_id,
        additional_semantic_model_ids=(
            arguments.additional_semantic_model_id
        ),
        enable_schedules=arguments.enable_schedules,
        intake_pipeline_id=arguments.intake_pipeline_id,
        registration_notebook_id=arguments.registration_notebook_id,
    )
    print(json.dumps(result, allow_nan=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

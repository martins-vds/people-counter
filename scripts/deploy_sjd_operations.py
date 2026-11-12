"""Deploy and schedule the stable production SJD control loop."""

from __future__ import annotations

import argparse
import ast
import base64
import copy
import dataclasses
import json
import os
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from people_counter.fabric_deployment_manifest import (
    DeploymentManifest, load_deployment_manifest, parse_deployment_manifest,
)
from people_counter.fabric_sjd_definition import (
    ENTRY_POINTS, build_sjd_definition, reviewed_compute_settings,
    validate_reviewed_compute_settings, validate_schedule_readback,
)
from people_counter.fabric_target import FabricTarget, validate_artifact_id
from people_counter.fabric_sjd import FabricSjdConfig


API_ROOT = "https://api.fabric.microsoft.com/v1"
JOB_TYPE = "sparkjob"
ITEM_NAMES = {role: f"pc-sjd-{role}" for role in ENTRY_POINTS}


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
    expression = "registration_release_digest()"
    if ast.dump(assignment.value) == ast.dump(ast.parse(expression, mode="eval").body):
        return updated
    replacement = (
        "from people_counter.fabric_release_provenance import registration_release_digest\n"
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
    document = json.loads(output)
    token = document.get("accessToken") if isinstance(document, dict) else None
    if not isinstance(token, str) or not token or token != token.strip():
        raise OperationsDeploymentError("Azure CLI returned an invalid or empty Fabric token")
    return token


class FabricApi:
    def __init__(
        self,
        target: FabricTarget,
        *,
        token: str,
        sleep: Any = time.sleep,
    ) -> None:
        self.target = FabricTarget(
            workspace_id=target.workspace_id, lakehouse_id=target.lakehouse_id,
            environment_id=target.environment_id,
        )
        self.config = target if isinstance(target, FabricSjdConfig) else FabricSjdConfig(
            workspace_id=target.workspace_id, lakehouse_id=target.lakehouse_id,
            environment_id=target.environment_id,
        )
        self.workspace_id = target.workspace_id
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
            f"/workspaces/{self.workspace_id}/environments/{self.target.environment_id}/sparkcompute?beta=false",
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
        base = f"/workspaces/{self.workspace_id}/sparkJobDefinitions"
        path: str | None = base
        seen: set[str] = set()
        items: list[dict[str, Any]] = []
        while path is not None:
            if path in seen:
                raise OperationsDeploymentError("repeated SJD listing continuation")
            seen.add(path)
            _, _, response = self.request("GET", path)
            values = response.get("value")
            if not isinstance(values, list) or not all(
                isinstance(item, dict) for item in values
            ):
                raise OperationsDeploymentError("invalid Spark Job Definition listing")
            for item in values:
                validate_artifact_id(item.get("id"), "listed SJD ID")
                if not isinstance(item.get("displayName"), str) or not item["displayName"]:
                    raise OperationsDeploymentError("SJD listing omitted a display name")
            items.extend(values)
            continuation = response.get("continuationUri")
            if continuation is not None and (
                not isinstance(continuation, str) or not continuation
            ):
                raise OperationsDeploymentError("invalid SJD listing continuation")
            if continuation is None:
                token = response.get("continuationToken")
                if token is not None and not isinstance(token, str):
                    raise OperationsDeploymentError("invalid SJD listing continuation token")
                if token:
                    continuation = base + "?" + urlencode({"continuationToken": token})
            path = continuation
        return items

    def upsert_sjd(
        self, kind: str, display_name: str, *, expected_item_id: str | None = None,
    ) -> str:
        matches = [
            item
            for item in self.list_sjds()
            if item.get("displayName") == display_name
        ]
        if len(matches) > 1:
            raise OperationsDeploymentError(
                f"multiple Spark Job Definitions named {display_name!r}"
            )
        if expected_item_id is not None:
            validate_artifact_id(expected_item_id, "configured SJD ID")
            if not matches or matches[0].get("id") != expected_item_id:
                raise OperationsDeploymentError(
                    f"configured SJD ID for {display_name!r} does not match discovery"
                )
        definition = build_sjd_definition(
            kind, config=self.config,
            command_line_arguments=_command(self.config.namespace_arguments()),
        )
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
                return validate_artifact_id(matches[0].get("id"), "created SJD ID")
            item_id = created.get("id")
            if status != 201 or not item_id:
                raise OperationsDeploymentError(
                    f"create SJD {display_name!r} returned no item ID"
                )
            return validate_artifact_id(item_id, "created SJD ID")
        item_id = validate_artifact_id(matches[0].get("id"), "discovered SJD ID")
        self.disable_schedules(item_id, remove=kind == "process")
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

    def disable_schedules(self, item_id: str, *, remove: bool = False) -> None:
        base = f"/workspaces/{self.workspace_id}/items/{item_id}/jobs/{JOB_TYPE}/schedules"
        _, _, response = self.request("GET", base)
        schedules = response.get("value")
        if not isinstance(schedules, list) or len(schedules) > 1:
            raise OperationsDeploymentError("invalid or ambiguous SJD schedule listing")
        for schedule in schedules:
            if not isinstance(schedule, Mapping):
                raise OperationsDeploymentError("invalid SJD schedule")
            schedule_id = validate_artifact_id(schedule.get("id"), "schedule ID")
            if remove:
                status, _, _ = self.request("DELETE", f"{base}/{schedule_id}")
                if status not in {200, 204}:
                    raise OperationsDeploymentError("on-demand schedule deletion failed")
                _, _, observed = self.request("GET", base)
                if observed.get("value") != []:
                    raise OperationsDeploymentError("on-demand role still has a schedule after deletion")
                continue
            if type(schedule.get("enabled")) is not bool:
                raise OperationsDeploymentError("schedule readback omitted enabled state")
            if schedule["enabled"]:
                path = f"{base}/{schedule_id}"
                configuration = schedule.get("configuration")
                if not isinstance(configuration, Mapping) or not configuration:
                    raise OperationsDeploymentError("schedule configuration is missing; cannot disable safely")
                payload = {"enabled": False, "configuration": dict(configuration)}
                if "executionData" in schedule:
                    payload["executionData"] = schedule["executionData"]
                self.request("PATCH", path, payload)
                _, _, observed = self.request("GET", path)
                if observed.get("id") != schedule_id:
                    raise OperationsDeploymentError("schedule disable readback ID differs")
                try:
                    validate_schedule_readback(observed, payload)
                except ValueError as error:
                    raise OperationsDeploymentError("schedule disable readback differs") from error

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
        schedules = response.get("value")
        if not isinstance(schedules, list) or not all(
            isinstance(schedule, Mapping) for schedule in schedules
        ):
            raise OperationsDeploymentError("invalid schedule listing")
        if len(schedules) > 1:
            raise OperationsDeploymentError(
                f"SJD {item_id} has multiple schedules; refusing ambiguous update"
            )
        if schedules:
            schedule_id = validate_artifact_id(schedules[0].get("id"), "schedule ID")
            status, headers, _ = self.request("PATCH", f"{base}/{schedule_id}", payload)
            allowed = {200, 202, 204}
        else:
            status, headers, created = self.request("POST", base, payload)
            schedule_id = created.get("id")
            allowed = {200, 201, 202}
        if status not in allowed:
            raise OperationsDeploymentError(f"schedule update returned unexpected HTTP {status}")
        if status == 202:
            self.wait(headers)
        if schedule_id is None:
            _, _, listing = self.request("GET", base)
            discovered = listing.get("value")
            if (
                not isinstance(discovered, list) or len(discovered) != 1
                or not isinstance(discovered[0], Mapping)
            ):
                raise OperationsDeploymentError("created schedule could not be uniquely read back")
            schedule_id = discovered[0].get("id")
        schedule_id = validate_artifact_id(schedule_id, "schedule ID")
        _, _, observed = self.request("GET", f"{base}/{schedule_id}")
        if observed.get("id") != schedule_id:
            raise OperationsDeploymentError("schedule readback ID differs from the requested schedule")
        _verify_schedule(item_id, observed, payload)
        return observed


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
    if type(enabled) is not bool:
        raise ValueError("schedule enabled must be a boolean")
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


def validate_definition_readback(
    kind: str,
    observed: Mapping[str, Any],
    *,
    config: FabricTarget,
) -> None:
    expected_parts = {
        part["path"]: part
        for part in build_sjd_definition(
            kind, config=config,
            command_line_arguments=(
                _command(config.namespace_arguments()) if isinstance(config, FabricSjdConfig) else ""
            ),
        )["definition"]["parts"]
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
    if len(observed_parts) != len(parts):
        raise OperationsDeploymentError(f"{kind} SJD readback has duplicate parts")
    observed_paths = set(observed_parts)
    if (
        not set(expected_parts).issubset(observed_paths)
        or not (observed_paths - set(expected_parts)).issubset({".platform", ".schedules"})
    ):
        raise OperationsDeploymentError(
            f"{kind} SJD definition readback has unexpected parts"
        )
    if definition.get("format") != "SparkJobDefinitionV2":
        raise OperationsDeploymentError(f"{kind} SJD definition readback has an invalid format")
    for path, expected in expected_parts.items():
        actual = observed_parts[path]
        if actual.get("payloadType") != "InlineBase64":
            raise OperationsDeploymentError(
                f"{kind} SJD part {path!r} has an invalid payload type"
            )
        try:
            expected_bytes = base64.b64decode(expected["payload"])
            actual_bytes = base64.b64decode(str(actual["payload"]), validate=True)
        except (KeyError, ValueError) as error:
            raise OperationsDeploymentError(
                f"{kind} SJD part {path!r} has an invalid payload"
            ) from error
        if path.endswith(".json"):
            try:
                observed_json = json.loads(actual_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise OperationsDeploymentError(f"{kind} SJD metadata readback is not valid JSON") from error
            if observed_json != json.loads(expected_bytes):
                raise OperationsDeploymentError(
                    f"{kind} SJD metadata readback differs from deployment"
                )
        elif actual_bytes != expected_bytes:
            raise OperationsDeploymentError(
                f"{kind} SJD source readback differs from deployment"
            )


def provision_operations(
    api: FabricApi, *, manifest: DeploymentManifest,
) -> dict[str, Any]:
    if api.target != manifest.target():
        raise OperationsDeploymentError("API target differs from deployment target")
    if api.config != manifest.runtime_config():
        raise OperationsDeploymentError("API target namespace differs from deployment configuration")
    items: dict[str, str] = {}
    roles = {}
    for role in ENTRY_POINTS:
        binding = manifest.role(role)
        item_id = api.upsert_sjd(
            role, binding.display_name, expected_item_id=binding.item_id,
        )
        validate_artifact_id(item_id, f"{role} SJD ID")
        if item_id in items.values():
            raise OperationsDeploymentError("Fabric returned the same SJD ID for multiple roles")
        validate_definition_readback(
            role, api.get_definition(item_id), config=api.config,
        )
        items[role] = item_id
        roles[role] = dataclasses.replace(binding, item_id=item_id)
    resolved = dataclasses.replace(manifest, roles=roles)
    return {
        "status": "provisioned",
        "enabled": False,
        "items": items,
        "deployment": resolved.to_document(),
    }


def _release_digest(value: str, label: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise OperationsDeploymentError(f"{label} must be a SHA-256 digest")


def _verify_schedule(
    role: str, observed: object, expected: Mapping[str, Any],
) -> None:
    try:
        validate_schedule_readback(observed, expected)
    except ValueError as error:
        raise OperationsDeploymentError(f"{role} schedule readback differs") from error


def _activate_schedules(
    api: FabricApi, items: Mapping[str, str], payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    readbacks = {}
    try:
        for role, payload in payloads.items():
            expected = {**payload, "enabled": True}
            observed = api.reconcile_schedule(items[role], payload=expected)
            _verify_schedule(role, observed, expected)
            readbacks[role] = observed
    except (OperationsDeploymentError, OSError, ValueError) as error:
        failed = []
        for role, payload in payloads.items():
            try:
                observed = api.reconcile_schedule(items[role], payload=payload)
                _verify_schedule(role, observed, payload)
            except (OperationsDeploymentError, OSError, ValueError):
                failed.append(role)
        if failed:
            raise OperationsDeploymentError(
                "Schedule activation failed; could not confirm disabled schedules for "
                + ", ".join(failed) + ". Inspect the target workspace before retrying."
            ) from error
        raise OperationsDeploymentError(
            "Schedule activation failed; all recurring schedules were disabled again. "
            "Previously accepted jobs are not cancelled."
        ) from error
    return readbacks


def _validate_configuration(
    *,
    release_manifest_sha256: str | None,
    release_receipt_sha256: str | None,
    semantic_model_id: str,
    additional_semantic_model_ids: Sequence[str],
    intake_pipeline_id: str | None,
    registration_notebook_id: str | None,
    enable_schedules: bool,
) -> None:
    if (release_manifest_sha256 is None) != (release_receipt_sha256 is None):
        raise OperationsDeploymentError("legacy release hashes must be supplied together")
    if release_manifest_sha256 is not None:
        _release_digest(release_manifest_sha256, "release manifest hash")
    if release_receipt_sha256 is not None:
        _release_digest(release_receipt_sha256, "release receipt hash")
    validate_artifact_id(semantic_model_id, "semantic model ID")
    for model_id in additional_semantic_model_ids:
        validate_artifact_id(model_id, "additional semantic model ID")
    if type(enable_schedules) is not bool:
        raise ValueError("enable_schedules must be a boolean")
    if (intake_pipeline_id is None) != (registration_notebook_id is None):
        raise OperationsDeploymentError("intake pipeline and registration notebook IDs must be supplied together")
    if intake_pipeline_id is not None:
        validate_artifact_id(intake_pipeline_id, "intake pipeline ID")
        validate_artifact_id(registration_notebook_id, "registration notebook ID")


def deploy_operations(
    api: FabricApi,
    *,
    manifest: DeploymentManifest,
    release_manifest_sha256: str | None = None,
    release_receipt_sha256: str | None = None,
    semantic_model_id: str,
    additional_semantic_model_ids: Sequence[str] = (),
    enable_schedules: bool,
    intake_pipeline_id: str | None = None,
    registration_notebook_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    _validate_configuration(
        release_manifest_sha256=release_manifest_sha256,
        release_receipt_sha256=release_receipt_sha256,
        semantic_model_id=semantic_model_id,
        additional_semantic_model_ids=additional_semantic_model_ids,
        intake_pipeline_id=intake_pipeline_id,
        registration_notebook_id=registration_notebook_id,
        enable_schedules=enable_schedules,
    )
    if api.target != manifest.target():
        raise OperationsDeploymentError("API target differs from deployment target")
    if api.config != manifest.runtime_config():
        raise OperationsDeploymentError("API target namespace differs from deployment configuration")
    api.verify_compute()
    result = provision_operations(api, manifest=manifest)
    items = result["items"]
    if intake_pipeline_id is not None and registration_notebook_id is not None:
        api.reconcile_registration_notebook(registration_notebook_id)
    resolved = parse_deployment_manifest(result["deployment"], manifest.stage)
    resolved = dataclasses.replace(
        resolved,
        release=dataclasses.replace(
            resolved.release, manifest_sha256=release_manifest_sha256,
            receipt_sha256=release_receipt_sha256,
        ),
        analytics_reporting=dataclasses.replace(
            resolved.analytics_reporting,
            semantic_model=dataclasses.replace(
                resolved.analytics_reporting.semantic_model, target_item_id=semantic_model_id,
            ),
        ),
    )
    model_ids = list(dict.fromkeys([semantic_model_id, *additional_semantic_model_ids]))
    operational_model = manifest.operational_reporting.semantic_model.target_item_id
    if operational_model is not None and operational_model not in model_ids:
        model_ids.append(operational_model)
    result["deployment"] = resolved.to_document()
    payloads = {}
    readbacks = {}
    for kind in ("dispatcher", "control", "reconciliation", "gold", "refresh"):
        schedule = manifest.role(kind).schedule
        if schedule is None:
            raise OperationsDeploymentError(f"{kind} deployment schedule is missing")
        expected = schedule_payload(
            interval_minutes=schedule.interval_minutes,
            command_line_arguments=resolved.schedule_arguments(
                kind, enabled=True, semantic_model_ids=model_ids,
            ),
            enabled=False,
            now=now,
        )
        observed = api.reconcile_schedule(items[kind], payload=expected)
        _verify_schedule(kind, observed, expected)
        payloads[kind] = expected
        readbacks[kind] = observed
    if enable_schedules:
        readbacks = _activate_schedules(api, items, payloads)
    return {
        **result,
        "status": "configured",
        "enabled": enable_schedules,
        "items": items,
        "schedules": readbacks,
        "intake_pipeline_id": intake_pipeline_id,
        "registration_notebook_id": registration_notebook_id,
        "release_manifest_sha256": release_manifest_sha256,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=("dev", "test", "prod"))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provision-only", action="store_true")
    parser.add_argument("--semantic-model-id")
    parser.add_argument(
        "--additional-semantic-model-id",
        action="append",
        default=[],
    )
    parser.add_argument("--release-manifest-sha256")
    parser.add_argument("--release-receipt-sha256")
    parser.add_argument("--intake-pipeline-id")
    parser.add_argument("--registration-notebook-id")
    parser.add_argument("--enable-schedules", action="store_true")
    arguments = parser.parse_args(argv)
    env = dict(os.environ)
    if arguments.config is not None:
        env["PEOPLE_COUNTER_DEPLOY_CONFIG_PATH"] = str(arguments.config)
        env.pop("PEOPLE_COUNTER_DEPLOY_CONFIG_JSON", None)
    manifest = load_deployment_manifest(arguments.stage, env=env)
    target = manifest.runtime_config()
    if arguments.provision_only and arguments.enable_schedules:
        parser.error("--provision-only cannot enable schedules")
    if (arguments.release_manifest_sha256 is None) != (arguments.release_receipt_sha256 is None):
        parser.error("legacy release hashes must be supplied together")
    model_id = arguments.semantic_model_id or manifest.analytics_reporting.semantic_model.target_item_id
    if not arguments.provision_only and model_id is None:
        parser.error("configuration requires --semantic-model-id or a resolved reporting target ID")
    if not arguments.provision_only:
        _validate_configuration(
            release_manifest_sha256=arguments.release_manifest_sha256,
            release_receipt_sha256=arguments.release_receipt_sha256,
            semantic_model_id=model_id,
            additional_semantic_model_ids=arguments.additional_semantic_model_id,
            intake_pipeline_id=arguments.intake_pipeline_id,
            registration_notebook_id=arguments.registration_notebook_id,
            enable_schedules=arguments.enable_schedules,
        )
    api = FabricApi(target, token=_token())
    if arguments.provision_only:
        result = provision_operations(api, manifest=manifest)
    else:
        result = deploy_operations(
            api, manifest=manifest,
            release_manifest_sha256=arguments.release_manifest_sha256,
            release_receipt_sha256=arguments.release_receipt_sha256,
            semantic_model_id=model_id,
            additional_semantic_model_ids=arguments.additional_semantic_model_id,
            enable_schedules=arguments.enable_schedules,
            intake_pipeline_id=arguments.intake_pipeline_id,
            registration_notebook_id=arguments.registration_notebook_id,
        )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(result["deployment"], allow_nan=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, allow_nan=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

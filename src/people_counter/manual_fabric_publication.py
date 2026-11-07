"""Small publication helpers for the browser-run manual deployment notebook."""

from __future__ import annotations

import base64
import copy
import json
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlencode
from uuid import UUID


PUBLICATION_DESCRIPTION = "People Counter manual guide publication v1"
_TOKEN = re.compile(r"\{\{\s*\.\w+\s*\}\}")


def render_definition(
    definition: Mapping[str, Any],
    tokens: Mapping[str, str],
    *,
    workspace_name: str | None = None,
) -> dict[str, Any]:
    result = copy.deepcopy(dict(definition))
    result["parts"] = [part for part in result["parts"] if part["path"] != ".platform"]
    for part in result["parts"]:
        content = base64.b64decode(part["payload"], validate=True)
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for name, value in tokens.items():
            text = text.replace("{{ ." + name + " }}", value)
        if workspace_name is not None and part["path"] == "definition.pbir":
            text = text.replace(
                "powerbi://api.powerbi.com/v1.0/myorg/people-counter-dev;",
                f"powerbi://api.powerbi.com/v1.0/myorg/{workspace_name};",
            )
        if _TOKEN.search(text):
            raise ValueError(f"Unresolved definition token in {part['path']}")
        part["payload"] = base64.b64encode(text.encode()).decode()
    return result


def stopped_activator_definition(
    definition: Mapping[str, Any],
    tokens: Mapping[str, str],
    *,
    tenant_id: str,
    dispatcher_name: str,
    action_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not action_rows:
        raise ValueError("Activator requires a valid portal-authored draft action")
    tenant = UUID(tenant_id)
    if str(tenant) != tenant_id or tenant.int == 0:
        raise ValueError("Activator tenant must be a canonical non-nil UUID")
    result = render_definition(definition, tokens)
    parts = [part for part in result["parts"] if part["path"] == "ReflexEntities.json"]
    if len(parts) != 1:
        raise ValueError("Activator must contain exactly one ReflexEntities.json")
    entities = json.loads(base64.b64decode(parts[0]["payload"], validate=True))
    for entity in entities:
        payload = entity["payload"]
        if payload.get("name") == "pc-dispatcher-00":
            payload["name"] = dispatcher_name
        if payload.get("name") == "alert_pc-dispatcher-00_failed":
            payload["name"] = f"alert_{dispatcher_name}_failed"
        if "connection" in payload:
            payload["connection"]["tenantId"] = tenant_id
        if "runSettings" in payload:
            payload["runSettings"]["isStopped"] = True
        rule = payload.get("definition", {})
        if rule.get("type") == "Rule":
            rule.setdefault("settings", {}).update(
                {"shouldRun": False, "shouldApplyRuleOnUpdate": False}
            )
            instance = json.loads(rule["instance"])
            for step in instance["steps"]:
                if step["name"] == "ActStep":
                    step["rows"] = copy.deepcopy(list(action_rows))
            rule["instance"] = json.dumps(instance, separators=(",", ":"))
    parts[0]["payload"] = base64.b64encode(
        json.dumps(entities, separators=(",", ":")).encode()
    ).decode()
    return result


def _definition_parts(definition: Mapping[str, Any]) -> dict[str, bytes]:
    result = {}
    for part in definition["parts"]:
        if part["path"] == ".platform":
            continue
        content = base64.b64decode(part["payload"], validate=True)
        if part["path"].endswith((".json", ".pbir", ".pbism")):
            content = json.dumps(
                json.loads(content), sort_keys=True, separators=(",", ":")
            ).encode()
        result[part["path"]] = content
    return result


def ensure_item(
    client: Any,
    name: str,
    item_type: str,
    definition: Mapping[str, Any] | None = None,
    *,
    approved_existing_id: str | None = None,
    creation_payload: Mapping[str, Any] | None = None,
) -> str:
    if definition is not None and creation_payload is not None:
        raise ValueError("Cannot supply both definition and creation payload")
    if definition is not None:
        definition = dict(definition)
        if definition.get("format") == "Default":
            definition.pop("format")
    base = f"/workspaces/{client.workspace_id}/items"
    same_name = [item for item in client.list_items() if item["displayName"] == name]
    matches = [item for item in same_name if item["type"] == item_type]
    if same_name and not matches:
        raise ValueError(f"Item type differs for {name}")
    if len(matches) > 1:
        raise ValueError(f"Ambiguous item name: {name}")
    if matches:
        item = matches[0]
        if (
            item.get("description") != PUBLICATION_DESCRIPTION
            and item["id"] != approved_existing_id
        ):
            raise ValueError(f"Existing item is not owned by this publication: {name}")
        item_id = str(item["id"])
        if definition is not None:
            client._lro_json(
                "POST", f"{base}/{item_id}/updateDefinition",
                value={"definition": definition},
            )
    else:
        payload: dict[str, Any] = {
            "displayName": name,
            "type": item_type,
            "description": PUBLICATION_DESCRIPTION,
        }
        if definition is not None:
            payload["definition"] = definition
        if creation_payload is not None:
            payload["creationPayload"] = creation_payload
        client._lro_json("POST", base, value=payload)
        created = [
            item for item in client.list_items()
            if item["displayName"] == name and item["type"] == item_type
        ]
        if len(created) != 1:
            raise ValueError(f"Created item could not be uniquely read back: {name}")
        item_id = str(created[0]["id"])
    if definition is not None:
        query = (
            "?" + urlencode({"format": definition["format"]})
            if definition.get("format") else ""
        )
        actual = client._lro_json("POST", f"{base}/{item_id}/getDefinition{query}")
        if _definition_parts(actual["definition"]) != _definition_parts(definition):
            raise ValueError(f"Definition readback differs for {name}")
    return item_id

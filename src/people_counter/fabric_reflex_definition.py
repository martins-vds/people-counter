"""Pure, fail-closed parsing for the one production Reflex writer rule."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


REFLEX_ID = "c39f8c1d-e363-402d-b7f0-34f53ce31bcc"
REFLEX_RULE_NAME = "run_pc_event_intake_on_manifest_renamed"
REFLEX_TARGET_WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
REFLEX_TARGET_PIPELINE_ID = "5548a877-38e0-4933-bf2d-0285250637d2"
REFLEX_TARGET_TYPE = "pipeline"
REFLEX_RULE_ENTITY_TYPE = "timeSeriesView-v1"
REFLEX_ACTION_ENTITY_TYPE = "fabricItemAction-v1"
REFLEX_TEMPLATE_ID = "EventTrigger"
REFLEX_TEMPLATE_VERSION = "1.3.0"

_KNOWN_ENTITY_TYPES = frozenset(
    {
        "container-v1",
        "eventstreamSource-v1",
        REFLEX_ACTION_ENTITY_TYPE,
        "kqlSource-v1",
        "realTimeHubSource-v1",
        REFLEX_RULE_ENTITY_TYPE,
    }
)


class ReflexDefinitionError(ValueError):
    """The Reflex definition does not identify one authoritative rule."""


@dataclass(frozen=True)
class ReflexTarget:
    workspace_id: str
    item_id: str
    item_type: str

    def to_dict(self) -> dict[str, str]:
        return {
            "item_id": self.item_id,
            "item_type": self.item_type,
            "workspace_id": self.workspace_id,
        }


@dataclass(frozen=True)
class ReflexRuleSnapshot:
    reflex_id: str
    rule_name: str
    target: ReflexTarget
    should_run: bool
    should_apply_rule_on_update: bool
    definition_sha256: str
    definition_size_bytes: int

    @property
    def enabled(self) -> bool:
        """Only the rule's authoritative ``shouldRun`` controls enablement."""

        return self.should_run

    def to_dict(self) -> dict[str, object]:
        return {
            "definition_sha256": self.definition_sha256,
            "definition_size_bytes": self.definition_size_bytes,
            "enabled": self.enabled,
            "reflex_id": self.reflex_id,
            "rule_name": self.rule_name,
            "should_apply_rule_on_update": self.should_apply_rule_on_update,
            "should_run": self.should_run,
            "target": self.target.to_dict(),
        }


def _definition_bytes(
    value: bytes | str | Mapping[str, Any] | Sequence[Any],
) -> tuple[bytes, Any]:
    if isinstance(value, bytes):
        raw = value
        try:
            parsed = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ReflexDefinitionError("Reflex definition is not valid JSON") from error
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as error:
            raise ReflexDefinitionError("Reflex definition is not valid JSON") from error
    elif isinstance(value, Mapping) or (
        isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray))
    ):
        parsed = value
        try:
            raw = json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise ReflexDefinitionError("Reflex definition is not JSON") from error
    else:
        raise ReflexDefinitionError(
            "Reflex definition must be JSON bytes, text, object, or entity list"
        )
    if not isinstance(parsed, (Mapping, list)):
        raise ReflexDefinitionError("Reflex definition root must be an object or list")
    return raw, parsed


def _walk_mappings(value: object) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        found.append(value)
        for child in value.values():
            found.extend(_walk_mappings(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_mappings(child))
    return found


def _authoritative_boolean(settings: Mapping[str, Any], key: str) -> bool:
    if key not in settings or type(settings[key]) is not bool:
        raise ReflexDefinitionError(f"{key} must be one authoritative boolean")
    return settings[key]


def _expected_target() -> ReflexTarget:
    return ReflexTarget(
        REFLEX_TARGET_WORKSPACE_ID,
        REFLEX_TARGET_PIPELINE_ID,
        REFLEX_TARGET_TYPE,
    )


def _validate_target(target: ReflexTarget) -> ReflexTarget:
    expected = _expected_target()
    if target != expected:
        raise ReflexDefinitionError(
            f"exact Reflex rule targets {target.to_dict()!r}, not {expected.to_dict()!r}"
        )
    return target


def _target_tuple(value: object, label: str) -> ReflexTarget:
    if not isinstance(value, Mapping) or set(value) != {
        "itemId",
        "itemType",
        "workspaceId",
    }:
        raise ReflexDefinitionError(f"{label} must be one exact target tuple")
    if any(not isinstance(value[key], str) or not value[key] for key in value):
        raise ReflexDefinitionError(f"{label} target values must be nonempty strings")
    return ReflexTarget(
        workspace_id=value["workspaceId"],
        item_id=value["itemId"],
        item_type=value["itemType"].lower(),
    )


def _normalized_rule(parsed: Mapping[str, Any]) -> tuple[bool, bool, ReflexTarget]:
    embedded_id = parsed.get("id")
    if embedded_id is not None and embedded_id != REFLEX_ID:
        raise ReflexDefinitionError("embedded Reflex artifact ID does not match")
    collections: list[object] = []
    if "rules" in parsed:
        collections.append(parsed["rules"])
    configuration = parsed.get("configuration")
    if isinstance(configuration, Mapping) and "rules" in configuration:
        collections.append(configuration["rules"])
    if len(collections) != 1 or not isinstance(collections[0], list):
        raise ReflexDefinitionError(
            "Reflex definition must contain one authoritative rules list"
        )
    rules = [
        rule
        for rule in collections[0]
        if isinstance(rule, Mapping) and rule.get("name") == REFLEX_RULE_NAME
    ]
    if len(rules) != 1:
        label = "missing" if not rules else "duplicate or ambiguous"
        raise ReflexDefinitionError(f"exact Reflex rule is {label}")
    rule = rules[0]
    settings = rule.get("rule_settings")
    if not isinstance(settings, Mapping):
        raise ReflexDefinitionError("exact Reflex rule has no rule_settings object")
    conflicts = [
        value
        for value in _walk_mappings(rule)
        if value is not settings and "shouldRun" in value
    ]
    if conflicts:
        raise ReflexDefinitionError("exact Reflex rule has conflicting shouldRun entries")
    if set(rule) != {"action", "name", "rule_settings"}:
        raise ReflexDefinitionError(
            "exact Reflex rule must contain exactly one unambiguous action"
        )
    target = _target_tuple(rule.get("action"), "Reflex rule action")
    return (
        _authoritative_boolean(settings, "shouldRun"),
        _authoritative_boolean(settings, "shouldApplyRuleOnUpdate"),
        _validate_target(target),
    )


def _entity_index(entities: list[Any]) -> dict[str, Mapping[str, Any]]:
    index: dict[str, Mapping[str, Any]] = {}
    for entity in entities:
        if not isinstance(entity, Mapping) or set(entity) != {
            "payload",
            "type",
            "uniqueIdentifier",
        }:
            raise ReflexDefinitionError("every Reflex entity must have the exact envelope")
        identifier = entity["uniqueIdentifier"]
        entity_type = entity["type"]
        if not isinstance(identifier, str) or not identifier:
            raise ReflexDefinitionError("Reflex entity ID must be a nonempty string")
        if identifier in index:
            raise ReflexDefinitionError("duplicate Reflex entity ID")
        if not isinstance(entity_type, str) or entity_type not in _KNOWN_ENTITY_TYPES:
            raise ReflexDefinitionError("unknown Reflex entity version or type")
        if not isinstance(entity["payload"], Mapping):
            raise ReflexDefinitionError("Reflex entity payload must be an object")
        index[identifier] = entity
    return index


def _argument_map(arguments: object, label: str) -> dict[str, Mapping[str, Any]]:
    if not isinstance(arguments, list) or any(
        not isinstance(value, Mapping) for value in arguments
    ):
        raise ReflexDefinitionError(f"{label} arguments must be an object list")
    result: dict[str, Mapping[str, Any]] = {}
    for argument in arguments:
        name = argument.get("name")
        if not isinstance(name, str) or not name or name in result:
            raise ReflexDefinitionError(f"{label} argument names must be unique")
        result[name] = argument
    return result


def _string_argument(
    arguments: Mapping[str, Mapping[str, Any]], name: str, label: str
) -> str:
    argument = arguments.get(name)
    keys = set(argument) if isinstance(argument, Mapping) else set()
    if (
        not isinstance(argument, Mapping)
        or keys not in (
            {"name", "type", "value"},
            {"__typename", "name", "type", "value"},
        )
        or (
            "__typename" in argument
            and argument.get("__typename") != "templateInstanceV2StringArg"
        )
        or argument.get("type") != "string"
        or not isinstance(argument.get("value"), str)
        or not argument["value"]
    ):
        raise ReflexDefinitionError(f"{label} {name} must be one string argument")
    return argument["value"]


def _decode_rule_instance(instance_text: object) -> Mapping[str, Any]:
    if not isinstance(instance_text, str):
        raise ReflexDefinitionError("rule instance must be a JSON string")
    try:
        instance = json.loads(instance_text)
    except json.JSONDecodeError as error:
        raise ReflexDefinitionError("rule instance is not valid JSON") from error
    if not isinstance(instance, Mapping) or set(instance) != {
        "steps",
        "templateId",
        "templateVersion",
    }:
        raise ReflexDefinitionError("rule instance has a conflicting structure")
    if (
        instance["templateId"] != REFLEX_TEMPLATE_ID
        or instance["templateVersion"] != REFLEX_TEMPLATE_VERSION
    ):
        raise ReflexDefinitionError("unknown Reflex rule template or version")
    return instance


def _validated_steps(instance: Mapping[str, Any]) -> None:
    steps = instance["steps"]
    if not isinstance(steps, list) or any(not isinstance(step, Mapping) for step in steps):
        raise ReflexDefinitionError("rule steps must be an object list")
    step_ids = [step.get("id") for step in steps]
    if any(not isinstance(value, str) or not value for value in step_ids):
        raise ReflexDefinitionError("rule step IDs must be nonempty strings")
    if len(set(step_ids)) != len(step_ids):
        raise ReflexDefinitionError("duplicate or cyclic Reflex rule steps")


def _rule_instance(instance_text: object) -> Mapping[str, Any]:
    instance = _decode_rule_instance(instance_text)
    _validated_steps(instance)
    return instance


def _action_binding(instance: Mapping[str, Any]) -> Mapping[str, Any]:
    steps = instance["steps"]
    action_steps = [step for step in steps if step.get("name") == "ActStep"]
    if len(action_steps) != 1:
        raise ReflexDefinitionError("rule must have exactly one action step")
    rows = action_steps[0].get("rows")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise ReflexDefinitionError("rule must have exactly one action binding")
    binding = rows[0]
    if (
        binding.get("kind") != "FabricItemInvocation"
        or binding.get("name") != "FabricItemBinding"
    ):
        raise ReflexDefinitionError("rule action binding type is not supported")
    return binding


def _action_reference(instance_text: object) -> tuple[str, ReflexTarget]:
    binding = _action_binding(_rule_instance(instance_text))
    arguments = _argument_map(binding.get("arguments"), "rule action")
    action_id = _string_argument(
        arguments, "fabricJobConnectionDocumentId", "rule action"
    )
    target = ReflexTarget(
        workspace_id=_string_argument(arguments, "workspaceId", "rule action"),
        item_id=_string_argument(arguments, "itemId", "rule action"),
        item_type=_string_argument(arguments, "itemType", "rule action").lower(),
    )
    if _string_argument(arguments, "jobType", "rule action").lower() != target.item_type:
        raise ReflexDefinitionError("rule action jobType conflicts with itemType")
    return action_id, _validate_target(target)


def _exact_rule_entity(entities: list[Any]) -> Mapping[str, Any]:
    rules = [
        entity
        for entity in entities
        if entity["type"] == REFLEX_RULE_ENTITY_TYPE
        and entity["payload"].get("name") == REFLEX_RULE_NAME
    ]
    if len(rules) != 1:
        label = "missing" if not rules else "duplicate or ambiguous"
        raise ReflexDefinitionError(f"exact Reflex rule is {label}")
    return rules[0]


def _rule_entity_definition(rule: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = rule["payload"]
    if set(payload) != {"definition", "name"}:
        raise ReflexDefinitionError("exact Reflex rule payload has conflicting fields")
    definition = payload["definition"]
    if not isinstance(definition, Mapping) or set(definition) != {
        "instance",
        "settings",
        "type",
    }:
        raise ReflexDefinitionError("exact Reflex rule definition is not authoritative")
    if definition["type"] != "Rule":
        raise ReflexDefinitionError("exact Reflex entity is not a Rule")
    return definition


def _linked_action(
    entities: list[Any],
    index: Mapping[str, Mapping[str, Any]],
    rule_id: str,
    action_id: str,
) -> Mapping[str, Any]:
    if action_id == rule_id:
        raise ReflexDefinitionError("cyclic Reflex rule action link")
    action = index.get(action_id)
    if action is None:
        raise ReflexDefinitionError("Reflex rule action link is missing")
    if action["type"] != REFLEX_ACTION_ENTITY_TYPE:
        raise ReflexDefinitionError("Reflex rule action link has the wrong entity type")
    actions = [
        entity for entity in entities if entity["type"] == REFLEX_ACTION_ENTITY_TYPE
    ]
    if len(actions) != 1 or actions[0]["uniqueIdentifier"] != action_id:
        raise ReflexDefinitionError("orphan or multiple Reflex action entities")
    return action


def _action_entity_target(action: Mapping[str, Any]) -> ReflexTarget:
    action_payload = action["payload"]
    if set(action_payload) != {"fabricItem", "jobType", "name"}:
        raise ReflexDefinitionError("Reflex action payload has conflicting fields")
    target = _target_tuple(action_payload["fabricItem"], "Reflex action")
    if not isinstance(action_payload["jobType"], str):
        raise ReflexDefinitionError("Reflex action jobType must be a string")
    if action_payload["jobType"].lower() != target.item_type:
        raise ReflexDefinitionError("Reflex action jobType conflicts with itemType")
    return _validate_target(target)


def _entity_rule(entities: list[Any]) -> tuple[bool, bool, ReflexTarget]:
    index = _entity_index(entities)
    rule = _exact_rule_entity(entities)
    definition = _rule_entity_definition(rule)
    settings = definition["settings"]
    if not isinstance(settings, Mapping) or set(settings) != {
        "shouldApplyRuleOnUpdate",
        "shouldRun",
    }:
        raise ReflexDefinitionError("exact Reflex rule settings are conflicting")
    action_id, embedded_target = _action_reference(definition["instance"])
    action = _linked_action(
        entities, index, str(rule["uniqueIdentifier"]), action_id
    )
    target = _action_entity_target(action)
    if target != embedded_target:
        raise ReflexDefinitionError("rule and action entity targets conflict")
    return (
        _authoritative_boolean(settings, "shouldRun"),
        _authoritative_boolean(settings, "shouldApplyRuleOnUpdate"),
        target,
    )


def parse_reflex_rule_definition(
    definition: bytes | str | Mapping[str, Any] | Sequence[Any],
    *,
    reflex_id: str = REFLEX_ID,
) -> ReflexRuleSnapshot:
    """Resolve the exact production rule and target, rejecting ambiguity.

    The authoritative Fabric export is the raw ``ReflexEntities.json`` list.
    The older normalized object shape remains accepted for offline callers.
    ``runSettings.isStopped`` is deliberately unrelated to rule enablement.
    """

    if reflex_id != REFLEX_ID:
        raise ReflexDefinitionError("unexpected Reflex artifact ID")
    raw, parsed = _definition_bytes(definition)
    if isinstance(parsed, list):
        should_run, should_apply, target = _entity_rule(parsed)
    else:
        should_run, should_apply, target = _normalized_rule(parsed)
    return ReflexRuleSnapshot(
        reflex_id=reflex_id,
        rule_name=REFLEX_RULE_NAME,
        target=target,
        should_run=should_run,
        should_apply_rule_on_update=should_apply,
        definition_sha256=hashlib.sha256(raw).hexdigest(),
        definition_size_bytes=len(raw),
    )

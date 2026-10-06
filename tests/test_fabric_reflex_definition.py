from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from people_counter.fabric_reflex_definition import (
    REFLEX_ID,
    REFLEX_RULE_NAME,
    REFLEX_TARGET_PIPELINE_ID,
    REFLEX_TARGET_WORKSPACE_ID,
    ReflexDefinitionError,
    parse_reflex_rule_definition,
)

FIXTURE = Path(__file__).parent / "fixtures" / "reflex_entities_live_sanitized.json"


def definition(
    *,
    should_run: object = False,
    pipeline_id: str = REFLEX_TARGET_PIPELINE_ID,
) -> dict[str, object]:
    return {
        "id": REFLEX_ID,
        "runSettings": {"isStopped": False},
        "configuration": {
            "rules": [
                {
                    "name": REFLEX_RULE_NAME,
                    "rule_settings": {
                        "shouldRun": should_run,
                        "shouldApplyRuleOnUpdate": True,
                    },
                    "action": {
                        "workspaceId": REFLEX_TARGET_WORKSPACE_ID,
                        "itemId": pipeline_id,
                        "itemType": "Pipeline",
                    },
                }
            ]
        },
    }


def test_actual_inactive_shape_uses_only_authoritative_should_run() -> None:
    raw = json.dumps(definition(), indent=2).encode()
    observed = parse_reflex_rule_definition(raw)

    assert observed.should_run is False
    assert observed.enabled is False
    assert observed.should_apply_rule_on_update is True
    assert observed.definition_size_bytes == len(raw)
    assert observed.reflex_id == REFLEX_ID
    assert observed.rule_name == REFLEX_RULE_NAME
    assert len(observed.definition_sha256) == 64


def test_unrelated_entity_run_settings_is_stopped_never_controls_rule() -> None:
    value = definition(should_run=True)
    value["runSettings"] = {"isStopped": True}

    assert parse_reflex_rule_definition(value).enabled is True


def test_active_shape_is_active_even_when_entity_is_stopped() -> None:
    value = definition(should_run=True)
    value["runSettings"] = {"isStopped": True}

    snapshot = parse_reflex_rule_definition(value)

    assert snapshot.should_run is True
    assert snapshot.enabled is True


def test_missing_duplicate_and_wrong_target_are_rejected() -> None:
    missing = definition()
    missing["configuration"] = {"rules": []}
    with pytest.raises(ReflexDefinitionError, match="missing"):
        parse_reflex_rule_definition(missing)

    duplicate = definition()
    rules = duplicate["configuration"]["rules"]  # type: ignore[index]
    rules.append(dict(rules[0]))  # type: ignore[union-attr,index]
    with pytest.raises(ReflexDefinitionError, match="duplicate or ambiguous"):
        parse_reflex_rule_definition(duplicate)

    with pytest.raises(ReflexDefinitionError, match="targets"):
        parse_reflex_rule_definition(definition(pipeline_id="wrong-pipeline"))

    unrelated = {"metadata": definition()["configuration"]["rules"][0]}  # type: ignore[index]
    with pytest.raises(ReflexDefinitionError, match="authoritative rules list"):
        parse_reflex_rule_definition(unrelated)


@pytest.mark.parametrize("value", [None, 0, 1, "false", [], {}])
def test_nonboolean_authoritative_state_is_rejected(value: object) -> None:
    with pytest.raises(ReflexDefinitionError, match="must be one authoritative boolean"):
        parse_reflex_rule_definition(definition(should_run=value))


def test_conflicting_should_run_entry_is_rejected() -> None:
    value = definition()
    rule = value["configuration"]["rules"][0]  # type: ignore[index]
    rule["other"] = {"shouldRun": True}

    with pytest.raises(ReflexDefinitionError, match="conflicting shouldRun"):
        parse_reflex_rule_definition(value)


def test_wrong_reflex_and_ambiguous_target_are_rejected() -> None:
    with pytest.raises(ReflexDefinitionError, match="artifact ID"):
        parse_reflex_rule_definition(definition(), reflex_id="wrong")
    embedded = definition()
    embedded["id"] = "wrong"
    with pytest.raises(ReflexDefinitionError, match="embedded Reflex"):
        parse_reflex_rule_definition(embedded)

    value = definition()
    rule = value["configuration"]["rules"][0]  # type: ignore[index]
    rule["secondAction"] = {
        "workspaceId": REFLEX_TARGET_WORKSPACE_ID,
        "itemId": "second",
        "itemType": "pipeline",
    }
    with pytest.raises(ReflexDefinitionError, match="exactly one unambiguous"):
        parse_reflex_rule_definition(value)


@pytest.mark.parametrize("missing", ["workspaceId", "itemType"])
def test_incomplete_target_tuple_is_rejected(missing: str) -> None:
    value = definition()
    action = value["configuration"]["rules"][0]["action"]  # type: ignore[index]
    del action[missing]
    with pytest.raises(ReflexDefinitionError, match="exact target tuple"):
        parse_reflex_rule_definition(value)


def live_entities() -> list[dict[str, object]]:
    return json.loads(FIXTURE.read_bytes())


def rule_entity(values: list[dict[str, object]]) -> dict[str, object]:
    return next(
        value
        for value in values
        if value["uniqueIdentifier"] == "rule-entity-stable-id"
    )


def action_entity(values: list[dict[str, object]]) -> dict[str, object]:
    return next(
        value
        for value in values
        if value["uniqueIdentifier"] == "action-entity-stable-id"
    )


def instance(values: list[dict[str, object]]) -> dict[str, object]:
    rule = rule_entity(values)
    text = rule["payload"]["definition"]["instance"]  # type: ignore[index]
    return json.loads(text)  # type: ignore[arg-type]


def replace_instance(
    values: list[dict[str, object]], value: dict[str, object]
) -> None:
    rule = rule_entity(values)
    rule["payload"]["definition"]["instance"] = json.dumps(  # type: ignore[index]
        value, separators=(",", ":")
    )


def action_arguments(values: list[dict[str, object]]) -> list[dict[str, object]]:
    value = instance(values)
    action_step = next(step for step in value["steps"] if step["name"] == "ActStep")  # type: ignore[index]
    return action_step["rows"][0]["arguments"]  # type: ignore[index,return-value]


def test_live_entity_export_preserves_exact_raw_bytes_and_cross_link() -> None:
    raw = FIXTURE.read_bytes()

    observed = parse_reflex_rule_definition(raw)

    assert observed.should_run is False
    assert observed.should_apply_rule_on_update is True
    assert observed.definition_size_bytes == len(raw)
    assert observed.definition_sha256 == __import__("hashlib").sha256(raw).hexdigest()
    assert observed.target.item_id == REFLEX_TARGET_PIPELINE_ID


def test_live_string_argument_typename_is_accepted_only_when_exact() -> None:
    values = live_entities()
    value = instance(values)
    action_step = next(step for step in value["steps"] if step["name"] == "ActStep")  # type: ignore[index]
    action_id = next(
        argument
        for argument in action_step["rows"][0]["arguments"]  # type: ignore[index]
        if argument["name"] == "fabricJobConnectionDocumentId"
    )
    action_id["__typename"] = "templateInstanceV2StringArg"
    replace_instance(values, value)

    parsed = parse_reflex_rule_definition(values)

    assert parsed.target.item_id == REFLEX_TARGET_PIPELINE_ID
    action_id["__typename"] = "unexpectedStringArg"
    replace_instance(values, value)
    with pytest.raises(ReflexDefinitionError, match="must be one string argument"):
        parse_reflex_rule_definition(values)


def test_live_unrelated_run_settings_never_controls_rule() -> None:
    values = live_entities()
    values[-1]["payload"]["runSettings"]["isStopped"] = True  # type: ignore[index]
    rule_entity(values)["payload"]["definition"]["settings"]["shouldRun"] = True  # type: ignore[index]

    assert parse_reflex_rule_definition(values).enabled is True


def test_live_missing_duplicate_or_orphan_action_is_rejected() -> None:
    missing = live_entities()
    missing[:] = [
        value for value in missing if value["type"] != "fabricItemAction-v1"
    ]
    with pytest.raises(ReflexDefinitionError, match="link is missing"):
        parse_reflex_rule_definition(missing)

    duplicate = live_entities()
    copy = deepcopy(rule_entity(duplicate))
    copy["uniqueIdentifier"] = "duplicate-rule"
    duplicate.append(copy)
    with pytest.raises(ReflexDefinitionError, match="duplicate or ambiguous"):
        parse_reflex_rule_definition(duplicate)

    orphan = live_entities()
    copy = deepcopy(action_entity(orphan))
    copy["uniqueIdentifier"] = "orphan-action"
    orphan.append(copy)
    with pytest.raises(ReflexDefinitionError, match="orphan or multiple"):
        parse_reflex_rule_definition(orphan)


def test_live_duplicate_ids_multi_action_and_cycles_are_rejected() -> None:
    duplicate_id = live_entities()
    copy = deepcopy(action_entity(duplicate_id))
    duplicate_id.append(copy)
    with pytest.raises(ReflexDefinitionError, match="duplicate Reflex entity ID"):
        parse_reflex_rule_definition(duplicate_id)

    multi = live_entities()
    value = instance(multi)
    action_step = next(step for step in value["steps"] if step["name"] == "ActStep")  # type: ignore[index]
    second = deepcopy(action_step)
    second["id"] = "second-action-step"
    value["steps"].append(second)  # type: ignore[union-attr]
    replace_instance(multi, value)
    with pytest.raises(ReflexDefinitionError, match="exactly one action step"):
        parse_reflex_rule_definition(multi)

    cyclic = live_entities()
    value = instance(cyclic)
    arguments = next(
        step for step in value["steps"] if step["name"] == "ActStep"  # type: ignore[index]
    )["rows"][0]["arguments"]
    next(
        argument
        for argument in arguments
        if argument["name"] == "fabricJobConnectionDocumentId"
    )["value"] = "rule-entity-stable-id"
    replace_instance(cyclic, value)
    with pytest.raises(ReflexDefinitionError, match="cyclic"):
        parse_reflex_rule_definition(cyclic)


def test_live_wrong_target_unknown_version_and_nonboolean_are_rejected() -> None:
    wrong = live_entities()
    action_entity(wrong)["payload"]["fabricItem"]["itemId"] = "wrong"  # type: ignore[index]
    with pytest.raises(ReflexDefinitionError, match="targets"):
        parse_reflex_rule_definition(wrong)

    unknown = live_entities()
    action_entity(unknown)["type"] = "fabricItemAction-v2"
    with pytest.raises(ReflexDefinitionError, match="unknown Reflex entity"):
        parse_reflex_rule_definition(unknown)

    nonboolean = live_entities()
    rule_entity(nonboolean)["payload"]["definition"]["settings"]["shouldRun"] = 0  # type: ignore[index]
    with pytest.raises(ReflexDefinitionError, match="authoritative boolean"):
        parse_reflex_rule_definition(nonboolean)


def test_live_conflicting_embedded_target_and_structure_are_rejected() -> None:
    conflicting = live_entities()
    value = instance(conflicting)
    arguments = next(
        step for step in value["steps"] if step["name"] == "ActStep"  # type: ignore[index]
    )["rows"][0]["arguments"]
    next(argument for argument in arguments if argument["name"] == "jobType")[
        "value"
    ] = "Notebook"
    replace_instance(conflicting, value)
    with pytest.raises(ReflexDefinitionError, match="jobType conflicts"):
        parse_reflex_rule_definition(conflicting)

    metadata_decoy = live_entities()
    metadata_decoy.append(
        {
            "uniqueIdentifier": "decoy",
            "payload": {"metadata": {"name": REFLEX_RULE_NAME}},
            "type": "container-v1",
        }
    )
    assert parse_reflex_rule_definition(metadata_decoy).enabled is False

    conflict = live_entities()
    rule_entity(conflict)["payload"]["extra"] = {"shouldRun": True}  # type: ignore[index]
    with pytest.raises(ReflexDefinitionError, match="conflicting fields"):
        parse_reflex_rule_definition(conflict)

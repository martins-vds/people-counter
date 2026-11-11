import copy
import json
from unittest.mock import Mock
from uuid import UUID

import pytest

from people_counter import manual_fabric_publication as publication
from scripts.prepare_manual_fabric_assets import prepare_assets
from tests.fabric_fixtures import ROLE_IDS
from tests.test_fabric_operations_portable import manifest


NAMES = {role: f"chosen-{role}" for role in ROLE_IDS}


def catalog():
    return [
        {"id": item_id, "displayName": NAMES[role], "type": "SparkJobDefinition"}
        for role, item_id in ROLE_IDS.items()
    ]


def test_manual_discovery_records_uploaded_ids_without_compiled_target_values():
    client = Mock()
    client.list_items.return_value = [*catalog(), {"id": "ignore", "type": "Report"}]
    assert publication.resolve_sjd_ids(client, NAMES) == ROLE_IDS
    client.list_items.assert_called_once_with()
    client._lro_json.assert_not_called()


@pytest.mark.parametrize("names", [{}, {"control": ""}, {"control": None},
    {"control": "same", "process": "SAME"}])
def test_invalid_manual_role_names_fail_before_reading_the_workspace(names):
    client = Mock()
    with pytest.raises(ValueError):
        publication.resolve_sjd_ids(client, names)
    client.list_items.assert_not_called()


@pytest.mark.parametrize("kind", ["missing", "ambiguous", "wrong_type", "same_id"])
def test_bad_manual_discovery_is_not_saved_as_success(kind):
    items = catalog()
    if kind == "missing":
        items.pop()
    elif kind == "ambiguous":
        items.append(items[0])
    elif kind == "wrong_type":
        items[0]["type"] = "Report"
    else:
        items[1]["id"] = items[0]["id"]
    client = Mock()
    client.list_items.return_value = items
    with pytest.raises(ValueError):
        publication.resolve_sjd_ids(client, NAMES)
    client._lro_json.assert_not_called()


@pytest.mark.parametrize("item_id", [
    None, "", "wrong", str(UUID(int=0)), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
])
def test_manual_discovery_rejects_missing_nil_and_noncanonical_ids(item_id):
    items = catalog()
    items[0]["id"] = item_id
    client = Mock()
    client.list_items.return_value = items
    with pytest.raises(ValueError):
        publication.resolve_sjd_ids(client, NAMES)


class Files:
    def __init__(self):
        self.content = {}
        self.writes = []
        self.fail_write = False
        self.corrupt_readback = False

    def exists(self, path):
        return path in self.content

    def head(self, path, limit):
        assert limit == 1024 * 1024
        return self.content[path] + ("corrupt" if self.corrupt_readback else "")

    def put(self, path, content, overwrite):
        self.writes.append((path, content, overwrite))
        if self.fail_write:
            return False
        self.content[path] = content
        return True


def state():
    document = manifest(ids=True).to_document()
    return document


def test_saved_manual_bindings_are_canonical_verified_and_retryable():
    files = Files()
    document = state()
    before = copy.deepcopy(document)
    publication.save_deployment_state(files, "state", document)
    publication.save_deployment_state(files, "state", document)
    assert document == before
    assert json.loads(files.content["state"]) == document
    assert files.content["state"] == json.dumps(
        document, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ) + "\n"
    assert all(call[2] is True for call in files.writes)


@pytest.mark.parametrize("field", [
    "workspace_id", "fabric_environment_id", "lakehouse_id", "roles", "control", "files_root",
])
def test_manual_state_never_overwrites_another_target_or_changed_job_id(field):
    files = Files()
    prior = state()
    if field == "lakehouse_id":
        prior["lakehouse"][field] = str(UUID(int=200))
    elif field == "files_root":
        prior["lakehouse"][field] = "Files/another-deployment/"
    elif field == "roles":
        del prior["roles"]["process"]
    elif field == "control":
        prior["roles"]["control"]["item_id"] = str(UUID(int=200))
    else:
        prior[field] = str(UUID(int=200))
    files.content["state"] = json.dumps(prior)
    before = dict(files.content)
    with pytest.raises(ValueError, match="target|role set|conflicts|namespace"):
        publication.save_deployment_state(files, "state", state())
    assert files.content == before
    assert files.writes == []


def test_manual_state_can_fill_previously_unresolved_job_ids():
    files = Files()
    prior = state()
    for binding in prior["roles"].values():
        binding["item_id"] = None
    files.content["state"] = json.dumps(prior)
    publication.save_deployment_state(files, "state", state())
    assert json.loads(files.content["state"])["roles"] == state()["roles"]


@pytest.mark.parametrize("failure", ["write", "readback"])
def test_manual_state_errors_are_explicit(failure):
    files = Files()
    files.fail_write = failure == "write"
    files.corrupt_readback = failure == "readback"
    with pytest.raises(OSError, match="write|readback"):
        publication.save_deployment_state(files, "state", state())


@pytest.mark.parametrize("field,value", [
    ("workspace_id", None), ("workspace_id", str(UUID(int=0))),
    ("fabric_environment_id", "not-an-environment"),
    ("schema", "unexpected"), ("roles", {}),
])
def test_invalid_manual_state_never_reaches_a_write(field, value):
    files = Files()
    document = state()
    document[field] = value
    with pytest.raises(ValueError):
        publication.save_deployment_state(files, "state", document)
    assert files.writes == []


def test_duplicate_ids_cannot_be_saved_as_six_distinct_jobs():
    files = Files()
    document = state()
    document["roles"]["process"]["item_id"] = document["roles"]["control"]["item_id"]
    with pytest.raises(ValueError, match="unique"):
        publication.save_deployment_state(files, "state", document)
    assert files.writes == []


def test_prepared_administrative_inputs_include_portable_six_role_templates(tmp_path):
    from pathlib import Path

    output = tmp_path / "prepared.json"
    prepare_assets(Path(__file__).resolve().parents[1], output)
    document = json.loads(output.read_text())
    for stage, template in document["deploymentTemplates"].items():
        assert template["stage"] == stage
        assert template["workspace_id"] is None
        assert template["lakehouse"]["lakehouse_id"] is None
        assert template["fabric_environment_id"] is None
        assert set(template["roles"]) == set(ROLE_IDS)
        assert all(binding["item_id"] is None for binding in template["roles"].values())

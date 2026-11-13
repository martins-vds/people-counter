import hashlib
import importlib.metadata
import warnings
from unittest.mock import Mock, patch

import pytest

from people_counter import fabric_release_provenance as provenance
from people_counter import backfill_operator as operator
from people_counter import fabric_sjd_runtime as runtime
from people_counter.fabric_sjd import FabricSjdConfig
from tests.fabric_fixtures import TARGET_IDS
from tests.test_backfill_operator import (
    cloud_mocks, forbid_live_cloud_clients, publication_shortcut, setup,
)


def test_installed_release_uses_only_standard_package_metadata(monkeypatch):
    monkeypatch.setattr(provenance.importlib.metadata, "version", lambda name: "1.2.3")
    release = provenance.installed_release()
    assert release.package_version == "1.2.3"
    assert release.digest == hashlib.sha256(b"people-counter==1.2.3").hexdigest()
    monkeypatch.setattr(provenance.importlib.metadata, "version", lambda name: "1.2.4")
    assert provenance.installed_release().digest != release.digest


def test_missing_package_metadata_fails_explicitly(monkeypatch):
    def missing(name):
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(provenance.importlib.metadata, "version", missing)
    with pytest.raises(provenance.ReleaseProvenanceError, match="not installed"):
        provenance.installed_release()


@pytest.mark.parametrize("version", ["", " \t"])
def test_empty_package_metadata_fails_explicitly(monkeypatch, version):
    monkeypatch.setattr(provenance.importlib.metadata, "version", lambda name: version)
    with pytest.raises(provenance.ReleaseProvenanceError, match="empty package version"):
        provenance.installed_release()


def test_version_compatibility_still_rejects_mixed_sdk_versions(monkeypatch):
    monkeypatch.setattr(provenance.importlib.metadata, "version", lambda name: "1.2.3")
    provenance.validate_installed_package_version(expected_version="1.2.3")
    with pytest.raises(provenance.ReleaseProvenanceError, match="expected version"):
        provenance.validate_installed_package_version(expected_version="1.2.4")
    provenance.validate_installed_package_version(
        expected_version="1.2.4", observed_version="1.2.4",
    )
    with pytest.raises(provenance.ReleaseProvenanceError, match="expected version"):
        provenance.validate_installed_package_version(
            expected_version="1.2.3", observed_version="",
        )


@pytest.mark.parametrize("legacy_argument", ["read_text", "deployment", "expected_digest"])
def test_legacy_registration_ignores_external_evidence_without_reading(monkeypatch, legacy_argument):
    monkeypatch.setattr(provenance.importlib.metadata, "version", lambda name: "1.2.3")
    reader = Mock(side_effect=AssertionError("No external evidence"))
    value = {"read_text": reader, "deployment": object(), "expected_digest": "a" * 64}
    with pytest.warns(FutureWarning, match="no longer used"):
        digest = provenance.registration_release_digest(
            **{legacy_argument: value[legacy_argument]},
        )
    assert digest == provenance.installed_release().digest
    reader.assert_not_called()
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        assert provenance.registration_release_digest() == digest
    assert not captured


@pytest.mark.parametrize("legacy_argument", [
    "release_manifest_path", "release_manifest_sha256",
    "release_receipt_path", "release_receipt_sha256",
])
def test_each_legacy_runtime_argument_warns_without_becoming_a_requirement(legacy_argument):
    import argparse

    with pytest.warns(FutureWarning, match="obsolete"):
        release = runtime._runtime_release(argparse.Namespace(**{legacy_argument: "old-value"}))
    assert release == provenance.installed_release()
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        assert runtime._runtime_release(argparse.Namespace()) == release
    assert not captured


@pytest.mark.parametrize("workspace_id", [
    "11111111-1111-1111-1111-111111111111",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
])
def test_backfill_submits_without_any_release_evidence(setup, workspace_id):
    import json

    setup.config["workspace_id"] = workspace_id
    setup.config_path.write_text(json.dumps(setup.config))
    storage, files, _ = cloud_mocks(setup)
    files.read_text.side_effect = AssertionError("SDK must not read release evidence")
    client = Mock()
    client.request_response.return_value = (202, {"Location": "/jobs/accepted"}, b"")
    client.request_json.return_value = {"status": "Completed"}
    client.list_pages.return_value = [publication_shortcut(setup)]
    with patch.object(operator, "OneLakeFiles", return_value=files), patch.object(
        operator, "AzureDataLakeStorage", return_value=storage,
    ), patch.object(operator, "FabricAutomationClient", return_value=client):
        assert operator.main(setup.args + ["--submit"]) == 0
    files.read_text.assert_not_called()
    client.request_response.assert_called_once()


def test_process_resume_needs_no_release_files_or_hash_arguments(monkeypatch, capsys):
    import json
    from people_counter.sjd_process import ProcessResult

    store = Mock()
    attempts = Mock()
    result = ProcessResult(
        batch_id="batch-1", process_attempt_id="attempt-1",
        staging_path="/lakehouse/default/Files/staging/attempt-1",
        record_count=1, failed_work_ids=(), publication_sequences=(1,),
        resumed=True, driver_package_version=provenance.installed_release().package_version,
    )
    resume = Mock(return_value=result)
    monkeypatch.setattr("people_counter.sjd_control.FabricControlStore", lambda *a, **k: store)
    monkeypatch.setattr("people_counter.sjd_process.OneLakeDeltaAttemptAdapter", lambda *a, **k: attempts)
    monkeypatch.setattr("people_counter.sjd_process.resume_committed_process_batch", resume)
    monkeypatch.setattr(runtime, "_spark", lambda: object())
    assert runtime.process_main(["--batch-id", "batch-1"], config=FabricSjdConfig(**TARGET_IDS)) == 0
    resume.assert_called_once_with(
        store, "batch-1", attempts, release_evidence=provenance.installed_release(),
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["sdk_release"] == {
        "package_version": provenance.installed_release().package_version,
        "digest": provenance.installed_release().digest,
    }


def test_dispatcher_needs_no_release_files_or_hash_arguments(monkeypatch):
    store = Mock()
    store.claim.return_value = None
    monkeypatch.setattr("people_counter.sjd_control.FabricControlStore", lambda *a, **k: store)
    monkeypatch.setattr(runtime, "_spark", lambda: object())
    monkeypatch.setattr(runtime, "_validate_fabric_runtime", lambda spark: None)
    assert runtime.dispatcher_main([], config=FabricSjdConfig(**TARGET_IDS)) == 0
    assert "expected_release_digest" not in store.claim.call_args.kwargs


def test_control_registration_derives_release_digest_without_manual_input():
    arguments = runtime._control_parser().parse_args([
        "register", "--work-id", "work-1", "--payload-json", "{}",
        "--runtime-key", "profile", "--duration-seconds", "1",
        "--config-sha256", "a" * 64,
    ])
    assert arguments.release_digest is None
    store = Mock()
    store.register.return_value = provenance.installed_release()
    runtime._control_dispatch(arguments, store, FabricSjdConfig(**TARGET_IDS))
    assert store.register.call_args.kwargs["release_digest"] == provenance.installed_release().digest

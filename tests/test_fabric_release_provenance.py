from __future__ import annotations

import dataclasses
import json
import uuid
from pathlib import Path

import pytest

from people_counter import fabric_release_provenance as provenance


def _manifest() -> dict[str, object]:
    identity = provenance.RELEASE_IDENTITY
    return {
        "schema": "people-counter-candidate-a-detached-release-v1",
        "release_identity_sha256": provenance.RELEASE_IDENTITY_SHA256,
        "package_version": identity.package_version,
        "wheel_filename": "people_counter-0.9.55-py3-none-any.whl",
        "wheel_sha256": "1" * 64,
        "cpu_lock_filename": "requirements-cpu.lock",
        "cpu_lock_sha256": "2" * 64,
        "cpu_bundle_filename": "people-counter-0.9.55-cpu.zip",
        "cpu_bundle_sha256": "3" * 64,
        "build_input_sha256": identity.build_input_sha256,
        "workspace_id": identity.workspace_id,
        "lakehouse_id": identity.lakehouse_id,
        "environment_id": identity.environment_id,
        "sjd_ids": dict(identity.sjd_ids),
        "public_dependencies": {
            "trackers": {
                "version": "2.6.0",
                "source_sha256": {
                    "core/botsort/tracker.py": (
                        "52bc0a0f9c24379a2143374c0695271cb5ac2134cf83088"
                        "b937f0b412a5a12c1"
                    )
                },
            }
        },
        "reviewer": "independent-gpt-5.6-sol",
        "reviewed_at": "2026-10-07T15:00:00Z",
        "deployment_intent": identity.deployment_intent,
    }


def _receipt(manifest_sha256: str) -> dict[str, object]:
    identity = provenance.RELEASE_IDENTITY
    return {
        "schema": "people-counter-candidate-a-postpublish-receipt-v1",
        "detached_manifest_sha256": manifest_sha256,
        "package_version": identity.package_version,
        "environment_id": identity.environment_id,
        "environment_target_version": "4" * 32,
        "public_package_policy_sha256": "5" * 64,
        "library_readback_sha256": "6" * 64,
        "sjd_readback_sha256": "7" * 64,
        "operations": [
            {
                "operation": "environment-publish",
                "status": "Succeeded",
                "recorded_at": "2026-10-07T15:01:00Z",
            }
        ],
        "published_at": "2026-10-07T15:01:00Z",
        "recorded_at": "2026-10-07T15:02:00Z",
    }


def _write(path: Path, value: dict[str, object]) -> str:
    return provenance.create_immutable_json(path, value)


def test_embedded_identity_contains_no_final_artifact_hashes() -> None:
    raw = json.loads(
        (
            Path(provenance.__file__).parent
            / "data"
            / "candidate_a_release_identity.json"
        ).read_text(encoding="utf-8")
    )
    assert raw["package_version"] == "0.9.55"
    assert "wheel_sha256" not in raw
    assert "cpu_bundle_sha256" not in raw
    assert "environment_target_version" not in raw
    assert provenance.RELEASE_IDENTITY.release_evidence_root.endswith("/0.9.55")
    assert all(
        str(uuid.UUID(item_id)) == item_id
        for item_id in provenance.RELEASE_IDENTITY.sjd_ids.values()
    )


def test_detached_manifest_and_receipt_bind_exact_independent_bytes(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_sha256 = _write(manifest_path, _manifest())
    receipt_path = tmp_path / "receipt.json"
    receipt_sha256 = _write(receipt_path, _receipt(manifest_sha256))

    evidence = provenance.load_runtime_release_evidence(
        manifest_path,
        receipt_path,
        expected_manifest_sha256=manifest_sha256,
        expected_receipt_sha256=receipt_sha256,
        observed_package_version="0.9.55",
    )

    assert evidence.manifest.wheel_sha256 == "1" * 64
    assert evidence.receipt.detached_manifest_sha256 == manifest_sha256
    assert evidence.identity == provenance.RELEASE_IDENTITY
    assert evidence.identity_sha256 == provenance.RELEASE_IDENTITY_SHA256
    assert evidence.manifest_sha256 == manifest_sha256
    assert evidence.receipt_sha256 == receipt_sha256


def test_runtime_evidence_rejects_observed_package_version_mismatch(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_sha256 = _write(manifest_path, _manifest())
    receipt_path = tmp_path / "receipt.json"
    receipt_sha256 = _write(receipt_path, _receipt(manifest_sha256))

    with pytest.raises(
        provenance.ReleaseProvenanceError,
        match="detached expected version",
    ):
        provenance.load_runtime_release_evidence(
            manifest_path,
            receipt_path,
            expected_manifest_sha256=manifest_sha256,
            expected_receipt_sha256=receipt_sha256,
            observed_package_version="0.0.0",
        )


def test_runtime_evidence_uses_detached_manifest_version(
    tmp_path: Path,
) -> None:
    manifest = _manifest()
    manifest["package_version"] = "9.8.7"
    manifest_path = tmp_path / "manifest.json"
    manifest_sha256 = _write(manifest_path, manifest)
    receipt = _receipt(manifest_sha256)
    receipt["package_version"] = "9.8.7"
    receipt_path = tmp_path / "receipt.json"
    receipt_sha256 = _write(receipt_path, receipt)

    evidence = provenance.load_runtime_release_evidence(
        manifest_path,
        receipt_path,
        expected_manifest_sha256=manifest_sha256,
        expected_receipt_sha256=receipt_sha256,
        observed_package_version="9.8.7",
    )

    assert evidence.manifest.package_version == "9.8.7"


def test_manifest_rejects_fixed_identity_or_expected_hash_mismatch(
    tmp_path: Path,
) -> None:
    value = _manifest()
    value["environment_id"] = "different-environment"
    path = tmp_path / "manifest.json"
    digest = _write(path, value)
    with pytest.raises(
        provenance.ReleaseProvenanceError,
        match="stable release identity",
    ):
        provenance.load_detached_release_manifest(
            path, expected_sha256=digest
        )
    with pytest.raises(
        provenance.ReleaseProvenanceError,
        match="manifest hash mismatch",
    ):
        provenance.load_detached_release_manifest(
            path, expected_sha256="e" * 64
        )


def test_receipt_rejects_different_manifest_binding(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_sha256 = _write(manifest_path, _manifest())
    manifest, _ = provenance.load_detached_release_manifest(
        manifest_path, expected_sha256=manifest_sha256
    )
    receipt_path = tmp_path / "receipt.json"
    receipt_sha256 = _write(receipt_path, _receipt("f" * 64))
    with pytest.raises(
        provenance.ReleaseProvenanceError,
        match="does not bind",
    ):
        provenance.load_postpublish_receipt(
            receipt_path,
            expected_sha256=receipt_sha256,
            manifest=manifest,
            manifest_sha256=manifest_sha256,
        )


def test_create_immutable_json_proves_readback_and_second_write_conflict(
    tmp_path: Path,
) -> None:
    path = tmp_path / "evidence.json"
    first = provenance.create_immutable_json(path, {"value": 1})
    second = provenance.create_immutable_json(path, {"value": 1})
    assert second == first
    with pytest.raises(
        provenance.ReleaseProvenanceError,
        match="conflicts",
    ):
        provenance.create_immutable_json(path, {"value": 2})


def test_build_input_hash_is_deterministic_and_path_sensitive(
    tmp_path: Path,
) -> None:
    (tmp_path / "a").write_bytes(b"one")
    (tmp_path / "b").write_bytes(b"two")
    first = provenance.build_input_sha256(tmp_path, ["b", "a"])
    assert first == provenance.build_input_sha256(tmp_path, ["a", "b"])
    (tmp_path / "b").write_bytes(b"changed")
    assert provenance.build_input_sha256(tmp_path, ["a", "b"]) != first


def test_release_build_hash_excludes_detached_identity_without_normalization(
    tmp_path: Path,
) -> None:
    package = tmp_path / "src" / "people_counter"
    data = package / "data"
    data.mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "README.md").write_text("readme")
    (package / "module.py").write_text("VALUE = 1\n")
    identity = {
        "schema": "identity",
        "build_input_sha256": "0" * 64,
        "package_version": "1.0",
    }
    identity_path = data / "candidate_a_release_identity.json"
    identity_path.write_text(json.dumps(identity, sort_keys=True))
    first = provenance.release_build_input_sha256(tmp_path)
    identity["build_input_sha256"] = "f" * 64
    identity_path.write_text(json.dumps(identity, sort_keys=True))
    assert provenance.release_build_input_sha256(tmp_path) == first
    identity["package_version"] = "1.1"
    identity_path.write_text(json.dumps(identity, sort_keys=True))
    assert provenance.release_build_input_sha256(tmp_path) == first
    (package / "module.py").write_text("VALUE = 2\n")
    assert provenance.release_build_input_sha256(tmp_path) != first


def test_detached_version_and_build_hash_are_not_derived_from_identity(
    tmp_path: Path,
) -> None:
    value = _manifest()
    value["package_version"] = "9.8.7"
    value["build_input_sha256"] = "f" * 64
    path = tmp_path / "manifest.json"
    digest = _write(path, value)
    manifest, observed = provenance.load_detached_release_manifest(
        path, expected_sha256=digest
    )
    assert observed == digest
    assert manifest.package_version == "9.8.7"
    assert manifest.build_input_sha256 == "f" * 64


def test_validate_installed_package_version_uses_independent_expected_value() -> None:
    provenance.validate_installed_package_version(
        observed_version="0.9.29", expected_version="0.9.29"
    )
    with pytest.raises(
        provenance.ReleaseProvenanceError,
        match="detached expected version",
    ):
        provenance.validate_installed_package_version(
            observed_version="0.9.28", expected_version="0.9.29"
        )


def test_validate_installed_package_version_resolves_when_observation_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        provenance,
        "resolve_installed_package_version",
        lambda: "resolved-version",
    )
    provenance.validate_installed_package_version(
        observed_version=None,
        expected_version="resolved-version",
    )


def test_verify_file_sha256_streams_and_rejects_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "large.bin"
    path.write_bytes(b"x" * (2 * 1024 * 1024 + 7))
    expected = provenance.verify_file_sha256_unchecked(path)
    assert provenance.verify_file_sha256(
        path, expected, label="large fixture"
    ) == expected
    with pytest.raises(provenance.ReleaseProvenanceError, match="expected"):
        provenance.verify_file_sha256(path, "0" * 64, label="large fixture")


def test_trackers_identity_is_checked_against_detached_manifest() -> None:
    manifest = provenance.DetachedReleaseManifest(
        schema="people-counter-candidate-a-detached-release-v1",
        release_identity_sha256=provenance.RELEASE_IDENTITY_SHA256,
        package_version="0.9.29",
        wheel_filename="wheel",
        wheel_sha256="1" * 64,
        cpu_lock_filename="lock",
        cpu_lock_sha256="2" * 64,
        cpu_bundle_filename="bundle",
        cpu_bundle_sha256="3" * 64,
        build_input_sha256=provenance.RELEASE_IDENTITY.build_input_sha256,
        workspace_id=provenance.RELEASE_IDENTITY.workspace_id,
        lakehouse_id=provenance.RELEASE_IDENTITY.lakehouse_id,
        environment_id=provenance.RELEASE_IDENTITY.environment_id,
        sjd_ids=provenance.RELEASE_IDENTITY.sjd_ids,
        public_dependencies={
            "trackers": provenance.DependencyIdentity(
                version="2.6.0",
                source_sha256={"core/botsort/tracker.py": "a" * 64},
            )
        },
        reviewer="reviewer",
        reviewed_at="time",
        deployment_intent=provenance.RELEASE_IDENTITY.deployment_intent,
    )
    observed = provenance.TrackersDistributionIdentity(
        version="2.6.0",
        is_vcs_installed=False,
        vcs_direct_url=None,
        source_sha256={"core/botsort/tracker.py": "a" * 64},
        resolution_error=None,
    )
    assert (
        provenance.validate_trackers_distribution_identity(
            observed, manifest=manifest
        )
        == observed
    )
    with pytest.raises(provenance.ReleaseProvenanceError, match="hash mismatch"):
        provenance.validate_trackers_distribution_identity(
            dataclasses.replace(
                observed,
                source_sha256={"core/botsort/tracker.py": "b" * 64},
            ),
            manifest=manifest,
        )


def test_observe_trackers_identity_covers_public_and_vcs_installs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Distribution:
        version = "2.6.0"

        @staticmethod
        def read_text(_name: str) -> str:
            return json.dumps(
                {"url": "https://example.invalid/trackers.git", "vcs_info": {}}
            )

    class Resource:
        @staticmethod
        def joinpath(relative: str) -> "Resource":
            assert relative == "core/botsort/tracker.py"
            return Resource()

        @staticmethod
        def read_bytes() -> bytes:
            return b"tracked source"

    monkeypatch.setattr(
        provenance.importlib.metadata,
        "distribution",
        lambda _name: Distribution(),
    )
    monkeypatch.setattr(
        provenance.importlib.resources,
        "files",
        lambda _name: Resource(),
    )
    observed = provenance.observe_trackers_distribution_identity()
    assert observed.version == "2.6.0"
    assert observed.is_vcs_installed is True
    assert observed.vcs_direct_url == "https://example.invalid/trackers.git"
    assert observed.source_sha256 == {
        "core/botsort/tracker.py": provenance._sha256_bytes(b"tracked source")
    }
    assert observed.resolution_error is None


def test_observe_trackers_identity_reports_absent_and_unreadable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def absent(_name: str) -> None:
        raise provenance.importlib.metadata.PackageNotFoundError("missing")

    monkeypatch.setattr(provenance.importlib.metadata, "distribution", absent)
    missing = provenance.observe_trackers_distribution_identity()
    assert missing.version is None
    assert "not installed" in str(missing.resolution_error)

    class Distribution:
        version = "2.6.0"

        @staticmethod
        def read_text(_name: str) -> str:
            return "{invalid"

    monkeypatch.setattr(
        provenance.importlib.metadata,
        "distribution",
        lambda _name: Distribution(),
    )
    monkeypatch.setattr(
        provenance.importlib.resources,
        "files",
        lambda _name: (_ for _ in ()).throw(FileNotFoundError("gone")),
    )
    unreadable = provenance.observe_trackers_distribution_identity()
    assert unreadable.is_vcs_installed is False
    assert unreadable.vcs_direct_url is None
    assert unreadable.source_sha256 == {}
    assert "could not read" in str(unreadable.resolution_error)

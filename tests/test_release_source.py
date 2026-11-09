from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

import people_counter.fabric_release_provenance as provenance


@pytest.fixture
def release_source(tmp_path: Path) -> Path:
    data = tmp_path / "src/people_counter/data"
    data.mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text("[project]\nversion='1.2.3'\n")
    (tmp_path / "README.md").write_text("Release fixture")
    identity = dataclasses.asdict(provenance.RELEASE_IDENTITY)
    identity["package_version"] = "1.2.3"
    identity["release_evidence_root"] = "Files/releases/1.2.3"
    path = data / "sjd_release_identity.json"
    path.write_text(json.dumps(identity, sort_keys=True, separators=(",", ":")))
    identity["build_input_sha256"] = provenance.release_build_input_sha256(tmp_path)
    path.write_text(json.dumps(identity, sort_keys=True, separators=(",", ":")))
    return tmp_path


@pytest.mark.parametrize("evidence_root", ["Files/releases/1.2.3", "Files/releases/1.2.3/"])
def test_validate_release_source_accepts_synchronized_metadata(
    release_source: Path, evidence_root: str,
) -> None:
    path = release_source / "src/people_counter/data/sjd_release_identity.json"
    identity = json.loads(path.read_text())
    identity["release_evidence_root"] = evidence_root
    path.write_text(json.dumps(identity, sort_keys=True, separators=(",", ":")))
    provenance.validate_release_source(release_source, "1.2.3")


@pytest.mark.parametrize(("field", "value", "message"), [
    ("package_version", "1.2.2", "release identity package_version"),
    ("release_evidence_root", "Files/releases/1.2.2", "release identity release_evidence_root"),
    ("release_evidence_root", "Files/releases/11.2.3", "release identity release_evidence_root"),
    ("release_evidence_root", "Files/releases/1.2.3X", "release identity release_evidence_root"),
    ("build_input_sha256", "0" * 64, "release identity build_input_sha256"),
])
def test_validate_release_source_rejects_stale_metadata(
    release_source: Path, field: str, value: str, message: str,
) -> None:
    path = release_source / "src/people_counter/data/sjd_release_identity.json"
    identity = json.loads(path.read_text())
    identity[field] = value
    path.write_text(json.dumps(identity, sort_keys=True, separators=(",", ":")))
    with pytest.raises(provenance.ReleaseProvenanceError, match=message):
        provenance.validate_release_source(release_source, "1.2.3")


def test_validate_release_source_rejects_changed_build_inputs(release_source: Path) -> None:
    (release_source / "README.md").write_text("Changed release source")
    with pytest.raises(provenance.ReleaseProvenanceError) as error:
        provenance.validate_release_source(release_source, "1.2.3")
    assert str(error.value) == (
        "release identity build_input_sha256 is stale; recompute it "
        "with release_build_input_sha256 after finalizing the release source"
    )

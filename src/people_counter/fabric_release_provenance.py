"""Detached, immutable Candidate A release provenance.

The wheel contains only :class:`ReleaseIdentity`: stable package/build-input
identity that does not depend on the wheel's final bytes. Exact wheel, lock,
and bundle hashes live in a detached prepublish manifest. Fabric-assigned
target/readback values live in a separate postpublish receipt bound to the
manifest hash. Runtime validation therefore compares installed observations
with two independent immutable objects instead of a manifest embedded in the
artifact it attempts to hash.
"""

from __future__ import annotations

import dataclasses
import hashlib
import importlib.metadata
import importlib.resources
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

PACKAGE_DISTRIBUTION = "people-counter"
UNRESOLVED_PACKAGE_VERSION = "<people-counter-distribution-unresolved>"
_IDENTITY_PATH = (
    Path(__file__).resolve().parent / "data" / "candidate_a_release_identity.json"
)
_HEX = frozenset("0123456789abcdef")


class ReleaseProvenanceError(RuntimeError):
    """Detached release evidence is absent, malformed, or contradictory."""


def resolve_installed_package_version(
    distribution: str = PACKAGE_DISTRIBUTION,
) -> str:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return UNRESOLVED_PACKAGE_VERSION


INSTALLED_PACKAGE_VERSION = resolve_installed_package_version()


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: object, label: str) -> str:
    text = str(value).lower()
    if len(text) != 64 or any(character not in _HEX for character in text):
        raise ReleaseProvenanceError(f"{label} must be a SHA-256 digest")
    return text


def _text(value: object, label: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ReleaseProvenanceError(f"{label} must be non-empty")
    return text


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReleaseProvenanceError(f"{label} must be an object")
    return value


def _load_json_object(path: Path, label: str) -> tuple[Mapping[str, Any], bytes]:
    try:
        content = path.read_bytes()
    except OSError as error:
        raise ReleaseProvenanceError(f"{label} not found at {path}") from error
    try:
        raw = json.loads(content)
    except json.JSONDecodeError as error:
        raise ReleaseProvenanceError(
            f"{label} at {path} is not valid JSON: {error}"
        ) from error
    if not isinstance(raw, Mapping):
        raise ReleaseProvenanceError(f"{label} at {path} must be an object")
    canonical = _canonical_bytes(raw)
    if content not in {canonical, canonical + b"\n"}:
        raise ReleaseProvenanceError(f"{label} is not canonical JSON")
    return raw, content


@dataclasses.dataclass(frozen=True, slots=True)
class DependencyIdentity:
    version: str
    source_sha256: Mapping[str, str]


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseIdentity:
    schema: str
    package_version: str
    build_input_sha256: str
    workspace_id: str
    lakehouse_id: str
    environment_id: str
    sjd_ids: Mapping[str, str]
    release_evidence_root: str
    deployment_intent: str

    @property
    def allowed_package_version(self) -> str:
        """Compatibility spelling for older expected-version call sites."""
        return self.package_version


@dataclasses.dataclass(frozen=True, slots=True)
class DetachedReleaseManifest:
    schema: str
    release_identity_sha256: str
    package_version: str
    wheel_filename: str
    wheel_sha256: str
    cpu_lock_filename: str
    cpu_lock_sha256: str
    cpu_bundle_filename: str
    cpu_bundle_sha256: str
    build_input_sha256: str
    workspace_id: str
    lakehouse_id: str
    environment_id: str
    sjd_ids: Mapping[str, str]
    public_dependencies: Mapping[str, DependencyIdentity]
    reviewer: str
    reviewed_at: str
    deployment_intent: str


@dataclasses.dataclass(frozen=True, slots=True)
class PostPublishReceipt:
    schema: str
    detached_manifest_sha256: str
    package_version: str
    environment_id: str
    environment_target_version: str
    public_package_policy_sha256: str
    library_readback_sha256: str
    sjd_readback_sha256: str
    operations: tuple[Mapping[str, Any], ...]
    published_at: str
    recorded_at: str


@dataclasses.dataclass(frozen=True, slots=True)
class RuntimeReleaseEvidence:
    identity: ReleaseIdentity
    manifest: DetachedReleaseManifest
    receipt: PostPublishReceipt
    identity_sha256: str
    manifest_sha256: str
    receipt_sha256: str


def _parse_identity(raw: Mapping[str, Any]) -> ReleaseIdentity:
    return ReleaseIdentity(
        schema=_text(raw.get("schema"), "identity.schema"),
        package_version=_text(
            raw.get("package_version"), "identity.package_version"
        ),
        build_input_sha256=_digest(
            raw.get("build_input_sha256"), "identity.build_input_sha256"
        ),
        workspace_id=_text(raw.get("workspace_id"), "identity.workspace_id"),
        lakehouse_id=_text(raw.get("lakehouse_id"), "identity.lakehouse_id"),
        environment_id=_text(
            raw.get("environment_id"), "identity.environment_id"
        ),
        sjd_ids={
            str(name): _text(value, f"identity.sjd_ids.{name}")
            for name, value in _mapping(
                raw.get("sjd_ids"), "identity.sjd_ids"
            ).items()
        },
        release_evidence_root=_text(
            raw.get("release_evidence_root"),
            "identity.release_evidence_root",
        ),
        deployment_intent=_text(
            raw.get("deployment_intent"), "identity.deployment_intent"
        ),
    )


def load_release_identity(
    path: Path = _IDENTITY_PATH,
) -> tuple[ReleaseIdentity, str]:
    raw, content = _load_json_object(path, "release identity")
    identity = _parse_identity(raw)
    if identity.schema != "people-counter-candidate-a-release-identity-v1":
        raise ReleaseProvenanceError("unsupported release identity schema")
    return identity, _sha256_bytes(content)


RELEASE_IDENTITY, RELEASE_IDENTITY_SHA256 = load_release_identity()


def detached_manifest_evidence_path(
    identity: ReleaseIdentity = RELEASE_IDENTITY,
) -> str:
    """Return the sole runtime path accepted for the detached manifest."""
    return f"{identity.release_evidence_root.rstrip('/')}/detached-manifest.json"


def postpublish_receipt_evidence_path(
    identity: ReleaseIdentity = RELEASE_IDENTITY,
) -> str:
    """Return the sole runtime path accepted for the postpublish receipt."""
    return f"{identity.release_evidence_root.rstrip('/')}/postpublish-receipt.json"


def _parse_dependencies(value: object) -> Mapping[str, DependencyIdentity]:
    dependencies = _mapping(value, "manifest.public_dependencies")
    return {
        str(name): DependencyIdentity(
            version=_text(entry.get("version"), f"dependency {name}.version"),
            source_sha256={
                str(path): _digest(digest, f"dependency {name}.{path}")
                for path, digest in _mapping(
                    entry.get("source_sha256"),
                    f"dependency {name}.source_sha256",
                ).items()
            },
        )
        for name, raw_entry in dependencies.items()
        for entry in [_mapping(raw_entry, f"dependency {name}")]
    }


def load_detached_release_manifest(
    path: Path,
    *,
    expected_sha256: str,
    identity: ReleaseIdentity = RELEASE_IDENTITY,
    identity_sha256: str = RELEASE_IDENTITY_SHA256,
) -> tuple[DetachedReleaseManifest, str]:
    raw, content = _load_json_object(path, "detached release manifest")
    observed_sha256 = _sha256_bytes(content)
    if observed_sha256 != _digest(expected_sha256, "expected manifest hash"):
        raise ReleaseProvenanceError("detached release manifest hash mismatch")
    manifest = DetachedReleaseManifest(
        schema=_text(raw.get("schema"), "manifest.schema"),
        release_identity_sha256=_digest(
            raw.get("release_identity_sha256"),
            "manifest.release_identity_sha256",
        ),
        package_version=_text(
            raw.get("package_version"), "manifest.package_version"
        ),
        wheel_filename=_text(
            raw.get("wheel_filename"), "manifest.wheel_filename"
        ),
        wheel_sha256=_digest(
            raw.get("wheel_sha256"), "manifest.wheel_sha256"
        ),
        cpu_lock_filename=_text(
            raw.get("cpu_lock_filename"), "manifest.cpu_lock_filename"
        ),
        cpu_lock_sha256=_digest(
            raw.get("cpu_lock_sha256"), "manifest.cpu_lock_sha256"
        ),
        cpu_bundle_filename=_text(
            raw.get("cpu_bundle_filename"), "manifest.cpu_bundle_filename"
        ),
        cpu_bundle_sha256=_digest(
            raw.get("cpu_bundle_sha256"), "manifest.cpu_bundle_sha256"
        ),
        build_input_sha256=_digest(
            raw.get("build_input_sha256"), "manifest.build_input_sha256"
        ),
        workspace_id=_text(raw.get("workspace_id"), "manifest.workspace_id"),
        lakehouse_id=_text(raw.get("lakehouse_id"), "manifest.lakehouse_id"),
        environment_id=_text(
            raw.get("environment_id"), "manifest.environment_id"
        ),
        sjd_ids={
            str(name): _text(value, f"manifest.sjd_ids.{name}")
            for name, value in _mapping(
                raw.get("sjd_ids"), "manifest.sjd_ids"
            ).items()
        },
        public_dependencies=_parse_dependencies(
            raw.get("public_dependencies")
        ),
        reviewer=_text(raw.get("reviewer"), "manifest.reviewer"),
        reviewed_at=_text(raw.get("reviewed_at"), "manifest.reviewed_at"),
        deployment_intent=_text(
            raw.get("deployment_intent"), "manifest.deployment_intent"
        ),
    )
    if manifest.schema != "people-counter-candidate-a-detached-release-v1":
        raise ReleaseProvenanceError("unsupported detached manifest schema")
    expected_identity = {
        "release_identity_sha256": identity_sha256,
        "workspace_id": identity.workspace_id,
        "lakehouse_id": identity.lakehouse_id,
        "environment_id": identity.environment_id,
        "sjd_ids": dict(identity.sjd_ids),
        "deployment_intent": identity.deployment_intent,
    }
    observed_identity = {
        "release_identity_sha256": manifest.release_identity_sha256,
        "workspace_id": manifest.workspace_id,
        "lakehouse_id": manifest.lakehouse_id,
        "environment_id": manifest.environment_id,
        "sjd_ids": dict(manifest.sjd_ids),
        "deployment_intent": manifest.deployment_intent,
    }
    if observed_identity != expected_identity:
        raise ReleaseProvenanceError(
            "detached manifest does not bind the wheel's stable release identity"
        )
    return manifest, observed_sha256


def load_postpublish_receipt(
    path: Path,
    *,
    expected_sha256: str,
    manifest: DetachedReleaseManifest,
    manifest_sha256: str,
) -> tuple[PostPublishReceipt, str]:
    raw, content = _load_json_object(path, "postpublish receipt")
    observed_sha256 = _sha256_bytes(content)
    if observed_sha256 != _digest(expected_sha256, "expected receipt hash"):
        raise ReleaseProvenanceError("postpublish receipt hash mismatch")
    operation_values = raw.get("operations")
    if not isinstance(operation_values, list) or not operation_values:
        raise ReleaseProvenanceError("receipt.operations must be a non-empty list")
    receipt = PostPublishReceipt(
        schema=_text(raw.get("schema"), "receipt.schema"),
        detached_manifest_sha256=_digest(
            raw.get("detached_manifest_sha256"),
            "receipt.detached_manifest_sha256",
        ),
        package_version=_text(
            raw.get("package_version"), "receipt.package_version"
        ),
        environment_id=_text(
            raw.get("environment_id"), "receipt.environment_id"
        ),
        environment_target_version=_text(
            raw.get("environment_target_version"),
            "receipt.environment_target_version",
        ),
        public_package_policy_sha256=_digest(
            raw.get("public_package_policy_sha256"),
            "receipt.public_package_policy_sha256",
        ),
        library_readback_sha256=_digest(
            raw.get("library_readback_sha256"),
            "receipt.library_readback_sha256",
        ),
        sjd_readback_sha256=_digest(
            raw.get("sjd_readback_sha256"),
            "receipt.sjd_readback_sha256",
        ),
        operations=tuple(
            dict(_mapping(operation, "receipt operation"))
            for operation in operation_values
        ),
        published_at=_text(raw.get("published_at"), "receipt.published_at"),
        recorded_at=_text(raw.get("recorded_at"), "receipt.recorded_at"),
    )
    if receipt.schema != "people-counter-candidate-a-postpublish-receipt-v1":
        raise ReleaseProvenanceError("unsupported postpublish receipt schema")
    if (
        receipt.detached_manifest_sha256 != manifest_sha256
        or receipt.package_version != manifest.package_version
        or receipt.environment_id != manifest.environment_id
    ):
        raise ReleaseProvenanceError(
            "postpublish receipt does not bind the detached manifest"
        )
    return receipt, observed_sha256


def load_runtime_release_evidence(
    manifest_path: Path,
    receipt_path: Path,
    *,
    expected_manifest_sha256: str,
    expected_receipt_sha256: str,
    observed_package_version: str | None = None,
) -> RuntimeReleaseEvidence:
    manifest, manifest_sha256 = load_detached_release_manifest(
        manifest_path, expected_sha256=expected_manifest_sha256
    )
    receipt, receipt_sha256 = load_postpublish_receipt(
        receipt_path,
        expected_sha256=expected_receipt_sha256,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
    )
    validate_installed_package_version(
        observed_version=observed_package_version,
        expected_version=manifest.package_version,
    )
    return RuntimeReleaseEvidence(
        identity=RELEASE_IDENTITY,
        manifest=manifest,
        receipt=receipt,
        identity_sha256=RELEASE_IDENTITY_SHA256,
        manifest_sha256=manifest_sha256,
        receipt_sha256=receipt_sha256,
    )


def validate_installed_package_version(
    *,
    observed_version: str | None = None,
    expected_version: str | None = None,
) -> None:
    observed = (
        observed_version
        if observed_version is not None
        else resolve_installed_package_version()
    )
    expected = expected_version or RELEASE_IDENTITY.package_version
    if observed != expected:
        raise ReleaseProvenanceError(
            f"installed {PACKAGE_DISTRIBUTION} version {observed!r} does not "
            f"match detached expected version {expected!r}"
        )


def verify_file_sha256(path: Path, expected_sha256: str, *, label: str) -> str:
    hasher = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                hasher.update(chunk)
    except OSError as error:
        raise ReleaseProvenanceError(f"{label} not readable at {path}") from error
    observed = hasher.hexdigest()
    if observed != _digest(expected_sha256, f"{label} expected hash"):
        raise ReleaseProvenanceError(
            f"{label} at {path} has sha256 {observed}, expected {expected_sha256}"
        )
    return observed


def build_input_sha256(root: Path, relative_paths: Sequence[str]) -> str:
    """Hash a deterministic path/digest inventory of release build inputs."""
    inventory: list[dict[str, str]] = []
    seen: set[str] = set()
    for relative in sorted(relative_paths):
        if relative in seen:
            raise ReleaseProvenanceError(f"duplicate build input {relative!r}")
        seen.add(relative)
        path = root / relative
        if not path.is_file() or path.is_symlink():
            raise ReleaseProvenanceError(f"invalid build input {relative!r}")
        inventory.append(
            {"path": relative, "sha256": verify_file_sha256_unchecked(path)}
        )
    if not inventory:
        raise ReleaseProvenanceError("build inputs must not be empty")
    return _sha256_bytes(_canonical_bytes(inventory))


def release_build_input_sha256(root: Path) -> str:
    """Hash every logical input to the project wheel.

    The embedded release-identity file is deliberately excluded. It is
    release metadata, not source/build input, and including its own
    ``build_input_sha256`` field would create a self-referential hash chain.
    The detached manifest independently binds both this source/build-input
    digest and the exact embedded identity digest; the wheel digest then
    binds the final packaged bytes, including that identity file.
    """
    package_root = root / "src" / "people_counter"
    identity_path = package_root / "data" / "candidate_a_release_identity.json"
    paths = [root / "pyproject.toml", root / "README.md"]
    paths.extend(
        path
        for path in package_root.rglob("*")
        if path.is_file()
        and path != identity_path
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix != ".pyc"
    )
    inventory: list[dict[str, str]] = []
    for path in sorted(paths):
        if not path.is_file() or path.is_symlink():
            raise ReleaseProvenanceError(
                f"invalid release build input {path.relative_to(root).as_posix()!r}"
            )
        inventory.append(
            {
                "path": path.relative_to(root).as_posix(),
                "sha256": verify_file_sha256_unchecked(path),
            }
        )
    if not identity_path.is_file() or identity_path.is_symlink():
        raise ReleaseProvenanceError("release identity is absent or invalid")
    return _sha256_bytes(_canonical_bytes(inventory))


def verify_file_sha256_unchecked(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def create_immutable_json(path: Path, value: Mapping[str, Any]) -> str:
    """Create canonical JSON once, read it back, and reject conflicting writes."""
    content = _canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    except FileExistsError:
        try:
            existing = path.read_bytes()
        except OSError as error:
            raise ReleaseProvenanceError(
                f"immutable evidence unreadable at {path}"
            ) from error
        if existing != content:
            raise ReleaseProvenanceError(
                f"immutable evidence conflicts at {path}"
            )
    else:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    if path.read_bytes() != content:
        raise ReleaseProvenanceError(
            f"immutable evidence readback differs at {path}"
        )
    return _sha256_bytes(content)


@dataclasses.dataclass(frozen=True, slots=True)
class TrackersDistributionIdentity:
    version: str | None
    is_vcs_installed: bool
    vcs_direct_url: str | None
    source_sha256: Mapping[str, str]
    resolution_error: str | None


def observe_trackers_distribution_identity(
    tracked_files: tuple[str, ...] = ("core/botsort/tracker.py",),
    *,
    distribution: str = "trackers",
) -> TrackersDistributionIdentity:
    try:
        dist = importlib.metadata.distribution(distribution)
    except importlib.metadata.PackageNotFoundError as error:
        return TrackersDistributionIdentity(
            version=None,
            is_vcs_installed=False,
            vcs_direct_url=None,
            source_sha256={},
            resolution_error=f"{distribution!r} distribution is not installed: {error}",
        )
    direct_url_text = dist.read_text("direct_url.json")
    direct_url: Mapping[str, Any] = {}
    if direct_url_text:
        try:
            decoded = json.loads(direct_url_text)
        except json.JSONDecodeError:
            decoded = {}
        if isinstance(decoded, Mapping):
            direct_url = decoded
    is_vcs_installed = "vcs_info" in direct_url
    source_sha256: dict[str, str] = {}
    resolution_error: str | None = None
    try:
        package_root = importlib.resources.files(distribution)
        for relative_path in tracked_files:
            data = package_root.joinpath(relative_path).read_bytes()
            source_sha256[relative_path] = _sha256_bytes(data)
    except (FileNotFoundError, ModuleNotFoundError, OSError) as error:
        resolution_error = (
            f"could not read tracked {distribution!r} source file(s): {error}"
        )
    return TrackersDistributionIdentity(
        version=dist.version,
        is_vcs_installed=is_vcs_installed,
        vcs_direct_url=(
            str(direct_url.get("url")) if is_vcs_installed else None
        ),
        source_sha256=source_sha256,
        resolution_error=resolution_error,
    )


def validate_trackers_distribution_identity(
    observed: TrackersDistributionIdentity | None = None,
    *,
    manifest: DetachedReleaseManifest,
) -> TrackersDistributionIdentity:
    value = observed or observe_trackers_distribution_identity()
    expected = manifest.public_dependencies.get("trackers")
    if expected is None:
        raise ReleaseProvenanceError(
            "detached release manifest has no trackers identity"
        )
    if value.resolution_error is not None:
        raise ReleaseProvenanceError(
            f"could not observe trackers identity: {value.resolution_error}"
        )
    if value.is_vcs_installed:
        raise ReleaseProvenanceError(
            "trackers is VCS-installed and cannot be proven by Fabric public "
            "library readback"
        )
    if value.version != expected.version:
        raise ReleaseProvenanceError("installed trackers version mismatch")
    for relative_path, expected_hash in expected.source_sha256.items():
        if value.source_sha256.get(relative_path) != expected_hash:
            raise ReleaseProvenanceError(
                f"installed trackers file {relative_path!r} hash mismatch"
            )
    return value

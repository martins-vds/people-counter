"""Portable SDK version compatibility, derived from wheel metadata only."""

from __future__ import annotations

import hashlib
import importlib.metadata
import warnings
from dataclasses import dataclass
from typing import Callable

PACKAGE_DISTRIBUTION = "people-counter"


class ReleaseProvenanceError(RuntimeError):
    """The installed SDK version is unavailable or incompatible with work."""


def resolve_installed_package_version(
    distribution: str = PACKAGE_DISTRIBUTION,
) -> str:
    try:
        version = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError as error:
        raise ReleaseProvenanceError(
            f"{distribution} is not installed; install the SDK wheel"
        ) from error
    if not version.strip():
        raise ReleaseProvenanceError(f"{distribution} has empty package version metadata")
    return version


@dataclass(frozen=True, slots=True)
class InstalledRelease:
    package_version: str
    digest: str


def installed_release() -> InstalledRelease:
    """Keep work from different SDK versions separate, without deployment IO."""
    version = resolve_installed_package_version()
    digest = hashlib.sha256(f"{PACKAGE_DISTRIBUTION}=={version}".encode("utf-8")).hexdigest()
    return InstalledRelease(version, digest)


def registration_release_digest(
    read_text: Callable[[str], str] | None = None,
    *,
    deployment: object | None = None,
    expected_digest: str = "",
) -> str:
    if read_text is not None or deployment is not None or expected_digest:
        warnings.warn(
            "Release files and deployment digests are no longer used. "
            "Call registration_release_digest() without arguments.",
            FutureWarning,
            stacklevel=2,
        )
    return installed_release().digest


def validate_installed_package_version(
    *,
    expected_version: str,
    observed_version: str | None = None,
) -> None:
    observed = (
        observed_version
        if observed_version is not None
        else resolve_installed_package_version()
    )
    if observed != expected_version:
        raise ReleaseProvenanceError(
            f"installed {PACKAGE_DISTRIBUTION} version {observed!r} does not "
            f"match expected version {expected_version!r}"
        )

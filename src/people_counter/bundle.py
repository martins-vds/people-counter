"""Build platform-specific, offline-installable SDK wheel bundles."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
from collections.abc import Sequence
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, TypedDict


Variant = Literal["cpu", "gpu"]

PYPI_INDEX = "https://pypi.org/simple"
PYTORCH_INDEXES: dict[Variant, str] = {
    "cpu": "https://download.pytorch.org/whl/cpu",
    "gpu": "https://download.pytorch.org/whl/cu128",
}


class ArtifactManifest(TypedDict):
    path: str
    sha256: str
    size: int


class BundleManifest(TypedDict):
    schema_version: int
    sdk_name: str
    sdk_version: str
    variant: Variant
    python_version: str
    implementation: str
    platform: str
    pytorch_index: str
    artifacts: list[ArtifactManifest]


def bundle_name(variant: Variant) -> str:
    sdk_version = version("people-counter")
    python_tag = sys.implementation.cache_tag or "python"
    platform_tag = sysconfig.get_platform()
    raw_name = (
        f"people-counter-{sdk_version}-{variant}-{platform_tag}-{python_tag}"
    )
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", raw_name)


def build_bundle(
    project_root: Path,
    variant: Variant,
    output_directory: Path,
    *,
    force: bool = False,
) -> Path:
    """Build and archive an SDK wheelhouse for the current platform."""
    project_root = project_root.resolve()
    _validate_project_root(project_root)
    output_directory = output_directory.expanduser().resolve()
    output_directory.mkdir(parents=True, exist_ok=True)

    name = bundle_name(variant)
    bundle_path = output_directory / f"{name}.zip"
    if bundle_path.exists() and not force:
        raise FileExistsError(
            f"Bundle already exists: {bundle_path}; pass --force to replace it"
        )

    with TemporaryDirectory(
        prefix=f".{name}-",
        dir=output_directory,
    ) as temporary_directory:
        temporary_root = Path(temporary_directory)
        bundle_directory = temporary_root / name
        wheels_directory = bundle_directory / "wheels"
        wheels_directory.mkdir(parents=True)

        requirements_path = bundle_directory / f"requirements-{variant}.lock"
        vcs_requirements_path = bundle_directory / "requirements-vcs.lock"
        raw_requirements_path = temporary_root / "requirements.raw"
        _run(
            _export_command(variant, raw_requirements_path),
            project_root,
        )
        has_vcs_requirements = _write_requirements(
            raw_requirements_path,
            requirements_path,
            vcs_requirements_path,
            variant,
        )

        _run(_sdk_build_command(wheels_directory), project_root)
        (wheels_directory / ".gitignore").unlink(missing_ok=True)
        _run(
            _dependency_wheel_command(
                requirements_path,
                wheels_directory,
            ),
            project_root,
        )
        if has_vcs_requirements:
            _run(
                _vcs_wheel_command(
                    vcs_requirements_path,
                    wheels_directory,
                ),
                project_root,
            )
        _validate_wheelhouse(wheels_directory)

        readme_path = bundle_directory / "README.txt"
        readme_path.write_text(
            _bundle_readme(name, variant),
            encoding="utf-8",
        )
        manifest_path = bundle_directory / "manifest.json"
        manifest = _build_manifest(
            bundle_directory,
            variant,
            (
                [requirements_path, vcs_requirements_path]
                if has_vcs_requirements
                else [requirements_path]
            ),
            readme_path,
            wheels_directory,
        )
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        archive_base = temporary_root / f"{name}.archive"
        archive = Path(
            shutil.make_archive(
                str(archive_base),
                "zip",
                root_dir=bundle_directory.parent,
                base_dir=bundle_directory.name,
            )
        )
        archive.replace(bundle_path)

    return bundle_path


def _export_command(
    variant: Variant,
    output_path: Path,
) -> list[str]:
    return [
        "uv",
        "export",
        "--quiet",
        "--extra",
        variant,
        "--no-dev",
        "--no-emit-project",
        "--no-annotate",
        "--no-header",
        "--locked",
        "--output-file",
        str(output_path),
    ]


def _sdk_build_command(wheels_directory: Path) -> list[str]:
    return [
        "uv",
        "build",
        "--wheel",
        "--out-dir",
        str(wheels_directory),
    ]


def _dependency_wheel_command(
    requirements_path: Path,
    wheels_directory: Path,
) -> list[str]:
    return [
        "uv",
        "run",
        "--isolated",
        "--no-project",
        "--with",
        "pip",
        "python",
        "-m",
        "pip",
        "--disable-pip-version-check",
        "wheel",
        "--require-hashes",
        "--requirement",
        str(requirements_path),
        "--wheel-dir",
        str(wheels_directory),
    ]


def _vcs_wheel_command(
    requirements_path: Path,
    wheels_directory: Path,
) -> list[str]:
    return [
        "uv",
        "run",
        "--isolated",
        "--no-project",
        "--with",
        "pip",
        "python",
        "-m",
        "pip",
        "--disable-pip-version-check",
        "wheel",
        "--no-deps",
        "--requirement",
        str(requirements_path),
        "--wheel-dir",
        str(wheels_directory),
    ]


def _write_requirements(
    raw_path: Path,
    output_path: Path,
    vcs_output_path: Path,
    variant: Variant,
) -> bool:
    locked_requirements = raw_path.read_text(encoding="utf-8")
    registry_blocks, vcs_blocks = _partition_requirement_blocks(
        locked_requirements
    )
    unhashed = [
        block.splitlines()[0]
        for block in registry_blocks
        if "--hash=" not in block
    ]
    if unhashed:
        raise RuntimeError(
            "Lock export contains unhashed non-VCS requirements: "
            + ", ".join(unhashed)
        )

    registry_requirements = "\n".join(registry_blocks)
    output_path.write_text(
        (
            "# Locked people-counter "
            f"{variant.upper()} deployment dependencies.\n"
            f"--index-url {PYPI_INDEX}\n"
            f"--extra-index-url {PYTORCH_INDEXES[variant]}\n\n"
            f"{registry_requirements}\n"
        ),
        encoding="utf-8",
    )
    if not vcs_blocks:
        return False

    vcs_requirements = "\n".join(vcs_blocks)
    vcs_output_path.write_text(
        (
            "# Pinned VCS dependencies built separately because pip cannot "
            "hash repositories.\n"
            f"{vcs_requirements}\n"
        ),
        encoding="utf-8",
    )
    return True


def _partition_requirement_blocks(
    locked_requirements: str,
) -> tuple[list[str], list[str]]:
    blocks: list[str] = []
    current: list[str] = []
    for line in locked_requirements.splitlines():
        if not line.strip():
            continue
        if line[:1].isspace():
            if not current:
                raise RuntimeError(
                    "Lock export starts with a continuation line"
                )
            current.append(line)
            continue
        if current:
            blocks.append("\n".join(current))
        current = [line]
    if current:
        blocks.append("\n".join(current))

    vcs_blocks = [
        block
        for block in blocks
        if re.search(r"\s@\s(?:git|hg|svn|bzr)\+", block.splitlines()[0])
    ]
    registry_blocks = [
        block for block in blocks if block not in vcs_blocks
    ]
    return registry_blocks, vcs_blocks


def _validate_wheelhouse(wheels_directory: Path) -> None:
    wheels = sorted(wheels_directory.glob("*.whl"))
    if not wheels:
        raise RuntimeError("Bundle build produced no wheels")
    if not any(wheel.name.startswith("people_counter-") for wheel in wheels):
        raise RuntimeError("Bundle build did not produce the people-counter wheel")
    unexpected = [
        path
        for path in wheels_directory.iterdir()
        if path.is_file() and path.suffix != ".whl"
    ]
    if unexpected:
        names = ", ".join(path.name for path in sorted(unexpected))
        raise RuntimeError(f"Bundle contains non-wheel dependencies: {names}")


def _build_manifest(
    bundle_directory: Path,
    variant: Variant,
    requirements_paths: Sequence[Path],
    readme_path: Path,
    wheels_directory: Path,
) -> BundleManifest:
    artifact_paths = [
        *requirements_paths,
        readme_path,
        *sorted(wheels_directory.glob("*.whl")),
    ]
    return {
        "schema_version": 1,
        "sdk_name": "people-counter",
        "sdk_version": version("people-counter"),
        "variant": variant,
        "python_version": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": sysconfig.get_platform(),
        "pytorch_index": PYTORCH_INDEXES[variant],
        "artifacts": [
            {
                "path": path.relative_to(bundle_directory).as_posix(),
                "sha256": _sha256(path),
                "size": path.stat().st_size,
            }
            for path in artifact_paths
        ],
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        while chunk := artifact.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _bundle_readme(name: str, variant: Variant) -> str:
    return (
        f"People Counter {variant.upper()} SDK bundle\n"
        f"{'=' * (27 + len(variant))}\n\n"
        "This bundle is specific to the Python version and platform named in "
        "manifest.json.\n\n"
        "Install offline from the extracted bundle directory:\n\n"
        f"  python -m pip install --no-index --find-links wheels "
        f"'people-counter[{variant}]=={version('people-counter')}'\n\n"
        "Verify artifact checksums against manifest.json before installation.\n"
        f"Bundle directory: {name}\n"
    )


def _validate_project_root(project_root: Path) -> None:
    missing = [
        name
        for name in ("pyproject.toml", "uv.lock")
        if not (project_root / name).is_file()
    ]
    if missing:
        raise FileNotFoundError(
            f"Project root {project_root} is missing: {', '.join(missing)}"
        )


def _run(command: Sequence[str], project_root: Path) -> None:
    subprocess.run(
        command,
        cwd=project_root,
        check=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a platform-specific people-counter SDK wheelhouse bundle."
        )
    )
    parser.add_argument(
        "variant",
        choices=tuple(PYTORCH_INDEXES),
        help="Dependency and PyTorch variant to bundle.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("dist") / "sdk-bundles",
        help="Bundle destination (default: dist/sdk-bundles).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Atomically replace an existing bundle with the same name.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the selected build inputs without downloading or writing.",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    project_root: Path | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = (project_root or Path.cwd()).resolve()
    variant: Variant = args.variant

    try:
        _validate_project_root(root)
        if args.dry_run:
            print(f"Variant: {variant}")
            print(f"Project root: {root}")
            print(f"Output: {(root / args.output_dir).resolve()}")
            print(f"PyTorch index: {PYTORCH_INDEXES[variant]}")
            return 0
        bundle_path = build_bundle(
            root,
            variant,
            root / args.output_dir,
            force=args.force,
        )
    except (FileExistsError, FileNotFoundError) as error:
        parser.error(str(error))

    print(f"Created SDK bundle: {bundle_path}")
    print(f"SHA256: {_sha256(bundle_path)}")
    return 0

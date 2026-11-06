import contextlib
import hashlib
import io
import json
import shutil
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import toml

from people_counter import bundle
from people_counter.bundle import (
    PYPI_INDEX,
    PYTORCH_INDEXES,
    _bundle_readme,
    _dependency_wheel_command,
    _export_command,
    _run,
    _sdk_build_command,
    _sha256,
    _validate_wheelhouse,
    _vcs_wheel_command,
    _write_requirements,
    build_bundle,
    bundle_name,
    main,
)


class BundleBuilderTests(unittest.TestCase):
    def _fabric_project(self):
        project = {
            "project": {
                "name": "people-counter",
                "version": "1.2.3",
                "dependencies": [
                    "numpy>=1.24",
                    "opencv-python>=4.11.0.86",
                ],
                "scripts": {"people-counter": "people_counter.cli:main"},
                "optional-dependencies": {
                    "cpu": [
                        "libreyolo==1.5.0",
                        "supervision==0.30.3",
                        "trackers==2.6.0",
                        "torch",
                        "torchvision",
                    ]
                },
            },
            "tool": {
                "uv": {
                    "conflicts": [[{"extra": "cpu"}, {"extra": "gpu"}]],
                    "sources": {"torch": [{"index": "pytorch-cpu", "extra": "cpu"}]},
                }
            },
        }
        original = toml.dumps(project)
        (self.project_root / "pyproject.toml").write_text(original)
        source = self.project_root / "src" / "people_counter"
        source.mkdir(parents=True)
        (source / "__init__.py").write_text('"""Test SDK."""\n')
        (self.project_root / "README.md").write_text("Test SDK\n")
        return project

    def test_fabric_onnx_profile_isolated_from_normal_project(self):
        project = self._fabric_project()
        original = toml.dumps(project)
        target = self.project_root / "fabric-profile"

        bundle._prepare_fabric_onnx_project(self.project_root, target)

        profile = toml.load(target / "pyproject.toml")
        self.assertEqual((self.project_root / "pyproject.toml").read_text(), original)
        self.assertEqual(profile["project"]["name"], "people-counter")
        self.assertEqual(profile["project"]["version"], "1.2.3")
        self.assertEqual(profile["project"]["scripts"], project["project"]["scripts"])
        self.assertEqual(profile["project"]["requires-python"], ">=3.13,<3.14")
        self.assertEqual(
            profile["project"]["dependencies"],
            ["numpy>=1.24", "opencv-python-headless==5.0.0.93"],
        )
        self.assertEqual(
            profile["project"]["optional-dependencies"],
            {"fabric-onnx": ["supervision==0.30.3", "torch", "torchvision", "onnxruntime>=1.24,<2"]},
        )
        self.assertNotIn("conflicts", profile["tool"]["uv"])
        self.assertEqual(
            profile["tool"]["uv"]["sources"],
            {"torch": {"index": "pytorch-cpu"}, "torchvision": {"index": "pytorch-cpu"}},
        )
        self.assertEqual((target / "uv.lock").read_text(), "version = 1\n")
        self.assertTrue((target / "src" / "people_counter" / "__init__.py").is_file())
        self.assertEqual((target / "README.md").read_text(), "Test SDK\n")

    def test_fabric_onnx_profile_rejects_unexpected_core_metadata(self):
        (self.project_root / "pyproject.toml").write_text(
            "[project]\nname='people-counter'\ndependencies=['numpy>=1.24']\n"
        )
        with self.assertRaisesRegex(RuntimeError, "exactly one opencv-python"):
            bundle._prepare_fabric_onnx_project(
                self.project_root, self.project_root / "fabric-profile"
            )

    def test_fabric_wheelhouse_requires_only_headless_opencv(self):
        wheels = self.project_root / "wheels"
        wheels.mkdir()
        (wheels / "opencv_python_headless-5.0.0.93-cp37-abi3-linux_x86_64.whl").touch()
        bundle._validate_fabric_wheelhouse(wheels)
        for package in (
            "opencv_python", "opencv_contrib_python", "opencv_contrib_python_headless",
            "libreyolo", "trackers", "rfdetr",
        ):
            with self.subTest(package=package):
                wheel = wheels / f"{package}-1.0-py3-none-any.whl"
                wheel.touch()
                with self.assertRaisesRegex(RuntimeError, "Fabric ONNX"):
                    bundle._validate_fabric_wheelhouse(wheels)
                wheel.unlink()
        next(wheels.iterdir()).unlink()
        with self.assertRaisesRegex(RuntimeError, "Fabric ONNX"):
            bundle._validate_fabric_wheelhouse(wheels)

    def test_fabric_bundle_uses_staged_metadata_and_a_distinct_wheel(self):
        self._fabric_project()
        original = (self.project_root / "pyproject.toml").read_text()

        def run(command, project_root):
            self.assertEqual(project_root.name, "fabric-project")
            profile = toml.load(project_root / "pyproject.toml")
            self.assertEqual(
                profile["project"]["dependencies"][-1],
                "opencv-python-headless==5.0.0.93",
            )
            if command[:2] == ["uv", "lock"]:
                return
            if command[:2] == ["uv", "export"]:
                self.assertEqual(command[command.index("--extra") + 1], "fabric-onnx")
                Path(command[command.index("--output-file") + 1]).write_text(
                    "opencv-python-headless==5.0.0.93 \\\n    --hash=sha256:headless\n"
                )
                return
            if command[:2] == ["uv", "build"]:
                self.assertIn(
                    "--config-setting=--build-option=--build-number=1fabriconnx",
                    command,
                )
                wheels = Path(command[command.index("--out-dir") + 1])
                (wheels / "people_counter-1.2.3-1fabriconnx-py3-none-any.whl").touch()
                return
            if "pip" in command and "wheel" in command:
                self.assertNotIn("--no-deps", command)
                wheels = Path(command[command.index("--wheel-dir") + 1])
                (wheels / "opencv_python_headless-5.0.0.93-cp37-abi3-linux_x86_64.whl").touch()
                return
            self.fail(f"Unexpected command: {command}")

        with (
            patch("people_counter.bundle.sys.version_info", (3, 13, 0)),
            patch("people_counter.bundle.platform.system", return_value="Linux"),
            patch("people_counter.bundle.platform.machine", return_value="x86_64"),
            patch("people_counter.bundle.version", return_value="1.2.3"),
            patch("people_counter.bundle._run", side_effect=run),
        ):
            archive_path = build_bundle(
                self.project_root, "fabric-onnx", self.output_directory
            )

        self.assertEqual((self.project_root / "pyproject.toml").read_text(), original)
        with zipfile.ZipFile(archive_path) as archive:
            root = archive.namelist()[0].split("/", 1)[0]
            manifest = json.loads(archive.read(f"{root}/manifest.json"))
            self.assertEqual(manifest["variant"], "fabric-onnx")
            self.assertEqual(manifest["pytorch_index"], PYTORCH_INDEXES["cpu"])
            self.assertTrue(any("1fabriconnx" in name for name in archive.namelist()))

    def test_fabric_bundle_rejects_an_incompatible_build_host(self):
        for python, system, machine in (
            ((3, 12, 0), "Linux", "x86_64"),
            ((3, 13, 0), "Linux", "aarch64"),
            ((3, 13, 0), "Windows", "x86_64"),
        ):
            with (
                self.subTest(python=python, system=system, machine=machine),
                patch("people_counter.bundle.sys.version_info", python),
                patch("people_counter.bundle.platform.system", return_value=system),
                patch("people_counter.bundle.platform.machine", return_value=machine),
                patch("people_counter.bundle._run") as run,
                self.assertRaisesRegex(RuntimeError, "Python 3.13 on Linux x86_64"),
            ):
                build_bundle(self.project_root, "fabric-onnx", self.output_directory)
            run.assert_not_called()
        self.assertFalse(self.output_directory.exists())

    def _fake_run(self, command, project_root):
        self.assertEqual(project_root, self.project_root)
        if command[:2] == ["uv", "export"]:
            output_path = Path(
                command[command.index("--output-file") + 1]
            )
            output_path.write_text(
                (
                    "dependency==1.0 \\\n"
                    "    --hash=sha256:dependency\n"
                    "trackers @ git+https://example.test/trackers.git@commit\n"
                ),
                encoding="utf-8",
            )
            return
        if command[:2] == ["uv", "build"]:
            wheels_path = Path(command[command.index("--out-dir") + 1])
            (wheels_path / ".gitignore").write_text(
                "*\n",
                encoding="utf-8",
            )
            (wheels_path / "people_counter-0.1.0-py3-none-any.whl").write_bytes(
                b"sdk"
            )
            return
        if "pip" in command and "wheel" in command:
            requirements_path = Path(
                command[command.index("--requirement") + 1]
            )
            wheels_path = Path(command[command.index("--wheel-dir") + 1])
            if "--no-deps" in command:
                (wheels_path / "trackers-1.0-py3-none-any.whl").write_bytes(
                    b"trackers"
                )
            else:
                self.exported_requirements = requirements_path.read_text(
                    encoding="utf-8"
                )
                (
                    wheels_path / "dependency-1.0-py3-none-any.whl"
                ).write_bytes(b"dependency")
            return
        self.fail(f"Unexpected command: {command}")

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.project_root = Path(self.temporary_directory.name) / "project"
        self.project_root.mkdir()
        (self.project_root / "pyproject.toml").write_text(
            "[project]\nname='people-counter'\n",
            encoding="utf-8",
        )
        (self.project_root / "uv.lock").write_text(
            "version = 1\n",
            encoding="utf-8",
        )
        self.output_directory = self.project_root / "bundles"
        self.exported_requirements = ""

    def test_build_bundle_isolates_selected_variant(self):
        for variant in ("cpu", "gpu"):
            with self.subTest(variant=variant):
                with patch(
                    "people_counter.bundle._run",
                    side_effect=self._fake_run,
                ):
                    bundle_path = build_bundle(
                        self.project_root,
                        variant,
                        self.output_directory,
                    )

                with zipfile.ZipFile(bundle_path) as archive:
                    names = archive.namelist()
                    bundle_root = names[0].split("/", 1)[0]
                    requirements = archive.read(
                        f"{bundle_root}/requirements-{variant}.lock"
                    ).decode()
                    vcs_requirements = archive.read(
                        f"{bundle_root}/requirements-vcs.lock"
                    ).decode()
                    manifest = json.loads(
                        archive.read(f"{bundle_root}/manifest.json")
                    )

                self.assertIn(PYTORCH_INDEXES[variant], requirements)
                other_variant = "gpu" if variant == "cpu" else "cpu"
                self.assertNotIn(
                    PYTORCH_INDEXES[other_variant],
                    requirements,
                )
                self.assertNotIn("git+", requirements)
                self.assertIn(
                    "trackers @ git+https://example.test/"
                    "trackers.git@commit",
                    vcs_requirements,
                )
                self.assertEqual(manifest["variant"], variant)
                self.assertEqual(
                    manifest["pytorch_index"],
                    PYTORCH_INDEXES[variant],
                )
                self.assertTrue(
                    any(
                        name.endswith("people_counter-0.1.0-py3-none-any.whl")
                        for name in names
                    )
                )
                self.assertFalse(
                    any(name.endswith("/.gitignore") for name in names)
                )

    def test_manifest_checksums_match_archived_artifacts(self):
        with patch(
            "people_counter.bundle._run",
            side_effect=self._fake_run,
        ):
            bundle_path = build_bundle(
                self.project_root,
                "cpu",
                self.output_directory,
            )

        with zipfile.ZipFile(bundle_path) as archive:
            bundle_root = archive.namelist()[0].split("/", 1)[0]
            manifest = json.loads(
                archive.read(f"{bundle_root}/manifest.json")
            )
            self.assertEqual(
                [artifact["path"] for artifact in manifest["artifacts"]],
                [
                    "requirements-cpu.lock",
                    "requirements-vcs.lock",
                    "README.txt",
                    "wheels/dependency-1.0-py3-none-any.whl",
                    "wheels/people_counter-0.1.0-py3-none-any.whl",
                    "wheels/trackers-1.0-py3-none-any.whl",
                ],
            )
            for artifact in manifest["artifacts"]:
                contents = archive.read(
                    f"{bundle_root}/{artifact['path']}"
                )
                self.assertEqual(len(contents), artifact["size"])
                self.assertEqual(
                    hashlib.sha256(contents).hexdigest(),
                    artifact["sha256"],
                )

    def test_build_orchestration_passes_variant_and_bundle_paths(self):
        with (
            patch(
                "people_counter.bundle._run",
                side_effect=self._fake_run,
            ),
            patch(
                "people_counter.bundle._export_command",
                wraps=_export_command,
            ) as export_command,
            patch(
                "people_counter.bundle._dependency_wheel_command",
                wraps=_dependency_wheel_command,
            ) as dependency_command,
            patch(
                "people_counter.bundle._vcs_wheel_command",
                wraps=_vcs_wheel_command,
            ) as vcs_command,
            patch(
                "people_counter.bundle.shutil.make_archive",
                wraps=shutil.make_archive,
            ) as make_archive,
        ):
            build_bundle(
                self.project_root,
                "gpu",
                self.output_directory,
            )

        export_args = export_command.call_args.args
        self.assertEqual(export_args[0], "gpu")
        self.assertEqual(export_args[1].name, "requirements.raw")
        dependency_args = dependency_command.call_args.args
        self.assertEqual(
            dependency_args[0].name,
            "requirements-gpu.lock",
        )
        self.assertEqual(dependency_args[1].name, "wheels")
        vcs_args = vcs_command.call_args.args
        self.assertEqual(vcs_args[0].name, "requirements-vcs.lock")
        self.assertEqual(vcs_args[1].name, "wheels")
        self.assertEqual(
            make_archive.call_args.kwargs["base_dir"],
            bundle_name("gpu"),
        )

    def test_existing_bundle_requires_force(self):
        with patch(
            "people_counter.bundle._run",
            side_effect=self._fake_run,
        ):
            original = build_bundle(
                self.project_root,
                "cpu",
                self.output_directory,
            )
            with self.assertRaisesRegex(
                FileExistsError,
                "pass --force to replace it",
            ):
                build_bundle(
                    self.project_root,
                    "cpu",
                    self.output_directory,
                )
            replacement = build_bundle(
                self.project_root,
                "cpu",
                self.output_directory,
                force=True,
            )

        self.assertEqual(replacement, original)
        self.assertTrue(replacement.is_file())

    def test_build_rejects_missing_project_files(self):
        (self.project_root / "uv.lock").unlink()

        with self.assertRaisesRegex(
            FileNotFoundError,
            "is missing: uv.lock",
        ):
            build_bundle(
                self.project_root,
                "cpu",
                self.output_directory,
            )

    def test_dry_run_does_not_write_or_execute_commands(self):
        output = io.StringIO()
        with (
            patch("people_counter.bundle._run") as run_command,
            contextlib.redirect_stdout(output),
        ):
            exit_code = main(
                ["gpu", "--dry-run", "--output-dir", "artifacts"],
                project_root=self.project_root,
            )

        self.assertEqual(exit_code, 0)
        run_command.assert_not_called()
        self.assertFalse((self.project_root / "artifacts").exists())
        self.assertIn("Variant: gpu", output.getvalue())
        self.assertIn(PYTORCH_INDEXES["gpu"], output.getvalue())

    def test_bundle_name_identifies_variant_and_runtime(self):
        with (
            patch(
                "people_counter.bundle.version",
                return_value="9.8.7+build",
            ),
            patch(
                "people_counter.bundle.sysconfig.get_platform",
                return_value="test platform/x86",
            ),
        ):
            name = bundle_name("cpu")

        self.assertEqual(
            name,
            (
                "people-counter-9.8.7-build-cpu-test-platform-x86-"
                f"{sys.implementation.cache_tag}"
            ),
        )

    def test_build_commands_are_locked_and_isolated(self):
        requirements = Path("/tmp/requirements.lock")
        wheels = Path("/tmp/wheels")

        self.assertEqual(
            _export_command("gpu", requirements),
            [
                "uv",
                "export",
                "--quiet",
                "--extra",
                "gpu",
                "--no-dev",
                "--no-emit-project",
                "--no-annotate",
                "--no-header",
                "--locked",
                "--output-file",
                str(requirements),
            ],
        )
        self.assertEqual(
            _sdk_build_command(wheels),
            [
                "uv",
                "build",
                "--wheel",
                "--out-dir",
                str(wheels),
            ],
        )
        self.assertEqual(
            _dependency_wheel_command(requirements, wheels),
            [
                "uv",
                "run",
                "--isolated",
                "--no-project",
                "--python",
                sys.executable,
                "--with",
                "pip",
                "python",
                "-m",
                "pip",
                "--disable-pip-version-check",
                "wheel",
                "--require-hashes",
                "--requirement",
                str(requirements),
                "--wheel-dir",
                str(wheels),
            ],
        )
        self.assertEqual(
            _vcs_wheel_command(requirements, wheels),
            [
                "uv",
                "run",
                "--isolated",
                "--no-project",
                "--python",
                sys.executable,
                "--with",
                "pip",
                "python",
                "-m",
                "pip",
                "--disable-pip-version-check",
                "wheel",
                "--no-deps",
                "--requirement",
                str(requirements),
                "--wheel-dir",
                str(wheels),
            ],
        )

    def test_requirements_split_hashed_registry_and_vcs_dependencies(self):
        raw = self.project_root / "raw.txt"
        output = self.project_root / "requirements.lock"
        vcs_output = self.project_root / "requirements-vcs.lock"
        raw.write_text(
            (
                "dependency==1.0 \\\n"
                "    --hash=sha256:dependency\n"
                "trackers @ git+https://example.test/trackers.git@commit\n"
            ),
            encoding="utf-8",
        )

        has_vcs = _write_requirements(
            raw,
            output,
            vcs_output,
            "cpu",
        )

        self.assertTrue(has_vcs)
        self.assertEqual(
            output.read_text(encoding="utf-8"),
            (
                "# Locked people-counter CPU deployment dependencies.\n"
                f"--index-url {PYPI_INDEX}\n"
                f"--extra-index-url {PYTORCH_INDEXES['cpu']}\n\n"
                "dependency==1.0 \\\n"
                "    --hash=sha256:dependency\n"
            ),
        )
        self.assertNotIn(PYTORCH_INDEXES["gpu"], output.read_text())
        self.assertEqual(
            vcs_output.read_text(encoding="utf-8"),
            (
                "# Pinned VCS dependencies built separately because pip "
                "cannot hash repositories.\n"
                "trackers @ git+https://example.test/trackers.git@commit\n"
            ),
        )

    def test_requirements_reject_unhashed_registry_dependency(self):
        raw = self.project_root / "raw.txt"
        output = self.project_root / "requirements.lock"
        vcs_output = self.project_root / "requirements-vcs.lock"
        raw.write_text("dependency==1.0\n", encoding="utf-8")

        with self.assertRaisesRegex(
            RuntimeError,
            (
                "^Lock export contains unhashed non-VCS requirements: "
                "dependency==1.0$"
            ),
        ):
            _write_requirements(
                raw,
                output,
                vcs_output,
                "cpu",
            )

    def test_wheelhouse_validation_rejects_incomplete_outputs(self):
        wheels = self.project_root / "wheels"
        wheels.mkdir()

        with self.assertRaisesRegex(
            RuntimeError,
            "^Bundle build produced no wheels$",
        ):
            _validate_wheelhouse(wheels)

        (wheels / "dependency.whl").write_bytes(b"dependency")
        with self.assertRaisesRegex(
            RuntimeError,
            "^Bundle build did not produce the people-counter wheel$",
        ):
            _validate_wheelhouse(wheels)

        (wheels / "people_counter-0.1.0.whl").write_bytes(b"sdk")
        (wheels / "dependency.tar.gz").write_bytes(b"sdist")
        with self.assertRaisesRegex(
            RuntimeError,
            "^Bundle contains non-wheel dependencies: dependency.tar.gz$",
        ):
            _validate_wheelhouse(wheels)

        (wheels / "dependency.tar.gz").unlink()
        _validate_wheelhouse(wheels)

    def test_checksum_and_bundle_readme_are_deterministic(self):
        artifact = self.project_root / "artifact"
        artifact.write_bytes(b"people-counter")

        self.assertEqual(
            _sha256(artifact),
            hashlib.sha256(b"people-counter").hexdigest(),
        )
        with patch(
            "people_counter.bundle.version",
            return_value="1.2.3",
        ):
            readme = _bundle_readme("bundle-name", "gpu")

        self.assertEqual(
            readme,
            (
                "People Counter GPU SDK bundle\n"
                "==============================\n\n"
                "This bundle is specific to the Python version and platform "
                "named in manifest.json.\n\n"
                "Install offline from the extracted bundle directory:\n\n"
                "  python -m pip install --no-index --find-links wheels "
                "'people-counter[gpu]==1.2.3'\n\n"
                "Verify artifact checksums against manifest.json before "
                "installation.\n"
                "Bundle directory: bundle-name\n"
            ),
        )

    def test_command_runner_uses_project_root_and_check(self):
        with patch("people_counter.bundle.subprocess.run") as run_process:
            _run(["uv", "build"], self.project_root)

        run_process.assert_called_once_with(
            ["uv", "build"],
            cwd=self.project_root,
            check=True,
        )

    def test_main_builds_selected_bundle_and_prints_checksum(self):
        bundle_path = self.project_root / "bundle.zip"
        output = io.StringIO()
        with (
            patch(
                "people_counter.bundle.build_bundle",
                return_value=bundle_path,
            ) as build,
            patch(
                "people_counter.bundle._sha256",
                return_value="bundle-sha256",
            ),
            contextlib.redirect_stdout(output),
        ):
            exit_code = main(
                ["cpu", "--output-dir", "artifacts", "--force"],
                project_root=self.project_root,
            )

        self.assertEqual(exit_code, 0)
        build.assert_called_once_with(
            self.project_root,
            "cpu",
            self.project_root / "artifacts",
            force=True,
        )
        self.assertEqual(
            output.getvalue(),
            (
                f"Created SDK bundle: {bundle_path}\n"
                "SHA256: bundle-sha256\n"
            ),
        )


if __name__ == "__main__":
    unittest.main()

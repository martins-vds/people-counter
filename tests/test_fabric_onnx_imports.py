import subprocess
import sys
import textwrap


def test_rtdetr_onnx_import_does_not_require_other_backends():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import importlib.abc
                import sys

                class RejectUnqualifiedBackend(importlib.abc.MetaPathFinder):
                    def find_spec(self, fullname, path=None, target=None):
                        if fullname.split(".", 1)[0] in {
                            "libreyolo", "trackers", "rfdetr"
                        }:
                            raise ModuleNotFoundError(
                                f"Unqualified Fabric ONNX backend: {fullname}"
                            )

                sys.meta_path.insert(0, RejectUnqualifiedBackend())
                from people_counter.pipelines import rtdetr_osnet
                assert callable(rtdetr_osnet.load_runtime)
                assert not {"libreyolo", "trackers", "rfdetr"}.intersection(sys.modules)
                """
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

import tempfile
import unittest
from pathlib import Path

from people_counter.model_artifacts import (
    OSNET_FILENAME,
    OSNET_MODEL_DIR,
    RFDETR_FILENAME,
    RFDETR_PIPELINE_DIR,
    RTDETR_MODEL_DIRS,
    RTDETR_PIPELINE_DIR,
    RTDETR_REQUIRED_FILES,
    resolve_rfdetr_checkpoint,
    resolve_rtdetr_osnet_artifacts,
)


class OfflineModelArtifactTests(unittest.TestCase):
    def test_resolves_downloaded_rtdetr_and_osnet_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            models_dir = Path(directory)
            detector_dir = (
                models_dir
                / RTDETR_PIPELINE_DIR
                / RTDETR_MODEL_DIRS["r50"]
            )
            detector_dir.mkdir(parents=True)
            for filename in RTDETR_REQUIRED_FILES:
                (detector_dir / filename).touch()
            reid_path = (
                models_dir
                / RTDETR_PIPELINE_DIR
                / OSNET_MODEL_DIR
                / OSNET_FILENAME
            )
            reid_path.parent.mkdir()
            reid_path.touch()

            resolved = resolve_rtdetr_osnet_artifacts(models_dir, "r50")

        self.assertEqual(resolved, (detector_dir, reid_path))

    def test_reports_every_missing_rtdetr_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            models_dir = Path(directory)
            with self.assertRaises(FileNotFoundError) as raised:
                resolve_rtdetr_osnet_artifacts(models_dir, "r18")

        expected_paths = [
            models_dir
            / RTDETR_PIPELINE_DIR
            / RTDETR_MODEL_DIRS["r18"]
            / filename
            for filename in RTDETR_REQUIRED_FILES
        ] + [
            models_dir
            / RTDETR_PIPELINE_DIR
            / OSNET_MODEL_DIR
            / OSNET_FILENAME
        ]
        self.assertEqual(
            str(raised.exception),
            (
                "Offline model artifacts are missing: "
                + ", ".join(str(path) for path in expected_paths)
            ),
        )

    def test_resolves_downloaded_rfdetr_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            models_dir = Path(directory)
            checkpoint = (
                models_dir
                / RFDETR_PIPELINE_DIR
                / RFDETR_FILENAME
            )
            checkpoint.parent.mkdir()
            checkpoint.touch()

            resolved = resolve_rfdetr_checkpoint(models_dir)

        self.assertEqual(resolved, checkpoint)

    def test_rejects_missing_rfdetr_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            expected = (
                Path(directory)
                / RFDETR_PIPELINE_DIR
                / RFDETR_FILENAME
            )
            with self.assertRaises(FileNotFoundError) as raised:
                resolve_rfdetr_checkpoint(Path(directory))

        self.assertEqual(
            str(raised.exception),
            f"Offline model artifacts are missing: {expected}",
        )

if __name__ == "__main__":
    unittest.main()

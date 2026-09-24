import hashlib
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import download_models


class DownloadModelsTests(unittest.TestCase):
    def test_downloads_pinned_hugging_face_artifacts(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                download_models,
                "snapshot_download",
            ) as snapshot,
            patch.object(
                download_models,
                "hf_hub_download",
            ) as hub_download,
        ):
            output_dir = Path(directory)
            hub_download.return_value = str(
                output_dir
                / "rtdetr_osnet"
                / "libre_reid_osnet"
                / "osnet_ain_x0_25.pt"
            )

            paths = download_models.download_rtdetr_osnet_models(
                output_dir
            )

        self.assertEqual(snapshot.call_count, 2)
        for call, model in zip(
            snapshot.call_args_list,
            download_models.RTDETR_MODELS,
        ):
            expected_destination = (
                output_dir / "rtdetr_osnet" / model.name
            )
            self.assertEqual(call.kwargs["repo_id"], model.repo_id)
            self.assertEqual(call.kwargs["revision"], model.revision)
            self.assertEqual(call.kwargs["allow_patterns"], list(model.files))
            self.assertEqual(
                call.kwargs["local_dir"],
                expected_destination,
            )
            self.assertFalse(call.kwargs["force_download"])
        hub_download.assert_called_once_with(
            repo_id=download_models.OSNET_MODEL.repo_id,
            filename="osnet_ain_x0_25.pt",
            revision=download_models.OSNET_MODEL.revision,
            local_dir=(
                output_dir / "rtdetr_osnet" / "libre_reid_osnet"
            ),
            force_download=False,
        )
        self.assertEqual(
            paths,
            [
                output_dir / "rtdetr_osnet" / model.name
                for model in download_models.RTDETR_MODELS
            ]
            + [Path(hub_download.return_value)],
        )

    def test_downloads_and_validates_rfdetr_checkpoint(self):
        contents = b"rfdetr checkpoint"
        expected_md5 = hashlib.md5(
            contents,
            usedforsecurity=False,
        ).hexdigest()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(download_models, "RFDETR_MD5", expected_md5),
            patch.object(
                download_models.urllib.request,
                "urlopen",
                return_value=io.BytesIO(contents),
            ) as urlopen,
        ):
            destination = download_models.download_rfdetr_botsort_model(
                Path(directory) / "nested" / "models"
            )

            self.assertEqual(destination.read_bytes(), contents)
            self.assertFalse(
                destination.with_suffix(".pth.part").exists()
            )

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, download_models.RFDETR_URL)
        self.assertEqual(
            request.headers["User-agent"],
            "people-counter-model-downloader",
        )
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)

    def test_reuses_valid_rfdetr_checkpoint(self):
        contents = b"cached checkpoint"
        expected_md5 = hashlib.md5(
            contents,
            usedforsecurity=False,
        ).hexdigest()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(download_models, "RFDETR_MD5", expected_md5),
            patch.object(
                download_models.urllib.request,
                "urlopen",
            ) as urlopen,
        ):
            destination = (
                Path(directory)
                / "rfdetr_botsort"
                / download_models.RFDETR_FILENAME
            )
            destination.parent.mkdir()
            destination.write_bytes(contents)

            result = download_models.download_rfdetr_botsort_model(
                Path(directory)
            )

        self.assertEqual(result, destination)
        urlopen.assert_not_called()

    def test_rejects_corrupt_cached_rfdetr_checkpoint(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(download_models, "RFDETR_MD5", "expected"),
        ):
            destination = (
                Path(directory)
                / "rfdetr_botsort"
                / download_models.RFDETR_FILENAME
            )
            destination.parent.mkdir()
            destination.write_bytes(b"corrupt")

            with self.assertRaisesRegex(
                RuntimeError,
                (
                    f"expected expected, got "
                    f"{hashlib.md5(b'corrupt', usedforsecurity=False).hexdigest()}"
                    r"\. Use --force to replace it\."
                ),
            ) as raised:
                download_models.download_rfdetr_botsort_model(
                    Path(directory)
                )

            self.assertIn(str(destination), str(raised.exception))
            self.assertEqual(destination.read_bytes(), b"corrupt")

    def test_force_replaces_corrupt_rfdetr_checkpoint(self):
        contents = b"replacement checkpoint"
        expected_md5 = hashlib.md5(
            contents,
            usedforsecurity=False,
        ).hexdigest()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(download_models, "RFDETR_MD5", expected_md5),
            patch.object(
                download_models.urllib.request,
                "urlopen",
                return_value=io.BytesIO(contents),
            ),
        ):
            destination = (
                Path(directory)
                / "rfdetr_botsort"
                / download_models.RFDETR_FILENAME
            )
            destination.parent.mkdir()
            destination.write_bytes(b"corrupt")

            result = download_models.download_rfdetr_botsort_model(
                Path(directory),
                force=True,
            )

            self.assertEqual(result, destination)
            self.assertEqual(destination.read_bytes(), contents)

    def test_rejects_corrupt_download_and_removes_partial_file(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(download_models, "RFDETR_MD5", "expected"),
            patch.object(
                download_models.urllib.request,
                "urlopen",
                return_value=io.BytesIO(b"corrupt"),
            ),
        ):
            output_dir = Path(directory)
            with self.assertRaisesRegex(
                RuntimeError,
                "Downloaded RF-DETR checkpoint checksum mismatch",
            ):
                download_models.download_rfdetr_botsort_model(output_dir)

            destination = (
                output_dir
                / "rfdetr_botsort"
                / download_models.RFDETR_FILENAME
            )
            self.assertFalse(destination.exists())
            self.assertFalse(
                destination.with_suffix(".pth.part").exists()
            )

    def test_pipeline_selection_downloads_all_models(self):
        output_dir = Path("models")
        with (
            patch.object(
                download_models,
                "download_rtdetr_osnet_models",
                return_value=[Path("detector"), Path("reid")],
            ) as rtdetr_download,
            patch.object(
                download_models,
                "download_rfdetr_botsort_model",
                return_value=Path("checkpoint.pth"),
            ) as rfdetr_download,
        ):
            result = download_models.download_models(
                output_dir,
                "all",
            )

        rtdetr_download.assert_called_once_with(output_dir, force=False)
        rfdetr_download.assert_called_once_with(output_dir, force=False)
        self.assertEqual(
            result,
            [Path("detector"), Path("reid"), Path("checkpoint.pth")],
        )

    def test_pipeline_selection_only_downloads_requested_models(self):
        output_dir = Path("models")
        with (
            patch.object(
                download_models,
                "download_rtdetr_osnet_models",
                return_value=[Path("detector"), Path("reid")],
            ) as rtdetr_download,
            patch.object(
                download_models,
                "download_rfdetr_botsort_model",
            ) as rfdetr_download,
        ):
            result = download_models.download_models(
                output_dir,
                "rtdetr-osnet",
                force=True,
            )

        rtdetr_download.assert_called_once_with(output_dir, force=True)
        rfdetr_download.assert_not_called()
        self.assertEqual(result, [Path("detector"), Path("reid")])

    def test_pipeline_selection_supports_rfdetr_only(self):
        output_dir = Path("models")
        with (
            patch.object(
                download_models,
                "download_rtdetr_osnet_models",
            ) as rtdetr_download,
            patch.object(
                download_models,
                "download_rfdetr_botsort_model",
                return_value=Path("checkpoint.pth"),
            ) as rfdetr_download,
        ):
            result = download_models.download_models(
                output_dir,
                "rfdetr-botsort",
                force=True,
            )

        rtdetr_download.assert_not_called()
        rfdetr_download.assert_called_once_with(output_dir, force=True)
        self.assertEqual(result, [Path("checkpoint.pth")])

    def test_main_resolves_output_and_reports_downloads(self):
        output_dir = Path("relative-models")
        expected_output = output_dir.resolve()
        stdout = io.StringIO()
        with (
            patch.object(
                download_models,
                "download_models",
                return_value=[expected_output / "model"],
            ) as download,
            redirect_stdout(stdout),
        ):
            result = download_models.main(
                [
                    "--pipeline",
                    "rfdetr-botsort",
                    "--output-dir",
                    str(output_dir),
                    "--force",
                ]
            )

        self.assertEqual(result, 0)
        download.assert_called_once_with(
            expected_output,
            "rfdetr-botsort",
            force=True,
        )
        self.assertEqual(
            stdout.getvalue(),
            (
                f"Downloaded model artifacts to {expected_output}:\n"
                f"  {expected_output / 'model'}\n"
            ),
        )


if __name__ == "__main__":
    unittest.main()

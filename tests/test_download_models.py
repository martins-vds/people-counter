import hashlib
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

from scripts import download_models


class DownloadModelsTests(unittest.TestCase):
    @staticmethod
    def _fake_torch():
        class Module:
            def eval(self):
                return self

        return SimpleNamespace(
            float32=object(),
            nn=SimpleNamespace(
                Module=Module,
                functional=SimpleNamespace(normalize=MagicMock()),
            ),
            onnx=SimpleNamespace(export=MagicMock()),
            zeros=MagicMock(return_value=object()),
        )

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
            patch.object(
                download_models,
                "convert_rtdetr_osnet_models",
                return_value=[
                    Path("detector.onnx"),
                    Path("reid.onnx"),
                ],
            ) as rtdetr_convert,
            patch.object(
                download_models,
                "convert_rfdetr_botsort_model",
                return_value=Path("checkpoint.onnx"),
            ) as rfdetr_convert,
        ):
            result = download_models.download_models(
                output_dir,
                "all",
                convert="onnx",
            )

        rtdetr_download.assert_called_once_with(output_dir, force=False)
        rfdetr_download.assert_called_once_with(output_dir, force=False)
        rtdetr_convert.assert_called_once_with(
            output_dir,
            "onnx",
            force=False,
        )
        rfdetr_convert.assert_called_once_with(
            output_dir,
            "onnx",
            force=False,
        )
        self.assertEqual(
            result,
            [
                Path("detector"),
                Path("reid"),
                Path("detector.onnx"),
                Path("reid.onnx"),
                Path("checkpoint.pth"),
                Path("checkpoint.onnx"),
            ],
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

    def test_openvino_conversion_reuses_complete_export(self):
        with tempfile.TemporaryDirectory() as directory:
            onnx_path = Path(directory) / "model.onnx"
            destination = onnx_path.with_suffix(".xml")
            destination.write_text("model")
            destination.with_suffix(".bin").write_bytes(b"weights")

            with patch.dict(
                sys.modules,
                {"openvino": None},
            ):
                result = download_models._convert_onnx_to_openvino(
                    onnx_path,
                    force=False,
                )

        self.assertEqual(result, destination)

    def test_openvino_conversion_writes_model(self):
        converted_model = object()
        openvino = SimpleNamespace(
            convert_model=lambda path: converted_model,
            save_model=lambda model, path, *, compress_to_fp16: path,
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"openvino": openvino}),
            patch.object(
                openvino,
                "convert_model",
                wraps=openvino.convert_model,
            ) as convert_model,
            patch.object(
                openvino,
                "save_model",
                wraps=openvino.save_model,
            ) as save_model,
        ):
            onnx_path = Path(directory) / "model.onnx"
            onnx_path.with_suffix(".xml").write_text("incomplete")
            result = download_models._convert_onnx_to_openvino(
                onnx_path,
                force=False,
            )

        convert_model.assert_called_once_with(onnx_path)
        save_model.assert_called_once_with(
            converted_model,
            onnx_path.with_suffix(".xml"),
            compress_to_fp16=False,
        )
        self.assertEqual(result, onnx_path.with_suffix(".xml"))

    def test_openvino_conversion_reports_missing_dependency(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"openvino": None}),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "OpenVINO conversion requires the export dependencies. "
                    "Run this script with "
                    "`uv run --extra cpu --extra export`."
                ),
            ),
        ):
            download_models._convert_onnx_to_openvino(
                Path(directory) / "model.onnx",
                force=False,
            )

    def test_exports_rtdetr_to_onnx(self):
        torch = self._fake_torch()
        logits = object()
        pred_boxes = object()
        loaded_model = MagicMock()
        loaded_model.eval.return_value = loaded_model
        loaded_model.return_value = SimpleNamespace(
            logits=logits,
            pred_boxes=pred_boxes,
        )
        model_type = MagicMock()
        model_type.from_pretrained.return_value = loaded_model
        transformers = ModuleType("transformers")
        transformers.RTDetrV2ForObjectDetection = model_type

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(
                sys.modules,
                {"torch": torch, "transformers": transformers},
            ),
        ):
            model_dir = Path(directory) / "rtdetr"
            result = download_models._export_rtdetr_onnx(
                model_dir,
                force=False,
            )

        model_type.from_pretrained.assert_called_once_with(
            model_dir,
            local_files_only=True,
        )
        torch.zeros.assert_called_once_with(
            (1, 3, 640, 640),
            dtype=torch.float32,
        )
        export_call = torch.onnx.export.call_args
        self.assertEqual(export_call.args[2], model_dir / "model.onnx")
        self.assertEqual(
            export_call.args[1],
            torch.zeros.return_value,
        )
        self.assertEqual(
            export_call.kwargs,
            {
                "input_names": ("pixel_values",),
                "output_names": ("logits", "pred_boxes"),
                "dynamic_axes": {
                    "pixel_values": {0: "batch"},
                    "logits": {0: "batch"},
                    "pred_boxes": {0: "batch"},
                },
                "opset_version": download_models.ONNX_OPSET_VERSION,
                "dynamo": False,
            },
        )
        self.assertEqual(result, model_dir / "model.onnx")
        wrapper = export_call.args[0]
        self.assertEqual(
            wrapper.forward(torch.zeros.return_value),
            (logits, pred_boxes),
        )
        loaded_model.assert_called_once_with(
            pixel_values=torch.zeros.return_value
        )

    def test_reuses_existing_rtdetr_onnx_export(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"torch": None}),
        ):
            model_dir = Path(directory)
            destination = model_dir / "model.onnx"
            destination.touch()
            result = download_models._export_rtdetr_onnx(
                model_dir,
                force=False,
            )

        self.assertEqual(result, destination)

    def test_rtdetr_export_reports_missing_dependency(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"torch": None}),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "RT-DETR conversion requires the CPU or GPU and export "
                    "dependencies. Run this script with "
                    "`uv run --extra cpu --extra export`."
                ),
            ),
        ):
            download_models._export_rtdetr_onnx(
                Path(directory),
                force=False,
            )

    def test_exports_normalized_osnet_to_onnx(self):
        torch = self._fake_torch()
        embeddings = object()
        normalized_embeddings = object()
        osnet_model = MagicMock(return_value=embeddings)
        torch.nn.functional.normalize.return_value = normalized_embeddings
        embedder_type = MagicMock()
        embedder_type.return_value.model = osnet_model
        reid = ModuleType("libreyolo.tracking.reid")
        reid.OSNetEmbedder = embedder_type

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(
                sys.modules,
                {
                    "torch": torch,
                    "libreyolo.tracking.reid": reid,
                },
            ),
        ):
            weights_path = Path(directory) / "osnet.pt"
            result = download_models._export_osnet_onnx(
                weights_path,
                force=False,
            )

        embedder_type.assert_called_once_with(
            variant="osnet_ain_x0_25",
            weights=weights_path,
            device="cpu",
        )
        torch.zeros.assert_called_once_with(
            (1, 3, 256, 128),
            dtype=torch.float32,
        )
        export_call = torch.onnx.export.call_args
        self.assertEqual(export_call.args[2], weights_path.with_suffix(".onnx"))
        self.assertEqual(
            export_call.args[1],
            torch.zeros.return_value,
        )
        self.assertEqual(
            export_call.kwargs,
            {
                "input_names": ("pixel_values",),
                "output_names": ("embeddings",),
                "dynamic_axes": {
                    "pixel_values": {0: "batch"},
                    "embeddings": {0: "batch"},
                },
                "opset_version": download_models.ONNX_OPSET_VERSION,
                "dynamo": False,
            },
        )
        self.assertEqual(result, weights_path.with_suffix(".onnx"))
        wrapper = export_call.args[0]
        self.assertIs(
            wrapper.forward(torch.zeros.return_value),
            normalized_embeddings,
        )
        osnet_model.assert_called_once_with(torch.zeros.return_value)
        torch.nn.functional.normalize.assert_called_once_with(
            embeddings,
            dim=-1,
        )

    def test_reuses_existing_osnet_onnx_export(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"torch": None}),
        ):
            weights_path = Path(directory) / "osnet.pt"
            destination = weights_path.with_suffix(".onnx")
            destination.touch()
            result = download_models._export_osnet_onnx(
                weights_path,
                force=False,
            )

        self.assertEqual(result, destination)

    def test_osnet_export_reports_missing_dependency(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"torch": None}),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "OSNet conversion requires the CPU or GPU and export "
                    "dependencies. Run this script with "
                    "`uv run --extra cpu --extra export`."
                ),
            ),
        ):
            download_models._export_osnet_onnx(
                Path(directory) / "osnet.pt",
                force=False,
            )

    def test_exports_rfdetr_to_expected_onnx_path(self):
        exported_model = MagicMock()
        rfdetr_type = MagicMock(return_value=exported_model)
        rfdetr = ModuleType("rfdetr")
        rfdetr.RFDETRLarge = rfdetr_type

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"rfdetr": rfdetr}),
        ):
            checkpoint = Path(directory) / "checkpoint.pth"
            exported_model.export.return_value = checkpoint.with_suffix(
                ".onnx"
            )
            result = download_models._export_rfdetr_onnx(
                checkpoint,
                force=False,
            )

        rfdetr_type.assert_called_once_with(
            pretrain_weights=str(checkpoint),
            device="cpu",
        )
        exported_model.export.assert_called_once_with(
            output_dir=str(checkpoint.parent),
            format="onnx",
            opset_version=download_models.ONNX_OPSET_VERSION,
            verbose=False,
            output_name=checkpoint.stem,
        )
        self.assertEqual(result, checkpoint.with_suffix(".onnx"))

    def test_reuses_existing_rfdetr_onnx_export(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"rfdetr": None}),
        ):
            checkpoint = Path(directory) / "checkpoint.pth"
            destination = checkpoint.with_suffix(".onnx")
            destination.touch()
            result = download_models._export_rfdetr_onnx(
                checkpoint,
                force=False,
            )

        self.assertEqual(result, destination)

    def test_rfdetr_export_reports_missing_dependency(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(sys.modules, {"rfdetr": None}),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "RF-DETR conversion requires the CPU or GPU and export "
                    "dependencies. Run this script with "
                    "`uv run --extra cpu --extra export`."
                ),
            ),
        ):
            download_models._export_rfdetr_onnx(
                Path(directory) / "checkpoint.pth",
                force=False,
            )

    def test_rejects_unexpected_rfdetr_export_path(self):
        exported_model = MagicMock()
        rfdetr_type = MagicMock(return_value=exported_model)
        exported_model.export.return_value = Path("unexpected.onnx")
        rfdetr = ModuleType("rfdetr")
        rfdetr.RFDETRLarge = rfdetr_type

        with (
            patch.dict(sys.modules, {"rfdetr": rfdetr}),
            self.assertRaisesRegex(
                RuntimeError,
                "RF-DETR exported to unexpected path",
            ),
        ):
            download_models._export_rfdetr_onnx(
                Path("models/checkpoint.pth"),
                force=True,
            )

    def test_converts_all_rtdetr_osnet_models(self):
        output_dir = Path("models")
        converted_paths = [
            Path("r18.xml"),
            Path("r50.xml"),
            Path("osnet.xml"),
        ]
        with (
            patch.object(
                download_models,
                "_export_rtdetr_onnx",
                side_effect=[Path("r18.onnx"), Path("r50.onnx")],
            ) as export_rtdetr,
            patch.object(
                download_models,
                "_export_osnet_onnx",
                return_value=Path("osnet.onnx"),
            ) as export_osnet,
            patch.object(
                download_models,
                "_converted_model",
                side_effect=converted_paths,
            ) as convert,
        ):
            result = download_models.convert_rtdetr_osnet_models(
                output_dir,
                "openvino",
            )

        self.assertEqual(
            export_rtdetr.call_args_list,
            [
                unittest.mock.call(
                    output_dir / "rtdetr_osnet" / model.name,
                    force=False,
                )
                for model in download_models.RTDETR_MODELS
            ],
        )
        export_osnet.assert_called_once_with(
            output_dir
            / "rtdetr_osnet"
            / "libre_reid_osnet"
            / "osnet_ain_x0_25.pt",
            force=False,
        )
        self.assertEqual(
            convert.call_args_list,
            [
                unittest.mock.call(
                    Path("r18.onnx"),
                    "openvino",
                    force=False,
                ),
                unittest.mock.call(
                    Path("r50.onnx"),
                    "openvino",
                    force=False,
                ),
                unittest.mock.call(
                    Path("osnet.onnx"),
                    "openvino",
                    force=False,
                ),
            ],
        )
        self.assertEqual(result, converted_paths)

    def test_converts_rfdetr_model(self):
        output_dir = Path("models")
        onnx_path = Path("checkpoint.onnx")
        xml_path = Path("checkpoint.xml")
        with (
            patch.object(
                download_models,
                "_export_rfdetr_onnx",
                return_value=onnx_path,
            ) as export,
            patch.object(
                download_models,
                "_converted_model",
                return_value=xml_path,
            ) as convert,
        ):
            result = download_models.convert_rfdetr_botsort_model(
                output_dir,
                "openvino",
            )

        export.assert_called_once_with(
            output_dir / "rfdetr_botsort" / "rf-detr-large-2026.pth",
            force=False,
        )
        convert.assert_called_once_with(
            onnx_path,
            "openvino",
            force=False,
        )
        self.assertEqual(result, xml_path)

    def test_converted_model_returns_onnx_without_openvino(self):
        onnx_path = Path("model.onnx")
        with patch.object(
            download_models,
            "_convert_onnx_to_openvino",
        ) as convert:
            result = download_models._converted_model(
                onnx_path,
                "onnx",
                force=True,
            )

        self.assertEqual(result, onnx_path)
        convert.assert_not_called()

    def test_converted_model_converts_openvino(self):
        onnx_path = Path("model.onnx")
        xml_path = Path("model.xml")
        with patch.object(
            download_models,
            "_convert_onnx_to_openvino",
            return_value=xml_path,
        ) as convert:
            result = download_models._converted_model(
                onnx_path,
                "openvino",
                force=True,
            )

        self.assertEqual(result, xml_path)
        convert.assert_called_once_with(onnx_path, force=True)

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
                    "--convert",
                    "openvino",
                ]
            )

        self.assertEqual(result, 0)
        download.assert_called_once_with(
            expected_output,
            "rfdetr-botsort",
            force=True,
            convert="openvino",
        )
        self.assertEqual(
            stdout.getvalue(),
            (
                f"Downloaded and converted model artifacts to "
                f"{expected_output}:\n"
                f"  {expected_output / 'model'}\n"
            ),
        )

    def test_main_reports_download_only_without_conversion(self):
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
                ["--output-dir", str(output_dir)]
            )

        self.assertEqual(result, 0)
        download.assert_called_once_with(
            expected_output,
            "all",
            force=False,
            convert=None,
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

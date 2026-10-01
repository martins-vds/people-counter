import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import supervision as sv
import torch

from people_counter.onnx_runtime import (
    OSNetOnnxEmbedder,
    RFDetrOnnxModel,
    RTDetrOnnxModel,
    create_onnx_session,
)


class FakeSession:
    def __init__(self, outputs, shape=None):
        self.outputs = outputs
        self.input = SimpleNamespace(
            name="input",
            shape=shape or ["batch", 3, 640, 640],
        )
        self.calls = []

    def get_inputs(self):
        return [self.input]

    def run(self, output_names, inputs):
        self.calls.append((output_names, inputs))
        return self.outputs


class OnnxRuntimeTests(unittest.TestCase):
    def test_creates_cpu_session_with_explicit_provider(self):
        runtime = SimpleNamespace(
            get_available_providers=lambda: ["CPUExecutionProvider"],
            InferenceSession=MagicMock(return_value=object()),
        )
        with patch.dict(sys.modules, {"onnxruntime": runtime}):
            result = create_onnx_session(Path("model.onnx"), "cpu")

        self.assertIs(result, runtime.InferenceSession.return_value)
        runtime.InferenceSession.assert_called_once_with(
            Path("model.onnx"),
            providers=["CPUExecutionProvider"],
        )

    def test_rejects_unavailable_gpu_provider(self):
        runtime = SimpleNamespace(
            get_available_providers=lambda: [
                "CPUExecutionProvider",
                "AzureExecutionProvider",
            ],
        )
        with (
            patch.dict(sys.modules, {"onnxruntime": runtime}),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "^ONNX Runtime provider CUDAExecutionProvider is "
                    "unavailable; available providers: "
                    "CPUExecutionProvider, AzureExecutionProvider$"
                ),
            ),
        ):
            create_onnx_session(Path("model.onnx"), "gpu")

    def test_reports_missing_onnx_runtime(self):
        with (
            patch.dict(sys.modules, {"onnxruntime": None}),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "^ONNX model loading requires ONNX Runtime. Run with "
                    "`uv run --extra cpu --extra export` or install "
                    "`onnxruntime-gpu` for CUDA\\.$"
                ),
            ),
        ):
            create_onnx_session(Path("model.onnx"), "cpu")

    def test_rtdetr_adapter_returns_torch_outputs(self):
        logits = np.ones((2, 3, 4), dtype=np.float32)
        boxes = np.full((2, 3, 4), 0.5, dtype=np.float32)
        session = FakeSession([logits, boxes])
        model = RTDetrOnnxModel(session, torch.device("cpu"))
        pixel_values = torch.zeros((2, 3, 2, 2), dtype=torch.float64)

        result = model(pixel_values=pixel_values)

        self.assertTrue(torch.equal(result.logits, torch.from_numpy(logits)))
        self.assertTrue(
            torch.equal(result.pred_boxes, torch.from_numpy(boxes))
        )
        output_names, inputs = session.calls[0]
        self.assertEqual(output_names, ("logits", "pred_boxes"))
        np.testing.assert_array_equal(
            inputs["input"],
            pixel_values.numpy().astype(np.float32),
        )
        self.assertEqual(inputs["input"].dtype, np.float32)

    def test_rtdetr_adapter_moves_outputs_to_requested_device(self):
        session = FakeSession(
            [
                np.ones((1, 1, 1), dtype=np.float32),
                np.ones((1, 1, 4), dtype=np.float32),
            ]
        )
        device = object()
        logits_tensor = MagicMock()
        boxes_tensor = MagicMock()
        with patch.object(
            torch,
            "from_numpy",
            side_effect=[logits_tensor, boxes_tensor],
        ):
            RTDetrOnnxModel(session, device)(
                pixel_values=np.zeros((1, 3, 2, 2), dtype=np.float32)
            )

        logits_tensor.to.assert_called_once_with(device)
        boxes_tensor.to.assert_called_once_with(device)

    def test_osnet_adapter_preprocesses_and_returns_embeddings(self):
        embeddings = np.full((1, 512), 0.25, dtype=np.float32)
        session = FakeSession([embeddings])
        embedder = OSNetOnnxEmbedder(session)
        image = np.full((20, 10, 3), 255, dtype=np.uint8)

        result = embedder(
            image,
            np.asarray([[0, 0, 10, 20, 0.99]], dtype=np.float32),
        )

        np.testing.assert_array_equal(result, embeddings)
        output_names, inputs = session.calls[0]
        self.assertEqual(output_names, ("embeddings",))
        self.assertEqual(inputs["input"].shape, (1, 3, 256, 128))
        self.assertEqual(inputs["input"].dtype, np.float32)
        expected = (1.0 - np.asarray((0.485, 0.456, 0.406))) / np.asarray(
            (0.229, 0.224, 0.225)
        )
        np.testing.assert_allclose(
            inputs["input"][0, :, 0, 0],
            expected,
            rtol=1e-5,
        )

    def test_osnet_adapter_handles_empty_boxes(self):
        session = FakeSession([])
        result = OSNetOnnxEmbedder(session)(
            np.zeros((10, 10, 3), dtype=np.uint8),
            np.empty((0, 4), dtype=np.float32),
        )

        self.assertEqual(result.shape, (0, 512))
        self.assertEqual(result.dtype, np.float32)
        self.assertEqual(session.calls, [])

    def test_rfdetr_adapter_returns_supervision_detections(self):
        boxes = np.asarray([[[0.5, 0.5, 0.5, 0.5]]], dtype=np.float32)
        logits = np.full((1, 1, 91), -10.0, dtype=np.float32)
        logits[0, 0, 1] = 10.0
        session = FakeSession(
            [boxes, logits],
            shape=[1, 3, 704, 704],
        )
        model = RFDetrOnnxModel(session)
        self.assertEqual(model._postprocess.num_select, 300)
        image = np.full((100, 200, 3), 255, dtype=np.uint8)

        detections = model.predict(
            [image],
            threshold=0.1,
            include_source_image=False,
        )

        self.assertIsInstance(detections, list)
        self.assertEqual(len(detections), 1)
        np.testing.assert_allclose(
            detections[0].xyxy,
            np.asarray([[50, 25, 150, 75]], dtype=np.float32),
        )
        np.testing.assert_array_equal(
            detections[0].data["class_name"],
            np.asarray(["person"]),
        )
        np.testing.assert_array_equal(
            detections[0].data["source_shape"],
            np.asarray([[100, 200]]),
        )
        output_names, inputs = session.calls[0]
        self.assertEqual(output_names, ("dets", "labels"))
        self.assertEqual(inputs["input"].shape, (1, 3, 704, 704))
        expected = (1.0 - np.asarray((0.485, 0.456, 0.406))) / np.asarray(
            (0.229, 0.224, 0.225)
        )
        np.testing.assert_allclose(
            inputs["input"][0, :, 0, 0],
            expected,
            rtol=1e-5,
        )

    def test_rfdetr_adapter_preserves_single_input_contract(self):
        boxes = np.asarray([[[0.5, 0.5, 0.5, 0.5]]], dtype=np.float32)
        logits = np.full((1, 1, 91), -10.0, dtype=np.float32)
        logits[0, 0, 1] = 1.0
        image = np.zeros((20, 30, 3), dtype=np.uint8)
        model = RFDetrOnnxModel(
            FakeSession([boxes, logits], shape=[1, 3, 704, 704])
        )

        detections = model.predict(image)

        self.assertIsInstance(detections, sv.Detections)
        self.assertEqual(len(detections), 1)
        self.assertIs(detections.metadata["source_image"], image)

    def test_rfdetr_adapter_rejects_dynamic_spatial_shape(self):
        for shape in (
            [1, 3, "height", 704],
            [1, 3, 704, "width"],
            [1, 3, 704],
        ):
            with (
                self.subTest(shape=shape),
                self.assertRaisesRegex(
                    RuntimeError,
                    "^RF-DETR ONNX input must have fixed NCHW spatial "
                    "dimensions$",
                ),
            ):
                RFDetrOnnxModel(FakeSession([], shape=shape))


if __name__ == "__main__":
    unittest.main()

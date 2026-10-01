"""ONNX Runtime adapters for the people-counter pipeline interfaces."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import cv2
import numpy as np
import torch

if TYPE_CHECKING:
    import supervision as sv


IMAGENET_MEAN = np.asarray((0.485, 0.456, 0.406), dtype=np.float32)
IMAGENET_STD = np.asarray((0.229, 0.224, 0.225), dtype=np.float32)


def create_onnx_session(model_path: Path, device_variant: str) -> Any:
    try:
        import onnxruntime
    except ImportError as exc:
        raise RuntimeError(
            "ONNX model loading requires ONNX Runtime. Run with "
            "`uv run --extra cpu --extra export` or install "
            "`onnxruntime-gpu` for CUDA."
        ) from exc

    available_providers = onnxruntime.get_available_providers()
    provider = (
        "CUDAExecutionProvider"
        if device_variant == "gpu"
        else "CPUExecutionProvider"
    )
    if provider not in available_providers:
        raise RuntimeError(
            f"ONNX Runtime provider {provider} is unavailable; "
            f"available providers: {', '.join(available_providers)}"
        )
    return onnxruntime.InferenceSession(
        model_path,
        providers=[provider],
    )


class RTDetrOnnxModel:
    def __init__(self, session: Any, device: torch.device) -> None:
        self._session = session
        self._device = device
        self._input_name = session.get_inputs()[0].name

    def __call__(self, **inputs: Any) -> SimpleNamespace:
        pixel_values = inputs["pixel_values"]
        if isinstance(pixel_values, torch.Tensor):
            pixel_values = pixel_values.detach().cpu().numpy()
        logits, pred_boxes = self._session.run(
            ("logits", "pred_boxes"),
            {self._input_name: np.asarray(pixel_values, dtype=np.float32)},
        )
        return SimpleNamespace(
            logits=torch.from_numpy(logits).to(self._device),
            pred_boxes=torch.from_numpy(pred_boxes).to(self._device),
        )


class OSNetOnnxEmbedder:
    crop_size = (128, 256)

    def __init__(self, session: Any) -> None:
        self._session = session
        self._input_name = session.get_inputs()[0].name

    def __call__(
        self,
        image: np.ndarray,
        boxes: np.ndarray,
    ) -> np.ndarray:
        boxes = np.asarray(boxes, dtype=np.float64)
        if boxes.shape[0] == 0:
            return np.zeros((0, 512), dtype=np.float32)

        height, width = image.shape[:2]
        coordinates = np.round(boxes[:, :4]).astype(np.int64)
        coordinates[:, 0] = coordinates[:, 0].clip(0, width)
        coordinates[:, 1] = coordinates[:, 1].clip(0, height)
        coordinates[:, 2] = coordinates[:, 2].clip(0, width)
        coordinates[:, 3] = coordinates[:, 3].clip(0, height)
        crops = []
        for x1, y1, x2, y2 in coordinates:
            crop = image[y1:y2, x1:x2]
            if crop.size == 0:
                crop = np.zeros((2, 2, 3), dtype=image.dtype)
            crop = cv2.resize(
                crop,
                self.crop_size,
                interpolation=cv2.INTER_LINEAR,
            )
            crop = crop.astype(np.float32) / 255.0
            crop = (crop - IMAGENET_MEAN) / IMAGENET_STD
            crops.append(crop.transpose(2, 0, 1))

        embeddings = self._session.run(
            ("embeddings",),
            {
                self._input_name: np.ascontiguousarray(
                    np.stack(crops),
                    dtype=np.float32,
                )
            },
        )[0]
        return np.asarray(embeddings, dtype=np.float32)


class RFDetrOnnxModel:
    def __init__(self, session: Any) -> None:
        from rfdetr.assets.coco_classes import (
            COCO_CLASSES,
            COCO_CLASS_NAMES,
        )
        from rfdetr.models import PostProcess

        self._session = session
        model_input = session.get_inputs()[0]
        self._input_name = model_input.name
        shape = model_input.shape
        if (
            len(shape) != 4
            or not isinstance(shape[2], int)
            or not isinstance(shape[3], int)
        ):
            raise RuntimeError(
                "RF-DETR ONNX input must have fixed NCHW spatial dimensions"
            )
        self._height = shape[2]
        self._width = shape[3]
        self._postprocess = PostProcess(num_select=300)
        self._class_names = {
            class_id: COCO_CLASS_NAMES[index]
            for index, class_id in enumerate(COCO_CLASSES)
        }

    def predict(
        self,
        images: np.ndarray | list[np.ndarray],
        threshold: float = 0.5,
        include_source_image: bool = True,
    ) -> sv.Detections | list[sv.Detections]:
        single_input = not isinstance(images, list)
        image_list = [images] if single_input else images
        predictions = [
            self._predict_one(image, threshold, include_source_image)
            for image in image_list
        ]
        return predictions[0] if single_input else predictions

    def _predict_one(
        self,
        image: np.ndarray,
        threshold: float,
        include_source_image: bool,
    ) -> sv.Detections:
        import supervision as sv

        source_height, source_width = image.shape[:2]
        resized = cv2.resize(
            image,
            (self._width, self._height),
            interpolation=cv2.INTER_LINEAR,
        )
        normalized = resized.astype(np.float32) / 255.0
        normalized = (normalized - IMAGENET_MEAN) / IMAGENET_STD
        batch = np.ascontiguousarray(
            normalized.transpose(2, 0, 1)[None],
            dtype=np.float32,
        )
        pred_boxes, pred_logits = self._session.run(
            ("dets", "labels"),
            {self._input_name: batch},
        )
        result = self._postprocess(
            {
                "pred_boxes": torch.from_numpy(pred_boxes),
                "pred_logits": torch.from_numpy(pred_logits),
            },
            target_sizes=torch.tensor(
                [[source_height, source_width]],
                dtype=torch.int64,
            ),
        )[0]
        keep = result["scores"] > threshold
        class_ids = result["labels"][keep].cpu().numpy()
        detections = sv.Detections(
            xyxy=result["boxes"][keep].float().cpu().numpy(),
            confidence=result["scores"][keep].float().cpu().numpy(),
            class_id=class_ids,
        )
        detections.data["class_name"] = np.asarray(
            [self._class_names.get(int(class_id), "") for class_id in class_ids]
        )
        detections.data["source_shape"] = np.tile(
            np.asarray((source_height, source_width), dtype=np.int64),
            (len(detections), 1),
        )
        if include_source_image:
            detections.metadata["source_image"] = image
        return detections

#!/usr/bin/env python3
"""Download model artifacts for offline people-counter deployments."""

import argparse
import hashlib
import shutil
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from huggingface_hub import hf_hub_download, snapshot_download
from people_counter.model_artifacts import (
    OSNET_FILENAME,
    OSNET_MODEL_DIR,
    RFDETR_FILENAME,
    RFDETR_PIPELINE_DIR,
    RTDETR_MODEL_DIRS,
    RTDETR_ONNX_FILENAME,
    RTDETR_PIPELINE_DIR,
)


@dataclass(frozen=True)
class HuggingFaceModel:
    name: str
    repo_id: str
    revision: str
    files: tuple[str, ...]


RTDETR_MODELS = (
    HuggingFaceModel(
        name=RTDETR_MODEL_DIRS["r18"],
        repo_id="PekingU/rtdetr_v2_r18vd",
        revision="5650961749fa93567c0d46fc7f43ea4f9e914107",
        files=("config.json", "model.safetensors", "preprocessor_config.json"),
    ),
    HuggingFaceModel(
        name=RTDETR_MODEL_DIRS["r50"],
        repo_id="PekingU/rtdetr_v2_r50vd",
        revision="282494075698cab9faa1096ae26856890030c817",
        files=("config.json", "model.safetensors", "preprocessor_config.json"),
    ),
)
OSNET_MODEL = HuggingFaceModel(
    name=OSNET_MODEL_DIR,
    repo_id="LibreYOLO/LibreReID-osnet",
    revision="5c7c20e54ccf80c9889a64020748f148ad5f7634",
    files=(OSNET_FILENAME,),
)
RFDETR_URL = (
    "https://storage.googleapis.com/rfdetr/rf-detr-large-2026.pth"
)
RFDETR_MD5 = "5cb72153541cbcb9aa6efa26222acc75"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "outputs" / "models"
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
ONNX_OPSET_VERSION = 17
ConversionFormat = Literal["onnx", "openvino"]


def _md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as model_file:
        for chunk in iter(lambda: model_file.read(DOWNLOAD_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_rtdetr_osnet_models(
    output_dir: Path,
    *,
    force: bool = False,
) -> list[Path]:
    pipeline_dir = output_dir / RTDETR_PIPELINE_DIR
    downloaded = []
    for model in RTDETR_MODELS:
        destination = pipeline_dir / model.name
        snapshot_download(
            repo_id=model.repo_id,
            revision=model.revision,
            allow_patterns=list(model.files),
            local_dir=destination,
            force_download=force,
        )
        downloaded.append(destination)

    osnet_dir = pipeline_dir / OSNET_MODEL.name
    osnet_path = Path(
        hf_hub_download(
            repo_id=OSNET_MODEL.repo_id,
            filename=OSNET_MODEL.files[0],
            revision=OSNET_MODEL.revision,
            local_dir=osnet_dir,
            force_download=force,
        )
    )
    downloaded.append(osnet_path)
    return downloaded


def download_rfdetr_botsort_model(
    output_dir: Path,
    *,
    force: bool = False,
) -> Path:
    destination = output_dir / RFDETR_PIPELINE_DIR / RFDETR_FILENAME
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and not force:
        digest = _md5(destination)
        if digest != RFDETR_MD5:
            raise RuntimeError(
                f"Existing RF-DETR checkpoint checksum mismatch at "
                f"{destination}: expected {RFDETR_MD5}, got {digest}. "
                "Use --force to replace it."
            )
        return destination

    partial = destination.with_suffix(f"{destination.suffix}.part")
    partial.unlink(missing_ok=True)
    request = urllib.request.Request(
        RFDETR_URL,
        headers={"User-Agent": "people-counter-model-downloader"},
    )
    try:
        with (
            urllib.request.urlopen(request, timeout=60) as response,
            partial.open("wb") as model_file,
        ):
            shutil.copyfileobj(
                response,
                model_file,
                length=DOWNLOAD_CHUNK_SIZE,
            )
        digest = _md5(partial)
        if digest != RFDETR_MD5:
            raise RuntimeError(
                f"Downloaded RF-DETR checkpoint checksum mismatch: "
                f"expected {RFDETR_MD5}, got {digest}"
            )
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)
    return destination


def _convert_onnx_to_openvino(
    onnx_path: Path,
    *,
    force: bool,
) -> Path:
    destination = onnx_path.with_suffix(".xml")
    weights_path = destination.with_suffix(".bin")
    if destination.is_file() and weights_path.is_file() and not force:
        return destination

    try:
        import openvino
    except ImportError as exc:
        raise RuntimeError(
            "OpenVINO conversion requires the export dependencies. "
            "Run this script with `uv run --extra cpu --extra export`."
        ) from exc

    model = openvino.convert_model(onnx_path)
    openvino.save_model(
        model,
        destination,
        compress_to_fp16=False,
    )
    return destination


def _export_rtdetr_onnx(
    model_dir: Path,
    *,
    force: bool,
) -> Path:
    destination = model_dir / RTDETR_ONNX_FILENAME
    if destination.is_file() and not force:
        return destination

    try:
        import torch
        from transformers import RTDetrV2ForObjectDetection
    except ImportError as exc:
        raise RuntimeError(
            "RT-DETR conversion requires the CPU or GPU and export "
            "dependencies. Run this script with "
            "`uv run --extra cpu --extra export`."
        ) from exc

    class RTDetrOnnxWrapper(torch.nn.Module):
        def __init__(self, model):
            super().__init__()
            self.model = model

        def forward(self, pixel_values):
            output = self.model(pixel_values=pixel_values)
            return output.logits, output.pred_boxes

    model = RTDetrV2ForObjectDetection.from_pretrained(
        model_dir,
        local_files_only=True,
    ).eval()
    wrapper = RTDetrOnnxWrapper(model).eval()
    sample = torch.zeros((1, 3, 640, 640), dtype=torch.float32)
    torch.onnx.export(
        wrapper,
        sample,
        destination,
        input_names=("pixel_values",),
        output_names=("logits", "pred_boxes"),
        dynamic_axes={
            "pixel_values": {0: "batch"},
            "logits": {0: "batch"},
            "pred_boxes": {0: "batch"},
        },
        opset_version=ONNX_OPSET_VERSION,
        dynamo=False,
    )
    return destination


def _export_osnet_onnx(
    weights_path: Path,
    *,
    force: bool,
) -> Path:
    destination = weights_path.with_suffix(".onnx")
    if destination.is_file() and not force:
        return destination

    try:
        import torch
        from libreyolo.tracking.reid import OSNetEmbedder
    except ImportError as exc:
        raise RuntimeError(
            "OSNet conversion requires the CPU or GPU and export "
            "dependencies. Run this script with "
            "`uv run --extra cpu --extra export`."
        ) from exc

    class NormalizedOSNet(torch.nn.Module):
        def __init__(self, model):
            super().__init__()
            self.model = model

        def forward(self, pixel_values):
            return torch.nn.functional.normalize(
                self.model(pixel_values),
                dim=-1,
            )

    embedder = OSNetEmbedder(
        variant="osnet_ain_x0_25",
        weights=weights_path,
        device="cpu",
    )
    model = NormalizedOSNet(embedder.model).eval()
    sample = torch.zeros((1, 3, 256, 128), dtype=torch.float32)
    torch.onnx.export(
        model,
        sample,
        destination,
        input_names=("pixel_values",),
        output_names=("embeddings",),
        dynamic_axes={
            "pixel_values": {0: "batch"},
            "embeddings": {0: "batch"},
        },
        opset_version=ONNX_OPSET_VERSION,
        dynamo=False,
    )
    return destination


def _export_rfdetr_onnx(
    checkpoint: Path,
    *,
    force: bool,
) -> Path:
    destination = checkpoint.with_suffix(".onnx")
    if destination.is_file() and not force:
        return destination

    try:
        from rfdetr import RFDETRLarge
    except ImportError as exc:
        raise RuntimeError(
            "RF-DETR conversion requires the CPU or GPU and export "
            "dependencies. Run this script with "
            "`uv run --extra cpu --extra export`."
        ) from exc

    model = RFDETRLarge(
        pretrain_weights=str(checkpoint),
        device="cpu",
    )
    exported = Path(
        model.export(
            output_dir=str(checkpoint.parent),
            format="onnx",
            opset_version=ONNX_OPSET_VERSION,
            verbose=False,
            output_name=checkpoint.stem,
        )
    )
    if exported != destination:
        raise RuntimeError(
            f"RF-DETR exported to unexpected path {exported}; "
            f"expected {destination}"
        )
    return destination


def _converted_model(
    onnx_path: Path,
    conversion_format: ConversionFormat,
    *,
    force: bool,
) -> Path:
    if conversion_format == "onnx":
        return onnx_path
    return _convert_onnx_to_openvino(onnx_path, force=force)


def convert_rtdetr_osnet_models(
    output_dir: Path,
    conversion_format: ConversionFormat,
    *,
    force: bool = False,
) -> list[Path]:
    pipeline_dir = output_dir / RTDETR_PIPELINE_DIR
    converted = []
    for model in RTDETR_MODELS:
        onnx_path = _export_rtdetr_onnx(
            pipeline_dir / model.name,
            force=force,
        )
        converted.append(
            _converted_model(onnx_path, conversion_format, force=force)
        )

    onnx_path = _export_osnet_onnx(
        pipeline_dir / OSNET_MODEL.name / OSNET_MODEL.files[0],
        force=force,
    )
    converted.append(
        _converted_model(onnx_path, conversion_format, force=force)
    )
    return converted


def convert_rfdetr_botsort_model(
    output_dir: Path,
    conversion_format: ConversionFormat,
    *,
    force: bool = False,
) -> Path:
    checkpoint = output_dir / RFDETR_PIPELINE_DIR / RFDETR_FILENAME
    onnx_path = _export_rfdetr_onnx(checkpoint, force=force)
    return _converted_model(onnx_path, conversion_format, force=force)


def download_models(
    output_dir: Path,
    pipeline: str,
    *,
    force: bool = False,
    convert: ConversionFormat | None = None,
) -> list[Path]:
    downloaded = []
    if pipeline in {"all", "rtdetr-osnet"}:
        downloaded.extend(
            download_rtdetr_osnet_models(output_dir, force=force)
        )
        if convert is not None:
            downloaded.extend(
                convert_rtdetr_osnet_models(
                    output_dir,
                    convert,
                    force=force,
                )
            )
    if pipeline in {"all", "rfdetr-botsort"}:
        downloaded.append(
            download_rfdetr_botsort_model(output_dir, force=force)
        )
        if convert is not None:
            downloaded.append(
                convert_rfdetr_botsort_model(
                    output_dir,
                    convert,
                    force=force,
                )
            )
    return downloaded


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download pinned model artifacts for later offline SDK use."
        )
    )
    parser.add_argument(
        "--pipeline",
        choices=("all", "rtdetr-osnet", "rfdetr-botsort"),
        default="all",
        help="Pipeline artifacts to download (default: all).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Artifact directory (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Redownload artifacts even when local copies exist.",
    )
    parser.add_argument(
        "--convert",
        choices=("onnx", "openvino"),
        help="Also convert each downloaded model to this format.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    output_dir = args.output_dir.expanduser().resolve()
    downloaded = download_models(
        output_dir,
        args.pipeline,
        force=args.force,
        convert=args.convert,
    )
    action = "Downloaded and converted" if args.convert else "Downloaded"
    print(f"{action} model artifacts to {output_dir}:")
    for path in downloaded:
        print(f"  {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

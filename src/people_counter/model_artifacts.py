"""Shared offline model artifact layout and validation."""

from pathlib import Path
from typing import Literal


RTDETR_PIPELINE_DIR = "rtdetr_osnet"
RTDETR_MODEL_DIRS = {
    "r18": "rtdetr_v2_r18vd",
    "r50": "rtdetr_v2_r50vd",
}
RTDETR_REQUIRED_FILES = (
    "config.json",
    "model.safetensors",
    "preprocessor_config.json",
)
OSNET_MODEL_DIR = "libre_reid_osnet"
OSNET_FILENAME = "osnet_ain_x0_25.pt"
RFDETR_PIPELINE_DIR = "rfdetr_botsort"
RFDETR_FILENAME = "rf-detr-large-2026.pth"


def _require_files(paths: tuple[Path, ...]) -> None:
    missing = [path for path in paths if not path.is_file()]
    if missing:
        formatted = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(
            f"Offline model artifacts are missing: {formatted}"
        )


def resolve_rtdetr_osnet_artifacts(
    models_dir: Path,
    detector_model: Literal["r18", "r50"],
) -> tuple[Path, Path]:
    pipeline_dir = models_dir.expanduser() / RTDETR_PIPELINE_DIR
    detector_dir = pipeline_dir / RTDETR_MODEL_DIRS[detector_model]
    reid_path = pipeline_dir / OSNET_MODEL_DIR / OSNET_FILENAME
    _require_files(
        tuple(detector_dir / filename for filename in RTDETR_REQUIRED_FILES)
        + (reid_path,)
    )
    return detector_dir, reid_path


def resolve_rfdetr_checkpoint(models_dir: Path) -> Path:
    checkpoint = (
        models_dir.expanduser() / RFDETR_PIPELINE_DIR / RFDETR_FILENAME
    )
    _require_files((checkpoint,))
    return checkpoint

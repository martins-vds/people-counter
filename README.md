# People Counter

Run exactly one PyTorch variant at a time. The script requires an explicit
device selection and will fail instead of falling back to another device.

## CPU-only

```bash
uv run --extra cpu main.py samples/three_people_walking.mp4 --device cpu
```

## NVIDIA GPU

The GPU variant uses the official PyTorch CUDA 12.8 wheels:

```bash
uv run --extra gpu main.py samples/three_people_walking.mp4 --device gpu
```

The telemetry CSV is written under `outputs/` with `_cpu` or `_gpu` in its
name. Its base name is derived from the input video. The script does not
generate an annotated video.

## Person re-identification

Person appearance is encoded with OSNet-AIN x0.25 from LibreYOLO. Active
tracks are assigned one-to-one with cosine similarity, centroid gating, and
Hungarian matching. An inactive identity gallery allows a person to retain
the same ID after leaving and re-entering the video.

The LibreYOLO implementation and the pinned LibreReID OSNet weight repository
are MIT licensed. The model card notes that its upstream training datasets
have research-oriented terms, so production deployments should review those
dataset, privacy, and biometric-use considerations independently.
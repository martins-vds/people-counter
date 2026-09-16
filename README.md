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
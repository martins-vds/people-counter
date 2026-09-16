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

By default, the script samples the source at 3 FPS, batches eight frames per
GPU detector call, uses the RT-DETRv2 R18 backbone, and uses FP16 detector
inference. Low-confidence detections below 0.6 are discarded. The R50 backbone
remains available when detection accuracy is more important than throughput.

```bash
# Tune GPU throughput and sampling
uv run --extra gpu main.py samples/subway.mp4 \
  --device gpu --sample-fps 3 --batch-size 8 \
  --detector-model r18 --detection-threshold 0.6

# Higher-accuracy detector, every source frame, FP32
uv run --extra gpu main.py samples/subway.mp4 \
  --device gpu --sample-fps all --batch-size 1 --no-fp16 --detector-model r50
```

CPU mode defaults to batch size 1 and never enables FP16. Progress and
effective sampled-frame throughput are printed while processing.

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

## RF-DETR Large and BoT-SORT comparison

`botsort_comparison.py` uses Roboflow RF-DETR Large detections, Supervision
`Detections`, and Roboflow Trackers' BoT-SORT implementation:

```bash
uv run --extra gpu botsort_comparison.py samples/subway.mp4 --device gpu
```

The CPU variant uses the same interface:

```bash
uv run --extra cpu botsort_comparison.py samples/subway.mp4 --device cpu
```

The script outputs only
`outputs/<video>_telemetry_rfdetr_large_botsort_<device>.csv`. Its sampling,
batch-size, confidence, and FP16 options match the primary script where
applicable.

Supervision is MIT licensed. RF-DETR code and RF-DETR Large weights are
Apache-2.0 licensed. Roboflow Trackers is Apache-2.0 licensed and pinned to
commit `3fb83d1618f29b7fdd4617757b6894e4ec71146d` from its `develop` branch for
reproducibility.

This BoT-SORT implementation does not include an appearance ReID branch.
Tracks that expire and later re-enter can receive a new ID, unlike the primary
OSNet identity gallery.
# People Counter

Run exactly one PyTorch variant at a time. The script requires an explicit
device selection and will fail instead of falling back to another device.

## CPU-only

```bash
uv run --extra cpu rtdetr_osnet_counter.py samples/three_people_walking.mp4 --device cpu
```

## NVIDIA GPU

The GPU variant uses the official PyTorch CUDA 12.8 wheels:

```bash
uv run --extra gpu rtdetr_osnet_counter.py samples/three_people_walking.mp4 --device gpu
```

By default, the script samples the source at 3 FPS, batches eight frames per
GPU detector call, uses the RT-DETRv2 R18 backbone, and uses FP16 detector
inference. Low-confidence detections below 0.6 are discarded. The R50 backbone
remains available when detection accuracy is more important than throughput.

```bash
# Tune GPU throughput and sampling
uv run --extra gpu rtdetr_osnet_counter.py samples/subway.mp4 \
  --device gpu --sample-fps 3 --batch-size 8 \
  --detector-model r18 --detection-threshold 0.6

# Higher-accuracy detector, every source frame, FP32
uv run --extra gpu rtdetr_osnet_counter.py samples/subway.mp4 \
  --device gpu --sample-fps all --batch-size 1 --no-fp16 --detector-model r50
```

CPU mode defaults to batch size 1 and never enables FP16. Progress and
effective sampled-frame throughput are printed while processing.

The telemetry CSV is written under `outputs/`. Its base name is derived from
the input video and ends with the device and a UTC run timestamp, for example
`subway_telemetry_gpu_20260916T230655123456Z.csv`. The script does not
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

`rfdetr_botsort_counter.py` uses Roboflow RF-DETR Large detections, Supervision
`Detections`, and Roboflow Trackers' BoT-SORT implementation:

```bash
uv run --extra gpu rfdetr_botsort_counter.py samples/subway.mp4 --device gpu
```

The CPU variant uses the same interface:

```bash
uv run --extra cpu rfdetr_botsort_counter.py samples/subway.mp4 --device cpu
```

The script outputs only a timestamped CSV such as
`outputs/subway_telemetry_rfdetr_large_botsort_gpu_20260916T230655123456Z.csv`.
Its sampling, batch-size, confidence, and FP16 options match the primary script
where applicable.

Supervision is MIT licensed. RF-DETR code and RF-DETR Large weights are
Apache-2.0 licensed. Roboflow Trackers is Apache-2.0 licensed and pinned to
commit `3fb83d1618f29b7fdd4617757b6894e4ec71146d` from its `develop` branch for
reproducibility.

This BoT-SORT implementation does not include an appearance ReID branch.
Tracks that expire and later re-enter can receive a new ID, unlike the primary
OSNet identity gallery.

## Line-crossing counts

Both scripts accept an optional directed counting line as source-video pixel
coordinates:

```bash
uv run --extra gpu rtdetr_osnet_counter.py samples/subway.mp4 \
  --device gpu --line 0 1080 3839 1080

uv run --extra gpu rfdetr_botsort_counter.py samples/subway.mp4 \
  --device gpu --line 0 1080 3839 1080
```

The coordinates are `X1 Y1 X2 Y2`, where `(X1, Y1)` is the start and
`(X2, Y2)` is the end of the directed line. Reversing the endpoints swaps the
meaning of `in` and `out`. Coordinates must be inside the source video frame.
Each script continues to write its identity telemetry CSV and additionally
writes a separate timestamped `line_counts` CSV containing per-sampled-frame
and cumulative in/out counts.

## Experiment notebooks

Install the notebook dependencies together with exactly one hardware variant:

```bash
uv sync --extra gpu --extra experiments
uv run --extra gpu --extra experiments jupyter lab
```

Use `--extra cpu` instead of `--extra gpu` for CPU experiments.

- `notebooks/scenario_benchmark.ipynb` runs both pipelines over the same
  scenario videos and records runtime, throughput, unique counts, and
  fragmentation proxies.
- `notebooks/tracking_quality_analysis.ipynb` compares benchmark results with
  optional count and entry/exit ground truth, then produces per-scenario
  rankings.

Run the benchmark notebook and the model subprocesses with the same hardware
variant. The summary telemetry can evaluate counts and temporal intervals but
cannot calculate frame-level MOT metrics such as IDF1 or HOTA.
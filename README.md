# People Counter

Run exactly one PyTorch variant at a time. Each command requires an explicit
device selection and will fail instead of falling back to another device.
The installed `people-counter` command provides `rtdetr-osnet` and
`rfdetr-botsort` subcommands; run `people-counter --help` for an overview.

The package separates its public Python SDK from the CLI, video sampling,
tracking, line counting, and CSV output. Applications can use
`people_counter.run(config)` with either typed pipeline configuration and
convert the result to dependency-free records for pandas, Spark, databases,
or APIs. The wheel includes a PEP 561 marker so downstream type checkers use
the SDK's inline annotations.

The legacy `people-counter-rtdetr` and `people-counter-rfdetr` commands remain
available as aliases for the two subcommands.

## Python SDK

Install the package with exactly one hardware variant, then import only from
the stable top-level API:

```python
from pathlib import Path

from people_counter import RTDetrOsnetConfig, run, telemetry_records

config = RTDetrOsnetConfig(
    video=Path("/data/entrance-camera.mp4"),
    device_variant="cpu",
    device="cpu",
    batch_size=1,
    sample_fps=3.0,
    detection_threshold=0.6,
    line=(0, 540, 1919, 540),
    progress_callback=lambda current: print(
        f"{current.processed_frames}/{current.total_sampled_frames}"
    ),
)

result = run(config)
records = telemetry_records(result)

print(f"Distinct people: {len(result.telemetry)}")
print(f"Entered: {result.line_in_count}")
print(f"Exited: {result.line_out_count}")
```

Use `RFDetrBotsortConfig` with the same `run` function to select the
RF-DETR/BoT-SORT pipeline. Each config and its mutable `RunResult` are
single-use. Create a new config for every invocation.

`telemetry_records(result)` and `line_count_records(result)` return typed
lists of built-in dictionaries. They intentionally do not depend on pandas or
Spark:

```python
import pandas as pd

from people_counter import line_count_records

telemetry_frame = pd.DataFrame(telemetry_records(result))
line_count_frame = pd.DataFrame(line_count_records(result))
```

If processing raises after video initialization, `config.result` retains
partial telemetry. Persist it if useful and re-raise the exception so an
orchestrator sees the failure:

```python
try:
    result = run(config)
finally:
    if config.result.initialized:
        records = telemetry_records(config.result)
        persist_records(records)
```

An exception still propagates from this `finally` block, allowing a data
pipeline to mark the activity as failed.

### Microsoft Fabric

Generate a platform-specific CPU or GPU deployment bundle, upload its wheels
to a Fabric Environment, and attach that environment to the notebook. A
Fabric Data Pipeline can then invoke the notebook as an activity.

Process each video sequentially in one notebook process because tracking state
depends on frame order. Parallelize across videos with separate notebook
activities rather than distributing frames from one video across Spark
executors.

Use a Lakehouse file path that OpenCV can open:

```python
from pathlib import Path

from people_counter import (
    RTDetrOsnetConfig,
    line_count_records,
    run,
    telemetry_records,
)

video = Path("/lakehouse/default/Files/incoming/entrance-camera.mp4")
if not video.is_file():
    raise FileNotFoundError(f"Video not found: {video}")

result = run(
    RTDetrOsnetConfig(
        video=video,
        device_variant="cpu",
        device="cpu",
        batch_size=1,
        line=(0, 540, 1919, 540),
    )
)

telemetry = [
    {"source_video": video.name, **record}
    for record in telemetry_records(result)
]
line_counts = [
    {"source_video": video.name, **record}
    for record in line_count_records(result)
]

if telemetry:
    (
        spark.createDataFrame(telemetry)
        .write.format("delta")
        .mode("append")
        .saveAsTable("people_counter_telemetry")
    )

if line_counts:
    (
        spark.createDataFrame(line_counts)
        .write.format("delta")
        .mode("append")
        .saveAsTable("people_counter_line_counts")
    )
```

For retry-safe pipelines, add a stable run identifier and use Delta merge
semantics instead of unconditional append. If an `abfss://` URI cannot be
opened by OpenCV, stage the video in notebook-local storage before invoking
the SDK and write the result back to OneLake.

See the
[retry-safe Fabric pipeline notebook](notebooks/fabric_retry_safe_pipeline.ipynb)
for parameter validation, local staging, input/configuration fingerprints,
idempotent Delta merges, failed-attempt snapshots, and a run-status ledger.
For event-driven ADLS ingestion, bounded dispatch, lease recovery,
observability, Direct Lake reporting, and large backfills, use the
[production Fabric implementation plan](notebooks/fabric/README.md).

### Publish backfill manifests

Preparation and publication are separate. First generate a
destination-independent manifest package without Azure access:

```bash
uv sync --extra publisher

uv run prepare-manifests \
  --catalog config/camera_catalog.csv \
  --video-root /mnt/source-videos \
  --partition-prefix north-entrance/camera-17/ \
  --output-dir prepared/camera-17
```

Later, the same or a different operator can publish that package:

```bash
uv run --extra publisher publish-manifests \
  --manifest-package-dir prepared/camera-17 \
  --video-root /mnt/source-videos \
  --storage-account <storage-account> \
  --filesystem <source-filesystem> \
  --checkpoint state/camera-17-publication.sqlite3
```

The host must also provide `ffprobe` from an approved FFmpeg installation.
Preparation writes `manifest-package.json`, prepared JSON manifests,
generated inventory, rejection report, summary, and checkpoint under
`--output-dir`; it does not copy videos. Provide `--inventory` only for files
whose capture time cannot be recovered from embedded metadata or a configured
filename rule. Publication validates all package checksums and the exact video
bytes before creating final manifests with destination URIs and ETags. See the
[camera catalog and publisher runbook](notebooks/fabric/README.md#71-create-the-camera-metadata-catalog)
for the complete CSV schema, Azure identity requirements, retry behavior, and
backfill registration procedure.

### Build CPU or GPU deployment bundles

The [bundle generator](scripts/build_sdk_bundle.py) builds the SDK wheel,
exports the selected locked dependency graph, builds or downloads every
dependency wheel, and creates a ZIP with a checksum manifest:

```bash
# CPU-only PyTorch bundle
uv run python scripts/build_sdk_bundle.py cpu

# CUDA 12.8 PyTorch bundle
uv run python scripts/build_sdk_bundle.py gpu
```

Use `--dry-run` to inspect the selected platform, output, and PyTorch index
without downloading anything. Use `--output-dir PATH` to change the
destination and `--force` to atomically replace an existing bundle.

The default artifact name identifies the SDK version, selected variant,
current platform, and Python ABI under `dist/sdk-bundles/`. Each ZIP contains:

- `wheels/` with the SDK and all variant-specific dependency wheels;
- `requirements-cpu.lock` or `requirements-gpu.lock`;
- `requirements-vcs.lock` with separately built, commit-pinned VCS
  dependencies;
- `manifest.json` with SHA-256 checksums and runtime metadata;
- `README.txt` with the offline installation command.

Bundles are specific to the operating system, architecture, and Python
version on which they are generated. Build the bundle on a host matching the
target Fabric runtime or deployment environment. The generator includes only
the selected PyTorch index, so a CPU bundle cannot accidentally resolve CUDA
wheels and a GPU bundle cannot resolve CPU-only wheels.

## CPU-only

```bash
uv run --extra cpu people-counter rtdetr-osnet samples/three_people_walking.mp4 --device cpu
```

## NVIDIA GPU

The GPU variant uses the official PyTorch CUDA 12.8 wheels:

```bash
uv run --extra gpu people-counter rtdetr-osnet samples/three_people_walking.mp4 --device gpu
```

By default, the command samples the source at 3 FPS, batches eight frames per
GPU detector call, uses the RT-DETRv2 R18 backbone, and uses FP16 detector
inference. Detections down to 0.1 can maintain an existing track, while the
configured detection threshold (0.6 by default) is required to start a track.
The R50 backbone remains available when detection accuracy is more important
than throughput.

```bash
# Tune GPU throughput and sampling
uv run --extra gpu people-counter rtdetr-osnet samples/subway.mp4 \
  --device gpu --sample-fps 3 --batch-size 8 \
  --detector-model r18 --detection-threshold 0.6

# Higher-accuracy detector, every source frame, FP32
uv run --extra gpu people-counter rtdetr-osnet samples/subway.mp4 \
  --device gpu --sample-fps all --batch-size 1 --no-fp16 --detector-model r50
```

CPU mode defaults to batch size 1 and never enables FP16. Progress and
effective sampled-frame throughput are printed while processing.

The telemetry CSV is written under `outputs/`. Its base name is derived from
the input video and ends with the device and a UTC run timestamp, for example
`subway_telemetry_gpu_20260916T230655123456Z.csv`. The subcommand does not
generate an annotated video. Use `--output-dir PATH` to select another output
directory.

## Person re-identification

Person appearance is encoded with OSNet-AIN x0.25 from LibreYOLO. Active
tracks are assigned one-to-one with cosine similarity, centroid gating, and
Hungarian matching. Tracks require detections in two sampled frames before
they are counted. The centroid gate scales with frame height and elapsed video
time, and a 30-second inactive identity gallery allows a person to retain the
same ID after an occlusion or re-entry without matching against arbitrarily
old appearances. Low-confidence detections are embedded only when they are
near an active track; re-entry requires the configured activation confidence.
When appearance matching is temporarily inconclusive, a looser appearance
floor plus one-to-one spatial matching can preserve a nearby live track. The
spatial bound primarily constrains short gaps; after a long absence, identity
recovery is effectively appearance-based within the 30-second gallery window.

The LibreYOLO implementation and the pinned LibreReID OSNet weight repository
are MIT licensed. The model card notes that its upstream training datasets
have research-oriented terms, so production deployments should review those
dataset, privacy, and biometric-use considerations independently.

## RF-DETR Large and BoT-SORT comparison

`people-counter rfdetr-botsort` uses Roboflow RF-DETR Large detections, Supervision
`Detections`, and Roboflow Trackers' BoT-SORT implementation:

```bash
uv run --extra gpu people-counter rfdetr-botsort samples/subway.mp4 --device gpu
```

The CPU variant uses the same interface:

```bash
uv run --extra cpu people-counter rfdetr-botsort samples/subway.mp4 --device cpu
```

The command outputs only a timestamped CSV such as
`outputs/subway_telemetry_rfdetr_large_botsort_gpu_20260916T230655123456Z.csv`.
Its sampling, batch-size, confidence, and FP16 options match the primary pipeline
where applicable. Both pipelines detect down to confidence 0.1 for secondary
association and require `--detection-threshold` confidence to activate a new
track. RT-DETR requires a consecutive second detection to confirm it, but that
second detection may use the lower secondary confidence when it is near the
tentative track. Values below 0.1 are rejected. BoT-SORT camera-motion
compensation defaults to off for sampled input and on when every source frame
is processed; `--cmc` and `--no-cmc` override that choice.

BoT-SORT telemetry backdates a newly confirmed track by one sampled frame to
approximate the primary OSNet pipeline's first-sighting convention. This is
exact when confirmation occurs in consecutive sampled frames. Counts remain
different by design after longer occlusions: BoT-SORT retains tracks for at
least one second and has no appearance gallery, while OSNet can re-identify a
person for up to 30 seconds.

Run the focused tracking and frame-reader regressions with:

```bash
uv run --extra cpu --extra publisher python -m unittest discover -s tests -v
```

Supervision is MIT licensed. RF-DETR code and RF-DETR Large weights are
Apache-2.0 licensed. Roboflow Trackers is Apache-2.0 licensed and pinned to
commit `3fb83d1618f29b7fdd4617757b6894e4ec71146d` from its `develop` branch for
reproducibility.

This BoT-SORT implementation does not include an appearance ReID branch.
Tracks that expire and later re-enter can receive a new ID, unlike the primary
OSNet identity gallery.

## Line-crossing counts

Both subcommands accept an optional directed counting line as source-video pixel
coordinates:

```bash
uv run --extra gpu people-counter rtdetr-osnet samples/subway.mp4 \
  --device gpu --line 0 1080 3839 1080

uv run --extra gpu people-counter rfdetr-botsort samples/subway.mp4 \
  --device gpu --line 0 1080 3839 1080
```

The coordinates are `X1 Y1 X2 Y2`, where `(X1, Y1)` is the start and
`(X2, Y2)` is the end of the directed line. Reversing the endpoints swaps the
meaning of `in` and `out`. Coordinates must be inside the source video frame.
Each subcommand continues to write its identity telemetry CSV and additionally
writes a separate timestamped `line_counts` CSV containing per-sampled-frame
and cumulative in/out counts.

A new track must establish its side of the line before a crossing can be
registered. The same warm-up applies after an occlusion or re-entry longer
than the track's coasting window, and crossings that occur entirely during
such a gap cannot be recovered. Place counting lines away from entry edges
and increase `--sample-fps` when people can reach the line within the first
three sampled frames.

## Notebooks

Install the notebook dependencies together with exactly one hardware variant:

```bash
uv sync --extra gpu --extra experiments
uv run --extra gpu --extra experiments jupyter lab
```

Use `--extra cpu` instead of `--extra gpu` for CPU experiments.

- The [Python SDK tutorial](notebooks/sdk_tutorial.ipynb) teaches typed
  configuration, pipeline selection, structured results, DataFrame
  conversion, partial-result handling, and Microsoft Fabric Delta writes.
- The [CLI tutorial](notebooks/cli_tutorial.ipynb) teaches safe CLI
  orchestration, option discovery, output ingestion, model selection, and
  Fabric notebook activity integration.
- `notebooks/scenario_benchmark.ipynb` runs both pipelines over the same
  scenario videos and records runtime, throughput, unique counts, and
  fragmentation proxies.
- `notebooks/tracking_quality_analysis.ipynb` compares benchmark results with
  optional count and entry/exit ground truth, then produces per-scenario
  rankings.

Run the benchmark notebook and the model subprocesses with the same hardware
variant. The summary telemetry can evaluate counts and temporal intervals but
cannot calculate frame-level MOT metrics such as IDF1 or HOTA.
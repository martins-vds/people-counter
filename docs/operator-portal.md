# Local operator portal

The portal is a server-rendered Python application, not a SPA. It reuses the
camera catalog, manifest package, profile, publisher, and explicit-backfill
implementations. It does not run inference locally or start Spark.

## Start the app

Install Docker with Compose v2.20+ and run from the repository root:

```bash
docker compose --profile portal up --build --wait portal
```

Open **http://127.0.0.1:8088** in your browser, or in VS Code's integrated browser.
Specifying `portal` starts only the portal; existing Spark services are unchanged.
The first build downloads Python dependencies, FFmpeg, and Azure CLI.
If VS Code does not expose a native ZIP save dialog, use a standard browser for
downloads; the download endpoint and archive contents are covered by Chromium E2E.

By default, the portal browses `samples/`. The default model mount is empty:
models are not needed to browse footage, calibrate cameras, or generate packages.
Git LFS sample pointers are not playable videos; fetch the sample clips or mount
your own footage.

To select local folders, add these values to `.env` before starting. Use
[.env.portal.example](../.env.portal.example) as a reference; **merge with an
existing `.env` rather than overwriting it**.

```dotenv
PC_PORTAL_FOOTAGE_DIR=/absolute/path/to/footage
PC_PORTAL_MODELS_DIR=/absolute/path/to/approved-models
PC_PORTAL_PORT=8088
PC_PORTAL_ALLOW_CLOUD=0
```

Both folders must already exist. Compose mounts footage and models read-only
at `/data/videos` and `/data/models`. The browser's folder field is relative
to the allowed footage root; it cannot browse the rest of the host filesystem.
Portal packages use the logical source prefix `videos/`.

The local source boundary is isolated in `LocalFootageSource`. Cloud **source**
adapters are not implemented yet; the disabled source selector says so.
Cloud publication of local footage is a separate, supported operation below.

Stop without deleting state:

```bash
docker compose --profile portal stop portal
```

Camera metadata, generated catalogs, packages, job inputs/results, and progress
live in the `portal-state` named volume. Azure CLI credentials have a separate
`portal-azure` volume. Treat both as private and do not share their backups.
Do not use `docker compose down --volumes` unless you intend to delete that state.

## Operator workflow

Start on **Locations** to add location IDs independently of footage and cameras.
A location may have zero cameras indefinitely. Location counts show distinct
camera IDs, not the number of effective-dated configurations. The camera form
selects an existing location instead of accepting arbitrary location text.

1. **Footage:** open a folder and select a video. Previews decode a frame on the
   server, so browser video-codec support is not required.
2. **Counting line:** select the preview time, then click two endpoints or enter
   their coordinates. Drawing uses the original frame's pixel coordinates,
   including when the canvas is scaled on a small screen. The arrow shows
   endpoint order, not walking direction; reversing endpoints swaps in/out.
3. **Camera metadata:** enter a camera ID, select an existing location, and set timezone, timestamp rules,
   and effective dates. A camera default applies to matching videos in that
   folder. Review the Cameras page or download its CSV.
   All configurations sharing a camera ID must use one location. A save that
   would split that camera across locations is rejected without changing data.
   A camera with a single configuration can be moved by editing its location;
   moving multiple configurations together is not an implicit side effect.
4. **Video exceptions:** choose "This video only" to override that video's line
   and frame dimensions. An optional authoritative UTC timestamp overrides
   capture time; a blank timestamp retains normal embedded/filename resolution.
   Camera defaults are not changed. A camera default must still cover the video.
5. **Packages:** generate an offline manifest package. Preparation snapshots
   metadata before queuing the job, probes/hashes videos, and displays progress
   and rejection reports. The safety bound is 10,000 videos per package; larger
   folders fail explicitly rather than being truncated. Use the CLI for a
   deliberately different bound. Resolve rejections before publication.
6. **Profiles:** an administrator enters target binding fields in the form and
   selects supported presets. The background job hashes the approved local model
   files and generates the target catalog. Nobody enters model/config/runtime
   hashes. Missing models fail explicitly. All selected model bytes must match
   the artifacts deployed in Fabric.
   Optional advanced controls select batch size and sampling FPS; the shared
   resolver validates them and recalculates identities automatically.
7. **Cloud work:** choose a completed package, target/profile, and explicit action.
   Publication-only and publish-plus-registration are separate choices.
   "Wait for processing" applies only to explicit submission.

Package ZIP downloads contain metadata, not source videos. Retain the unchanged
footage. Changing camera metadata requires a new package. Changing a profile or
release is a new processing run, not recovery of an existing run.

### Per-video calibration CSV

The shared exception inventory retains its original timestamp-only format and
also accepts these optional columns:

```text
frame_width,frame_height,counting_line_x1,counting_line_y1,counting_line_x2,counting_line_y2
```

If any calibration value is supplied, all six are required and validated.
`captured_at_utc` may be blank only for a complete calibration override. Explicit
`capture_time_source=inventory` still requires a real UTC timestamp.

## Enable cloud actions deliberately

Cloud actions are disabled by default, and the server rejects forged requests
even if a button is bypassed. No Azure credentials are needed for local work.

When ready, authenticate **inside this container**, without copying secrets
into the repository or using the host's Azure cache:

```bash
docker compose --profile portal exec portal az login --use-device-code
docker compose --profile portal exec portal az account show
```

Review the identity/tenant and deployment, set `PC_PORTAL_ALLOW_CLOUD=1` in `.env`,
then apply the change:

```bash
docker compose --profile portal up -d --wait portal
```

Every publication/submission also requires confirmation in the UI.
Publishing can trigger arrival-driven automation. Coordinate competing intake
before registering explicit work. Model files, release evidence, target
permissions, and matching SDK/Environment releases must already be deployed.
See the [explicit backfill runbook](explicit-backfill.md) for those prerequisites.
The UI does not provision infrastructure, grant permissions, or upload models.

## Job lifetime and recovery

On upgrade, existing camera location IDs are backfilled into the independent
location registry automatically. Existing job snapshots and package files are
not rewritten. Conflicting legacy assignments for the same camera ID fail
closed with an explicit error and no partial migration; correct those
assignments before upgrading rather than guessing which location owns a camera.

One background worker owns the durable SQLite queue; the app holds an exclusive
process lock. Do not scale this Compose service or share its state with another
running portal. A distributed worker/queue and authentication are prerequisites
for future shared/cloud hosting.

Closing the browser does not cancel jobs. After a server interruption, in-flight
jobs become `INTERRUPTED`, never successful automatically. "Resume with retained
state" keeps the same job ID, snapshots, publication checkpoint, and Fabric job
locations. Cloud resumes require confirmation again. Unknown submission outcomes
remain blocked pending investigation; the UI never deletes registration evidence
or generates a replacement identity to hide a conflict.

`SUCCEEDED` for a submission without waiting means registration completed.
Waiting requires every exact work ID to succeed with a committed attempt.
Gold/Reconciliation/report refresh remain separate checks. A fatal queue-worker
error is shown in the UI and makes the health check fail.

## Development and UI tests

Without Docker:

```bash
uv sync --extra portal --extra publisher --extra demo
uv run --no-sync pc-admin --source-root /path/to/footage \
  --models-root /path/to/models --state-dir .people-counter-portal
```

Run Playwright with synthetic footage and **fake cloud adapters**, not Azure
credentials or actual model inference:

```bash
uv sync --extra portal --extra publisher --extra demo --group ui
uv run --no-sync playwright install --with-deps chromium
uv run --no-sync pytest tests/ui --portal-e2e --browser chromium \
  --tracing retain-on-failure --screenshot only-on-failure \
  --output evidence/portal-ui/playwright \
  --cov=people_counter --cov-branch --crap
```

Ordinary pytest runs skip browser tests unless `--portal-e2e` is specified.
CI's credential-free validation builds and health-checks the Compose image,
runs Playwright, and retains failed-test traces/screenshots and Compose logs.
Tests cover desktop/mobile line drawing, different-resolution video exceptions,
camera metadata, package ZIP contents, durable job/error display, profile
generation, publication/submission/wait wiring, CSRF, and source/cloud gates.

This is a single trusted operator's localhost tool, **not an authenticated
multi-user service**. Keep the loopback port binding. Network/cloud hosting
requires authentication, authorization, a storage-source adapter, and a
multi-worker-safe job system rather than just changing the listen address.

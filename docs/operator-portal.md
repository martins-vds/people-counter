# Local operator portal

The portal is a server-rendered Python application, not a SPA. It reuses the
camera catalog, manifest package, profile, publisher, and explicit-backfill
implementations. It does not run inference locally or start Spark.

## Start the app

Install Docker with Compose v2.20+ and run from the repository root:

```bash
./scripts/start-portal.sh
```

The script runs `docker compose --profile portal up --build --wait portal`
from the repository root, even when invoked by absolute path from another
directory. It uses existing `.env` values and environment overrides without
rewriting configuration, and exits nonzero if Compose fails. Compose waits for
healthy services and leaves them running in the background.

Open **http://127.0.0.1:8088** in your browser, or in VS Code's integrated browser.
Specifying `portal` starts the portal and its Tailwind compiler dependency;
existing Spark services are unchanged. The first build downloads Python
dependencies, FFmpeg, Azure CLI, and the checksum-verified Tailwind CLI.
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
at `/data/videos` and `/data/models`. The browser's optional folder field is relative
to the allowed footage root; it cannot browse the rest of the host filesystem.
Portal packages use the logical source prefix `videos/`.

The local source boundary is isolated in `LocalFootageSource`. Cloud **source**
adapters are not implemented yet; the disabled source selector says so.
Cloud publication of local footage is a separate, supported operation below.

Stop without deleting state:

```bash
docker compose --profile portal stop portal portal-tailwind
```

Camera metadata, generated catalogs, packages, job inputs/results, and progress
live in the `portal-state` named volume. Azure CLI credentials have a separate
`portal-azure` volume. Treat both as private and do not share their backups.
Do not use `docker compose down --volumes` unless you intend to delete that state.
The separate `portal-css` volume contains only generated public styles.

## Live Tailwind design

The UI follows the reference repository's Python-served HTML plus standalone
Tailwind watcher pattern, without a SPA, runtime CSS CDN, or runtime Node package tree.
Tailwind v4.3.0 is pinned and its Linux x64/arm64 downloads are SHA-256 verified.

- Design tokens and shared form/table/button/badge styles:
  `src/people_counter/portal_styles/input.css`.
- Server-rendered pages and shared components:
  `src/people_counter/portal_templates/`.
- Generated production asset:
  `src/people_counter/portal_static/build/portal.css`.

The `portal-tailwind` Compose service builds once, then checks content hashes of
the small template/CSS/portal-JavaScript source set once per second. It invokes
the same standalone compiler only when content changes. This works detached
and across Docker bind mounts that do not deliver native file-watch events.
It writes only to the shared CSS volume; it cannot change the checkout, footage,
models, or portal state and has no network access at runtime.
The portal mounts UI sources and generated styles read-only. Jinja template
auto-reload and uncached assets let you refresh the browser to see edits without
restarting jobs. Python/business-logic changes still require rebuilding the image.

Compiled CSS is also committed and packaged for standalone/image use without
the watcher. Regenerate it after changing templates or design sources:

```bash
docker compose --profile portal build portal-tailwind
mkdir -p src/people_counter/portal_static/build
docker compose --profile portal run --rm --no-deps \
  --entrypoint sh portal-tailwind -c \
  'tailwindcss -i /source/portal_styles/input.css -o /tmp/portal.css --minify && cat /tmp/portal.css' \
  > src/people_counter/portal_static/build/portal.css
```

CI compares the running compiler's output against that committed asset, so stale
production styles fail validation instead of shipping silently.

## JavaScript tests and change risk

Node 24 and npm are needed only for development checks, not to run the portal.
From the repository root:

```bash
npm ci --ignore-scripts --no-audit --no-fund
npm run test:portal
npm run test:portal:mutation
```

Jest runs DOM behavior tests and collects Istanbul coverage for every JavaScript
file under `src/people_counter/portal_static/`, including untested files.
The tests use intent-based assertions: accessible roles and labels identify
controls, `user-event` performs supported user interactions, and `jest-dom`
matchers describe visible, enabled, checked, and focused states. Async tests
wait for observable results with `findBy` or `waitFor`, not fixed sleeps.
Fixtures retain the production controls' labels and semantics. Direct events
remain appropriate for native validation, image loading, and malformed inputs.

Assert behavior rather than internal loading flags, CSS class names, or incidental
URL parameter order. Request counts still verify lazy loading and caching.
Canvas API assertions verify the graphics boundary; independent geometry tests
check arrow direction, symmetry, and scaling. These mocked-canvas tests do not
prove actual browser rendering; the Playwright suite covers browser integration.

[`crap-score`](https://github.com/ahilke/js-crap-score) combines each function's
cyclomatic complexity and statement coverage. The check fails if any function
scores **10 or higher**, has an invalid score, or the report has no functions.
Adding behavior tests lowers coverage-related risk; functions with complexity
10 or higher need refactoring even with full coverage.

[`StrykerJS`](https://github.com/stryker-mutator/stryker-js) mutates every portal
JavaScript source with its default mutation operators and runs the related Jest
tests. The mutation gate requires **at least 96%**, which is strictly above 95%.
No production files or mutation operators are excluded to raise the score.
The sandbox contains only JavaScript sources, tests, and their configuration.
Node resolves dependencies from `node_modules` at the repository root;
automatic dependency symlinking is disabled to avoid recursively scanning
unrelated local Spark data directories.
The mutation timeout includes a 15-second margin so multiple asynchronous
assertions can report failures before Stryker classifies a mutant as timed out.

JSON/HTML coverage, CRAP, and mutation reports are written under
`evidence/portal-js/`; open `mutation.html` to inspect surviving mutants.
PR validation runs both commands and uploads these reports for 14 days.
The existing Playwright tests remain responsible for real-browser integration.

## VS Code and Codespaces

The dev container forwards port **8088**, labels it **People Counter operator
portal**, and notifies you when it becomes available. Rebuild the dev container
to apply changed forwarding settings, start Compose, then open the port from the
VS Code **Ports** panel.

Keep the Codespaces port visibility **Private**. This remains a single-operator
tool, not a publicly authenticated service. Compose passes only the nonsecret
Codespaces forwarding variables into the portal. The app allows the exact
forwarded hostname for this codespace and port, accepts its HTTPS form origin
through the proxy, and uses secure cookies there. It does not trust wildcard
`app.github.dev` hosts or arbitrary forwarded headers. Localhost operation is
unchanged. Live Codespaces access requires a codespace; proxy/host behavior is
covered by request tests.

## Footage explorer

The footage page uses folder navigation beside a compact file table.
Breadcrumbs and the parent link retain the current folder context. On narrow
screens, choose **Folders** to show or hide the navigation pane. Folder nodes
use native disclosures with keyboard-accessible folder links.

Only the selected folder and its ancestors are expanded initially. Other
branches request their immediate subfolders from the read-only `/folders`
endpoint when expanded; there is no recursive video scan or thumbnail decoding.
Loaded branches are retained for the current page. Loading failures remain
visible with a Retry action instead of appearing as an empty folder.

Name filtering is a case-insensitive substring match in the **current folder**,
not a recursive search or a glob expression. The type filter supports all items,
videos, and folders. Filters persist through folder navigation; **Clear filters**
retains the folder. Empty folders and filters with no matches have distinct states.

Folders sort first, followed by files and blocked links. Size is real filesystem
metadata, and modification times are explicitly UTC. Modification time is not
the recording/capture time used in manifests. Unavailable metadata is reported
on its row. Links outside the mounted source are blocked without exposing their
metadata. Browsing does not modify source files.

Each video has one **Calibrate** action, opening the existing counting-line
editor. **Generate manifest package** opens the existing package dialog with the
current folder retained. Source/mount information and manual path entry remain
under **Source configuration**. There are no upload, rename, or delete actions.

## Operator workflow

Start on **Locations** and choose **Add location** to add a display name
independently of footage and cameras.
The portal previews and generates its ID: `North Store` becomes
`north-store`. Names are trimmed, accented Latin characters are normalized,
and IDs use lowercase ASCII letters, digits, and hyphens. Names without a usable
letter/digit and duplicate derived IDs are rejected explicitly.

Use **Add camera** on a location row or the **Cameras** page to register a camera
without opening footage. Choose its location and display name. For example,
`North Store` / `Main Entrance` produces `north-store-main-entrance`. The location
prefix allows different sites to use the same camera display name. Each camera
belongs to one location, even before calibration. A location may have zero or
many cameras; counts include registered cameras that are not calibrated.

Existing IDs, calibration records, and queued/completed job snapshots are never
re-slugged. On upgrade, legacy locations/cameras acquire their existing IDs as
display names. Registration is separate from calibration; uncalibrated cameras
are not exported as fabricated camera-catalog rows.

1. **Footage:** select one or more video checkboxes, choose a registered camera,
   and use **Assign selected footage**. A camera can own many recordings, including
   recordings from different days/folders. Repeating assignment updates ownership
   without changing the saved counting line. The camera's footage page lists all
   associated files and identifies the calibration example separately.
   Assignment is allowed before calibration: the UI says **Not calibrated** until
   a counting line is saved. Package preparation rejects selected footage assigned
   to an uncalibrated camera; association alone never creates a synthetic line.
   Existing calibration example files are retained as assignments on upgrade.
2. **Counting line:** select the preview time, then click two endpoints or enter
   their coordinates under **Manual line coordinates**. Drawing uses the original frame's pixel coordinates,
   including when the canvas is scaled on a small screen. The arrow shows
   endpoint order, not walking direction; reversing endpoints swaps in/out.
3. **Camera metadata:** when first calibrating, select a location, then one of that location's
   registered cameras. Changing location clears the previous camera selection.
   Choose the camera timezone from the IANA timezone list and set timestamp rules
   and effective dates under **Recording time and effective dates**. Editing a
   calibration restores its saved selections and expands nondefault recording rules.
   The example video is not the complete assignment list. Existing counting lines
   are restored when reviewing another recording from the same camera. Multiple
   effective-dated configurations remain separate. Use **Edit metadata** on the
   camera page to change recording-time rules/effective dates without opening
   geometry or decoding footage; use **Edit counting line** for geometry.
   Legacy folder defaults continue to apply where a folder has one camera.
   When different cameras share a folder, assign each video explicitly.
   Review the Cameras page or download its catalog CSV. Calibration cannot
   create an unregistered camera or move one to another location; forged requests
   are rejected without changing metadata.
4. **Video exceptions:** choose "This video only" to override that video's line
   and frame dimensions. An optional authoritative UTC timestamp overrides
   capture time; a blank timestamp retains normal embedded/filename resolution.
   Camera defaults are not changed. A camera configuration must still cover the video.
5. **Packages:** choose **Generate manifest package** and review the folder in
   its dialog. **Prepare this folder** opens it with the selected folder retained.
   Preparation snapshots metadata and footage ownership before queuing the job,
   probes/hashes videos, and displays progress and rejection reports.
   This is an **offline preparation job only**: it does not upload footage,
   publish to cloud storage, submit Fabric work, or run inference. Cloud actions
   remain separate and require approval.
   Use **View** beside each JSON/CSV report on the job page to inspect it in the
   UI; original downloads remain available. CSV rows and JSON record collections
   have 50-item pagination, with section links for multiple JSON collections.
   JSON uses labeled fields and expandable records rather than raw JSON.
   Inline reports are limited to 5 MiB, 50 nested records/fields per collection,
   and six levels of nesting; larger/deeper content is explicitly marked and the
   original download is available. Empty and invalid reports are reported visibly.
   The safety bound is 10,000 videos per package; larger
   folders fail explicitly rather than being truncated. Use the CLI for a
   deliberately different bound. Resolve rejections before publication.
6. **Profiles:** the page keeps the approved target catalog visible. Choose
   **Add target and profiles** to open the creation form in a modal dialog.
   Escape, Close, and Cancel return to the catalog without creating a job;
   entered values remain available until navigating away.
   An administrator enters target binding fields in the form and
   selects supported presets. The background job hashes the approved local model
   files and generates the target catalog. Nobody enters model/config/runtime
   hashes. Missing models fail explicitly. All selected model bytes must match
   the artifacts deployed in Fabric.
   Optional advanced controls select batch size and sampling FPS; the shared
   resolver validates them and recalculates identities automatically.
7. **Cloud work:** choose **Start cloud action** to review a completed package,
   target/profile, and explicit action in its dialog.
   Publication-only and publish-plus-registration are separate choices.
   "Wait for processing" applies only to explicit submission.

Package ZIP downloads contain metadata, not source videos. Retain the unchanged
footage. Changing camera metadata requires a new package. Changing a profile or
release is a new processing run, not recovery of an existing run.

### Progressive disclosure

Directory tables and catalogs stay visible. Location/camera creation, package
generation, profile setup, cloud actions, and resume confirmations share the same
native modal pattern: a named action button, labeled dialog, Close/Cancel/Escape,
viewport-bounded scrolling, and keyboard focus returned to the action button.
Creation shortcuts retain their selected location or folder. Required fields
and explicit approvals remain visible within the active dialog. Draft values
survive closing/reopening on the same page, but cloud approval checkboxes are
cleared so each submission/resume needs fresh consent.

Folder browsing and frame-preview controls stay inline, as does the counting-line
editor with its location/camera/timezone selections. Job status, downloads,
errors, safety warnings, and recovery guidance remain visible outside dialogs.
Source configuration, manual coordinates, recording rules, camera geometry/date
details, package explanations, inference overrides, completed-job logs, and raw
result JSON are available through labeled native disclosures. They work with a
keyboard without changing pages or losing entered values.

The calibration scope shows only the applicable camera or video-exception fields.
Wait controls and their explanation appear only for submission; switching to
publication clears the wait selection. Active/failed job progress, safety warnings,
and resume confirmations are never collapsed automatically. If native form
validation finds an invalid field in a closed disclosure, it opens before the
browser focuses the field.

### Per-video calibration CSV

The camera catalog also accepts an optional `video_relative_path` column for an
exact footage-to-camera assignment. Its path is relative to the same `video_root`
as source discovery (portal exports use `videos/...` with the default mount).
Exact assignments take precedence over folder-prefix defaults, including for
timestamp/configuration errors: an invalid exact assignment does not silently
fall back to another camera. Duplicate exact assignments with overlapping effective
dates are rejected. The portal exports these bindings with the catalog and embeds
the resolved camera ID in each prepared video manifest. Use an SDK containing
this exact-assignment support when consuming an exported portal catalog.

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

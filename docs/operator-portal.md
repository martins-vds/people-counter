# Local operator portal

The portal is a server-rendered Python application, not a SPA. It reuses the
camera catalog, manifest package, profile, publisher, and explicit-backfill
implementations. It does not run inference locally or start Spark.

## Configure ADLS and runtime credentials

The portal has one environment-configured ADLS Gen2 source. It does not browse
local footage folders or discover subscriptions/accounts. Configure an existing
account, filesystem and optional allowed root before starting:

```bash
export PC_PORTAL_ADLS_ACCOUNT_URL=https://youraccount.dfs.core.windows.net
export PC_PORTAL_ADLS_FILESYSTEM=videos
export PC_PORTAL_ADLS_ROOT=intake
export PC_PORTAL_MAX_UPLOAD_BYTES=5368709120
./scripts/start-portal-local.sh
```

This Docker-free launcher requires `uv` and Python 3.11.5 or newer. It installs
the locked portal, publisher and demo dependencies as needed, then runs in the
foreground on loopback port 8088. Stop it with Ctrl+C. It does not start Spark
or a Tailwind watcher; it serves the checked-in CSS. Set `PC_PORTAL_PORT` or pass
`--port` to change the port. Other `pc-admin` arguments are forwarded unchanged:

```bash
./scripts/start-portal-local.sh --port 8090 --models-root /path/to/models
```

The launcher loads the repository-root `.env` file with `uv` when it exists;
exported shell variables take precedence. This includes credentials, ADLS settings
and `PC_PORTAL_PORT`. It does not execute the file as shell code. Keep credentials
outside source control. Without `.env`, it uses the current shell alone.
Models default to `offline-models` and state to
`.people-counter-portal`, both relative to the repository root. Package outputs
default to the state directory's `packages` subdirectory. Host native libraries
needed by OpenCV/PyAV must be available (on Debian/Ubuntu, `libgl1` and
`libglib2.0-0`); dependency or startup failures are not suppressed.

Add `--allow-cloud` only when publication/submission should be enabled.
ADLS intake writes are explicitly requested file-management actions; the cloud
gate still controls publication/submission, not uploads or native storage moves.
Read operations require data-plane read/list access; uploads, folder creation,
rename and move require appropriate data-plane RBAC/ACL permissions and network
access. Missing configuration fails at startup; unavailable credentials and
permission failures are shown as errors, never empty successful listings.
ADLS bounded native decoding currently requires a fork-capable host; unsupported
hosts report that limitation explicitly. Linux is the supported host path.

The application uses one shared native SDK credential owner, with the environment
credential and workload identity precedence documented by
[DefaultAzureCredential](https://learn.microsoft.com/python/api/azure-identity/azure.identity.defaultazurecredential):

1. A nonempty `AZURE_CLIENT_SECRET` or `AZURE_CLIENT_CERTIFICATE_PATH` selects
   [EnvironmentCredential](https://learn.microsoft.com/python/api/azure-identity/azure.identity.environmentcredential).
   Set `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` as well. The SDK prefers a client
   secret over a certificate if both are configured.
2. A nonempty `AZURE_FEDERATED_TOKEN_FILE` selects
   [WorkloadIdentityCredential](https://learn.microsoft.com/python/api/azure-identity/azure.identity.workloadidentitycredential).
   Set `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` and configure the federated identity
   trust for the token's issuer, subject and audience.
3. Otherwise, Azure hosting or local development determines the credential.

These explicitly configured identities take precedence even on an Azure host.
Incomplete configuration or authentication failures are surfaced; the portal
does not silently switch to a different credential.

For certificates, use `AZURE_CLIENT_CERTIFICATE_PATH` for a PEM or PKCS12 file
containing the private key. Set `AZURE_CLIENT_CERTIFICATE_PASSWORD` only for an
encrypted file. Set `AZURE_CLIENT_SEND_CERTIFICATE_CHAIN=True` only when Subject
Name/Issuer authentication requires the public chain; otherwise leave it unset.
`AZURE_AUTHORITY_HOST` optionally selects the Microsoft Entra authority and
defaults to Azure Public Cloud. Compose forwards these names from the shell or the
selected `--env-file`, using empty strings when optional variables are unavailable.
The portal prevents empty defaults from becoming an incomplete SDK environment
credential. Explicit service-principal/workload credentials are selected before
the local default chain, which excludes those already-handled credential types.
For certificates with an empty client-secret default, the portal uses the native
`CertificateCredential` with the same documented environment settings and treats
empty optional password/chain values as disabled.

Certificate and
federated-token paths must refer to files inside the container. Mount those files
read-only with a Compose override or your hosting service's secret mounts; setting
an environment variable does not mount a host file or copy a private key.
Use the commented authentication options in `.env.portal.example`; never commit
an env file containing credentials.

Following the
[Microsoft Learn managed-identity example](https://learn.microsoft.com/azure/developer/python/sdk/authentication/system-assigned-managed-identity),
a nonempty `WEBSITE_HOSTNAME` selects Azure-hosted authentication:
`ManagedIdentityCredential(client_id=AZURE_CLIENT_ID)` when `AZURE_CLIENT_ID` is
nonempty, otherwise `ManagedIdentityCredential()` for the system-assigned identity.
Azure-hosted authentication does not fall back to a local credential if it fails.
Without explicit environment/workload settings, when `WEBSITE_HOSTNAME` is absent
or empty, the portal uses `DefaultAzureCredential` for local development.
SDK-managed `AzureCliCredential` fallback is allowed locally, but
the application does not issue CLI commands, launch device-code login, or change
a CLI subscription.

`WEBSITE_HOSTNAME` is an App Service hosting marker, not universal Azure detection.
For another Azure hosting service, explicitly supply a nonempty marker to use this
managed-identity selection. Leave it unset locally. Configure and grant access to
the selected managed identity on the hosting resource before starting the portal.

The header shows the execution principal and tenant, with user initials only for
a resolved user. Application/workload identities are supported. Unavailable
identity details are explicit; they never fabricate a signed-in user. Credentials
are configured outside the portal; there is no Sign-in button.

Metadata pages render the cached identity immediately and refresh it in the
background. **Refresh execution identity** preserves unsaved form values.
The popup closes on Escape or when focus leaves; long names wrap on mobile.
Queued actions retain token-free reviewed principal/tenant snapshots. SDK token
refresh cannot silently switch execution principal; changed credentials require
fresh review. Tokens never enter forms, URLs, logs or persistent package metadata.

Keep the HTTP listener on loopback or private Codespaces forwarding. The user
badge identifies Azure execution credentials, not the browser visitor; this
is still a single-operator tool without portal login authentication.

## Minimum permissions for portal features

Assign permissions to the **execution principal shown in the portal header**:
the signed-in user, service principal, federated workload's service principal,
or managed identity. Use its **principal/object ID**, not its application/client
ID, when assigning Azure roles or ACL entries. Authentication settings alone do
not grant access. Azure RBAC and Fabric workspace roles are separate permission
systems; an Azure subscription role does not grant Fabric workspace access.

### Minimum built-in role assignments for all features

For an existing, correctly deployed target, use this baseline:

- **Storage Blob Data Contributor** on the configured source ADLS **container**
  (filesystem).
- **Storage Blob Data Contributor** on each publication target ADLS **container**,
  if different from the source.
- **Contributor** in each target **Fabric workspace** containing the Lakehouse
  and Control Spark job definition (SJD).

Contributor is the lowest **workspace role** that supplies both OneLake writes
and Spark job execution. It is not a claim that workspace-wide access is the
smallest possible custom/item-level grant. The storage roles above are likewise
the minimum built-in data roles for the full read/write workflow, not an
instruction to grant subscription-wide access.

| Portal feature | Minimum access | Scope and reason |
| --- | --- | --- |
| Identity badge/refresh | A valid Azure credential; no additional Azure management or Microsoft Graph role | Identity display comes from SDK-acquired token claims, not directory lookup. |
| Locations, cameras, calibration settings, saved drafts, local job/report history and package metadata downloads | No Azure/Fabric role for metadata-only operations | Requires filesystem access to portal state/output and read access to local models. Operations that probe footage also need source read access. |
| Browse, preview, full-preview approval, individual/ZIP footage download, hash inspection and package generation | **Storage Blob Data Reader** | Source container. Generation reads every selected original byte but writes its package locally. |
| Upload/verify, create folders, rename/move/replace, cancel upload and reconcile intake | **Storage Blob Data Contributor** | Source container. These operations need reads and writes plus rename/delete capabilities; cancellation deletes only owned staging files. Data Reader alone is insufficient. |
| Publish a package | Source read access plus **Storage Blob Data Contributor** | Publication target container, covering configured staging/incoming prefixes. Publication checks existing content, uploads, promotes by rename and may remove conflicting resumable staging data. No Fabric role is needed for publish-only. |
| Submit processing | Publication access plus Fabric **Contributor** | Target workspace. Submission reads `Files/models/...`, lists the existing Lakehouse shortcut, verifies published content through it, writes registration partitions under the configured `files_root/intake/backfill/`, runs the Control SJD and polls its job. |
| Wait for processing / resume monitoring | Submission access plus OneLake read access to the work Delta table | Already included in target-workspace Contributor. Monitoring reads the configured `table_directory/people_counter_sjd_work` through OneLake, including its Delta log/data files; SQL endpoint read access alone is insufficient. |

With the full baseline, do **not** add Data Reader as well: Data Contributor
already includes reads. No **Storage Blob Data Owner**, Azure **Owner**,
**Storage Account Contributor**, Fabric **Member/Admin**, or subscription-wide
**Contributor** is required for normal portal operations. Generic Azure
Owner/Contributor/Reader roles are not substitutes for Blob **data** roles.
The portal does not grant roles, edit ACLs, create accounts/workspaces, deploy
SJDs, provision capacity, or manage recurring schedules. Capacity SKU selection
is operator-entered metadata, not permission to resize capacity.

### Least-privilege scopes and shortcut identities

Use the container resource as the Azure assignment scope:
`/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<account>/blobServices/default/containers/<filesystem>`.
Repeat only for containers actually used. `PC_PORTAL_ADLS_ROOT` constrains the
portal's paths; it is **not** an Azure security boundary. A container-wide role
still grants access outside that root to other tools using the same identity.

For directory-level isolation, an administrator can instead design ADLS
ACL/ABAC grants: traversal (`x`) on ancestors, read/list on browsed directories,
read on footage, and write/execute on both source and destination parents for
mutations. Include staging cleanup, replacement and every moved subtree; default
ACLs affect new children, not existing recordings. Validate all operations,
including container/root property checks, against the restricted grants.
ACLs cannot restrict access already granted by an unconditional data-role
assignment. The portal does not require permission to change ACLs.

OneLake is not your Azure storage-account resource: **do not** try to assign an
Azure Blob role to `onelake.dfs.fabric.microsoft.com`. Grant Fabric permissions
on the workspace/items. A more restrictive design must retain Lakehouse metadata
Read (including listing shortcuts), OneLake read access to models, published
shortcut data and the work table, write access to registration partitions, and
Control SJD execution/job-status permissions. A Fabric Viewer role alone does
not supply that combination.

The existing **ADLS shortcut connection identity** can differ from the portal
identity. Fabric uses that connection's credentials to reach ADLS; giving the
portal Contributor does not fix a broken connection. For read-only verification
and downstream footage reads, grant that identity **Storage Blob Data Reader**
on its target container. Microsoft's ADLS shortcut authorization also requires
the account-level **Generate a user delegation key** action for Entra-based
connections: when data access is container-scoped, add **Storage Blob Delegator**
at the **storage-account** scope for the connection identity. An account-scoped
Data Reader/Contributor role already includes that action but grants broader
data access. A portal identity used only for direct SDK reads/uploads does not
need this extra shortcut-connection grant. If the same principal fills both
roles, it needs both sets of permissions.

The deployed Spark jobs and shortcut/workspace identities must independently
retain their deployment's read/write permissions. Portal role assignments do
not replace those runtime grants or guarantee downstream Gold/refresh success.

### Fabric tenant and runtime prerequisites

- For service principals, workload identities and managed identities, a Fabric
  administrator must allow the principal under **Service principals can call
  Fabric public APIs** (also called **Service principals can use Fabric APIs**).
  Prefer an allowed security group instead of enabling access for all principals.
  The separate setting for creating workspaces/connections/deployment pipelines
  is not required by this portal.
- Allow **Users can access data stored in OneLake with apps external to Fabric**
  for the applicable users. The local portal and its processing monitor use
  OneLake APIs from outside Fabric.
- For delegated user authentication, the client application also needs the
  endpoints' delegated scopes: `OneLake.Read.All` (or `OneLake.ReadWrite.All`)
  for shortcut listing, `SparkJobDefinition.Execute.All` (or `Item.Execute.All`)
  for Control execution, and `SparkJobDefinition.Read.All` (or `Item.Read.All`,
  or the corresponding ReadWrite scope) for job polling. These are OAuth scopes,
  not Azure RBAC roles, and do not replace workspace/item grants. Do not assign
  these delegated scopes as application permissions to a managed identity.
- The target workspace must have usable Fabric capacity, a deployed Control
  SJD/Environment, model files, a valid shortcut and the initialized work table.
  Storage/OneLake firewalls, private endpoints, tenant policies and reachable
  token/certificate files remain independent prerequisites; more RBAC will not
  repair a network or credential configuration error.

An administrator should validate source list/read and a small upload/rename/
cancel cycle, publication into the approved target prefixes, and submission/
monitoring against the selected target. Allow role-assignment propagation before
retrying 403s; refresh the execution identity and re-review affected jobs.

Interactive ADLS metadata reads disable Storage SDK retries and use explicit
10-second connection and read timeouts. These are per-network-operation limits,
not a 10-second deadline for an entire batch or Azure credential acquisition.
Remote probe/frame workers use process-local Storage HTTP pools: forked decoders
never use or close the portal process's pooled Storage TLS connections. Identity guards,
ETag pinning and preview byte/time limits still apply to decoder reads.
Failures identify the video, exception type, HTTP status, storage error code and
request ID when available. The same safe details are logged without raw SDK
response bodies or credential material. Resolve the reported storage condition
before retrying; assignment validation still completes before ownership changes
are committed.

Permission references (Microsoft Learn):
[ADLS RBAC and ACL evaluation](https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-access-control-model),
[ADLS ACL operations/inheritance](https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-access-control),
[Fabric workspace roles](https://learn.microsoft.com/fabric/fundamentals/roles-workspaces),
[OneLake security](https://learn.microsoft.com/fabric/onelake/security/get-started-security),
[ADLS shortcut authorization](https://learn.microsoft.com/fabric/onelake/create-adls-shortcut#authorization),
[list shortcuts](https://learn.microsoft.com/rest/api/fabric/core/onelake-shortcuts/list-shortcuts),
[run an SJD](https://learn.microsoft.com/rest/api/fabric/sparkjobdefinition/background-jobs/run-on-demand-spark-job-definition),
[read job status](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/get-item-job-instance),
[service-principal tenant settings](https://learn.microsoft.com/fabric/admin/service-admin-portal-developer),
and [external OneLake access](https://learn.microsoft.com/fabric/admin/service-admin-portal-onelake).

## Existing Docker startup

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
Standalone `pc-admin --host` accepts only loopback addresses. The Docker image
uses `--container-loopback-publish` to listen inside its container; Compose
publishes that port only on `127.0.0.1`. Do not use this container option on a
host process or change the host mapping to a public interface. The portal has
no operator authentication; CSRF and host checks are not substitutes for it.
Specifying `portal` starts the portal and its Tailwind compiler dependency;
existing Spark services are unchanged. The first build downloads Python
dependencies, FFmpeg, Azure CLI, and the checksum-verified Tailwind CLI.
If VS Code does not expose a native ZIP save dialog, use a standard browser for
downloads; the download endpoint and archive contents are covered by Chromium E2E.

The default model mount is empty:
models are not needed to browse footage, calibrate cameras, or generate packages.
Upload videos from your computer to the chosen ADLS folder before selecting them
for a package. To configure the portal, add these values to `.env`. Use
[.env.portal.example](../.env.portal.example) as a reference; **merge with an
existing `.env` rather than overwriting it**.

```dotenv
PC_PORTAL_ADLS_ACCOUNT_URL=https://youraccount.dfs.core.windows.net
PC_PORTAL_ADLS_FILESYSTEM=videos
PC_PORTAL_ADLS_ROOT=intake
PC_PORTAL_MAX_UPLOAD_BYTES=5368709120
PC_PORTAL_MODELS_DIR=/absolute/path/to/approved-models
PC_PORTAL_PORT=8088
PC_PORTAL_ALLOW_CLOUD=0
```

The account/filesystem must already exist. Paths are confined to the configured
ADLS root; an empty root allows the filesystem root. The endpoint currently uses
the public Azure `dfs.core.windows.net` domain. Compose mounts models read-only
at `/data/models`; there is no local footage mount. Standard Azure Identity
variables are forwarded to the container. Keep secrets outside source control;
a workload token file must be mounted separately if that identity is used.

Package outputs are separate from source footage. By default they live in
`portal-state` at `/var/lib/people-counter/portal/packages`. Administrators can set
`PC_PORTAL_OUTPUT_ROOT` (or standalone `pc-admin --output-root`) to another writable,
persistent directory. For Compose, an alternative directory must be mounted writable
with a Compose override; changing the environment variable alone does not create a
host mount or make the read-only container filesystem writable. Never use a directory
inside the footage root or a parent containing that root.

Operators do not choose or manage output folders. New drafts allocate named
revisions automatically under `<generated-package-name>/vN/` in application-managed
storage. Completed jobs show **Package ready to download** and a prominent
**Download manifest package ZIP** action. Package history also offers
**Download package ZIP**, which saves a copy to the operator's computer.
Existing jobs and legacy destinations retain their saved locations and remain
accessible; no package migration is required. Traversal, symlink escape, and
overwriting an existing revision remain prohibited.

`./scripts/start-portal.sh` retains its Compose behavior. Docker does not
automatically reuse host credentials. Its retained `portal-azure` volume can
serve the SDK's development credential fallback, but credentials are not copied
or host-mounted by the application. Configure an identity that resolves in the
actual runtime.

## File explorer and current-folder uploads

Growing portal tables show ten rows at a time, with **Previous page**, **Next page**
and a row range above the table. Pagination applies to the rows loaded in the
current view; existing ADLS continuation links and server-side report pages remain
available to load more records. Changing a table page retains checkbox choices.
Save footage changes before navigating to another folder or server-side page.
Storage actions and camera-assignment batches use only checked rows on the
displayed table page, never hidden checked rows from another page.
Package **Save selection**, **Save selection and assign cameras**, and
**Review generation** actions stay above their tables so the next step is visible
before the operator scrolls through records.

The explorer lists all entries with pagination; non-video files are read-only.
File actions support the existing AVI, MKV, MOV and MP4 formats. Folder creation
and native folder/file moves are supported. Folder trees containing unsupported
files or protected references cannot be moved, and directory trees are never
merged or replaced.

1. Browse to the destination folder and select **Upload videos** above the file list.
2. The dialog shows the upload folder as information. Choose local videos directly;
   no destination entry or confirmation is needed. To upload elsewhere, close the
   dialog and browse to that folder first. Server-side destination validation and
   explicit confirmation before replacing an existing file still apply.
3. Choose multiple videos, then **Upload selected videos**.
4. Review per-file progress through transfer and full-byte verification.
5. Refresh the folder and explicitly select uploaded/existing ADLS videos for
   the package. Uploading does not create, select, publish or process a package.

The default per-file limit is **5 GiB** (`5368709120` bytes), configurable with
`PC_PORTAL_MAX_UPLOAD_BYTES`. Existing ADLS footage/downloads are not limited by
this upload setting. Raw chunks are bounded; full videos are not retained in
portal memory or on local disk. Verification computes SHA-256 from all original
ADLS bytes and may incur additional transfer costs. An ETag is a version
precondition, not a checksum. Generation independently verifies current content.

Successful files remain uploaded when another file fails. **Retry** restarts only
the failed file, at its original destination; partial-byte resume is not offered.
Cancel stops an unfinished transfer and cleans only owned staging paths. Leaving
the page stops unfinished transfers; completed files and queued verification jobs
remain available. Transfer progress reaching 100% is not verification success.

File replacement needs explicit, version-bound confirmation. Both source and
destination references are checked. Generated/published packages and active jobs
protect their footage; saved evidence is never rewritten. Editable draft paths
and camera ownership follow successful moves. Replacement invalidates old
per-recording overrides and requires fresh footage review.

Bulk actions apply to checked items on the displayed page. Bulk downloads stream
a ZIP with safe relative paths. Completed bulk mutations remain completed if a
later item fails; reported outcomes are not an atomic rollback guarantee.
Interrupted remote/metadata updates require reconciliation before conflicting
actions become available. There is no general ADLS delete action.
Moves rejected before a remote request, or definitively rejected by Azure, can
be cancelled or reconciled to release unattempted paths. Completed bulk items
remain completed. Unknown remote outcomes remain reserved.
Cancellation stops remaining move items before their remote requests. Moves
retain only previews bound to the reviewed source version; stale previews are
invalidated.

Legacy local-source drafts remain read-only history. Their confirmation-based
metadata cleanup removes the selected draft and unshared footage-specific rows,
not local videos, independent locations/cameras, or job/audit history. Active
local recovery jobs block cleanup. Existing metadata package-artifact deletion
retains its existing safeguards. No local footage is uploaded or rebound by name
automatically.

Stop without deleting state:

```bash
docker compose --profile portal stop portal portal-tailwind
```

Graceful shutdown stops new work, disconnects HTTP streams, and drains request
threads and the current background job before closing SDK clients and credentials.
The state-directory lock remains held while jobs drain. Identity refresh also
waits for active source reads before closing their client.
Allow enough Docker stop timeout for current work to finish; forced termination
cannot run Python cleanup. Accepted Fabric jobs are not cancelled by closing
local resources. Repeated termination signals do not interrupt graceful cleanup.

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

The dev container uses Docker outside Docker: the Docker CLI connects to the
host daemon through its mounted socket instead of running a nested daemon.
Images, build cache, and named volumes now belong to the host daemon. This avoids
a separate nested Docker store, but does not increase the Codespace's total disk
capacity or automatically remove the previous daemon's data.

After changing this configuration, run **Codespaces: Rebuild Container** (or
**Dev Containers: Rebuild Container** locally), then check `docker info`,
`docker system df`, and `df -h` before retrying `./scripts/start-portal.sh`.
Do not prune volumes to recover space unless their data can be deleted.
Existing portal state and Azure credentials in the old daemon's named volumes
do not migrate automatically; back up any needed state before rebuilding.

Bind-mount sources must exist on the Docker host at the paths Compose resolves.
Use the shared `/workspaces` checkout in Codespaces and keep footage/model folders
there. On local dev containers, the checkout must be mounted at the same absolute
path on the host and in the dev container for relative Compose mounts to work.
Access to the host Docker socket grants host-level control; use only trusted code.

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

CSV downloads neutralize spreadsheet formula cells without changing stored
catalogs, package metadata, or checkpoint inputs. Rejection CSVs are also safe
for spreadsheet review. Generated inventories retain an exact machine-readable
CSV and include a separate `*.spreadsheet.csv` export. Open the latter in a
spreadsheet; use the original only as a machine input. Formula-leading values
receive an apostrophe in spreadsheet exports and must not be copied back into
operational catalogs or inventory inputs.

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
not a recursive search or a glob expression. The default type filter is **Videos**;
**All types** and **Folders** remain available. Folder navigation is independent of
the table filter. Filters persist through folder navigation; **Clear filters**
retains the folder and restores Videos. Empty folders and filters with no matches
have distinct states.

Folders sort first, followed by files and blocked links. Size is real filesystem
metadata, and modification times are explicitly UTC. Modification time is not
the recording/capture time used in manifests. Unavailable metadata is reported
on its row. Links outside the mounted source are blocked without exposing their
metadata. Browsing does not modify source files.

Each video has a **Review footage** action, opening separate Counting line and
Recording details sections side by side on wide screens and stacked on phones.
Readiness and Camera association are separate sections above them.
**Generate manifest package** opens the package dialog with the
current folder retained. Source/mount information and manual path entry remain
under **Source configuration**. There are no upload, rename, or delete actions.

## Operator workflow

Start on **Packages** and choose **New package**. Give the draft a name, then:

1. **Footage:** explicitly select individual ADLS recordings across folders, or choose
   a folder checkbox to enable recursive selection automatically. Folder names
   open folders; the breadcrumb returns to any ancestor. **Select all** and
   **Unselect all** affect only selectable videos on the displayed page, not
   folders or other pages, and switch to individual selection. The selection
   count updates immediately. Recursive mode remains under the collapsed
   **Selection options** control and becomes active when a folder is checked.
   **Upload videos** opens
   an upload-only dialog for the current folder. **Create folder**, **Move selected**,
   **Rename selected**, and **Download selected** sit directly above the files
   table, outside dialogs. Dialogs have an X button labelled **Close dialog**.
   Storage dialogs are centered. The refresh icon retains the current folder
   and is labelled **Refresh folder** for assistive technology. Rename requires
   exactly one selected item; invalid-selection feedback appears at the toolbar.
   Uploads do not automatically select footage. Save checkbox changes before
   navigating or refreshing.
   Recursive selection freezes the current file list; later additions are not
   included without explicitly refreshing selection. Both modes enforce 10,000
   videos without truncation. ADLS browsing is shallow and paged.
2. **Cameras and readiness:** associate selected recordings with existing cameras.
   Use a recording's **Assign camera** action, or use **Select all** / **Unselect all**
   for the displayed page before choosing **Assign selected recordings**.
   The page shows the current selection count. The dialog lists exactly which recordings
   will change and starts with a fresh camera choice. Saving clears the batch
   selection. Camera selection determines location. Known ownership is shown.
   Assignments are persistent and require an explicit save; reassignment retains
   capture timestamps and requires a keep/clear decision for video-specific lines.
   Saving in either the assignment dialog or footage library immediately shows an
   indeterminate progress bar and the recording count while media validation and
   saving run. The form blocks edits and duplicate submissions, and a pending
   assignment dialog cannot be dismissed. The request runs without navigating
   away while pending. Success returns to the assignment page with the existing
   notice. Server and connection errors stay visible in the form, restore its
   controls, and advise reviewing current ownership before retrying.
   **Review footage** and camera calibration return to the saved package draft.
   Both pages open before ADLS preview/probe work starts; checks load in the
   background with a retry action on failure. Navigation remains available.
3. **Generate:** review the selected count/bytes, then generate the package.
   The app manages storage automatically; no output destination is required.
   Download the ZIP from the completed job or package history.
   Generation freezes source observations, camera rules, ownership, and exceptions
   before queuing. It probes/validates footage and computes a complete SHA-256.
   Failed checks produce job errors/rejection reports, never a successful package.
4. **Publish:** use the existing completed-revision cloud review, target/profile
   selection, and explicit approval. Generation itself never uploads, submits
   Fabric work, or runs inference.

Drafts survive navigation and portal restarts. The configured ADLS source is
fixed, not an editable step. Other-source drafts are read-only history; changing
environment configuration never reinterprets their footage paths. Source-qualified
ownership and generated revisions remain intact.

Locations/cameras are independent of source folders. Use **Locations** and
**Add location** to add a display name
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
   The footage library shows camera ownership and a **Review footage** action.
   Review footage opens geometry and recording details directly. Assignment
   controls align beside each other on larger screens and stack at full width on
   phones. Narrow layouts keep selection, filename, and camera visible; type, size,
   and modification time remain available under **File details**.
   Assignment is allowed before calibration: the camera pages say **Not calibrated** until
   a counting line is saved. Package preparation rejects selected footage assigned
   to an uncalibrated camera; association alone never creates a synthetic line.
   Existing calibration example files are retained as assignments on upgrade.
   Intrinsic media checks run before assignment. Unreadable/unsupported footage
   blocks the entire selected batch without partial ownership changes. Assignment
   to an uncalibrated camera is allowed so its first reference can be registered.
   When changing cameras, explicitly keep or clear any video line exception;
   authoritative capture time is retained either way.
2. **Camera calibration:** use **Calibrate camera** or **Recalibrate camera**
   on the camera's footage page. The camera and location are fixed. Choose a
   **Reference footage** recording already associated with that camera, then
   select the preview time and click two endpoints or enter
   their coordinates under **Manual line coordinates**. Drawing uses the original frame's pixel coordinates,
   including when the canvas is scaled on a small screen. The arrow shows
   endpoint order, not walking direction; reversing endpoints swaps in/out.
   Choosing a reference never changes its ownership, capture time, or video
   geometry exception. Saving replaces the selected camera line for future
   package preparation, without creating another dated calibration. Existing
   recording rules, dates, and prepared package snapshots are retained.
   The Camera calibration summary replaces the technical configuration list.
   If older data has several recording periods, select the period explicitly
   before recalibrating; the others remain unchanged.
3. **Camera metadata:** recording rules are edited separately:
   use **Edit camera recording rules** in Review footage, or **Edit metadata**
   on the camera page. Choose a timezone, timestamp rules, and effective dates
   without changing the saved geometry or decoding footage.
   The example video is not the complete assignment list. Existing counting lines
   are restored when reviewing another recording from the same camera. Multiple
   effective-dated configurations remain separate. Use **Edit metadata** on the
   camera page to change recording-time rules/effective dates without opening
   geometry or decoding footage; use **Recalibrate camera** for geometry.
   Legacy folder defaults continue to apply where a folder has one camera.
   When different cameras share a folder, assign each video explicitly.
   Review the Cameras page or download its catalog CSV. Calibration cannot
   create an unregistered camera or move one to another location; forged requests
   are rejected without changing metadata.
4. **Video exceptions:** the Counting line section in **Review footage** edits
   only that video's line and frame dimensions. **Save video line exception**
   cannot change camera defaults; **Calibrate camera separately** opens the
   fixed-camera workflow. Recording details always exposes
   **Capture time override (UTC, optional)** independently of geometry.
   Use its calendar and time picker to choose the recording date and time,
   including seconds if needed. Enter UTC, not the browser or camera timezone;
   the portal saves the selection as an authoritative UTC capture timestamp.
   Save each section independently. Saving a line retains the capture-time
   exception; saving recording details retains geometry. Clear the capture time
   and save to restore normal resolution. **Use camera line** removes only
   video geometry, preserving any timestamp exception. Timestamp-only storage
   does not decode frames; automatic review checks probe media again afterward.
   Legacy calibration links redirect to the appropriate separated screen.
   Camera defaults are not changed. A camera configuration must still cover the video.
5. **Packages:** choose **Generate manifest package**, enter a package name,
   and review its source folder. The app manages output storage automatically.
   Reuse a name to allocate the next integer revision. Failed attempts retain
   their numbers; existing revisions and their saved inputs are not overwritten.
   Preparation snapshots metadata and footage ownership before queuing the job,
   probes/hashes videos, and displays progress and rejection reports.
   This is an **offline preparation job only**: it does not upload footage,
   publish to cloud storage, submit Fabric work, or run inference. Cloud actions
   remain separate and require approval.
   Package history shows revision, status, schema version, ZIP availability, and
   links to downloads, reports and cloud selection. Historical packages remain ungrouped.
   Rerunning a retained revision creates a new cloud job and validates unchanged
   source bytes, rather than modifying the package or resuming an old run.
   **Delete artifacts** requires explicit confirmation. Active/interrupted work
   blocks deletion. Source footage, reports, job history, and recovery evidence
   remain; deleted revisions cannot be downloaded or used for new work.
   There is no automatic deletion.
   Job **Reports** opens with a readable Summary when available. Choose
   **Rejections**, **Footage inventory**, or **Manifest package** to inspect
   details; downloads identify purpose and format rather than internal filenames.
   The inventory is a CSV of footage, not a rendered video. The portal does not
   generate annotated videos or fetch new cloud video artifacts.
   Original downloads remain available. CSV rows and JSON record collections
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
   selects supported presets. Opening the form creates no job. Submitting
   **Generate profiles** creates a local background job, which hashes the approved local model
   files and generates the target catalog. Nobody enters model/config/runtime
   hashes. Missing models fail explicitly. All selected model bytes must match
   the artifacts deployed in Fabric.
   This job does not deploy resources, upload footage, submit backfill, or run
   inference; those actions are not implied by adding a target.
   Optional advanced controls select detection threshold, batch size, and
   sampling FPS; the shared resolver validates them and recalculates identities
   automatically. Threshold is finite and between 0.1 and 1, inclusive,
   matching the detector's existing candidate floor.
   Supported detector batch sizes remain 1, 2, and 4; FPS must be positive
   and finite.
7. **Cloud work:** choose **Start cloud action** to review a completed package,
   target/profile, and explicit action in its dialog.
   Publication-only and publish-plus-registration are separate choices.
   "Wait for processing" applies only to explicit submission.
   Optional job inference overrides apply only to processing submissions.
   **Review cloud action** shows profile defaults, overrides, and final effective
   settings before a fresh approval starts work. Profile defaults are unchanged,
   and resume retains the queued job's exact configuration.
   The capacity picker records operator-declared target context, not a verified
   resource lookup or a deployment change. Unknown capacity is explicit.
   Guidance is advisory: Fabric F64 does not establish a safe batch maximum.
   Detector batching differs from concurrent jobs and Spark sizing. Model/runtime
   memory and concurrent work also affect feasibility. Sampling FPS controls
   the number of frames processed per second of footage.

Package ZIP downloads contain metadata, not source videos. Retain the unchanged
footage. Changing camera metadata requires a new package. Changing a profile or
release is a new processing run, not recovery of an existing run.

### Local readiness

Review footage automatically checks local media and metadata and exposes issues
with corrective actions and reason codes. **Recheck** reruns the checks without
starting a package job. Intrinsic media failures block assignment; missing camera
calibration or recording metadata blocks local package readiness, but corrective
assignment remains available. Camera association is an independent save, not a
counting-line option.

Editing camera association, video-line choices, manual coordinates, drawn
endpoints, or capture time triggers a read-only draft check after a short pause.
The page hides outdated results while checking and labels preview results as
unsaved. Requests are sequential and coalesced; superseded responses cannot
overwrite the latest values. Checks update only the readiness panel, not the
forms or stored metadata. During editing, the results area keeps its existing
height and scrolls internally so validation messages do not move the line editor.

Draft checks combine the proposed association, its geometry choice, and any
explicitly edited video line or recording time. **Draft checks passed (not
saved)** does not mean the saved package inputs are ready. Each Save action
still applies only its own section and reruns checks against saved values.
**Retry draft check** preserves edits after a request failure. **Recheck** reloads
saved values and discards unsaved edits. Preview time is display-only, not a
footage metadata change.

Checks reuse the manifest resolver for timestamps, effective-date coverage,
dimensions, and video overrides. Media probing and a preview-frame check do not
hash the complete video. Invalid media still has a review page with diagnostics,
but no unusable canvas or enabled assignment button. If a probe cannot run,
the check fails explicitly rather than reporting success.

**Local checks passed** is not a cloud-publication guarantee. Full content
hashing and final preparation/publication validation remain necessary. The
page does not contact cloud targets or test remote permissions/availability.

A dimension mismatch refers to the frame size saved with the counting line,
not necessarily a visibly misplaced line. Review footage shows the current video
resolution and whether the saved reference belongs to the camera or a video-only
exception. **Review and confirm this video's line** opens the line editor.
If its camera association and line placement are correct, keep the coordinates and choose **Save video line
exception**; otherwise adjust them first. This saves the current video's frame
dimensions without changing camera defaults. No automatic scaling is performed.
Use camera recalibration only for an intentional camera-wide correction.

### ADLS media and integrity

ADLS source browsing/probing never writes to source storage. The CLI user needs
management-plane subscription/account read access for discovery and data-plane
Storage Blob Data Reader permissions (or applicable ADLS ACL traversal/read
permissions) for folders and files. Publication requires the existing target
write permissions. Network/firewall/private-endpoint restrictions still apply.
Account discovery permission is not proof of data access.

Normal remote previews use seekable byte-range reads, including container metadata
near the end of a video when needed. Each preview/probe is limited to **64 MiB
actually transferred**, **16 MiB in-memory cache**, and **30 seconds**. Unsupported
layouts, expensive seeks, and limit exhaustion are reported explicitly. A separate
full-download fallback shows the complete size and requires approval bound to
that source, recording, size, and ETag. Replacement after review requires a new
approval before any download starts. It checks
private disk space, extracts a preview and source-version-bound metadata, and
deletes the temporary video; it retains only the JPEG and metadata.

Generation reads **every original byte**, sequentially in bounded chunks, to
calculate the same SHA-256 as hashing a complete downloaded file. Preview bytes
are not sufficient, and an ETag is a concurrency guard, not a content hash.
ETag/size changes and truncated reads fail instead of producing a complete package.
No normal full-video disk cache or transcoded substitute is used.

Publication revalidates source bytes against the generated revision and streams
them into the existing target staging path, checking integrity before promotion
and final manifests. Preparation, validation, upload, and retries can each read
the full video, with associated transfer costs. Source access and unchanged bytes
must remain available for later publication. A metadata ZIP is not a video backup.
Publication rejects staging paths that would rename or overwrite selected ADLS
source footage. Token acquisition and refresh use the shared SDK credential owner.
Individual selection saves inspect only newly selected recordings and retain
existing source-version observations. To adopt a changed recording, explicitly
remove and reselect it, or refresh a recursive folder selection.
Real Azure transfer performance depends on the host and network.
CLI principal/tenant changes block authenticated queued/resumed work until the
reviewed identity is restored or a freshly reviewed action is created.

### Progressive disclosure

Directory tables and catalogs stay visible. Location/camera creation, package
generation, profile setup, cloud actions, and resume confirmations share the same
native modal pattern: a named action button, labeled dialog, Close/Cancel/Escape,
viewport-bounded scrolling, and keyboard focus returned to the action button.
Creation shortcuts retain their selected location or folder. Required fields
and explicit approvals remain visible within the active dialog. Draft values
survive closing/reopening on the same page, but cloud approval checkboxes are
cleared so each submission/resume needs fresh consent.

Folder browsing and frame-preview controls stay inline. Review footage keeps its
geometry and recording-details saves independent; camera recording rules have a
separate page. Job status, downloads,
errors, safety warnings, and recovery guidance remain visible outside dialogs.
Source configuration, manual coordinates, recording rules, camera geometry/date
details, package explanations, inference overrides, completed-job logs, and raw
result JSON are available through labeled native disclosures. They work with a
keyboard without changing pages or losing entered values.

Camera calibration has no editable camera/location identity or video-exception
fields. Footage line edits are always video-only, with recording details and
association kept independent.
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
even if a button is bypassed. Independent metadata/history screens do not require
storage access, but ADLS operations do.

When ready, configure the selected managed identity in Azure or credentials that
`DefaultAzureCredential` resolves locally. Do not copy secrets into the repository or assume host credentials
are available inside Docker. Use **Refresh execution identity** to inspect the
actual user or workload principal; the portal does not initiate authentication.

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
# Set the required PC_PORTAL_ADLS_* environment variables first.
uv run --no-sync pc-admin --models-root /path/to/models --state-dir .people-counter-portal
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
Python CRAP reports calculate function coverage over the complete source-line
span, including non-executable continuation and control lines. Read those values
alongside coverage.py's executable-line and branch coverage. Its JSON report
contains per-function coverage and missing branches. Reduce complex validation
stages and test missing decision paths; do not compress formatting or exclude
lines merely to raise the CRAP coverage percentage.

CI's credential-free validation builds and health-checks the Compose image,
runs Playwright, and retains failed-test traces/screenshots and Compose logs.
Tests cover desktop/mobile line drawing, different-resolution video exceptions,
camera metadata, package ZIP contents, durable job/error display, profile
generation, publication/submission/wait wiring, CSRF, and source/cloud gates.

This is a single trusted operator's localhost tool, **not an authenticated
multi-user service**. Keep the loopback port binding. Network/cloud hosting
requires authentication, authorization, a storage-source adapter, and a
multi-worker-safe job system rather than just changing the listen address.

# Manual Azure and Fabric deployment

This is the **portal-only infrastructure and Lakehouse-bootstrap path**. Do not run
Terraform, `bootstrap-local.sh`, or `deploy-local.sh` while following it.
The deployment script is the requirements checklist, not an instruction to
create a Terraform backend or a GitHub deployment identity.

Open [Azure](https://portal.azure.com/) and
[Fabric](https://app.fabric.microsoft.com/) in VS Code's integrated browser.
Keep the page being operated visible. Red outlines in the screenshots mark
the next control or the value to verify. Dark masks hide account details.

> **Stop after bootstrap on the captured F2 setup.** The live bootstrap
> succeeded, but its run received **one executor**, not the **two** required
> by the reviewed inference contract. The operator chose to keep F2 and stop
> here. Do not run Dispatcher/Process, enable recurring schedules, or call
> this a production-accepted deployment.

## Scope and evidence

The screenshots are real captures from the shared integrated browser on
2026-11-06. The walkthrough uses an isolated Development workspace; it does not
change the existing `people-counter-dev` workspace.

**Creation-form evidence is not creation evidence.** The resource group, F2
capacity, ADLS account, and `footage` container had already been provisioned
before the browser-only requirement was clarified. They were retained at the
operator's request. Their Azure creation forms were subsequently walked and
captured without submitting duplicate resources. The Fabric workspace,
workspace identity, monitoring, Lakehouse, shortcut, and subsequent item setup
were performed in the browser.

This guide does not claim the script's saved-plan approval, zero-Terraform-drift,
two-pass reconciliation, or clean-room acceptance guarantees. Creating Fabric
items alone does not establish a working or production-ready People Counter
pipeline. The final acceptance checklist identifies the additional gates.

## Before starting

Have the following:

- An Azure subscription and permission to create a resource group, storage
  account, and Fabric capacity.
- Permission to assign Azure roles on the ADLS account. Azure resource
  **Owner** alone is not Blob data-plane access.
- A Fabric administrator who can permit Fabric item creation and monitoring
  for the approved users/security groups.
- Approval for the ongoing capacity and storage charges.
- A reviewed People Counter wheel containing the **manual-binding support
  added with this change**, and the seven role-specific `main.py` files from
  the same source revision. Do not use an unmodified older SDK wheel for
  manual execution. Artifact preparation is not cloud deployment: a release
  owner can supply these files or build them locally. Browser upload requires
  files accessible on the browser host.

Choose unique names. Use one resource group and the same approved Canadian
region throughout. This walkthrough retained:

| Resource | Walkthrough value |
|---|---|
| Resource group | `rg-pc-clickthrough-dev-20261106` |
| Region | Canada East |
| Capacity | `fabpcdevc49b`, F2 |
| ADLS account | `stpcguideadls1106` |
| Private container | `footage` |
| Manual Fabric workspace | `people-counter-guide-dev` |
| Lakehouse | `pc_dev_lakehouse` |
| Spark Environment | `pc_dev_environment` |

The creation forms show unused example names where necessary. Substitute your
own names consistently; do not create resources with someone else's example
names or select an existing managed workspace.

## 1. Create a resource group

1. In Azure, search for **Resource groups** and open the service.
2. Click **Create**.
3. Select the approved subscription, enter a unique resource-group name, and
   select **Canada East**.
4. Click **Review + create**. Check the subscription, name, and region.
5. For a genuinely new deployment, click **Create** and wait for completion.
   If reusing an approved group, cancel this form instead.

![Resource-group creation form; Review + create is outlined.](images/manual-deployment/01-create-resource-group.png)

## 2. Create ADLS Gen2 storage

1. Search Azure for **Storage accounts**. Open the service and click **Create**.
2. On **Basics**, select your subscription and resource group.
3. Enter a globally unique storage-account name using lowercase letters and
   numbers.
4. Set **Region** to **Canada East**.
5. Select **Azure Blob Storage or Azure Data Lake Storage** as the primary
   service and **Standard** performance.
6. For this approved Development example, choose **Locally redundant storage
   (LRS)**. The walkthrough found that ZRS was unavailable in Canada East.
   LRS is not a recommendation for Production durability.
7. Open **Advanced**.

![Storage Basics with Canada East and LRS; Advanced is outlined.](images/manual-deployment/02a-storage-basics.png)

8. Select **Enable hierarchical namespace**. This makes the account ADLS
   Gen2. Leave SFTP and NFS off unless independently required.

![Enable hierarchical namespace on the Advanced tab.](images/manual-deployment/02b-storage-hierarchical-namespace.png)

9. Open **Security**:
   - Keep **Require secure transfer for REST API operations** selected.
   - Leave anonymous access disabled.
   - Clear **Enable storage account key access**.
   - Select **Default to Microsoft Entra authorization in the Azure portal**.
   - Keep minimum TLS at **Version 1.2** or the organization's stronger policy.
10. Review **Networking**, **Data protection**, and **Encryption** against
    organizational policy. The Fabric connection must be able to reach the
    storage endpoint; do not weaken an existing firewall to pass a test.
11. Click **Review + create**, inspect the settings and charges, then
    **Create** for a new account. Wait for successful deployment.

![Storage authentication settings; account-key access is cleared.](images/manual-deployment/02c-storage-security.png)

### Create the private source container

1. Open the new storage account.
2. Expand **Data storage** and click **Containers**.
3. Click **Add container**.
4. Enter `footage`. Keep **Private (no anonymous access)**.
5. Click **Create container** only if it does not already exist.

![Private footage container creation form.](images/manual-deployment/02d-private-container.png)

The captured account already contained `footage`, so its replayed form shows
the expected duplicate-name error. Reuse that container; **do not** try to
override the error. A new deployment must use a name not already in its account.

Return to the storage account's **Overview** and confirm **Hierarchical
namespace: Enabled**. Keep the account open for the identity grant in step 6.

![ADLS overview with hierarchical namespace and Access Control (IAM).](images/manual-deployment/03-adls-overview-and-iam.png)

## 3. Create an F2 Fabric capacity

1. In Azure, search for **Microsoft Fabric** and open the service.
2. Click **Create**.
3. Select the subscription and resource group. Enter a unique capacity name.
4. Set **Region** to **Canada East**.
5. Click **Change size**. The portal initially offered F64 in this walkthrough:
   **do not accept that default accidentally**.
6. Select **F2**, then click **Select**. Prices shown in the screenshot are
   historical estimates, not a quote.

![Capacity size picker with the F2 row outlined.](images/manual-deployment/04a-select-f2.png)

7. Select the approved **Fabric capacity administrator**. This must include a
   human operator who can assign the new workspace to the capacity.
8. Review overage controls and the cost estimate.
9. Click **Review + create**, verify **F2** and **Canada East**, then **Create**
   for a new capacity.
10. Wait until the capacity's Overview reports **Active**.

![Capacity creation with Canada East and Review + create outlined.](images/manual-deployment/04b-create-capacity.png)

![Retained capacity readback: Active, Canada East, and F2.](images/manual-deployment/04c-active-f2.png)

F2 is the requested Development capacity. Provisioning it is not proof that
the reviewed Spark workload fits its concurrency and pool limits. Do not
silently reduce the application's required compute settings to make a run fit.

## 4. Check Fabric tenant prerequisites

In Fabric, open the top-right **Settings** gear, then **Admin portal** >
**Tenant settings**.

- Confirm **Users can create Fabric items** permits the operator.
- Search for **Allow monitoring data collection** and confirm it permits
  the workspace/operator.
- Use approved security-group scope. A screenshot showing an existing
  tenant-wide setting is not an instruction to broaden another tenant.

For manual user-driven deployment, do **not** create a GitHub OIDC application,
federated credential, client secret, or Terraform state account. Service-principal
API settings used by automated deployments are not a substitute for the
operator's permissions.

## 5. Create the manual Fabric workspace

1. From Fabric Home, click **New workspace**. Alternatively, open
   **Workspaces** > **New workspace**.
2. Enter a unique name, such as `people-counter-guide-dev`, and a description.
3. Expand **Advanced**.
4. Under **Workspace type** (**License mode** in some views), select **Fabric**.
5. Under **Capacity**, choose your new F2 capacity in Canada East.
6. Click **Apply** and wait for the empty workspace to open.

![Workspace name and Apply; the capacity controls are further down the form.](images/manual-deployment/05-create-workspace.png)

7. Use **Manage access** to confirm the intended administrators and
   contributors. Do not add broad groups just to bypass a permissions error.
8. Click **Workspace settings**.

![New manual workspace and Workspace settings.](images/manual-deployment/06-workspace-settings.png)

9. Open **Workspace type**. Verify the actual capacity name, **SKU: F2**,
   **Region: Canada East**, and Fabric workspace type after creation.

![Actual workspace-to-capacity binding, not just a selection in an unsaved form.](images/manual-deployment/05b-confirm-workspace-capacity.png)

### Create its workspace identity

1. Select **Workspace identity**.
2. Click the **Workspace identity** creation button.
3. Wait until its **State** is **Active**.

![Create the workspace identity.](images/manual-deployment/07-create-workspace-identity.png)

4. Record the identity's **ID** for the storage grant. Use this principal
   object ID, **not App ID**, and not the workspace ID in the browser URL.

![Active workspace identity; its principal ID is outlined.](images/manual-deployment/08-workspace-identity-id.png)

## 6. Grant that identity access to ADLS

1. Return to Azure and open **your ADLS account**, not another storage account.
2. Open **Access Control (IAM)** > **Add role assignment**.
3. In **Job function roles**, search for and select **Storage Blob Data
   Contributor**. Click **Next**.
4. Keep **User, group, or service principal** selected.
5. Click **Select members**. Search for the new workspace's identity name.
6. Select the matching application and click **Select**.

![Select the new workspace identity, not a pre-existing deployment application.](images/manual-deployment/15-select-workspace-identity.png)

7. Click **Review + assign**.
8. Confirm all three:
   - Role: **Storage Blob Data Contributor**.
   - Scope: the intended ADLS account.
   - Member object ID: the **ID** recorded in step 5.
9. Click **Review + assign** again to create the assignment.
10. Wait for success and allow RBAC propagation before testing the connection.

![Final ADLS role-assignment review.](images/manual-deployment/16-confirm-adls-permissions.png)

Grant a human uploader a separate, approved Blob data-plane role if that
operator will upload source videos through Azure. The workspace identity grant
does not grant the signed-in human access.

## 7. Create the Lakehouse

1. Return to the new Fabric workspace's item list.
2. Click **New item** > **Lakehouse**.

![Select Lakehouse in the New item catalog.](images/manual-deployment/11-new-lakehouse.png)

3. Enter `pc_dev_lakehouse`.
4. Leave **Lakehouse schemas** selected.
5. Click **Create** and wait for the Lakehouse and its SQL analytics endpoint.

![Create the schema-enabled Lakehouse.](images/manual-deployment/12-create-lakehouse.png)

## 8. Connect ADLS through a keyless shortcut

1. In the Lakehouse Explorer, right-click **Files**.
2. Click **New shortcut**. Do not start under **Tables** for raw video files.

![Files > New shortcut.](images/manual-deployment/13-files-new-shortcut.png)

3. Select **Azure Data Lake Storage Gen2**.

![Select the ADLS Gen2 connector.](images/manual-deployment/14-adls-shortcut-source.png)

4. Choose **New connection**.
5. Enter the container's DFS URL:
   `https://<your-account>.dfs.core.windows.net/footage`.
   Do not substitute a `.blob.core.windows.net` URL.
6. Give the connection a descriptive name.
7. Set **Authentication kind** to **Workspace identity**.
8. Click **Next**. If authorization fails, verify the principal, scope, endpoint,
   network policy, and RBAC propagation. Do not fall back to account keys.

![DFS endpoint and Workspace identity authentication.](images/manual-deployment/17-adls-workspace-identity-connection.png)

9. Select the `footage` container checkbox and click **Next**. An empty
   container legitimately displays no child files.

![Select footage as the shortcut target.](images/manual-deployment/18-select-footage.png)

10. Review **Shortcut location: pc_dev_lakehouse > Files** and the `footage`
    name, then click **Create**.

![Review and create the Files shortcut.](images/manual-deployment/19-create-shortcut.png)

11. Open **Files** > **footage**. Confirm that listing completes successfully,
    rather than merely that the shortcut icon exists.

![Successful listing of the new Files/footage shortcut.](images/manual-deployment/20-shortcut-readback.png)

## 9. Turn on workspace monitoring

1. Open **Workspace settings** > **Monitoring**.
2. In the experience shown in this tenant, click **+ Eventhouse**.
3. Stay on the page until provisioning finishes.
4. Confirm **Log workspace activity: On** and that the monitoring database
   link is available.

![Monitoring options; use + Eventhouse below the migration banner.](images/manual-deployment/09-monitoring-options.png)

![Monitoring Eventhouse created and workspace logging enabled.](images/manual-deployment/10-monitoring-enabled.png)

The portal also offered **Use new experience**. Its migration control did not
advance during this walkthrough, so the verified path above used **+ Eventhouse**.
Do not treat this as the script's newer `Monitoring_item_*` definition or migrate
an existing monitoring setup casually: the UI warns that migration is irreversible.
Eventstream metrics require their own Eventstream monitoring setting.

## 10. Install and publish the Spark Environment

1. Return to the workspace item list and click **New item** > **Environment**.
2. Enter `pc_dev_environment`, then click **Create**.
3. Skip the optional teaching tour if it covers the editor.

![Create the Spark Environment.](images/manual-deployment/21-create-environment.png)

4. Confirm **Runtime 2.0 (Spark 4.1, Delta 4.2)**.
5. Open **Libraries** > **Custom**.
6. Click **Upload** > **Upload to Full mode**. Select the reviewed
   `people_counter-<version>-py3-none-any.whl`.

![Custom library upload control.](images/manual-deployment/22-upload-custom-wheel.png)

At a narrow editor width, first click **Open navigation menu** to reveal
Libraries and Spark compute. Close that menu after choosing a section if it
covers the editor.

![Wheel staging in Full mode; this earlier example filename differs from the final manual-binding build.](images/manual-deployment/23-wheel-staged.png)

7. Under **Spark compute** > **Compute**, set:

   | Setting | Reviewed value |
   |---|---|
   | Driver cores / memory | 8 / 56 GB |
   | Executor cores / memory | 8 / 56 GB |
   | Dynamically allocate executors | Off |

![Reviewed compute settings and dynamic allocation disabled.](images/manual-deployment/25-fixed-compute.png)

8. Under **Spark properties**, click **Add** for each row:

   | Property | Value |
   |---|---|
   | `spark.executor.instances` | `2` |
   | `spark.speculation` | `false` |
   | `spark.task.cpus` | `1` |

![The three required Spark properties.](images/manual-deployment/24-spark-properties.png)

9. Click **Publish**, review the wheel and compute changes, then **Publish all**.
10. Confirm **Publish** in the final confirmation dialog.
11. Use **View progress** and require successful completion. **Save** is not
    **Publish**, and a staged wheel is not yet a usable published dependency.

![Review the Environment changes and select Publish all.](images/manual-deployment/26-publish-environment.png)

![Successful publication of the distinct wheel build containing manual-binding support.](images/manual-deployment/31-environment-published.png)

The compute values come from
[`reviewed_compute_settings`](../src/people_counter/fabric_sjd_definition.py).
The selected pool and capacity must actually support them. Stop if publication,
allocation, dependency installation, or runtime validation fails.

When replacing a published wheel, use a distinct reviewed version/build
filename. Remove only the previous People Counter wheel from this Environment,
upload its replacement, and confirm the pending changes show the old file
deleted and the new file added. Publish again and check the new filename.
For example, the walkthrough's rebuilt wheel used build tag `1`:

```bash
uv build --wheel --config-setting=--build-option=--build-number=1
```

This command prepares an upload artifact locally; it does not deploy cloud
resources. Do not equate an unchanged older library's **Success** row with
successful installation of its replacement.

## 11. Create the seven Spark Job Definitions

1. From the workspace list, click **New item** > **Spark Job Definition**.
2. Create `pc_sjd_dev_control`.

The portal rejected the script's hyphenated `pc-sjd-dev-control` name.
**Use underscores for manual SJD creation.** These are manual item names, not
Terraform resource names.

![Create a portal-compatible Control job name.](images/manual-deployment/27-create-spark-job.png)

3. Under **Main definition file**, choose **Upload a local file**, click
   **Upload**, and select the Control role's `Main/main.py`.
4. Leave language at **PySpark (Python)**.
5. In **Environment**, click **Workspace default** > **Change environment**.

![Main-file upload and Environment selection.](images/manual-deployment/28-upload-job-main.png)

6. Select `pc_dev_environment` in **your manual workspace** and click
   **Confirm**. Identically named items in other workspaces are not equivalent.

![Select the Environment from the correct workspace.](images/manual-deployment/29-select-job-environment.png)

7. In **Lakehouse Reference**, click **Add**, select `pc_dev_lakehouse` in
   your manual workspace, and click **Add**. Confirm it is the default Lakehouse.
8. Click **Save** if there are unsaved settings. Do not click **Run** yet.

### Repeat for every role

Use the configured job's toolbar **Copy** button. Name the copy as below,
keep its location in the manual workspace, and click **Save**.
For each copy, upload the matching role's `main.py`, confirm replacement when
prompted, and verify that the Environment and Lakehouse bindings were retained.

![Copy the configured job into the next canonical role.](images/manual-deployment/30-copy-next-role.png)

| Job name | Main file |
|---|---|
| `pc_sjd_dev_control` | [`control/Main/main.py`](../fabric/spark-job-definitions/control/Main/main.py) |
| `pc_sjd_dev_dispatcher` | [`dispatcher/Main/main.py`](../fabric/spark-job-definitions/dispatcher/Main/main.py) |
| `pc_sjd_dev_process` | [`process/Main/main.py`](../fabric/spark-job-definitions/process/Main/main.py) |
| `pc_sjd_dev_reconciliation` | [`reconciliation/Main/main.py`](../fabric/spark-job-definitions/reconciliation/Main/main.py) |
| `pc_sjd_dev_gold` | [`gold/Main/main.py`](../fabric/spark-job-definitions/gold/Main/main.py) |
| `pc_sjd_dev_refresh` | [`refresh/Main/main.py`](../fabric/spark-job-definitions/refresh/Main/main.py) |
| `pc_sjd_dev_cutover` | [`cutover/Main/main.py`](../fabric/spark-job-definitions/cutover/Main/main.py) |

**Replacing the copied main file is mandatory.** Merely renaming a Control job
does not make it a Dispatcher, Process, or other role.

Keep all recurring schedules disabled throughout setup. Process and Cutover
are on-demand roles. Never enable schedules to test whether an incomplete
deployment works.

![The seven role-specific job definitions in the manual workspace.](images/manual-deployment/32-seven-jobs.png)

## 12. Bind the installed application to the manual artifacts

Use a wheel that includes the manual-binding support added with this guide.
Older wheels retain fixed production IDs and must not be used for this step.

Record the **workspace**, **Lakehouse**, and **Environment** IDs from their
Fabric browser URLs. These are not the Azure subscription, capacity, or
workspace-identity IDs.

For reference, the captured deployment used:

| Binding | ID |
|---|---|
| Workspace | `3e27ef75-7bb3-40be-9d29-d0c6c9c9256d` |
| Lakehouse | `1ed9464c-5585-4b3a-ad4a-373bef0d3762` |
| Environment | `41e60961-d16d-49f7-beaf-db262d31c20f` |

1. Open each job and set **Command line arguments** to the following prefix,
   replacing all three placeholders with that deployment's IDs:

   ```text
   --manual-artifacts <workspace-id> <lakehouse-id> <environment-id>
   ```

2. Click **Save**, refresh the job editor, and check the saved value.
3. Keep the Environment and default Lakehouse bound to the same manual
   workspace.
4. Add the role's command/release arguments only after this prefix. Do not
   invent release manifest or receipt hashes.

The prefix is explicit and all-or-nothing. The application rejects malformed
or nil UUIDs and checks the live NotebookUtils context before calling a
write-enabled role:

- `currentWorkspaceId` must match the requested workspace.
- `defaultLakehouseId` and `defaultLakehouseWorkspaceId` must match the
  requested Lakehouse and workspace.
- `environmentId` and `environmentWorkspaceId` must match the requested
  Environment and workspace.

Missing metadata is a failure, not permission to fall back to another
deployment. Without the prefix, the existing fixed production behavior is
unchanged. Never remove the prefix to work around a context error.

The **Cutover** job is deliberately different: its legacy migration commands
and stopped-writer gate are fixed to an existing migration. Leave the manual
prefix on that job, leave it unscheduled, and do not run it in a fresh
deployment. The unsupported prefix makes that legacy entry point reject the
invocation before starting Spark. Greenfield table initialization uses Control,
not the legacy Cutover migration.

### Initialize the new Lakehouse

After the updated Environment has successfully published:

1. Open `pc_sjd_dev_control`.
2. Append `bootstrap` to its manual prefix:

   ```text
   --manual-artifacts <workspace-id> <lakehouse-id> <environment-id> bootstrap
   ```

3. Click **Save**, refresh, and verify all IDs and the Environment/Lakehouse.
4. Click **Run** once. Inspect **Runs** and the driver output.
5. Require a terminal successful run and the expected `people_counter_sjd_*`
   tables in the new Lakehouse. A submitted, queued, or running job is not
   successful initialization.

![Saved explicit manual Control binding and bootstrap arguments.](images/manual-deployment/33-manual-job-arguments.png)

6. In **Runs**, open the submitted activity. Refresh that run's details;
   do not submit another run just because startup is slow.
7. Require **Status: Success**.

![Bootstrap run reached terminal Success; submitter information is masked.](images/manual-deployment/34-bootstrap-success.png)

8. Open **Logs**, expand **Driver (stdout)**, and click **Latest stdout**.
   The captured run printed `{"bootstrapped": true}`.

![Actual bootstrap stdout confirms bootstrapped true.](images/manual-deployment/35-bootstrap-stdout.png)

9. Return to the Lakehouse, click **Refresh**, and expand **Tables** > **dbo**.
   Scroll the Explorer: it virtualizes the table list, so one viewport is not
   an inventory. The walkthrough read back **all 22 expected bootstrap table
   names** and compared them with `BOOTSTRAP_TABLE_SUFFIXES`.
10. Open `people_counter_sjd_locks`. Verify one row with `lock_name=global`,
    `owner_id=NULL`, and `acquired_at=NULL`.

![New Lakehouse lock table contains the single expected global lock row.](images/manual-deployment/36-bootstrap-lock-table.png)

Reconciliation defaults to its `reconcile` command when only the manual prefix
is present. Refresh additionally needs `--workspace-id` equal to the manual
workspace plus both `--semantic-model-id` arguments once those models exist.
An attempt to refresh a different workspace is rejected before creating the
Gold state or requesting a refresh.

## Verified outcome and F2 stop point

| Check | Captured result |
|---|---|
| Workspace capacity | F2, Canada East, correct retained capacity ID |
| Workspace identity | Active; ADLS Blob Data Contributor granted to its principal ID |
| ADLS shortcut | `Files/footage` created; directory listing succeeded |
| Monitoring | Monitoring Eventhouse/database created; logging On |
| Environment | Updated, distinctly named wheel published successfully |
| Job definitions | Seven canonical roles created with their own `main.py` files |
| Runtime binding | Explicit manual arguments saved on all seven jobs |
| Bootstrap | One manual Control run; terminal Success; `bootstrapped=true` |
| Tables | Exact expected set of 22 bootstrap tables read back through the browser |
| Lock seed | Exactly one `global` row with no owner/acquisition time |
| Actual executor allocation | **1**; the reviewed inference requirement is **2** |
| Inference, reporting/alert acceptance, recurring schedules | **Not run / not enabled** |

![Run-details compute readback: actual executor count is one.](images/manual-deployment/37-f2-runtime-limit.png)

The executor count is the **run's observed allocation**, not a claim that every
F2 configuration always has this limit. A correctly entered Spark property
does not prove that the pool actually granted it. The reviewed two-executor
check has not been weakened. Continue only after a separately approved capacity/
pool configuration meets the contract and the remaining acceptance gates pass.

F2 was left **Active** at the operator's request. Capacity/storage charges
continue. A finished Spark job does not pause the Azure capacity.

## Acceptance boundary

Before calling this a working application, independently verify:

- Successful Environment publication and the intended installed SDK/dependencies.
- Control `bootstrap` succeeds and the expected `people_counter_sjd_*` Delta
  tables exist. The script runs this command through the Control job; it does
  not run the inference pipeline as a substitute.
- All seven roles have the correct source file, Environment, and default
  Lakehouse in this workspace.
- Deployment references and release provenance contain the **new** workspace,
  Lakehouse, Environment, and job IDs, not copied IDs from another environment.
- The immutable release manifest, postpublish receipt, and published release
  references are valid. See
  [canonical job release admission](../fabric/spark-job-definitions/README.md#release-admission).
- Both analytics and operational semantic models/reports are bound to the
  new Gold tables; refresh and readback succeed.
- Required RTI queries/dashboard and Activator rules/actions are configured
  and independently checked. A Monitoring Eventhouse alone is not these items.
- Activator action execution has its required operator checkpoint, or is
  explicitly recorded as deferred rather than passed.
- All five recurring schedules remain disabled until a separately approved
  operational handover.

Do not upload tokenized definition JSON from [`fabric/`](../fabric/README.md)
as if it were a ready-to-import PBIX or an already bound model. Those bundles
contain target-specific placeholders that the automated deployment normally
resolves. Browser item creation is not a replacement for that binding work.

If any acceptance item is unresolved, record it as **not verified** and keep
the deployment out of service. Use the
[automated local runbook](local-deployment.md) only if the operator separately
chooses the Terraform-managed path; do not mix the two management models.

## Application-change validation

The additional manual-binding application change was developed test-first.
The existing fixed deployment defaults remain unchanged; the manual path
requires explicit IDs and matching live runtime context before dispatch.
This was exercised in the successful browser-run bootstrap, not only mocked
locally.

Focused regression command:

```bash
uv run --frozen python -m pytest \
  tests/test_fabric_manual_deployment.py \
  tests/test_fabric_sjd.py \
  tests/test_fabric_sjd_runtime_localize.py \
  tests/test_fabric_sjd_runtime.py \
  -q --crap --cov=src/people_counter --cov-report=term:skip-covered
```

- **Tests:** 228 passed, 1 skipped.
- **CRAP:** threshold 30. A second run limited coverage to the three changed
  source modules. `validate_runtime_context`: score 4.04, complexity 4,
  coverage 86.7%; `_runtime_binding`: score 3.07, complexity 3, coverage 80%.
  The highest reported pre-existing risks were
  `_probe_and_persist_consumer_decision` (49.82, complexity 28, coverage 69.7%)
  and `_control_dispatch` (40.24, complexity 24, coverage 69.6%). Their
  additional coverage remains advisory follow-up, not a claim of full
  pipeline validation.
- **Mutation:** 592 selected terminal mutants: 538 killed, 54 survived,
  zero timeouts/suspicious/no-test outcomes in that selected set. Assertions
  were strengthened for nil IDs, option abbreviations, invalid reconciliation
  commands, nested model paths, and refresh/outbox behavior. Survivors include
  message-only changes, equivalent argparse behavior, strict-JSON reporting,
  and unexercised refresh error-diagnostic paths; this is not a perfect
  mutation score.

The mutation invocation was:

```bash
uv run --frozen mutmut run \
  'people_counter.fabric_sjd.*_validate_config*' \
  'people_counter.fabric_sjd.*__post_init__*' \
  'people_counter.fabric_sjd.*ManualFabricSjdConfig*' \
  'people_counter.fabric_sjd_jobs.*' \
  'people_counter.fabric_sjd_runtime.*resolve_localized*' \
  'people_counter.fabric_sjd_runtime.*localize_process_inputs*' \
  'people_counter.fabric_sjd_runtime.*refresh_main*' \
  --max-children 2
```

For that invocation, `PYTEST_ADDOPTS` supplied exact `--ignore=<path>` entries
for tests outside the four-file regression scope above; the repository's
mutmut configuration was not changed. Its unfiltered `tests/` stats collection
was blocked by an unrelated missing `torch` dependency, so no full-suite
mutation claim is made. Local Pylance checks found no type errors in the
changed production modules; Fabric-only `notebookutils`, Spark, and Delta
imports remain unavailable in the local editor interpreter.

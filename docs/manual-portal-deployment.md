# Manual Azure and Fabric deployment

This is the **portal-operated deployment walkthrough**. Do not run
Terraform, `bootstrap-local.sh`, or `deploy-local.sh` while following it.
The deployment script is the requirements checklist, not an instruction to
create a Terraform backend or a GitHub deployment identity.

Open [Azure](https://portal.azure.com/) and
[Fabric](https://app.fabric.microsoft.com/) in VS Code's integrated browser.
Keep the page being operated visible. Red outlines in the screenshots mark
the next control or the value to verify. Dark masks hide account details.

> **The bootstrap checkpoint is not application acceptance.** Its captured
> run received **one executor**, not the **two** required by the reviewed
> inference contract. The operator subsequently approved continuing component
> deployment and resolving the execution gates, without an unapproved capacity
> upgrade or backend replacement. External notifications must remain disabled.
> Do not enable unattended processing or call this production-accepted while
> an acceptance gate remains unresolved.

## Scope and evidence

The screenshots are real captures from the shared integrated browser on
2026-11-06 and 2026-11-07 (UTC). The walkthrough uses an isolated Development workspace; it does not
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

For the completed post-bootstrap component instructions, continue with
[publication inputs](#15-prepare-the-browser-run-publication-notebook),
[managed monitoring](#16-provision-and-enable-managed-workspace-monitoring),
[variables](#17-publish-and-verify-the-variable-library),
[reporting](#18-publish-the-analytics-and-operations-reporting),
[RTI and operations rules](#19-complete-the-rti-operations-surfaces), and
[release configuration and its explicit admission hold](#20-release-configuration-and-operational-handover).

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
  added with this change**, and the six role-specific `main.py` files from
  the same source revision. Do not use an unmodified older SDK wheel for
  manual execution. Artifact preparation is not cloud deployment: a release
  owner can supply these files or build them locally. Browser upload requires
  files accessible on the browser host. Prepare the SDK and its dependencies
  using [Build the upload artifacts](#build-the-upload-artifacts), then upload
  them in [step 10](#10-install-and-publish-the-spark-environment).
- For Fabric inference, a separately reviewed **headless OpenCV-compatible**
  dependency set and SDK packaging. This walkthrough requires headless OpenCV
  for Fabric, based on the operator's legacy-workspace experience. The current
  general-purpose SDK/CPU dependency metadata does not yet satisfy that
  requirement; see the [Fabric compatibility gate](#headless-opencv-compatibility-gate).
  Do not treat the existing bootstrap evidence as approval for inference.

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

**For a new deployment, use the managed monitoring procedure in
[section 16](#16-provision-and-enable-managed-workspace-monitoring), not the
classic `+ Eventhouse` procedure below.** Complete that section before accepting
RTI queries or alerts. The following captures explain the older artifacts
already present in this walkthrough; they are historical evidence, not the
recommended greenfield path.

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

### Build the upload artifacts

The release owner prepares artifacts from the same reviewed source revision
as the six job entry points. Run these commands from the repository root;
they do not create or change cloud resources:

```bash
# SDK plus all locked CPU dependencies, built on a target-compatible Linux host.
# Runtime 2.0 requires Python 3.13.
uv run --python 3.13 --locked --no-dev --extra cpu \
  python scripts/build_sdk_bundle.py cpu
```

See the [bundle generator](../scripts/build_sdk_bundle.py) and
[bundle contents and options](../README.md#build-cpu-or-gpu-deployment-bundles).
Extract the ZIP from `dist/sdk-bundles/`. It contains the SDK wheel and dependency
wheels under `wheels/`, a locked requirements file, and `manifest.json` with
artifact checksums. Verify those checksums before uploading. Transfer the
extracted files to the browser host if the build host is a different machine.
Use the SDK wheel from that same bundle rather than mixing release revisions.
The general CPU bundle currently contains **non-headless** `opencv-python`.
It is a release inventory, not an already qualified Fabric-headless
installation. Do not bulk-upload its OpenCV wheel to this Fabric Environment.

**Match the Fabric runtime, not the operator's laptop.** Confirm the runtime's
Python version and machine architecture before selecting dependency wheels.
[Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
uses Python 3.13 and Azure Linux 3.0. Build or obtain compatible Linux wheels
for that runtime; an ARM64 laptop bundle is not a substitute for an amd64
Fabric environment. The [SDK release workflow](../.github/workflows/release.yml)
now targets Python 3.13; select the matching Linux architecture and verify
the bundle manifest. Older Python 3.12 bundles must **not** be reused for
this Python 3.13 deployment. Matching Python and architecture does not replace
validation against Fabric's Linux runtime. The SDK's `py3-none-any` wheel is universal;
its NumPy, OpenCV, PyTorch, and other native dependencies are not.

### Include dependencies, not just the SDK wheel

The SDK wheel contains People Counter code and dependency metadata, **not**
copies of the packages it imports. The Environment must contain compatible
versions of the complete dependency graph. This includes the base packages
and the selected hardware extra from
[`pyproject.toml`](../pyproject.toml): for CPU inference, that includes
Transformers, Hugging Face Hub, OpenCV, NumPy, SciPy, Regex, LibreYOLO, RF-DETR,
Supervision, Trackers, PyTorch, and TorchVision, plus their transitive
dependencies. Uploading the SDK wheel alone does not request its optional
`cpu` extra.

**Fabric-only requirement:** use `opencv-python-headless`, not `opencv-python`,
in the Fabric Environment. This deployment policy does not change the default
dependencies for local SDK users or the general Linux SDK releases.

**Once the compatibility gate below is satisfied**, use the portal's
repository-backed installation: upload the reviewed Fabric-compatible SDK wheel
from the bundle under **Libraries > Custom > Upload to Full mode**. Then add
the approved dependencies through **Libraries > External repositories**, using
the package names and exact versions below. Fabric resolves transitive
dependencies during Full-mode publication; the operator does not need to type
every transitive package separately. Repository access is required.
Do not publish the current unmodified SDK wheel alongside headless OpenCV and
assume its conflicting dependency metadata has been resolved.

Do not upload the bundle ZIP, its requirements file, or its manifest as a
Python library. Do not upload all dependency wheels as well as adding the same
packages through PyPI. An offline deployment needs a separately reviewed
Fabric-headless wheelhouse; the existing general CPU bundle is not that
wheelhouse.

#### Direct dependency versions for the portal

This table describes the current SDK **0.10.1**, CPU variant, Linux/Python
3.13. Only some requirements in [`pyproject.toml`](../pyproject.toml) are exact
pins; the others are ranges or unversioned names. The exact versions below
come from the corresponding [`uv.lock`](../uv.lock), selected for this runtime,
except for the explicitly identified Fabric-only OpenCV substitution.
The table is a dependency-planning inventory, **not evidence that the current
SDK supports a clean headless installation**. Blocked rows must not be added.
For another release, use its reviewed Fabric inventory rather than copying this
table unchanged.

| Library name | Requirement in project configuration | Exact version to select |
|---|---|---|
| `transformers` | `>=5.1.0,<6` | `5.17.0` |
| `huggingface-hub` | `>=0.34.0` | `1.31.0` |
| `opencv-python-headless` | Fabric-only requirement; SDK currently declares `opencv-python>=4.11.0.86` | `5.0.0.93`; published package, not yet Fabric-qualified here |
| `numpy` | `>=1.24.0,<2.5` | `2.4.6` |
| `scipy` | `>=1.10.0` | `1.18.1` |
| `regex` | `>=2026.9.10` | `2026.9.10` |
| `libreyolo` | `==1.5.0` | **Do not add:** current `1.5.0` metadata conflicts with the Fabric-headless requirement |
| `rfdetr` | `==1.10.1` | `1.10.1` |
| `supervision` | `==0.30.3` | `0.30.3` |
| `trackers` | `==2.6.0` | **Do not add:** current `2.6.0` metadata has the same conflict |
| `torch` | CPU index; `>=2.13.0` | `2.15.0+cpu`; see exception below |
| `torchvision` | CPU index; no version constraint | `0.29.2+cpu`; see exception below |

**CPU PyTorch exception:** the last two versions are from
`https://download.pytorch.org/whl/cpu`, not the portal's default PyPI index.
Do **not** substitute the PyPI builds without `+cpu` or switch to CUDA packages
just because those appear in the version list. Check the Environment's built-in
inventory first. If it does not already provide the reviewed CPU builds, upload
the matching `torch` and `torchvision` wheels from the same Python 3.13/Linux
bundle under **Custom > Upload to Full mode**. These two dependencies are
exceptions to entering the public libraries through the PyPI form.

The CPU bundle includes only the `cpu` extra. For the reviewed **ONNX**
inference profile, also add `onnxruntime` version **`1.30.0`** through the same
PyPI form. It is an additional profile dependency from the current lock's
`export` graph, not a direct base/CPU requirement or a wheel included in the
CPU bundle. Fabric resolves its dependencies during publication. Model export
tooling is separate; inspect that graph with
`uv export --locked --extra cpu --extra export --no-dev --no-emit-project`.
Do not assume a successful CPU-only publication proves ONNX inference is ready.

#### Enter each public package through the UI

1. In `pc_dev_environment`, open **Libraries > External repositories**.
   Keep **List view** and the **Full mode** tab selected.
2. Click **Add library** > **Add library from public repository**.

![Environment External repositories page; Add library is outlined.](images/manual-deployment/38-environment-external-repositories.png)

3. Select **PyPI** as the public repository.
4. In **Name**, type the approved library name. For the Fabric-only OpenCV
   requirement, use `opencv-python-headless`, not `opencv-python`.
5. Wait for the version list, open **Version**, and choose the inventory's
   exact version. The current headless OpenCV candidate is **`5.0.0.93`**.
   Do not accept an automatically selected latest version.

![PyPI version picker for opencv-python-headless; the candidate version is 5.0.0.93.](images/manual-deployment/39-pypi-select-pinned-version.png)

6. Require **Publish Mode: Full mode**, then click **Add** to stage the row.
7. Repeat only for the approved, non-blocked PyPI packages in the reviewed
   Fabric inventory, plus `onnxruntime` when the selected profile requires it.
   Enter the name and version separately, not `name==version` in the Name box.
8. Review the complete staged inventory, including the SDK custom wheel and
   the CPU PyTorch exception. Resolve conflicts with built-in libraries before
   continuing to **Publish all** below. Stop if a required version is unavailable;
   do not silently select a newer version.

![Headless OpenCV 5.0.0.93 with Full mode selected. This form does not establish backend compatibility.](images/manual-deployment/40-pypi-full-mode-library.png)

These are real browser captures of package entry. The demonstration selected
the pinned version and then used **Cancel**; it did not add or publish libraries.
They prove the UI path, not successful installation of the complete dependency
set. Full mode resolves transitive versions at publication time; direct pins
alone do not guarantee the entire graph matches the bundle lock. Review the
resolved inventory and retain the publication evidence before accepting it.

#### Headless OpenCV compatibility gate

**For this manual Fabric deployment, headless OpenCV is required.** The
operator reported needing it in the legacy Fabric workspace. This is the
Fabric deployment requirement, not a claim that every Fabric runtime image
universally fails with every non-headless OpenCV build. Headless is an OpenCV
choice, not a PyTorch variant; CPU-only PyTorch still uses the `+cpu` builds
described above. Local SDK installations and general release bundles are
unchanged.

The current SDK metadata requires `opencv-python`, and the pinned
`libreyolo==1.5.0` and `trackers==2.6.0` dependencies also require that
distribution. LibreYOLO `1.7.2` and Trackers `2.7.0`, checked on 2026-11-06,
still require it. Merely
replacing the SDK's own requirement or entering `opencv-python-headless` in
the portal does not migrate the dependency graph: normal resolution can
install `opencv-python` as well.

Do **not** install both OpenCV variants. They provide the same `cv2` namespace
and can overwrite each other's files; see the
[OpenCV package installation guidance](https://pypi.org/project/opencv-python-headless/).
Treat the current LibreYOLO-dependent method as **incompatible with this
deployment's approved dependency policy**, not as proof that its algorithms
cannot execute with headless `cv2`. A clean installation is blocked until
the SDK and upstream metadata are compatible or a separately reviewed
replacement is approved. No package/lock changes are implied by this guide.

Choose another inference option **only after its complete dependency graph
and executor-side execution have been qualified for headless Fabric**:

| Current SDK option | Fabric-headless status |
|---|---|
| RT-DETR/OSNet, PyTorch | Not approved: imports `OSNetEmbedder` from LibreYOLO. |
| RT-DETR/OSNet, ONNX | Not a bypass: the pipeline still imports LibreYOLO at module load, before selecting ONNX. |
| RF-DETR/BoT-SORT, PyTorch or ONNX | Not an automatically compatible alternative: imports `BoTSORTTracker` from Trackers, whose metadata also requires `opencv-python`. |

These imports are in the
[RT-DETR/OSNet pipeline](../src/people_counter/pipelines/rtdetr_osnet.py) and
[RF-DETR/BoT-SORT pipeline](../src/people_counter/pipelines/rfdetr_botsort.py).
The current SDK therefore has **no qualified headless alternative established
by this walkthrough**. Do not omit LibreYOLO/Trackers and assume selecting
ONNX will make their imports optional. Keep inference and recurring schedules
disabled until a compatible backend and Fabric-specific package set are
provided and verified.

Before accepting the Environment, verify OpenCV import and video decoding on
the driver **and executors**. If loading fails because a GUI/system library
such as `libGL.so.1` is unavailable, stop and resolve the dependency/runtime
plan with the release owner. Do not mask the failure by adding both variants,
disabling dependency checks, or treating bootstrap success as inference
compatibility evidence.

Do not upload local `pyspark` or `delta-spark` packages over Fabric's managed
Spark/Delta runtime. Model weights and converted ONNX files are separate
artifacts, not Python libraries; provision them through the reviewed model
loading/release process before inference. See
[offline model loading](../README.md#offline-model-loading).

Use **Full mode** for every library needed by these jobs. Quick mode is
not supported for Spark Job Definitions. See Microsoft's
[Environment library management](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-library)
for dependency resolution, supported repository sources, and network restrictions.

### Upload and publish

For a new Fabric-headless deployment, proceed only after the
[compatibility gate](#headless-opencv-compatibility-gate) has passed. The
historical successful SDK publication shown below does not establish that
the current stock SDK wheel has headless-compatible dependency metadata.

1. Return to the workspace item list and click **New item** > **Environment**.
2. Enter `pc_dev_environment`, then click **Create**.
3. Skip the optional teaching tour if it covers the editor.

![Create the Spark Environment.](images/manual-deployment/21-create-environment.png)

4. Confirm **Runtime 2.0 (Spark 4.1, Delta 4.2)**.
5. Open **Libraries** > **Custom**.
6. Click **Upload** > **Upload to Full mode**. Select
   `people_counter-<version>-py3-none-any.whl` from the bundle's `wheels/`
   directory prepared [above](#build-the-upload-artifacts). Add the direct
   public dependencies using the [PyPI steps](#enter-each-public-package-through-the-ui)
   and handle the CPU PyTorch exception before publishing. Verify the staged
   inventory includes the selected CPU and model-format dependencies, not only
   the SDK wheel.

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

After publishing, start a **new** session attached to this Environment and
Lakehouse. Check the installed SDK/dependency versions against the reviewed
inventory and test the selected model backend on the executors, not only the
driver. Existing sessions can retain the old library snapshot. A bootstrap
success alone does not test the inference dependencies.

### Capacity, executors, and concurrent executions

These are different controls; increasing one does not automatically increase
the others:

| Control | Meaning and operator action |
|---|---|
| Capacity SKU and current load | Bound the Spark resources available across workloads. Other jobs, notebooks, throttling, bursting policy, and pool limits affect admission. A larger SKU is not a promise of a particular allocation. |
| Workspace Spark pool | Sets the available node sizes and scaling limits. Check **Workspace settings > Data Engineering/Science > Spark settings** and the pool selected for the Environment. Its limits must accommodate the reviewed session. |
| Driver cores / memory | Resources for coordinating the Spark application. The driver is not an inference executor. The reviewed request is 8 cores / 56 GB. |
| Executor cores / memory | Resources per worker executor, not the total for the job. The reviewed request is 8 cores / 56 GB per executor. |
| `spark.executor.instances=2` | Requests two executors for one session. It does not mean two job executions and does not prove that two were allocated. |
| Dynamic executor allocation off | Keeps the reviewed fixed-allocation request; do not turn it on merely to fit a smaller capacity. Pool node autoscaling and Spark executor allocation are separate controls. |
| `spark.task.cpus=1` | Reserves one executor core per Spark task. Two 8-core executors give a nominal ceiling of 16 simultaneous task slots, not a guarantee of 16 safe inference workers. Memory, model/thread budgets, partitions, and runtime admission can lower it. |
| `spark.speculation=false` | Disables speculative duplicate task execution. This is a correctness setting, not a capacity-tuning knob. |
| Dispatcher `--maximum-active-batches` | Application-level limit on active batches; the current default is 1. It is not an Environment Spark property or a Fabric capacity quota. Increase only after the release, resource, and concurrency checks have passed. |
| Dispatcher `--max-items` | Maximum items considered for a claimed batch; the current default is 64. It does not request 64 executors or 64 concurrent Spark jobs. |
| Schedule frequency / overlapping runs | Controls how often jobs are submitted. More frequent schedules can create overlap or queueing, not more compute. Keep schedules disabled throughout this walkthrough. |

The requested sizes therefore depend on capacity and workload, but the values
in this guide are a **reviewed deployment profile**, not an automatic SKU-to-
executor sizing formula. The source of that profile is
[`reviewed_compute_settings`](../src/people_counter/fabric_sjd_definition.py);
the [runtime admission code](../src/people_counter/fabric_sjd_runtime.py)
also checks live runtime/executor evidence and memory-safe placement.

Before approving a different profile:

1. Have the capacity owner review the SKU, competing workloads, pool limits,
   and [Fabric Spark concurrency and queueing rules](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-job-concurrency-and-queueing).
   Do not budget only executor cores and forget driver/session overhead.
2. Obtain approval for the revised compute profile and any corresponding
   application/release changes. Do not change only the portal values while
   leaving a different reviewed contract in the release.
3. Publish the Environment and start a fresh test session. In the run details,
   inspect the actual executor count, cores/memory, and Spark configuration.
   Record observed values, not just the saved request.
4. Validate executor-side package/model loading, warmed memory usage, task
   placement, and representative throughput before increasing active batches
   or enabling schedules.

For the captured F2 setup, retain the stop-after-bootstrap boundary: the run
received one executor, so it did not demonstrate the reviewed two-executor
profile. Neither changing a concurrency flag nor rerunning more jobs resolves
that evidence gap.

## 11. Create the six Spark Job Definitions

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

7. After uploading the main file, scroll to **Lakehouse Reference** and expand
   **All lakehouses**. Check the associated Lakehouse **even if the file upload
   succeeded**; uploading a Python file does not attach a Lakehouse.
   - If the list is empty or no Lakehouse is attached, click **Add**, find
     `pc_dev_lakehouse`, verify its **Location/workspace** is your manual
     workspace, select it, and click **Add** in the picker.
   - If the correct Lakehouse is already listed, keep it; do not attach a
     duplicate or choose an identically named Lakehouse from another workspace.
   - Confirm it is the **default Lakehouse**. The captured editor marks it with
     a pin; the control's accessible label is **Default lakehouse**.
     If a different Lakehouse is default, use the intended row's default
     control to set it correctly before proceeding.
8. Click **Save** if there are unsaved settings. Refresh/reopen the job and
   confirm the Lakehouse name, workspace, and default marker persisted.
   Do not click **Run** yet.

![Lakehouse Reference with the correct workspace and default pin.](images/manual-deployment/41-sjd-default-lakehouse.png)

![Lakehouse picker; verify Location as well as name. The correct row is already attached, so it is selected and disabled. Owner details are masked.](images/manual-deployment/42-sjd-lakehouse-picker.png)

The picker capture is a readback of an existing attachment, not an instruction
to detach it for a screenshot. If a new job's list is empty, select the correct
available row and use **Add**; never select the other workspace's same-name
Lakehouse to bypass a disabled row.

### Repeat for every role

Use the configured job's toolbar **Copy** button. Name the copy as below,
keep its location in the manual workspace, and click **Save**.
For each copy, upload the matching role's `main.py`, confirm replacement when
prompted, and verify that the Environment and Lakehouse bindings were retained.
Recheck **Lakehouse Reference** on **every** role: attach the intended Lakehouse
if missing, confirm the default marker and workspace, save, and reopen to verify.
Do not assume a copied or uploaded definition retained the attachment.

![Copy the configured job into the next canonical role.](images/manual-deployment/30-copy-next-role.png)

| Job name | Main file |
|---|---|
| `pc_sjd_dev_control` | [`control/Main/main.py`](../fabric/spark-job-definitions/control/Main/main.py) |
| `pc_sjd_dev_dispatcher` | [`dispatcher/Main/main.py`](../fabric/spark-job-definitions/dispatcher/Main/main.py) |
| `pc_sjd_dev_process` | [`process/Main/main.py`](../fabric/spark-job-definitions/process/Main/main.py) |
| `pc_sjd_dev_reconciliation` | [`reconciliation/Main/main.py`](../fabric/spark-job-definitions/reconciliation/Main/main.py) |
| `pc_sjd_dev_gold` | [`gold/Main/main.py`](../fabric/spark-job-definitions/gold/Main/main.py) |
| `pc_sjd_dev_refresh` | [`refresh/Main/main.py`](../fabric/spark-job-definitions/refresh/Main/main.py) |

**Replacing the copied main file is mandatory.** Merely renaming a Control job
does not make it a Dispatcher, Process, or other role.

Keep all recurring schedules disabled throughout setup. Process is the
on-demand role. Never enable schedules to test whether an incomplete
deployment works.

![Historical role inventory, including the now-retired Cutover job.](images/manual-deployment/32-seven-jobs.png)

Cutover tooling and its SJD definition have been removed. Do not create a new
Cutover job. Removing the code does not migrate work or delete existing
Lakehouse tables. Review any existing obsolete SJD separately before removal.

## 12. Bind the installed application to the manual artifacts

Use a portable SDK wheel with deployment-independent release identity v2.
Older wheels retain fixed production defaults and must not be used for this
workflow. Workspace, Lakehouse, and Environment bindings are runtime inputs;
SJD IDs are captured only after Fabric creates or accepts the uploaded jobs.

Record the **workspace**, **Lakehouse**, and **Environment** IDs from their
Fabric browser URLs. These are not the Azure subscription, capacity, or
workspace-identity IDs.

### Find and verify the three IDs

Start from the manual workspace's item list, not the monitoring activity page
or an identically named item in another workspace:

| ID to record | Portal steps |
|---|---|
| Workspace ID | Open `people-counter-guide-dev` (or your chosen workspace). In its browser URL, copy the workspace UUID following `/groups/`; some Fabric experiences expose it as the `workspaceObjectId` query parameter. Record the UUID only, not the whole URL. |
| Lakehouse ID | From that workspace's item list, open `pc_dev_lakehouse`. Copy the Lakehouse item UUID from its item URL, commonly after `/lakehouses/`. The workspace UUID in the same URL is not the Lakehouse UUID. Do not use the SQL analytics endpoint ID. |
| Environment ID | Return to the same workspace list and open `pc_dev_environment` in the Environment editor. Copy that Environment item's UUID from the item URL. Do not copy a notebook ID, SJD ID, Spark session ID, or run/activity ID. |

For example, a Lakehouse URL shaped as
`https://app.fabric.microsoft.com/groups/<workspace-id>/lakehouses/<lakehouse-id>`
contains **two different IDs**. UI routes can vary by experience; the required
values are the workspace UUID and each opened item's own UUID, not simply the
last UUID on any Fabric page. If the route is unclear, stop and have a workspace
administrator confirm the item ID before saving job arguments.

Keep a small binding record with each item's display name, item type, UUID,
and workspace UUID. Reopen each saved item link and confirm the workspace
breadcrumb and item name before using the IDs. All three UUIDs must belong
to this deployment. A successful runtime-context check below is the final
binding check; it is not permission to guess IDs.

For reference, the captured deployment used:

| Binding | ID |
|---|---|
| Workspace | `3e27ef75-7bb3-40be-9d29-d0c6c9c9256d` |
| Lakehouse | `1ed9464c-5585-4b3a-ad4a-373bef0d3762` |
| Environment | `41e60961-d16d-49f7-beaf-db262d31c20f` |

1. Open each job and set **Command line arguments** to the following prefix,
   replacing all three placeholders with that deployment's IDs:

   ```text
   --artifacts <workspace-id> <lakehouse-id> <environment-id>
   ```

2. Click **Save**, refresh the job editor, and check the saved value.
3. Keep the Environment and default Lakehouse bound to the same manual
   workspace.
4. Add the role's command/release arguments only after this prefix. Do not
   invent release manifest or receipt hashes.

The older `--manual-artifacts` spelling is retained as an alias. If you use
a state root other than `Files/people-counter/sjd/v1/`, append
`--files-root Files/<deployment-path>/` consistently to each role and set the
same `files_root` in backfill configuration. Paths remain confined to that
validated directory.

The prefix is explicit and all-or-nothing. The application rejects malformed
or nil UUIDs and checks the live NotebookUtils context before calling a
write-enabled role:

- `currentWorkspaceId` must match the requested workspace.
- `defaultLakehouseId` and `defaultLakehouseWorkspaceId` must match the
  requested Lakehouse and workspace.
- `environmentId` and `environmentWorkspaceId` must match the requested
  Environment and workspace.

Missing metadata is a failure, not permission to fall back to another
deployment. Without the prefix, the SDK derives and checks the attached
runtime context; it never falls back to a reference workspace. Never change
bindings to bypass a context error. Greenfield table initialization uses
Control `bootstrap`; no Cutover or migration step is required.

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

## 13. Create the operations Activator

The operations Activator monitors job health. It is **not** the separate
manifest-arrival Activator that invokes the intake pipeline.

1. Open the **new manual workspace**. Click **New item**, then **Activator**.
   Fabric creates an item with a timestamped default name; a separate naming
   dialog is not required in the captured UI.

![New item gallery with the Activator card outlined.](images/manual-deployment/43-new-activator.png)

2. Open the item-name drop-down in the top bar. Set **Name** to
   `pc_dev_operations_activator` and press **Enter**.
3. Click **Get data**. In the source picker, select **Fabric events**, then
   **Job events**.
4. Open **Event type(s)**. Deselect Created, StatusChanged, and Succeeded.
   Leave only **Microsoft.Fabric.JobEvents.ItemJobFailed** selected.

![Only the failed-job event type remains selected.](images/manual-deployment/44-activator-failed-job-event.png)

5. Keep **Event scope** at **By item**. Check **Workspace** is your manual
   workspace. Under **Item**, select `pc_sjd_dev_dispatcher`, not a job in
   another workspace. Click **Next**.
6. On **Review + connect**, verify the event type, workspace, dispatcher,
   and destination Activator. Click **Connect**.

![Review the manual workspace and dispatcher before connecting the source.](images/manual-deployment/45-activator-dispatcher-source-review.png)

7. Wait for **Ingest Job events into Activator: Successful**. Click
   **Finish**. Dismiss the first-use tour with **Get started**, if shown.
8. Select the dispatcher event source and click **New rule**. The source
   already filters failures, so **On every value** means every received
   dispatcher-failure event; it does not mean every dispatcher run.
9. Click the rule-name pencil. Rename the rule to
   `alert_pc_sjd_dev_dispatcher_failed` and press **Enter**.
10. **Do not click Start or Send me a test action.** The portal initially
    supplies an email action addressed to its current author. That default
    is not recipient approval. Leave the rule stopped, close the definition
    pane, and verify **Start** is available and **Stop** is disabled.

![Dispatcher failure rule exists but is stopped; no notification was sent.](images/manual-deployment/46-activator-failure-rule-stopped.png)

The walkthrough created Activator
`126d9ab1-b1d9-4a2a-8733-397ef8cdd174` and rule
`5b0d5fd8-1cf6-475c-8c47-b1afc7942b76`. These are evidence IDs, not values to
paste into a different deployment.

**Verification boundary:** the connection task succeeded and the rule was
created stopped. No dispatcher-failure event has yet been proven to arrive.
The initial Live feed/analytics panels displayed no data and returned HTTP
400 errors; these are not evidence of healthy event delivery. Resolve that
readback and verify a controlled failure reaches this exact source before
accepting the monitoring path. Notification delivery remains explicitly
deferred by operator choice.

## 14. Prepare offline model artifacts, then upload through Fabric

### 14.1 Download and convert with the existing script

Run these commands from the repository root on a connected preparation
machine with `uv` installed. This is the model-preparation exception to the
browser-only cloud setup: use the existing
[`scripts/download_models.py`](../scripts/download_models.py), not manual
downloads from individual model-hosting pages. No Terraform or cloud
deployment script is involved.

Download both RT-DETR backbones (R18 and R50), OSNet, and RF-DETR Large:

```bash
uv run --frozen --extra cpu python scripts/download_models.py \
  --pipeline all \
  --output-dir outputs/models
```

Original `.safetensors`, `.pt`, and `.pth` weights are not ONNX models.
For the selected RT-DETR/OSNet ONNX runtime, also run:

```bash
uv run --frozen --extra cpu --extra export python scripts/download_models.py \
  --pipeline rtdetr-osnet \
  --output-dir outputs/models \
  --convert onnx
```

This exports **both** RT-DETR backbones and OSNet. If preparing the alternative
RF-DETR ONNX backend as well, run:

```bash
uv run --frozen --extra cpu --extra export python scripts/download_models.py \
  --pipeline rfdetr-botsort \
  --output-dir outputs/models \
  --convert onnx
```

Alternatively, `--pipeline all --convert onnx` downloads and converts the
complete set in one invocation. Conversion uses the preparation machine's
CPU; its export dependencies are not the Fabric headless runtime profile.
Keep that preparation environment separate from the published Fabric
Environment. A successful export is not executor qualification.

The script pins the Hugging Face revisions and checks the RF-DETR checkpoint
checksum. It reuses existing downloads/exports unless `--force` is supplied.
Do not use `--force` routinely, and do not assume an existing ONNX file was
revalidated merely because the command reused it. Stop on any download,
checksum, or conversion error.

**Executed evidence:** all three commands above completed successfully during
this walkthrough. All four exported ONNX files passed `onnx.checker` and a
local ONNX Runtime CPU zero-input smoke run with finite outputs:

| Model | Input shape | Output shapes |
|---|---|---|
| RT-DETR R18 | `1 x 3 x 640 x 640` | `1 x 300 x 80`, `1 x 300 x 4` |
| RT-DETR R50 | `1 x 3 x 640 x 640` | `1 x 300 x 80`, `1 x 300 x 4` |
| OSNet | `1 x 3 x 256 x 128` | `1 x 512` |
| RF-DETR Large | `1 x 3 x 704 x 704` | `1 x 300 x 4`, `1 x 300 x 91` |

These are structural/local runtime checks, not accuracy tests or Fabric
inference acceptance. Export emitted Torch tracing/deprecation warnings;
representative input and batch-size qualification remain required.

### 14.2 Preserve the Lakehouse file layout

Upload model artifacts through the **Fabric Lakehouse UI**, not a terminal
uploader, Terraform, or a notebook upload API. The target is the manual
Lakehouse's **Files** area, not Tables and not the ADLS footage shortcut.
Preserve this layout relative to **Files**:

```text
models/
  rtdetr_osnet/
    rtdetr_v2_r18vd/
      config.json
      preprocessor_config.json
      model.safetensors
      model.onnx
    rtdetr_v2_r50vd/
      config.json
      preprocessor_config.json
      model.safetensors
      model.onnx
    libre_reid_osnet/
      osnet_ain_x0_25.pt
      osnet_ain_x0_25.onnx
  rfdetr_botsort/
    rf-detr-large-2026.pth
    rf-detr-large-2026.onnx
```

For the selected R18/OSNet ONNX runtime, the required runtime files are the
R18 `config.json`, `preprocessor_config.json`, and `model.onnx`, plus
`libre_reid_osnet/osnet_ain_x0_25.onnx`. Keep originals and alternative
backbones when staging the full artifact set. Do not upload Hugging Face
`.cache` directories, flatten the folders, add another `models` level, or
rename original weights to `.onnx`. The runtime model root is
`Files/models`, not `Files/people-counter/sjd/v1`.

### 14.3 Select the prepared folder through the Fabric UI

1. Open the manual workspace and `pc_dev_lakehouse`. In Explorer, click
   **Files**. Verify the destination breadcrumb ends at **Files**, not
   `footage` or a table.
2. Open **More options** beside the **Files** tree item. Click **Upload**,
   then **Upload folder**.

![Files context menu with Upload folder outlined.](images/manual-deployment/52-lakehouse-upload-model-folder.png)

3. In **Upload folder**, verify the destination is
   `<manual-workspace>/<manual-lakehouse>/Files/`. Leave **Overwrite if
   folder already exists** unchecked for a new deployment.
4. Click the outlined file-selection field. Select the prepared **models**
   folder itself, not its parent `outputs` and not a single backbone folder.
   The native file picker reads files on the computer running VS Code's
   integrated browser. If preparation ran on a remote Linux host, first copy
   the prepared folder to that computer while preserving its internal paths.
   Exclude Hugging Face `.cache` directories from the upload copy.

![Upload-folder dialog with the native folder-selection field outlined.](images/manual-deployment/53-lakehouse-select-model-folder.png)

5. After selecting the folder, verify the field reads **models** and the
   **Upload** button is enabled. Click **Upload** once.

![Prepared models folder selected, with the Upload button outlined.](images/manual-deployment/54-lakehouse-submit-model-upload.png)

6. Wait for every entry under **Current uploads** to complete. A queued
   **Waiting...** entry or a full byte counter with a running progress bar
   is not a completed upload. Do not close the browser or dismiss all tasks
   while uploads are running.

![Completed model uploads; the close-dialog control is outlined.](images/manual-deployment/55-lakehouse-model-uploads-completed.png)

7. Verify every task has the **Completed file upload** checkmark. The full
   artifact set has **12** files. Click **Close upload dialog**.
8. Under **Files**, double-click **models**, then **rtdetr_osnet**, then
   **rtdetr_v2_r18vd**. Verify the four expected filenames and sizes.

![R18 model files read back from the Lakehouse, with model.onnx outlined.](images/manual-deployment/56-lakehouse-r18-model-readback.png)

9. Use the Explorer tree to inspect the other folders. R50 must have four
   files; `libre_reid_osnet` and `rfdetr_botsort` must each have two. If a
   breadcrumb collapses a parent into an overflow menu, select the parent
   from that menu or use Explorer instead.

| Destination under `Files/models` | Verified file count | Portal-displayed sizes |
|---|---|---|
| `rtdetr_osnet/rtdetr_v2_r18vd` | 4 | Config 5 KB; preprocessor 444 B; ONNX 77 MB; weights 77 MB |
| `rtdetr_osnet/rtdetr_v2_r50vd` | 4 | Config 5 KB; preprocessor 444 B; ONNX 164 MB; weights 164 MB |
| `rtdetr_osnet/libre_reid_osnet` | 2 | ONNX 888 KB; weights 978 KB |
| `rfdetr_botsort` | 2 | ONNX 116 MB; weights 129 MB |

**Executed evidence:** all 12 upload tasks reported **Completed file upload**.
All four destination folders were opened and their filenames/counts and
displayed sizes read back through the native browser. Before submission,
all 12 selected model/configuration files matched the Linux-prepared
SHA-256 hashes; `.cache` entries were excluded from the browser selection.
Portal size labels are rounded, not exact byte counts or remote checksums.
Post-upload SHA-256 readback and executor-side model loading remain release
acceptance gates; neither has been demonstrated by this upload walkthrough.

## 15. Prepare the browser-run publication notebook

The analytics and operations reports are repository definitions, not PBIX
files. Their semantic models are TMDL bundles. A report-only upload cannot
create the matching model or bind it to this Lakehouse.

The operator approved using a **small notebook created and run in the native
browser** to publish these exact definitions through Fabric's APIs. This is
the explicit notebook-assisted part of the manual path. It does not run
Terraform, connect workspace Git, or deploy from a terminal.

### Prepare and upload the checked definition inputs

1. On the preparation machine, from the repository root, run:

   ```bash
   uv run --frozen python scripts/prepare_manual_fabric_assets.py
   ```

   This validates the hashes, sizes, complete part lists, and token evidence
   of the seven reporting/RTI/Activator bundles using the existing definition reader.
   It also packages the repository REST client, tested publication helpers,
   and variable-library template. **It makes no cloud requests.**

2. Record the printed **SHA256**. If the command refuses to overwrite different
   prepared inputs, choose a fresh `--output` path and use that new filename
   consistently. Do not silently replace an earlier publication input.
3. Make the resulting `manual-publication.json` accessible to the computer
   running the integrated browser. Optional compression uses Python's
   standard library:

   ```bash
   uv run python -c "import gzip; from pathlib import Path; p=Path('outputs/manual-fabric/manual-publication.json'); p.with_suffix('.json.gz').write_bytes(gzip.compress(p.read_bytes(), mtime=0))"
   ```

   The notebook checks the **uncompressed JSON's** SHA-256, not the `.gz`
   file's SHA-256.
4. Open the manual Lakehouse and select **Files**. Use **More options** beside
   Files > **Upload** > **Upload files**. Select the prepared JSON or its gzip
   envelope. Leave overwrite unchecked. Click **Upload** and wait for
   **Completed file upload**. Verify the file appears directly under Files.

![Upload the checked publication input to the manual Lakehouse.](images/manual-deployment/58-upload-publication-input.png)

The captured upload used `Files/manual-publication.json.gz`. This input is
not a model artifact and must not go into `Files/models`.

### Create, bind, and run the notebook

1. Return to the manual workspace. Click **New item** > **Notebook**.
   Enter `pc_dev_deployment`, verify the location is the manual workspace,
   and click **Create**. If a previously created notebook exists, open it
   instead of creating a duplicate.

![Create the publication notebook in the manual workspace.](images/manual-deployment/57-create-deployment-notebook.png)

2. Dismiss the actual first-use dialog with **Skip for now**, if shown.
   Do not attempt to operate a cell through an onboarding overlay.
3. Under **Add data items**, choose **From OneLake catalog**. Select the
   `pc_dev_lakehouse` row in **people-counter-guide-dev** whose icon/type is
   **Lakehouse**, not its SQL analytics endpoint or an identically named
   item in another workspace. Click **Add**.
4. Open the supplied
   [publication notebook](../notebooks/fabric/manual_workspace_publication.ipynb)
   in VS Code. Copy its code cells into the browser notebook in order.
   To insert below the selected browser cell, press **Esc**, then **B**.
5. In the first cell, set the published inference Environment ID and expected
   workspace name. Workspace and Lakehouse IDs come from the attached default
   Lakehouse. Set `DEPLOYMENT_STAGE`, `SJD_NAME_PREFIX`, and
   `DEPLOYMENT_FILES_ROOT` to this deployment's configuration.
   Set `EXPECTED_INPUT_SHA256` to the exact fresh preparation output and
   `INPUT_FILE_NAME` to the uploaded filename. Regenerate older prepared inputs
   that do not contain the portable deployment templates.
6. Click **Run cell** for the setup cell. Confirm **Workspace verified**
   identifies the intended manual workspace and the printed input hash is
   the one recorded during preparation. The live workspace check runs before
   publication. Never print the token or session authorization headers.
   If the administrative session has timed out, select **Connect** >
   **New standard session**, wait for **Session ready**, and rerun setup
   before any publication cell. A queued cell in **Not connected** state
   has not executed and must not be recorded as successful.
7. Run the subsequent cells separately at the checkpoints below. This
   administrative notebook can use **Workspace default**; that does not
   qualify the inference SDK, Environment, or executor profile.

The walkthrough notebook is
`31e89e1a-cb68-447c-8a3d-fa1552160635`. The input prepared during the initial
browser run had SHA-256
`784ff71c581dbdd8421746efcce3702dcc8de115976a2fa8aa10964699093c42`.
These identify that captured run, not the hash to paste after rebuilding
inputs from changed source. The publication helpers were subsequently
corrected and tested for Fabric JSON normalization, default-format handling,
and same-name Eventhouse/database items. Always prepare from the current
checked-out sources.

After the walkthrough, this live notebook was reduced to a two-cell audit:
a summary and a read-only configuration-evidence check. Its historical
publication and recovery cells were archived privately in OneLake and
removed from the executable notebook. For a fresh deployment, use the
repository template above, not the finished audit notebook or its recovery
archive.

The helper only updates items it previously marked as its own or an
explicitly approved existing item ID. It refuses ambiguous same-type names
and unrelated existing items. Do not remove those checks to force an update.

## 16. Provision and enable managed workspace monitoring

Managed monitoring is separate from the dedicated operations Eventhouse in
section 19. Its database is the datasource for the repository Queryset and
dashboard.

**Pause classic collection before enrolling.** If the workspace already has
classic monitoring, open **Workspace settings** > **Monitoring** and turn
**Log workspace activity** from **On** to **Off**. Verify the paused message.
Do not delete the classic Eventhouse or database. A genuinely new workspace
without classic monitoring can skip this pause. Microsoft explicitly states
that migration cannot proceed while legacy collection is running; see
[the supported migration prerequisites](https://learn.microsoft.com/en-us/fabric/fundamentals/enable-workspace-monitoring#migrate-from-legacy-workspace-monitoring).

![Pause legacy collection without deleting its stored history.](images/manual-deployment/70-pause-legacy-monitoring.png)

1. Run **Provision managed monitoring** in the publication notebook. It
   creates `Monitoring_item_<workspace-name>`, configures its Eventhouse
   destination, waits for the managed database, and reads the definition
   back. Keep the existing classic artifacts and their history; do not
   click **Delete legacy monitoring artifacts**.
2. Return to the workspace. Click **Workspace settings** > **Monitoring**.
   Wait until **Fetching monitoring definition** disappears.
3. Click **Manage settings**. Check **Enable diagnostic data collection**.
   Verify **Monitoring item name** identifies this workspace's new item.
4. Leave **Provision an Event stream custom endpoint** and **Enable AI
   powered investigations** unchecked unless separately approved.
5. Click **Save**.

![Managed monitoring settings with diagnostic data collection outlined.](images/manual-deployment/59-managed-monitoring-diagnostics.png)

![Enable only the approved diagnostic capability, then save.](images/manual-deployment/60-save-managed-monitoring.png)

6. Reopen the settings and verify **Status: Enabled** and diagnostic data
   collection is listed under **Capabilities enabled**. A created item,
   successful definition update, or provisioned database is not this check.
7. In the workspace list, inspect the managed item and its generated
   Eventhouse, database, Eventstream, and Activator. Do not manually create
   duplicate supporting items to reproduce their names.

**Captured provisioning:** managed item
`ba765085-8f64-47b4-b6b2-4b256ad48962`; database
`3892ea43-e64b-48dc-9bef-d65d600c7c95`.
The managed database's service-returned query endpoint was used for RTI
binding. Do not reuse the classic database's ID or guess an endpoint URL.

**Captured correction:** the initial Save returned HTTP 400 / `UnknownError`
from `createDiagnosticsettings` and displayed **Disabled / None** because
classic collection was still running. This was not a capacity-sizing problem.
The successful notebook definition readback had not established collection.

![Historical failed enablement, before pausing legacy collection.](images/manual-deployment/61-managed-monitoring-status.png)

For this already partially configured walkthrough, the new monitoring
Eventhouse was verified to have **0 B** stored data, and private,
readback-verified backups of the three dependent RTI definitions were retained. Only those
new consumers were temporarily detached; lineage was checked before the
empty premature enrollment was moved to the recycle bin. That exposed the
classic pause control. After pausing classic logging, the monitoring item
and its supporting items were restored together from the recycle bin,
retaining their original IDs. No classic monitoring data was deleted.

This repair is **not** a routine greenfield step. Do not copy it onto a
collector with data or unreviewed dependents. It requires an owner-reviewed
backup, dependency, and restoration plan. The integrated browser's **Use new
experience** confirmation did not render a usable input in this session;
no typed Move confirmation is claimed. The notebook-assisted enrollment and
supported item restoration were the verified route.

![Recovery only: the exact empty premature enrollment, not classic history.](images/manual-deployment/69-revert-empty-monitoring-enrollment.png)

![The migration banner becomes available after classic logging is paused.](images/manual-deployment/71-migrate-after-pausing-legacy.png)

![Restore only the selected monitoring enrollment and its supporting items.](images/manual-deployment/72-restore-monitoring-after-legacy-pause.png)

**Final capability readback:** Save then succeeded. The native settings pane
showed **Status: Enabled**, **Capabilities enabled: Diagnostic data collection**,
and **Updated workspace monitoring item successfully**. AI investigations and
custom endpoint export were not enabled.

![Verified managed monitoring enabled with diagnostic collection.](images/manual-deployment/73-managed-monitoring-enabled.png)

All three temporarily detached consumers were subsequently restored from
their SHA-256-verified private backups: the Queryset, dashboard, and operations
Activator retained their original IDs and their exact bound definitions.
New
telemetry can take **up to one hour** after migration while cached legacy
destination settings expire. Zero rows during that interval are not proof of
failed configuration, nor proof of successful event delivery. Generate and
read back new relevant activity before accepting collection end to end.

**Later post-repair readback:** at `2026-11-07 02:31 UTC`, the restored
Queryset completed in **0.301 s** and returned `Completed: 2` and
`InProgress: 1` from this managed database. This establishes that job events
are reaching managed monitoring. It does not establish a particular
Dispatcher failure, intake-duration trigger, or inference run.

![Run the restored Queryset and verify actual managed-monitoring job events.](images/manual-deployment/77-managed-monitoring-events-readback.png)

Eventstream metrics are a separate setting on each applicable Eventstream:
open that item > **Settings** > **Monitoring** and verify the required metrics
configuration. Provisioning an Eventstream or its custom endpoint does not
prove that metrics or workspace diagnostics are flowing.

## 17. Publish and verify the variable library

1. Run **Publish typed deployment variables** in the publication notebook.
   It uniquely resolves the six uploaded job IDs from this workspace, not
   IDs from another environment, then publishes `pc_dev_variables`.
   It first retains the actual foundation and role bindings in
   `Files/deployment/deployment.json`. Missing/duplicate roles, malformed IDs,
   or conflicting existing state stop the run; no SDK rebuild is needed.
2. Wait for **Variable library configured** and the typed-reference list.
   Readback compares JSON values, not formatting: Fabric reformats JSON
   during export. Changed values still fail verification.
3. Return to the workspace and open `pc_dev_variables`.
4. Verify the base/default values below. Do not add a value-set override
   pointing to a different workspace.

| Variable | Type | Required value |
|---|---|---|
| `DeploymentStage` | String | `dev` for this walkthrough |
| `WorkspaceId` | String | The manual workspace ID |
| `Lakehouse` | ItemReference | Manual workspace + manual Lakehouse |
| `Environment` | ItemReference | Manual workspace + published Environment |
| `ControlSparkJob` | ItemReference | Manual Control job |
| `DispatcherSparkJob` | ItemReference | Manual Dispatcher job |
| `ProcessSparkJob` | ItemReference | Manual Process job |
| `RefreshSparkJob` | ItemReference | Manual Refresh job |
| `ReconciliationSparkJob` | ItemReference | Manual Reconciliation job |
| `GoldSparkJob` | ItemReference | Manual Gold job |

The walkthrough library is `d8540289-cb2d-45e8-bcc2-4998be59a757`.
The historical capture read back eleven values, including the retired role.
The current six-role library contains ten values. Verify that current set
after publication. Reporting publication also retains the assigned semantic
model and report IDs in the deployment state file. This file records bindings;
it is not SDK release approval or immutable admission evidence.

![Verify the active default value set and typed item references.](images/manual-deployment/62-variable-library-default-values.png)

## 18. Publish the analytics and operations reporting

Use the **exact** definitions under
[`fabric/semantic-models/`](../fabric/semantic-models/) and
[`fabric/reports/`](../fabric/reports/). Do not build substitute reports from
blank canvases. Do not connect Git to this populated workspace as a shortcut:
an initial synchronization can replace workspace content.

### Publish and verify both pairs

1. Confirm the Power BI author has an appropriate license. The walkthrough
   used the explicitly approved free 60-day trial, not a paid purchase.
   Record its expiry and arrange ongoing author/viewer access separately.
   An F2 capacity alone is not a Power BI author license.
2. Run **Publish the exact repository reporting definitions** in the notebook.
3. Wait for both **Semantic model definition verified** and **Report
   definition verified** for **each** bundle.
4. Inspect the printed bindings and the workspace item list:

| Bundle | Semantic model | Report |
|---|---|---|
| Analytics | `pc_sjd_dev_analytics_model` | `pc_sjd_dev_analytics_report` |
| Operations | `pc_sjd_dev_operations_model` | `pc_sjd_dev_operations_report` |

The notebook binds `TargetWorkspaceId`, `TargetLakehouseId`, and
`TargetLakehouseName` in the models. It binds the actual newly created model
ID/name in each report **and** changes the report's Power BI workspace
connection from the old workspace name to this manual workspace.

Legacy-looking logical table names inside the models are not proof of an
incorrect source: the repository's physical partitions already target
`people_counter_sjd_*` tables. Do not rename those logical objects and break
the report visuals. Template `.platform` metadata is omitted from publication,
not reused as a live item identity.

**Verified definition bindings in this walkthrough:**

| Artifact | ID |
|---|---|
| Analytics model | `3b014b01-3f01-4395-805c-5346c65a2684` |
| Analytics report | `e6e16e24-009e-4db2-bc5e-1ac04d8a425e` |
| Operations model | `9eb0a2b3-f78b-4363-94ef-73352aa0fa7f` |
| Operations report | `bdb6b65b-bc84-4d93-8a48-0f579828dfba` |

### Refresh and open the reports

5. Submit each model refresh with **NoNotification** from the browser
   notebook. The same checked REST client can use the Power BI API root:

   ```python
   power_bi = FabricRESTController(
       NotebookTokenProvider(),
       workspace_id=TARGET_WORKSPACE_ID,
       api_root="https://api.powerbi.com/v1.0/myorg",
   )
   for bundle, binding in reporting_ids.items():
       path = (
           f"/groups/{TARGET_WORKSPACE_ID}/datasets/"
           f"{binding['semanticModelId']}/refreshes"
       )
       power_bi._json(
           "POST", path,
           value={"notifyOption": "NoNotification"},
           expected=(202,),
       )
       print(bundle, "refresh accepted; completion still requires readback")
   ```

6. Open each semantic model's **Refresh history** in Fabric and wait for a
   terminal **Completed** result. An HTTP 202 response is only submission.
   If several refreshes appear, inspect their IDs, types, timestamps, and
   outcomes rather than guessing which request completed.
7. Open **both reports**. Visit their pages and verify the visuals load
   without missing-model, credential, missing-table, or broken-field errors.
   An empty chart is expected before representative processing and Gold
   publication; it is not end-to-end inference acceptance.
8. Reopen the Refresh job and bind **both new semantic-model IDs** in its
   role arguments. Retain the manual-artifact prefix and same workspace:

   ```text
   --manual-artifacts <workspace-id> <lakehouse-id> <environment-id> --workspace-id <workspace-id> --semantic-model-id <analytics-model-id> --semantic-model-id <operations-model-id>
   ```

Do not enable a Refresh schedule until the release-configuration checkpoint
below passes. If refresh fails on credentials, fix the model's actual data
connection and permissions in its settings; do not rebind it to an older
working workspace to make the error disappear.

**Captured refresh evidence:** analytics ViaApi request
`5a4e4bae-f7f9-45b3-80d2-845dc84a8d4b` completed at
`2026-11-07T00:37:41.517Z`; operations ViaApi request
`d8a66174-fdaa-43b8-b0e6-04673dd64d8c` completed at
`2026-11-07T00:37:59.907Z`. The history also contains automatic
`DirectLakeFraming` entries. Those are not additional caller-submitted API
refreshes and must not be confused with their request IDs.

All six analytics pages and all three operations pages were opened in the
native browser. The captured report views rendered without visible
missing-model or broken-field error cards. No representative processing data
has been loaded. The Forecast page contains the repository's baseline
scenario, not measured throughput for this F2 capacity.

![Repository analytics report, with the Forecast page control outlined.](images/manual-deployment/63-analytics-report-forecast.png)

![Repository operations report, with the Operations page control outlined.](images/manual-deployment/64-operations-report-rendered.png)

## 19. Complete the RTI operations surfaces

### Eventhouse and database

1. Run **Publish RTI definitions** in the notebook.
2. Verify `pc_operations_eventhouse` and its `pc_operations` ReadWrite
   database exist. Open the Eventhouse and check the database belongs to it.
   Fabric can also create a default database named
   `pc_operations_eventhouse`; that is a different item type, not a duplicate
   Eventhouse. Do not delete it merely because the display names match.

Walkthrough Eventhouse: `46a7385b-a79a-4de1-9963-c1acff5813f5`.
Operations database: `db68b713-7a91-4410-862b-d81025518eeb`.

![Verify the operations Eventhouse and its two differently named databases.](images/manual-deployment/68-operations-eventhouse-databases.png)

### Queryset

3. Open `pc_dev_operations_queries`. If adopting an existing manually created
   Queryset, set its verified ID in `APPROVED_QUERYSET_ID` in the template
   before publication. Leave that parameter empty for a new item.
4. Check its datasource is the **managed workspace-monitoring database**,
   not the separate operations database or classic monitoring database.
5. Check the saved query is the repository query:

   ```kusto
   ItemJobEventLogs
   | where isnotempty(JobStatus)
   | summarize Events = count() by JobStatus
   | order by JobStatus asc
   ```

6. Run it in the browser. Confirm a valid result/schema; zero rows alone
   does not establish event delivery. A missing table or disabled collection
   must be resolved at the monitoring source, not hidden by changing the KQL.

The existing Queryset `052ad7f3-2b3b-49fa-9fd5-8c5637cb4ac2` was explicitly
adopted and its bound definition read back successfully.

The saved query was run in the native browser: **Done (0.398 s)**, one
dataset, columns **JobStatus** and **Events**, and zero rows. This proves
query/schema access, not event collection. This is the earlier checkpoint;
the post-repair repeat in section 16 subsequently returned actual events.

![Run the bound Queryset and inspect its actual result schema.](images/manual-deployment/67-queryset-executed-empty-results.png)

### Real-Time Dashboard

7. Open `pc_operations_dashboard`, not either Power BI report. Its bound
   definition must retain all eight repository tiles:
   **Jobs by status**, **Running Jobs**, **Not-started Jobs**,
   **Oldest not-started age**, **P95 terminal duration**, **Recent failures**,
   **Terminal job trend**, and **Intake jobs at or above 20 minutes**.
8. Verify its datasource is the same managed monitoring database and its
   parameters/queries are present. Exercise the time-range control and check
   for datasource or query errors.

The walkthrough dashboard is `e18782d7-33cd-446e-ae93-df3e5ab73731`.
Its full bound definition was read back successfully. A published dashboard
does not establish that its monitoring source is enabled.

To inspect the datasource in this UI, open **Viewing** > **Editing**, expand
**Data sources**, and click **Details** for `workspace_monitoring`. Verify
the manual workspace and service-returned connection URI. Return to Viewing
without changing the definition. The source was shown as used by **8 Tiles,
3 Parameters, 0 Base queries**, with **Pass-through identity** access.

![Choose Editing to inspect the published operations dashboard's bindings.](images/manual-deployment/65-operations-rti-dashboard.png)

![Verify the workspace-monitoring datasource details.](images/manual-deployment/66-dashboard-managed-datasource.png)

The captured empty dashboard showed zero running/not-started jobs, no result
rows, and `NaN` for empty duration/age gauges. Those are not measured healthy
operational values. Resolve collection and verify new events before accepting
the dashboard.

On the post-repair repeat, all eight tiles finished loading without
datasource/query error cards. **Jobs by status** showed two Completed jobs,
and **Terminal job trend** had two populated time buckets. Both failure and
intake-duration tables returned zero rows. The dashboard was returned to
**Viewing** without editing its definitions.

The repository's gauge presentation still uses percent-style axes, and
**Oldest not-started age** remained `NaN` for an empty set. Validate metric
units and empty-state presentation before operational acceptance; neither
those gauges nor these job events establish inference health.

![Restored dashboard with actual job telemetry; the time-range control is outlined.](images/manual-deployment/78-restored-dashboard-job-events.png)

### Operations Activator completion

9. Keep the dispatcher-failure rule from section 13 stopped.
10. In the template's **Publish both operations rules** cell, set
    `APPROVED_ACTIVATOR_ID` to the verified existing manual item ID. Run the
    cell to replace the earlier one-rule setup with the full repository
    operations definition. This intentionally changes internal source/rule
    IDs; the section 13 rule ID describes the earlier checkpoint.
11. The tested preparation helper binds this tenant and the manual Dispatcher,
    monitoring database, and dashboard. It **replaces the repository action
    rows with the existing portal-authored draft action**, sets both rules
    stopped, and pauses the KQL query source. Fabric rejects an empty action
    step with `InvalidTemplateInstance`; do not work around that with invented
    recipients. Retaining the portal's draft is not delivery approval. Verify the
    dispatcher-failure rule and `alert_pc_event_intake_duration_20m` in the
    Activator UI. The latter source's reviewed polling interval is 300 seconds
    and its query identifies `NotStarted`/`InProgress` intake jobs with elapsed
    time of at least 20 minutes. One dispatcher-failure rule is not equivalent
    to this two-rule definition.
12. Configure and approve recipients, channels, messages, and referenced fields
    only after separate approval. A structurally valid draft is not
    notification acceptance. Do not
    click Start or send a test action under the current no-notifications
    approval.
13. Before acceptance, demonstrate that a controlled job event reaches the
    correct source and that the stopped rule's condition sees the expected
    event/query row. Enabling external delivery remains a separate checkpoint.

The intake-duration query names `pc-event-intake` and `01_register_event`.
Neither item was present in the reference workspace comparison. Preserve the
reviewed definition, but do not claim this alert covers a working intake path
until those intended producers are separately established. A permanently
empty query is not a successful duration-alert test.

On the final browser pass, each rule had **Start** available and **Stop**
disabled, consistent with the stopped-definition readback. The intake rule's
condition displayed **Is greater than or equal to 20**. Both rule previews
showed **No data to show**, and preview requests returned errors; no working
live feed or action delivery is claimed. Do not start the rules or send test
actions to make those previews populate.

![Select the intake-duration rule and inspect its condition without starting it.](images/manual-deployment/74-operations-activator-two-rules.png)

## 20. Release configuration and operational handover

The publication notebook is an **administrative tool**, not an inference
release or an admission bypass. Keep the deployment out of service while
any gate below is unresolved.

### Record the new target references

1. Retain the preparation input hash and source revision alongside the
   actual workspace, Lakehouse, Environment, six job, variable-library,
   reporting, monitoring, and RTI IDs. Use the values read back from this
   workspace, not the reference workspace.
2. Retain the model-file hashes and the exact SDK wheel, dependency lock,
   and bundle hashes. A filename or package version alone is not release
   identity. Do not hash a different local wheel and describe it as the
   published Environment's wheel.
3. Create and retain deployment evidence under the intended Lakehouse
   `Files/deployment-evidence` and release artifacts under their reviewed
   release root. An inventory/evidence record must clearly distinguish
   **configured**, **verified**, **blocked**, and **not tested**. It is not
   required SDK runtime dependency.

### Manual SDK upgrade workflow

The SDK uses standard wheel metadata for version compatibility. It has no
custom embedded identity and requires no detached manifest, postpublish
receipt, or versioned release directory in OneLake.

#### Build the wheel

Use the [SDK release procedure](../README.md#github-sdk-releases). For example,
when preparing a new `1.0.0` release:

```bash
git fetch origin --tags
git switch -c releases/v1.0.0
bash scripts/bump-sdk-version.sh 1.0.0
```

Replace the example version with the intended unused release version. The
SDK build is independent of the target. Workspace, Lakehouse, Environment,
and job IDs belong in configuration, environment variables, or runtime context,
not the wheel. Do not publish different builds under the same package version.

#### Install in Fabric

1. Upload the wheel to the Fabric Environment's custom libraries and publish
   the Environment. Uploading the wheel to Lakehouse Files alone does not install it.
2. Attach the notebook and SJDs to Environments with the same SDK version.
   Start fresh sessions so they load that version.
3. Keep the existing target configuration, models, shortcuts, and processing
   state. No release-evidence upload or digest entry is needed.

Registration, claims, and workers derive a compatibility digest automatically
from the installed SDK version. This is not a hash of the wheel or proof of an
Environment publication. Model and source hashes remain independently verified.
Drain old registered work with the old SDK or explicitly quarantine/re-register
it before switching; do not rewrite immutable work digests or delete retry state.
See [SDK upgrades](explicit-backfill.md#sdk-upgrades) for migration details.

#### Optional API-assisted provisioning

The browser workflow above remains supported. If API-assisted deployment is
approved, keep the target configuration outside the SDK source. It needs the
actual workspace, Lakehouse, and Environment IDs and the desired role names;
leave all role `item_id` values unset before first upload.

```bash
uv run --frozen python scripts/deploy_sjd_operations.py \
  --stage dev --config config/deployment-input.json \
  --provision-only --output outputs/deployment.json
```

The script creates or uniquely discovers **all six** roles and records Fabric's
returned IDs. Existing schedules are disabled before definition updates.
Provisioning does not need a receipt and does not enable schedules. Reuse its
output as the next `--config`; a pinned ID cannot silently change to another
same-name item.

Reporting can use the checked repository bundles without a reference/source
workspace or copied semantic-model IDs:

```bash
uv run --frozen python scripts/deploy_sjd_analytics.py \
  --stage dev --config outputs/deployment.json --output outputs/analytics.json
uv run --frozen python scripts/deploy_sjd_operational_report.py \
  --stage dev --config outputs/analytics.json --output outputs/reporting.json
```

Each reporting output retains the resolved deployment manifest, so the next
phase can consume it directly. The workspace, Lakehouse, and report connection
are rendered for the selected target, and model/report IDs come from Fabric.
Existing unmanaged items still require explicit ownership approval through
their configured target IDs.

After publishing the SDK in the Environment, configure the
disabled schedules using that same resolved target:

```bash
uv run --frozen python scripts/deploy_sjd_operations.py \
  --stage dev --config outputs/reporting.json \
  --output outputs/configured-deployment.json
```

Only add `--enable-schedules` after acceptance. Every disabled schedule must
read back correctly before enablement begins. If activation fails, the script
attempts to disable all recurring schedules again and reports any disablement
it cannot confirm. Accepted jobs are not cancelled. No command here fabricates
review data, a publication receipt, or executor qualification.

**Migration:** older SDK wheels still read release-evidence files. Publish
the updated wheel and restart the attached sessions to remove that dependency.
Model/source integrity, target bindings, and executor qualification remain
required; this packaging change does not demonstrate live Fabric qualification.

### Configure recurring roles after target and compute checks

8. The script's Development schedule cadences are:

| Role | Cadence | Required initial state |
|---|---|---|
| Control | Every 5 minutes | Disabled |
| Dispatcher | Every 5 minutes | Disabled |
| Refresh | Every 15 minutes | Disabled |
| Reconciliation | Every 15 minutes | Disabled |
| Gold | Every 15 minutes | Disabled |
| Process | On demand | No recurring schedule |

The Dispatcher schedule is the normal trigger for processing. The explicit
backfill notebook invokes Control to register work as `READY`, then monitors
it; neither registration nor the notebook's wait loop launches Dispatcher.
Each Dispatcher run claims a bounded batch and processes it. Variable-library
job references and the Dispatcher-failure Activator rule do not trigger these
runs. Without an enabled schedule or an approved manual Dispatcher run, work
remains `READY`.

9. Open each recurring SJD's **Schedule** settings. Use UTC, the reviewed
   cadence and date range, and keep it disabled. Reopen/read back exactly
   one schedule per recurring role and none for Process. An absent
   schedule is not a configured disabled schedule.
   In the captured UI, open the job's toolbar **Setting** button, select
   **Schedule**, and expand **Show more**. For a new schedule, use
   **Add schedule**, enter the reviewed cadence/date range and UTC, and
   explicitly leave it inactive. If the editor requires initial activation,
   cancel rather than briefly enabling an unadmitted job. Reopen and verify
   the card reads **Inactive**; **Turn On** must remain unclicked. Leave
   failure-notification recipients empty under the no-notifications approval.

![Refresh schedule read back as Every 15 minutes, Inactive, UTC.](images/manual-deployment/75-refresh-schedule-inactive.png)

10. Set and verify the role-specific execution arguments:
    - Control: manual-artifact prefix followed by `reconcile`, **not**
      the initial `bootstrap` command.
    - Reconciliation: its entry point performs `reconcile`; retain the
      manual-artifact prefix.
    - Gold: manual-artifact prefix followed by its required mode, `run`.
    - Refresh: the workspace and **both new model IDs**, as shown above.
    - Dispatcher: the `--artifacts <workspace-id> <lakehouse-id>
      <environment-id>` prefix (`--manual-artifacts` remains an alias), plus
      `--files-root` when the deployment uses a non-default state root.
      No release-manifest/receipt paths or hashes are required. Optional
      bounds are `--max-items 64 --maximum-active-batches 1
      --lease-seconds 14400 --minimum-speed-x 1 --safety-factor 1.25
      --margin-seconds 60`; these are also the current defaults.
      Verify the scheduled execution uses the same target, state root,
      published SDK Environment, and arguments as a direct job run.
11. Start a fresh qualified session and verify two observed executors and
    the complete runtime/package/model contract before any processing.
    A saved Environment request is not allocation evidence. The separately
    approved F4 change must be performed and its actual limits checked
    before using it as inference evidence; it has not been applied here.
12. Recheck all bindings and readbacks on a second pass. This manual path
    cannot claim a Terraform drift result. Keep schedules and external
    actions disabled until an explicitly accepted operational handover.
    Stop the administrative notebook's Spark session when finished.
13. After runtime, model, target, and operational acceptance, return to each
    recurring role's **Schedule** settings and turn on its reviewed schedule.
    For Dispatcher, verify **Active**, every 5 minutes, UTC, and a date range
    that includes now and the intended operating period. Reopen the settings
    to confirm they persisted. Do not leave an unintended one-day expiry.
    Verify a Dispatcher job instance appears at the next scheduled time and
    monitor the registered work progressing from `READY` to committed
    processing. A schedule being active does not prove a run succeeded.
    An explicitly approved manual Dispatcher run can process a batch once;
    it does not replace the recurring schedule. Do not resubmit the backfill
    merely because its work is waiting for Dispatcher.

### Captured configuration checkpoint (historical)

The historical walkthrough captured **30 items**. It predates removal of the
SDK's detached release-evidence requirement and is not the required final
configuration for a current deployment. Preserve the captured results as
evidence; use the current scheduling instructions above for new deployments.
Compared with the script's
26-item reference inventory, the four extras are the three retained classic
monitoring artifacts and the administrative notebook. The managed monitoring
supporting items are service-generated, not extra application Eventstreams.
Neither an intake pipeline nor a registration notebook was present in that
reference inventory; their absence must not be hidden by claiming a working
intake-duration alert.

| Surface | Verified checkpoint |
|---|---|
| Reporting | Exact repository definitions; both ViaApi refreshes completed; six analytics and three operations pages opened |
| Variable library | Eleven typed references; Default active; no alternative value sets |
| Managed monitoring | Enabled / Diagnostic data collection; classic collection paused and history retained |
| Operations RTI | Eventhouse/database, bound Queryset, eight-tile dashboard, and two stopped operations rules; KQL source paused |
| Control schedule | Every 5 minutes, disabled; ID `b4aea835-ec51-452b-98f2-914de1d2b2e0` |
| Reconciliation schedule | Every 15 minutes, disabled; ID `178b6bdf-723d-4b1d-9ebe-d728cf73134a` |
| Gold schedule | Every 15 minutes, disabled; ID `1b377c38-88a0-4c96-be45-61adfee60bf0` |
| Refresh schedule | Every 15 minutes, disabled, both current model IDs; ID `7262ab4d-4a78-486c-ac49-cbec48a25915` |
| Dispatcher schedule | Historically absent under the former release-evidence gate; current deployments must create it disabled and enable it after acceptance |
| Process schedule | Absent, on demand; the historical capture also included the retired role |
| SDK admission / inference | Historically blocked / not accepted; this capture does not establish current inference or operational acceptance |

The four configured schedules and their arguments were read back, including
the retained Lakehouse/Environment bindings. That historical omission is not
guidance to leave Dispatcher unscheduled. Current Dispatcher configuration
requires no detached manifest or publication receipt; retain the target,
compute, SDK-version, and model checks before activation.

A non-overwriting, content-addressed configuration snapshot was written to
the manual Lakehouse and read back byte-for-byte:

```text
Files/deployment-evidence/configuration-3dde60c1cf9a7284d516d518e78be42296f39019c6309828346272d572b512f4.json
SHA256: 3dde60c1cf9a7284d516d518e78be42296f39019c6309828346272d572b512f4
Schema: people-counter-manual-configuration-evidence-v1
Status: configuration-verified-admission-blocked
```

To inspect it through the UI, open `pc_dev_lakehouse` > **Files** >
**deployment-evidence**, locate the exact content-addressed filename, and
open/download it. Use the finished administrative notebook's read-only cell
to verify its hash. This is a captured configuration record, not live drift
validation or proof of current runtime qualification. Do not use its hash as
an SDK compatibility digest. Current Dispatcher jobs derive SDK compatibility
from installed package metadata and need no release-manifest/receipt arguments.

![Run the read-only hash/schema check and inspect the captured admission hold.](images/manual-deployment/76-configuration-evidence-audit.png)

The audit check was executed successfully against the saved OneLake file:
30 captured resources, `configuration-verified-admission-blocked`, and SDK
admission `blocked` under the former evidence gate. These retained historical
labels do not impose that gate on the current SDK. The administrative Spark session was then stopped;
the browser confirmed **Session stopped successfully**. This does not pause
the F2 capacity.

Remaining acceptance work is explicit: target-bound SDK release evidence,
Fabric x86_64 runtime and two-executor qualification, post-upload model
hash/loading checks, representative inference/Gold data, controlled
role-specific alert-event verification, the intended intake producers, and separately approved notification
delivery. F2 remains provisioned; F4 has not been applied.

## Historical bootstrap outcome and F2 checkpoint

| Check | Captured result |
|---|---|
| Workspace capacity | F2, Canada East, correct retained capacity ID |
| Workspace identity | Active; ADLS Blob Data Contributor granted to its principal ID |
| ADLS shortcut | `Files/footage` created; directory listing succeeded |
| Monitoring | Monitoring Eventhouse/database created; logging On |
| Environment | Updated, distinctly named wheel published successfully |
| Job definitions | Seven canonical roles created with their own `main.py` files |
| Runtime binding | Historical capture saved explicit arguments on seven jobs; the current workflow requires six |
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
- For this Fabric-only headless profile, `opencv-python-headless` is the only
  installed OpenCV distribution. SDK/backend dependency metadata must agree,
  and the selected inference backend must pass executor-side validation.
  The current stock LibreYOLO/Trackers-based options have not passed this gate.
- Control `bootstrap` succeeds and the expected `people_counter_sjd_*` Delta
  tables exist. The script runs this command through the Control job; it does
  not run the inference pipeline as a substitute.
- All six roles have the correct source file, Environment, and default
  Lakehouse in this workspace.
- Deployment references and release provenance contain the **new** workspace,
  Lakehouse, Environment, and job IDs, not copied IDs from another environment.
- Notebook and job Environments use the same SDK version. See
  [canonical job SDK compatibility](../fabric/spark-job-definitions/README.md#sdk-compatibility).
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

## Publication-helper validation

The offline preparer and publication helpers were tested separately from
the earlier runtime-binding changes. The final focused command was:

```bash
uv run --frozen --extra cpu python -m pytest \
  tests/test_manual_fabric_publication.py \
  tests/test_fabric_rest.py \
  tests/test_fabric_definition_bundle.py \
  -q --crap --cov \
  --cov-config=/home/vinima/.copilot/session-state/4a4e384b-db2d-4212-9be7-534fcca49c39/files/manual-publication-coverage.ini \
  --cov-report=term:skip-covered
```

That session-local coverage file limits measurement to
`*/manual_fabric_publication.py` and `*/prepare_manual_fabric_assets.py`;
use an equivalent focused configuration on another machine.

- **82 tests passed.** The publication module has 100% measured line
  coverage; the preparer has 74% (its CLI wrapper is not targeted).
- **CRAP threshold 30:** highest `ensure_item` 26.34, complexity 25,
  function coverage 87.1%; `stopped_activator_definition` 15.02,
  complexity 15, function coverage 95.2%. No measured function exceeds
  the threshold. No additional CRAP-driven refactor is required here.
- **Mutation:** the final command below completed with **460 killed,
  47 survived** among 507 selected terminal mutants, with no selected
  timeout, suspicious, or no-test outcomes. Another 27 CLI-wrapper mutants
  were not selected. This is not a perfect mutation score.

```bash
PYTEST_ADDOPTS='-k manual_fabric_publication' \
  uv run --frozen --extra cpu mutmut run \
  'people_counter.manual_fabric_publication.*' \
  'scripts.prepare_manual_fabric_assets.*prepare_assets*' \
  --max-children 2
```

Inspection of surviving mutations strengthened the tenant rejection test
to use an otherwise valid definition, and added explicit Dispatcher-container
rename assertions. Representative remaining Activator survivors include
`x_stopped_activator_definition__mutmut_3` (message-only wording),
`__mutmut_40` (redundant inner Base64 validation after rendering already
validates it), `__mutmut_105` (missing-settings fallback not exercised by
the repository bundle), `__mutmut_134` (shallow versus deep action copy
before immediate serialization), and `__mutmut_140` / `__mutmut_153`
(JSON whitespace). Remaining diagnostic, encoding, and preparer-serialization
survivors are advisory; the suite is not evidence that every malformed bundle
is covered.

Python source and every supplied notebook code cell passed syntax/AST
validation; the reusable notebook has no stored outputs or execution counts.
Embedded screenshots resolve and `git diff --check` passes. Pylance syntax
requests timed out, so no fresh Pylance/type-analysis pass is claimed.
These checks do not replace the live release/inference gates above.

## Earlier runtime-binding validation

This historical record describes the earlier manual-binding change, which was
developed test-first and exercised in a browser-run bootstrap. That version
still had fixed defaults. The portable SDK described above removes those
defaults; this older live result does not qualify the new build.

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

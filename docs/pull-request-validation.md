# Pull request validation

`pr-validate.yml` calls the credential-free validation workflow for every pull
request to `main`. The workflow uses read-only repository access. Forks can run
it safely.

- Ruff checks changed Python files.
- Pyright checks changed production and utility files using the locked uv
  environment's interpreter, including its installed optional dependencies.
- Pytest runs the non-Compose suite with branch coverage and CRAP results.
- Jest tests portal behavior with accessible queries, user interactions, semantic
  state assertions, and Istanbul coverage. `crap-score` generates
  per-function CRAP results; every function must score strictly below 10.
  StrykerJS mutates all portal JavaScript with its default operators and requires
  at least a 96% mutation score (strictly above 95%). JavaScript coverage and
  JSON/HTML CRAP and mutation reports are uploaded as evidence.
- Python validation installs the `demo` extra for the real DuckDB/Delta tests.
  Test and mutation jobs use Temurin Java 21 for local Spark/Delta tests rather
  than the runner's default Java runtime.
- Mutmut runs a scoped mutation test when tests change.
- Terraform format, backend-free initialization, validation, and tests for
  bootstrap, the reusable module, and all environment roots.
- deterministic Fabric definition hash checks plus JSON, PBIP, PBIR, TMDL, and
  definition-export manifest validation.
- actionlint, ShellCheck, immutable action-pin checks, and a zizmor audit of
  every workflow, including deployment and drift workflows.
- shell regression tests for SDK version preparation and rollback, release
  source ancestry, version validation, tag conflicts, draft recovery, and
  idempotent publication.

The separate [CodeQL Advanced workflow](../.github/workflows/codeql.yml) analyzes
GitHub Actions, portal JavaScript, and Python on pushes and pull requests to
`main`, plus Saturdays at 11:37 UTC. All three languages use build mode `none`;
no SDK dependencies, model downloads, or Fabric credentials are required.
Actions are pinned to immutable commits, checkout credentials do not persist,
and results appear in GitHub code scanning. If CodeQL default setup is enabled,
disable it in the repository's code scanning settings before using this advanced
workflow.

`package.json` overrides vulnerable transitive dependencies whose parent
packages still require older versions. The `deepmerge` override uses the
API-compatible `@shivammathur/deepmerge` fork because upstream has no patched
release for GHSA-gcqx-w48m-w2h4. Regression tests cover prototype poisoning and
merge compatibility. Remove an override only after the parent resolves a safe
version; validate with `npm audit`, `npm run test:portal`, and
`npm run test:portal:mutation`.

Commit tutorial notebooks with cleared outputs and execution counts. The
backfill notebook tests reject saved execution state; clear it before pushing:

```bash
uv run --no-sync jupyter nbconvert --clear-output --inplace \
  notebooks/explicit_backfill_operator_tutorial.ipynb
```

Pull requests from `releases/` branches also run **Release version and unused
tag**. The branch must be named `releases/vMAJOR.MINOR.PATCH`, its version must
exactly match `project.version` in `pyproject.toml`, and the corresponding tag
must not exist in this repository. A mismatch fails CI; maintainers run
`bash scripts/bump-sdk-version.sh MAJOR.MINOR.PATCH` to synchronize the project
version and `uv.lock`. Alternatively, pass exactly one of `--major`, `--minor`,
or `--patch` to increment the current version. An explicit version takes
precedence over a bump flag, regardless of argument order. Rerun with the same version
after source changes to refresh the digest. The check rejects an out-of-date
or missing lockfile and stale embedded Fabric release identity version,
evidence path, or source/build-input digest. It uses only uv and the Python
standard library, so it runs before installing SDK dependencies. Other branches
skip this check. See the SDK release instructions for refreshing these files.
Require this check in the `main` ruleset. On merge, the release workflow
revalidates all release files before creating the tag, and publishes both SDK bundles
and the model archive. See [SDK releases](../README.md#github-sdk-releases).

The release workflow first checks out `main` with full history. Before checking
out or executing the selected release revision, it requires that revision to be
an ancestor of `origin/main`. Manual dispatch accepts historical release tags
from `main`, but rejects tags pointing to unmerged commits. Release jobs keep
dependency caching disabled.

Each job uploads machine-readable or text evidence for 14 days. Mutmut requires
a green changed-test baseline and complete results for every test-associated
mutant. The workflow reports surviving mutants as advisory evidence. It also
reports functions with no associated tests. Unchecked mutants, suspicious results, timeouts,
segfaults, execution failures, or a selection with no active test-associated
mutants block the pull request. The workflow retries timed-out mutants once.
It uses one worker for each retry. The timeout gate then checks the result.

`.github/scripts/validate-python-quality.sh` excludes one Fabric reconciler
type issue. The issue affects mutable mapping annotations. All other changed
production and utility Python files must pass the type check.

## Environment-authorized Terraform plans

Fork pull requests never receive OIDC credentials. A maintainer can manually
run **Authorized PR Terraform plan** with a pull request number and target
environment. The workflow rejects forks and non-`main` pull requests, resolves
the current head to a full commit SHA, and then waits for the selected protected
GitHub Environment approval before using OIDC.

The authorized workflow only creates a plan. It cannot apply. It uploads the binary
plan, full JSON plan, and a reduced action/address evidence file for 14 days.
Reviewers should treat plan artifacts as environment-sensitive and grant
GitHub Environment access accordingly.

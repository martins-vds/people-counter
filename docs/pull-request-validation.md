# Pull request validation

`pr-validate.yml` calls the credential-free validation workflow for every pull
request to `main`. The workflow uses read-only repository access. Forks can run
it safely.

- Ruff checks changed Python files.
- Pyright checks changed production and utility files.
- Pytest runs the non-Compose suite with branch coverage and CRAP results.
- Mutmut runs a scoped mutation test when tests change.
- Terraform format, backend-free initialization, validation, and tests for
  bootstrap, the reusable module, and all environment roots.
- deterministic Fabric definition hash checks plus JSON, PBIP, PBIR, TMDL, and
  definition-export manifest validation.
- actionlint, ShellCheck, immutable action-pin checks, and a zizmor audit of
  every workflow, including deployment and drift workflows.
- shell regression tests for release version validation, tag conflicts,
  draft recovery, and idempotent publication.

Pull requests from `releases/` branches also run **Release version and unused
tag**. The branch must be named `releases/vMAJOR.MINOR.PATCH`, its version must
exactly match `project.version` in `pyproject.toml`, and the corresponding tag
must not exist in this repository. A mismatch fails CI; maintainers must update
the project version and `uv.lock` themselves. Other branches skip this check.
Require this check in the `main` ruleset. On merge, the release workflow
revalidates the merged source, creates the tag, and publishes both SDK bundles
and the model archive. See [SDK releases](../README.md#github-sdk-releases).

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

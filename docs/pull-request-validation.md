# Pull request validation

`pr-validate.yml` calls the credential-free validation workflow for every pull
request to `main`. It has read-only repository access and is safe for forks. It
runs:

- Ruff on changed Python files and Pyright on changed production/utility files;
- the non-Compose Python suite with branch coverage and pytest-crap reporting;
- scoped mutmut runs whenever tests change;
- Terraform format, backend-free initialization, validation, and tests for
  bootstrap, the reusable module, and all environment roots;
- deterministic Fabric definition hash checks plus JSON, PBIP, PBIR, TMDL, and
  migration-manifest validation; and
- actionlint, ShellCheck, immutable action-pin checks, and a zizmor audit of
  every workflow, including deployment and drift workflows.

Each job uploads machine-readable or text evidence for 14 days. Mutmut requires
a green changed-test baseline and complete results for every test-associated
mutant. Surviving mutants and functions with no associated tests are reported
as advisory evidence. Unchecked mutants, suspicious results, timeouts,
segfaults, execution failures, or a selection with no active test-associated
mutants block the pull request. Mutants that time out under the initial
four-worker run are retried once serially before the timeout gate is evaluated.

One pre-existing type issue is isolated in
`.github/scripts/validate-python-quality.sh`: the Fabric reconciler's mutable
mapping annotations. All other changed production and utility Python files are
type-gated. Remove the entry from that baseline as soon as its annotations are
corrected.

## Environment-authorized Terraform plans

Fork pull requests never receive OIDC credentials. A maintainer can manually
run **Authorized PR Terraform plan** with a pull request number and target
environment. The workflow rejects forks and non-`main` pull requests, resolves
the current head to a full commit SHA, and then waits for the selected protected
GitHub Environment approval before using OIDC.

The authorized workflow only plans; it cannot apply. It uploads the binary
plan, full JSON plan, and a reduced action/address evidence file for 14 days.
Reviewers should treat plan artifacts as environment-sensitive and grant
GitHub Environment access accordingly.

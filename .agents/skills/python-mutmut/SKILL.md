---
name: python-mutmut
description: "Run Python mutation testing with mutmut whenever tests are added, modified, or deleted. Use this skill for any change to Python test files, test cases, fixtures, parametrizations, snapshots, or test helpers, even when the user only asks to update tests and does not mention mutation testing. Also use it when asked about surviving mutants, mutation score, test effectiveness, or whether tests detect behavioral regressions."
compatibility: Requires Python 3.10+, pytest, mutmut, and an operating system with fork support (POSIX or Windows through WSL). Requires shell access to the project's existing Python package manager and test workflow.
---

# Python mutation testing with mutmut

Use mutation testing to verify that changed tests detect behavioral changes rather than merely execute code. Mutmut changes production code one mutation at a time and expects an effective test suite to fail for each meaningful mutation.

## Trigger

Apply this workflow whenever Python tests are:

- added;
- modified;
- deleted;
- indirectly changed through fixtures, test helpers, parametrizations, or snapshots.

Run it even when the user does not explicitly request mutation testing. Do not trigger solely because production Python code changed unless tests also changed or the user asks for mutation analysis.

## Workflow

1. Inspect the project before choosing commands:
   - Reuse its package manager, virtual environment, pytest configuration, and documented test commands.
   - Inspect the test diff, including deleted content, to identify affected behavior.
   - Preserve existing mutmut configuration in `pyproject.toml` or `setup.cfg`.
   - Mutmut 3 requires Python 3.10+ and `fork`; on native Windows, explain that WSL is required rather than attempting an unsupported run.

2. Ensure mutmut is a development/test dependency:
   - If missing, add `mutmut` through the established package manager and development dependency group.
   - Update the corresponding lockfile through the package manager.
   - Do not install globally or create a second dependency workflow.
   - Surface Python-version, platform, `libcst`, Rust-toolchain, or package-resolution failures explicitly.

3. Establish a green baseline:
   - Run the smallest normal test scope covering the changed tests before mutation testing.
   - For a deleted test, run the remaining relevant tests.
   - If baseline tests fail, stop before mutation testing. A mutation result is not trustworthy when the unmutated suite is already failing.
   - Treat collection errors, command failures, and absence of active tests as blocking.

4. Select mutation scope:
   - Trace imports, fixtures, and exercised behavior from the changed tests to production modules or functions.
   - For deleted tests, inspect the pre-deletion content from the available diff or version-control history.
   - Prefer the narrowest meaningful target using quoted mutmut globs:

     ```text
     <project mutmut command> run "package.module.function*"
     ```

   - Pass multiple mutant globs when a test change covers multiple production behaviors.
   - If the affected production target cannot be identified reliably, run the repository's configured mutmut scope rather than guessing.
   - Respect `source_paths`, `only_mutate`, `do_not_mutate`, test-selection arguments, and other existing configuration.
   - If mutmut cannot infer source or test paths and no configuration exists, add only the minimal project-appropriate configuration. In `pyproject.toml`, path options are arrays:

     ```toml
     [tool.mutmut]
     source_paths = ["src/"]
     pytest_add_cli_args_test_selection = ["tests/"]
     ```

5. Run mutation tests and collect non-interactive evidence:

   ```text
   <project mutmut command> run [quoted mutant globs]
   <project mutmut command> results
   ```

   - Use `mutmut show <mutant-name>` for each surviving, suspicious, or timed-out mutant needed to explain a finding.
   - Do not use the interactive `mutmut browse` command in automated or headless validation.
   - Reuse mutmut's incremental cache. Do not delete mutation state merely to force a fresh-looking run.
   - If a run is interrupted or exceeds an external execution limit, report it as incomplete; mutmut can resume later.

6. Interpret outcomes:
   - **Killed mutants** show that a test detected the mutation.
   - **Surviving mutants** expose behavior the tests did not distinguish. Treat them as advisory test-quality findings, not test failures.
   - **Suspicious or timed-out mutants** need investigation and must not be counted as killed.
   - **No tests associated with mutants**, stats-collection failures, invalid configuration, or mutmut execution failures block successful mutation validation.
   - Do not weaken production behavior merely to kill mutants. Add focused assertions for meaningful survivors; identify equivalent mutants when no observable behavior differs.

7. Leave source code safe:
   - Do not run `mutmut apply` unless the user explicitly asks to inspect a mutant on disk.
   - Before applying a mutant, require the affected source to be under version control and preserve unrelated worktree changes.
   - Never leave an applied mutant in production code after analysis.

## Report format

Keep the final report concise:

- **Baseline:** exact test command and pass/fail result.
- **Mutation command:** exact `mutmut run` command and selected production scope.
- **Results:** verified counts or statuses for killed, survived, suspicious, timed out, and skipped mutants when available.
- **Survivors:** mutant names and behavioral changes from `mutmut show`, prioritized by relevance to the changed tests.
- **Action:** focused assertions to add, equivalent mutants to document, or confirmation that no mutation-driven action is needed.

Do not claim mutation testing passed when the run was incomplete, mutmut failed, no active tests were associated, or results were not inspected. Do not invent a mutation score; if calculating one, state the exact denominator and use only verified terminal statuses.

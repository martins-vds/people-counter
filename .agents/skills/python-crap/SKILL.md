---
name: python-crap
description: "Run Change Risk Anti-Patterns (CRAP) analysis with pytest-crap for Python work. Use this skill whenever Python production code is written or changed, whenever Python tests are run, or whenever the user asks about complexity, coverage, risky functions, testing priorities, or CRAP scores. Trigger even if the user does not explicitly request CRAP analysis: Python edits and pytest execution should include it as part of validation."
compatibility: Requires Python 3.10+, pytest 7+, pytest-cov, and pytest-crap. Requires shell access to run the project's existing Python package manager and pytest workflow.
---

# Python CRAP analysis

Use `pytest-crap` to expose functions that combine high cyclomatic complexity with weak test coverage. Treat the report as a prioritization aid, not as a replacement for test results or engineering judgment.

## Required behavior

Apply this workflow whenever you:

- write or modify Python production code;
- add, modify, or execute Python tests;
- investigate complexity, coverage gaps, risky functions, or testing priorities.

If tests were already run without CRAP reporting during the current task, rerun the relevant pytest scope with `--crap` before completing.

## Workflow

1. Inspect the repository's Python tooling before choosing a command:
   - Reuse the existing package manager, environment, test runner configuration, and documented commands.
   - Preserve existing pytest arguments, markers, environment setup, and coverage options.
   - Do not replace a non-pytest test workflow. If pytest is not used or cannot execute the relevant tests, explain that CRAP analysis is unavailable for that scope.

2. Ensure the required development dependencies are present:
   - Required packages are `pytest-crap` and `pytest-cov`.
   - If either is missing, add it as a development/test dependency using the repository's established package manager and dependency grouping.
   - Update the corresponding lockfile through the package manager.
   - Do not install globally or invent a second package-management workflow.
   - `pytest-crap` requires Python 3.10+ and pytest 7+; surface an incompatible project version instead of forcing an unsafe upgrade.

3. Select the smallest meaningful test scope:
   - After a Python code change, run the tests directly relevant to the changed behavior.
   - When the user requests a specific test command or scope, retain that scope.
   - Expand to broader tests only when repository conventions require it, targeted coverage cannot measure the changed code, or targeted results reveal a need for wider validation.

4. Run pytest with CRAP reporting:

   ```text
   <project pytest command> <existing arguments> --crap
   ```

   Preserve explicit CRAP options already configured by the project. Otherwise use plugin defaults:

   - `--crap-threshold=30`
   - `--crap-top-n=20`

   Add `--cov-branch` only when branch coverage is already part of project practice or is needed for the task; do not silently change established coverage semantics.

5. Interpret the result correctly:
   - Test failures, collection errors, and command failures block successful completion.
   - A high CRAP score is advisory and does not by itself fail the task.
   - Prioritize functions at or above the configured threshold.
   - Use the function, file, and folder tables to distinguish isolated risky functions from concentrated problem areas.
   - Recommend focused tests when low coverage is the main driver, simplification when cyclomatic complexity is the main driver, or both when both are high.

6. Report the validation result in the final response.

## Report format

Keep the report concise and include:

- **Command:** the exact CRAP-enabled pytest command that ran.
- **Tests:** pass/fail counts or the blocking error.
- **CRAP:** the threshold and the highest-risk functions, including score, complexity, and coverage when available.
- **Action:** whether no CRAP-driven action is needed or which tests/refactors should be prioritized.

Do not claim CRAP validation succeeded when the plugin did not run, coverage data was unavailable, or pytest failed. When the console output is truncated, report only values you can verify and point to the saved test output if available.

## CRAP reference

For a function `m`:

```text
CRAP(m) = CC(m)^2 * (1 - coverage(m))^3 + CC(m)
```

General interpretation:

| Score | Interpretation |
|---:|---|
| below 5 | Excellent |
| 5 to below 15 | Acceptable |
| 15 to below 30 | Warning |
| 30 or above | Critical/high risk |

The configured repository threshold takes precedence over these general bands.

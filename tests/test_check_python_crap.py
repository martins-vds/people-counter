import json
import textwrap

import coverage
import pytest

from scripts.check_python_crap import build_report, main


def measure(tmp_path, source, exercise):
    path = tmp_path / "example.py"
    path.write_text(textwrap.dedent(source).lstrip())
    collector = coverage.Coverage(data_file=str(tmp_path / ".coverage"), include=[str(path)])
    namespace = {}
    collector.start()
    try:
        exec(compile(path.read_text(), str(path), "exec"), namespace)
        exercise(namespace)
    finally:
        collector.stop()
    report_path = tmp_path / "coverage.json"
    collector.json_report(outfile=str(report_path))
    return report_path, json.loads(report_path.read_text())


def test_crap_uses_executable_statements_not_docstrings_or_multiline_sql(tmp_path):
    _, document = measure(tmp_path, '''
        def choose(enabled):
            """Choose the query.

            Documentation is not an uncovered branch.
            """
            query = (
                "SELECT "
                "1"
            )
            if enabled:
                return query
            return "disabled"
    ''', lambda namespace: namespace["choose"](True))
    report = build_report(document, tmp_path)
    row, = report["functions"]
    assert row == {
        "file": next(iter(document["files"])),
        "function": "choose",
        "line": 1,
        "statements": 4,
        "covered_statements": 3,
        "coverage_percent": 75,
        "complexity": 2,
        "crap": pytest.approx(2.0625),
    }
    assert set(report) == {"threshold", "maximum_crap", "functions", "violations"}


def test_report_includes_method_complexity_and_separates_uncalled_closures(tmp_path):
    _, document = measure(tmp_path, '''
        class Worker:
            def choose(self, enabled):
                if enabled:
                    return 1
                return 0

        def outer():
            def inner(enabled):
                if enabled:
                    return 1
                return 0
            return 3
    ''', lambda namespace: (namespace["Worker"]().choose(True), namespace["outer"]()))
    functions = build_report(document, tmp_path)["functions"]
    rows = {row["function"]: row for row in functions}
    assert len(functions) == len(rows) == 3
    assert rows["Worker.choose"]["complexity"] == 2
    assert rows["Worker.choose"]["covered_statements"] == 2
    assert rows["Worker.choose"]["statements"] == 3
    assert rows["outer"]["coverage_percent"] == 100
    assert rows["outer"]["statements"] == rows["outer"]["covered_statements"] == 2
    assert rows["outer.inner"]["coverage_percent"] == 0
    assert rows["outer.inner"]["crap"] == 6


def test_report_honors_existing_coverage_exclusions(tmp_path):
    _, document = measure(tmp_path, '''
        def choose(enabled):
            if enabled:  # pragma: no cover
                return "excluded"
            return "covered"
    ''', lambda namespace: namespace["choose"](False))
    row, = build_report(document, tmp_path)["functions"]
    assert row["statements"] == row["covered_statements"] == 1
    assert row["coverage_percent"] == 100


def test_cli_emits_evidence_and_fails_at_the_exact_threshold(tmp_path, capsys):
    path, _ = measure(tmp_path, '''
        def risk(value):
            if value == 1:
                return 1
            if value == 2:
                return 2
            if value == 3:
                return 3
            if value == 4:
                return 4
            return 0

        def documentation():
            """No executable body."""

        def exercised(value):
            return value
    ''', lambda namespace: namespace["exercised"](1))
    output = tmp_path / "crap.json"
    assert main([str(path), "--output", str(output)]) == 1
    report = json.loads(output.read_text())
    assert report["maximum_crap"] == 30
    assert report["violations"][0]["function"] == "risk"
    assert [row["function"] for row in report["functions"]] == ["risk", "exercised"]
    assert "risk" in capsys.readouterr().err


def test_cli_passes_fully_exercised_functions(tmp_path):
    path, _ = measure(tmp_path, '''
        def identity(value):
            return value
    ''', lambda namespace: namespace["identity"](1))
    output = tmp_path / "crap.json"
    assert main([str(path), "--output", str(output)]) == 0
    assert json.loads(output.read_text())["violations"] == []


def test_cli_default_output_matches_ci_evidence_path(tmp_path, monkeypatch):
    path, _ = measure(tmp_path, '''
        def identity(value):
            return value
    ''', lambda namespace: namespace["identity"](1))
    monkeypatch.chdir(tmp_path)
    assert main([str(path)]) == 0
    report = json.loads((tmp_path / "evidence/python-tests/crap-report.json").read_text())
    assert report["maximum_crap"] == 1


@pytest.mark.parametrize("content", ["{", "{}", "[]", '{"files": {}}', '{"files": ["not-a-mapping"]}'])
def test_cli_rejects_missing_or_invalid_coverage_data(tmp_path, capsys, content):
    path = tmp_path / "coverage.json"
    path.write_text(content)
    output = tmp_path / "crap.json"
    assert main([str(path), "--output", str(output)]) == 2
    error = capsys.readouterr().err
    if content == "{":
        assert error.startswith("error:")
    elif content == "[]":
        assert error == "error: Coverage report must be an object\n"
    else:
        assert error == "error: Coverage report contains no source files\n"
    assert not output.exists()


def test_report_includes_executable_single_line_functions(tmp_path):
    _, document = measure(tmp_path, '''
        class Worker:
            def choose(self): return 7
    ''', lambda namespace: namespace["Worker"]().choose())
    row, = build_report(document, tmp_path)["functions"]
    assert row["function"] == "Worker.choose"
    assert row["statements"] == row["covered_statements"] == 1
    assert row["coverage_percent"] == 100


@pytest.mark.parametrize("source", [
    "VALUE = 1",
    'def documented():\n    """Documentation only."""',
    'def documented(): """Documentation only."""',
])
def test_cli_rejects_reports_without_executable_functions(tmp_path, capsys, source):
    path, _ = measure(tmp_path, source, lambda namespace: None)
    assert main([str(path), "--output", str(tmp_path / "crap.json")]) == 2
    assert capsys.readouterr().err == "error: Coverage report contains no executable functions\n"

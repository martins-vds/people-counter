"""Report CRAP using executable statements from coverage.py JSON evidence."""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any

from radon.complexity import add_inner_blocks, cc_visit
from radon.visitors import Function


THRESHOLD = 30


def build_report(document: dict[str, Any], root: Path) -> dict[str, Any]:
    files = document.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("Coverage report contains no source files")
    rows = []
    for filename, evidence in files.items():
        path = root / filename
        source = path.read_text(encoding="utf-8")
        blocks = cc_visit(source)
        definitions = {
            node.lineno: node for node in ast.walk(ast.parse(source))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        functions = [
            block for block in add_inner_blocks(blocks)
            if isinstance(block, Function)
        ]
        covered = set(evidence["executed_lines"])
        statements = (covered | set(evidence["missing_lines"])) - set(evidence["excluded_lines"])
        for block in functions:
            body = definitions[block.lineno].body
            first = body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                body = body[1:]
            if not body:
                continue
            span = {line for line in statements if body[0].lineno <= line <= block.endline}
            for child in functions:
                if block.lineno < child.lineno <= child.endline <= block.endline:
                    span.difference_update(range(child.lineno + 1, child.endline + 1))
            if not span:
                continue
            hits = len(span & covered)
            fraction = hits / len(span)
            score = block.complexity**2 * (1 - fraction)**3 + block.complexity
            rows.append({
                "file": filename,
                "function": f"{block.classname}.{block.name}" if block.is_method else block.name,
                "line": block.lineno,
                "complexity": block.complexity,
                "statements": len(span),
                "covered_statements": hits,
                "coverage_percent": 100 * fraction,
                "crap": score,
            })
    if not rows:
        raise ValueError("Coverage report contains no executable functions")
    rows.sort(key=lambda row: (-row["crap"], row["file"], row["line"]))
    return {
        "threshold": THRESHOLD,
        "maximum_crap": rows[0]["crap"],
        "functions": rows,
        "violations": [row for row in rows if row["crap"] >= THRESHOLD],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage", type=Path)
    parser.add_argument("--output", type=Path, default=Path("evidence/python-tests/crap-report.json"))
    arguments = parser.parse_args(argv)
    try:
        document = json.loads(arguments.coverage.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise ValueError("Coverage report must be an object")
        report = build_report(document, Path.cwd())
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(
            json.dumps(report, allow_nan=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, ValueError, KeyError, TypeError, SyntaxError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    for row in report["violations"]:
        print(
            f"{row['file']}:{row['line']}: {row['function']}: CRAP {row['crap']:.2f} must be < {THRESHOLD}",
            file=sys.stderr,
        )
    print(f"Python executable-line maximum CRAP: {report['maximum_crap']:.2f} (< {THRESHOLD} required)")
    return 1 if report["violations"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

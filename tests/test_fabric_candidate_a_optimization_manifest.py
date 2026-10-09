"""Tests for the machine-readable Candidate A optimization manifest."""

from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

from people_counter.fabric_candidate_a_optimization_manifest import (
    Applicability,
    ImplementationStatus,
    ManifestError,
    OptimizationItem,
    load_manifest,
    summarize,
    verify_implementation_symbols_are_importable,
)


def _write_manifest(tmp_path: Path, items: list[dict], **envelope_overrides) -> Path:
    envelope = {
        "schema_version": 1,
        "source_document": "spark-candidate-a-optimization.md",
        "baseline": {
            "release_version": "0.9.11",
            "wheel_sha256": "abc",
            "environment_id": "env",
            "cpu_lock_sha256": "lock",
            "cpu_bundle_sha256": "bundle",
            "best_measured_aggregate_cpu_throughput_factor": 1.5899,
            "scaling_efficiency_pct": 39.44,
            "target_throughput_factor": 416.67,
            "workspace_id": "ws",
            "lakehouse_id": "lh",
            "benchmark_environment_id": "benv",
            "capacity_id": "cap",
            "capacity_sku": "F64",
        },
        "constraints": ["Fabric-only"],
        "items": items,
    }
    envelope.update(envelope_overrides)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(envelope), encoding="utf-8")
    return path


def _valid_item(**overrides) -> dict:
    item = {
        "id": "item-1",
        "section": "Section",
        "title": "Title",
        "recommendation": "Do the thing.",
        "footnote_refs": ["^1"],
        "applicability": "applicable",
        "implementation_symbol": "people_counter.fabric_capability_probe.probe_capability",
        "test_symbol": "tests.test_fabric_capability_probe",
        "correctness_guardrail": "Must not break.",
        "status": "implemented",
        "evidence": "Verified by inspection.",
    }
    item.update(overrides)
    return item


class LoadManifestTests(unittest.TestCase):
    def test_loads_the_real_repo_manifest_without_error(self) -> None:
        manifest = load_manifest()
        self.assertGreater(len(manifest.items), 0)
        self.assertEqual(manifest.schema_version, 1)
        self.assertIn("spark-candidate-a-optimization.md", manifest.source_document)
        self.assertIsNotNone(manifest.baseline)
        self.assertEqual(manifest.baseline.release_version, "0.9.11")
        self.assertIsInstance(manifest.constraints, tuple)
        self.assertGreater(len(manifest.constraints), 0)

    def test_envelope_fields_round_trip_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [_valid_item(id="a")],
                constraints=["Fabric-only", "No-external-compute"],
            )
            manifest = load_manifest(path)
            self.assertEqual(manifest.source_document, "spark-candidate-a-optimization.md")
            self.assertEqual(
                manifest.constraints, ("Fabric-only", "No-external-compute")
            )
            self.assertEqual(manifest.baseline.release_version, "0.9.11")
            self.assertEqual(manifest.baseline.capacity_sku, "F64")

    def test_real_manifest_has_no_duplicate_ids(self) -> None:
        manifest = load_manifest()
        ids = [item.id for item in manifest.items]
        self.assertEqual(len(ids), len(set(ids)))

    def test_rejects_missing_file(self) -> None:
        with self.assertRaises(ManifestError) as ctx:
            load_manifest(Path("/nonexistent/manifest.json"))
        self.assertIn("not found", str(ctx.exception))
        self.assertIn("/nonexistent/manifest.json", str(ctx.exception))

    def test_rejects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.json"
            path.write_text("{not json", encoding="utf-8")
            with self.assertRaises(ManifestError) as ctx:
                load_manifest(path)
            self.assertIn("not valid JSON", str(ctx.exception))

    def test_rejects_duplicate_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp), [_valid_item(id="dupe"), _valid_item(id="dupe")]
            )
            with self.assertRaises(ManifestError) as ctx:
                load_manifest(path)
            self.assertEqual(
                str(ctx.exception),
                "manifest has duplicate item ids: ['dupe']",
            )

    def test_rejects_malformed_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [_valid_item(id="a")],
                baseline={"release_version": "0.9.11", "unexpected_field": "oops"},
            )
            with self.assertRaises(ManifestError) as ctx:
                load_manifest(path)
            self.assertIn("baseline is malformed", str(ctx.exception))

    def test_rejects_missing_envelope_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            envelope_path = Path(tmp) / "manifest.json"
            envelope = {"schema_version": 1, "items": []}
            envelope_path.write_text(json.dumps(envelope), encoding="utf-8")
            with self.assertRaises(ManifestError) as ctx:
                load_manifest(envelope_path)
            self.assertIn("required key", str(ctx.exception))

    def test_rejects_item_missing_required_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad_item = _valid_item()
            del bad_item["title"]
            path = _write_manifest(Path(tmp), [bad_item])
            with self.assertRaises(ManifestError) as ctx:
                load_manifest(path)
            self.assertIn("required key", str(ctx.exception))

    def test_rejects_invalid_enum_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp), [_valid_item(id="item-1", applicability="not-a-real-value")]
            )
            with self.assertRaises(ManifestError) as ctx:
                load_manifest(path)
            self.assertEqual(
                str(ctx.exception),
                "manifest item 'item-1' invalid: 'not-a-real-value' is not a "
                "valid Applicability",
            )

    def test_load_manifest_reads_the_file_as_exactly_utf8(self) -> None:
        """The manifest file must always be read with an explicit utf-8
        encoding, never an unspecified/platform-default encoding."""
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(Path(tmp), [_valid_item(id="a")])
            calls: list[dict] = []
            original_read_text = Path.read_text

            def spying_read_text(self_path: Path, *args: object, **kwargs: object) -> str:
                if self_path == path:
                    calls.append(kwargs)
                return original_read_text(self_path, *args, **kwargs)

            from unittest.mock import patch

            with patch.object(Path, "read_text", spying_read_text):
                load_manifest(path)
            self.assertEqual(calls, [{"encoding": "utf-8"}])

    def test_item_test_symbol_and_footnote_refs_round_trip_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        id="a",
                        footnote_refs=["^5", "^6"],
                        test_symbol="tests.test_fabric_capability_probe.SomeTest",
                    )
                ],
            )
            manifest = load_manifest(path)
            item = manifest.by_id("a")
            self.assertEqual(item.footnote_refs, ("^5", "^6"))
            self.assertEqual(
                item.test_symbol, "tests.test_fabric_capability_probe.SomeTest"
            )

    def test_by_id_returns_matching_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(Path(tmp), [_valid_item(id="findme")])
            manifest = load_manifest(path)
            self.assertEqual(manifest.by_id("findme").id, "findme")

    def test_by_id_raises_key_error_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(Path(tmp), [_valid_item(id="a")])
            manifest = load_manifest(path)
            with self.assertRaises(KeyError) as ctx:
                manifest.by_id("missing")
            self.assertEqual(ctx.exception.args[0], "missing")


class OptimizationItemValidationTests(unittest.TestCase):
    def test_implemented_status_requires_implementation_symbol(self) -> None:
        with self.assertRaises(ManifestError) as ctx:
            OptimizationItem(
                id="x",
                section="s",
                title="t",
                recommendation="r",
                applicability=Applicability.APPLICABLE,
                status=ImplementationStatus.IMPLEMENTED,
                correctness_guardrail="g",
                evidence="e",
                implementation_symbol=None,
            )
        self.assertEqual(
            str(ctx.exception),
            "item 'x' has status 'implemented' but no implementation_symbol; "
            "a claimed implementation must cite the symbol that implements it",
        )

    def test_live_proven_status_requires_implementation_symbol(self) -> None:
        with self.assertRaises(ManifestError):
            OptimizationItem(
                id="x",
                section="s",
                title="t",
                recommendation="r",
                applicability=Applicability.APPLICABLE,
                status=ImplementationStatus.LIVE_PROVEN,
                correctness_guardrail="g",
                evidence="e",
                implementation_symbol=None,
            )

    def test_non_applicable_scope_requires_not_applicable_status(self) -> None:
        with self.assertRaises(ManifestError) as ctx:
            OptimizationItem(
                id="x",
                section="s",
                title="t",
                recommendation="r",
                applicability=Applicability.PROTECTED_SURFACE_OUT_OF_SCOPE,
                status=ImplementationStatus.IMPLEMENTED,
                correctness_guardrail="g",
                evidence="e",
                implementation_symbol="people_counter.sjd_gold",
            )
        self.assertEqual(
            str(ctx.exception),
            "item 'x' has applicability 'protected_surface_out_of_scope' but "
            "status 'implemented'; any non-applicable item must carry "
            "status=not_applicable so it cannot be silently mislabeled as "
            "implemented work",
        )

    def test_protected_surface_with_not_applicable_status_is_valid(self) -> None:
        item = OptimizationItem(
            id="x",
            section="s",
            title="t",
            recommendation="r",
            applicability=Applicability.PROTECTED_SURFACE_OUT_OF_SCOPE,
            status=ImplementationStatus.NOT_APPLICABLE,
            correctness_guardrail="g",
            evidence="e",
        )
        self.assertEqual(item.applicability, Applicability.PROTECTED_SURFACE_OUT_OF_SCOPE)

    def test_partial_status_does_not_require_implementation_symbol(self) -> None:
        item = OptimizationItem(
            id="x",
            section="s",
            title="t",
            recommendation="r",
            applicability=Applicability.APPLICABLE,
            status=ImplementationStatus.PARTIAL,
            correctness_guardrail="g",
            evidence="e",
        )
        self.assertIsNone(item.implementation_symbol)

    def test_not_implemented_status_does_not_require_implementation_symbol(self) -> None:
        item = OptimizationItem(
            id="x",
            section="s",
            title="t",
            recommendation="r",
            applicability=Applicability.APPLICABLE,
            status=ImplementationStatus.NOT_IMPLEMENTED,
            correctness_guardrail="g",
            evidence="e",
        )
        self.assertIsNone(item.implementation_symbol)
        self.assertEqual(item.status.value, "not_implemented")

    def test_deferred_pending_telemetry_status_is_a_distinct_applicable_state(self) -> None:
        item = OptimizationItem(
            id="x",
            section="s",
            title="t",
            recommendation="r",
            applicability=Applicability.APPLICABLE,
            status=ImplementationStatus.DEFERRED_PENDING_TELEMETRY,
            correctness_guardrail="g",
            evidence="e",
        )
        self.assertIsNone(item.implementation_symbol)
        self.assertEqual(item.status.value, "deferred_pending_telemetry")
        self.assertNotEqual(item.status, ImplementationStatus.NOT_APPLICABLE)

    def test_rejects_empty_required_field(self) -> None:
        with self.assertRaises(ManifestError) as ctx:
            OptimizationItem(
                id="x",
                section="s",
                title="",
                recommendation="r",
                applicability=Applicability.APPLICABLE,
                status=ImplementationStatus.PARTIAL,
                correctness_guardrail="g",
                evidence="e",
            )
        self.assertEqual(
            str(ctx.exception), "item is missing required field 'title'"
        )


class VerifyImplementationSymbolsTests(unittest.TestCase):
    def test_every_implemented_or_live_proven_symbol_in_real_manifest_imports(self) -> None:
        manifest = load_manifest()
        failures = verify_implementation_symbols_are_importable(manifest)
        self.assertEqual(
            failures,
            {},
            f"manifest cites implementation symbols that do not resolve: {failures}",
        )

    def test_detects_unresolvable_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [_valid_item(implementation_symbol="people_counter.not_a_real_module.thing")],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertIn("item-1", failures)
            self.assertIsInstance(failures["item-1"], str)
            self.assertIn(
                "people_counter.not_a_real_module.thing", failures["item-1"]
            )

    def test_detects_unresolvable_attribute(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        implementation_symbol="people_counter.fabric_capability_probe.not_a_real_attr"
                    )
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertIn("item-1", failures)
            self.assertIsInstance(failures["item-1"], str)
            self.assertIn(
                "people_counter.fabric_capability_probe.not_a_real_attr",
                failures["item-1"],
            )

    def test_skips_items_with_no_implementation_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        status="partial",
                        implementation_symbol=None,
                    )
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertEqual(failures, {})

    def test_continues_checking_later_items_after_skipping_one_without_symbol(
        self,
    ) -> None:
        # Distinguishes `continue` from `break` after the "no symbol" skip:
        # a buggy `break` would silently stop verifying every item that
        # follows one with no implementation_symbol.
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        id="no-symbol-item", status="partial", implementation_symbol=None
                    ),
                    _valid_item(
                        id="broken-item",
                        implementation_symbol="people_counter.not_a_real_module.thing",
                    ),
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertIn("broken-item", failures)
            self.assertNotIn("no-symbol-item", failures)

    def test_continues_checking_later_items_after_a_resolved_dotted_symbol(
        self,
    ) -> None:
        # Distinguishes `continue` from `break` after the dotted-form
        # resolution branch: a buggy `break` would silently stop verifying
        # every item that follows a successfully-resolved dotted symbol.
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        id="resolved-item",
                        implementation_symbol="people_counter.sjd_gold",
                    ),
                    _valid_item(
                        id="broken-item",
                        implementation_symbol="people_counter.not_a_real_module.thing",
                    ),
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertNotIn("resolved-item", failures)
            self.assertIn("broken-item", failures)

    def test_dotted_form_tries_every_split_point_not_only_alternating_ones(
        self,
    ) -> None:
        # Registers fully synthetic fake modules in sys.modules so the test
        # does not depend on ambient import ordering elsewhere in the suite.
        # "fakepkg" is importable but has NO "innermod" attribute bound on
        # it (unlike real submodules, which Python auto-binds onto their
        # parent package only once actually imported via the real import
        # system). The only way to resolve "fakepkg.innermod.Thing" is to
        # try the module prefix "fakepkg.innermod" directly -- a split point
        # that sits exactly one position below the full string, which a
        # buggy step=-2 walk (full length, then skip one, ...) would skip
        # over entirely, falling through to the "fakepkg" prefix instead
        # (which cannot resolve "innermod" as an attribute) and reporting a
        # false failure.
        fake_pkg = types.ModuleType("fakepkg")
        fake_innermod = types.ModuleType("fakepkg.innermod")
        fake_innermod.Thing = object()
        sys.modules["fakepkg"] = fake_pkg
        sys.modules["fakepkg.innermod"] = fake_innermod
        self.addCleanup(sys.modules.pop, "fakepkg", None)
        self.addCleanup(sys.modules.pop, "fakepkg.innermod", None)
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [_valid_item(implementation_symbol="fakepkg.innermod.Thing")],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertEqual(failures, {})

    def test_resolves_module_only_symbol_with_no_attribute(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [_valid_item(implementation_symbol="people_counter.sjd_gold")],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertEqual(failures, {})

    def test_resolves_nested_attribute_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        implementation_symbol=(
                            "people_counter.sjd_process.ExecutionProfile.spark_settings"
                        )
                    )
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertEqual(failures, {})

    def test_colon_form_resolves_nested_attribute_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        implementation_symbol=(
                            "people_counter.sjd_process:ExecutionProfile.spark_settings"
                        )
                    )
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertEqual(failures, {})

    def test_colon_form_detects_unresolvable_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [_valid_item(implementation_symbol="people_counter.not_a_real_module:thing")],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertIn("item-1", failures)
            self.assertIsInstance(failures["item-1"], str)
            self.assertIn("people_counter.not_a_real_module", failures["item-1"])

    def test_colon_form_detects_unresolvable_attribute(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        implementation_symbol="people_counter.fabric_capability_probe:not_a_real_attr"
                    )
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertIn("item-1", failures)
            self.assertIsInstance(failures["item-1"], str)
            self.assertIn("not_a_real_attr", failures["item-1"])

    def test_continues_checking_later_items_after_colon_form_module_failure(
        self,
    ) -> None:
        # Distinguishes `continue` from `break` after a colon-form module
        # import failure: a buggy `break` would silently stop verifying
        # every item that follows.
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(
                        id="broken-colon-module",
                        implementation_symbol="people_counter.not_a_real_module:thing",
                    ),
                    _valid_item(
                        id="broken-dotted",
                        implementation_symbol="people_counter.not_a_real_module.thing",
                    ),
                ],
            )
            manifest = load_manifest(path)
            failures = verify_implementation_symbols_are_importable(manifest)
            self.assertIn("broken-colon-module", failures)
            self.assertIn("broken-dotted", failures)


class SummarizeTests(unittest.TestCase):
    def test_summarize_counts_match_item_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_manifest(
                Path(tmp),
                [
                    _valid_item(id="a", status="implemented"),
                    _valid_item(id="b", status="live_proven"),
                    _valid_item(id="c", status="partial", implementation_symbol=None),
                    _valid_item(
                        id="d",
                        status="not_applicable",
                        applicability="protected_surface_out_of_scope",
                        implementation_symbol=None,
                    ),
                    _valid_item(id="e", status="not_implemented", implementation_symbol=None),
                    _valid_item(
                        id="f", status="deferred_pending_telemetry", implementation_symbol=None
                    ),
                ],
            )
            manifest = load_manifest(path)
            summary = summarize(manifest)
            self.assertEqual(summary["total_items"], 6)
            self.assertEqual(summary["implemented_or_live_proven"], 2)
            self.assertEqual(summary["partial"], 1)
            self.assertEqual(summary["not_applicable"], 1)
            self.assertEqual(summary["not_implemented"], 1)
            self.assertEqual(summary["deferred_pending_telemetry"], 1)
            self.assertEqual(summary["fabric_platform_blocked"], 0)

    def test_summarize_real_manifest_totals_are_internally_consistent(self) -> None:
        manifest = load_manifest()
        summary = summarize(manifest)
        self.assertEqual(
            summary["total_items"],
            sum(summary["by_status"].values()),
        )
        self.assertEqual(
            summary["total_items"],
            sum(summary["by_applicability"].values()),
        )


if __name__ == "__main__":
    unittest.main()

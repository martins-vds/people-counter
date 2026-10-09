import unittest
from unittest.mock import MagicMock

from people_counter.fabric_executor_inventory import (
    ExecutorInventoryError,
    ExecutorRecord,
    TaskWidthPlacement,
    assert_executor_placement_safe,
    assert_matches_reviewed_profile,
    assert_minimum_videos_per_group,
    compute_memory_cap,
    compute_slots,
    conservative_task_cpus_for_unmeasured_rss,
    discover_active_executors,
    executor_records_from_executor_summaries,
    per_executor_memory_slots,
    plan_task_width,
)


def _info(executor_id, host, cores, memory):
    """Build a fake ``ExecutorSummary`` exposing real-shaped callables.

    The live JVM ``org.apache.spark.status.api.v1.ExecutorSummary`` exposes
    every field as a zero-argument method (``e.id()``, ``e.host()``, ...),
    not a plain attribute; ``spec=`` pins the fake to exactly that surface
    so a test can't accidentally pass by relying on an attribute the real
    object doesn't have.
    """
    info = MagicMock(spec=["id", "host", "totalCores", "maxMemory"])
    info.id.return_value = executor_id
    info.host.return_value = host
    info.totalCores.return_value = cores
    info.maxMemory.return_value = memory
    return info


class ExecutorRecordTests(unittest.TestCase):
    def test_rejects_invalid_fields(self):
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="", host="h", total_cores=1, max_memory_bytes=1)
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="1", host="h", total_cores=0, max_memory_bytes=1)
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="1", host="h", total_cores=1, max_memory_bytes=0)

    def test_rejects_invalid_fields_with_the_exact_messages(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            ExecutorRecord(executor_id="", host="h", total_cores=1, max_memory_bytes=1)
        self.assertEqual(str(error.exception), "executor_id is required")
        with self.assertRaises(ExecutorInventoryError) as error:
            ExecutorRecord(executor_id="1", host="", total_cores=1, max_memory_bytes=1)
        self.assertEqual(str(error.exception), "host is required")
        with self.assertRaises(ExecutorInventoryError) as error:
            ExecutorRecord(executor_id="abc", host="h", total_cores=0, max_memory_bytes=1)
        self.assertEqual(
            str(error.exception),
            "executor 'abc' reports a non-positive core count",
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            ExecutorRecord(executor_id="abc", host="h", total_cores=1, max_memory_bytes=0)
        self.assertEqual(
            str(error.exception),
            "executor 'abc' reports non-positive max memory",
        )

    def test_rejects_non_string_truthy_executor_id_and_host(self):
        # A truthy but non-string value must still fail validation: the
        # field check is `not value or not isinstance(value, str)` (an OR),
        # not an AND, so a non-empty non-string value must not slip through.
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id=123, host="h", total_cores=1, max_memory_bytes=1)
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="1", host=123, total_cores=1, max_memory_bytes=1)

    def test_rejects_non_int_core_and_memory_types(self):
        # `type(x) is not int` must reject bool and float, not just values
        # below the threshold.
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="1", host="h", total_cores=4.0, max_memory_bytes=1)
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="1", host="h", total_cores=True, max_memory_bytes=1)
        with self.assertRaises(ExecutorInventoryError):
            ExecutorRecord(executor_id="1", host="h", total_cores=1, max_memory_bytes=1.0)


class ExecutorSummaryConversionTests(unittest.TestCase):
    def test_excludes_driver_and_sorts_by_executor_id(self):
        summaries = [
            _info("driver", "driver-host", 8, 10_000),
            _info("2", "host-b", 4, 8_000),
            _info("1", "host-a", 4, 8_000),
        ]
        records = executor_records_from_executor_summaries(summaries)
        self.assertEqual([r.executor_id for r in records], ["1", "2"])

    def test_converts_every_field_exactly_via_callable_methods(self):
        # The live JVM ExecutorSummary exposes every field as a callable
        # (e.g. ``e.id()``), not a plain attribute; this is the real shape
        # verified against a live local SparkSession's statusStore().
        (record,) = executor_records_from_executor_summaries(
            [_info("1", "host-a", 4, 8_000)]
        )
        self.assertEqual(record.executor_id, "1")
        self.assertEqual(record.host, "host-a")
        self.assertEqual(record.total_cores, 4)
        self.assertEqual(record.max_memory_bytes, 8_000)

    def test_falls_back_to_snake_case_attributes_when_camel_case_is_absent(self):
        # _field tries the camelCase Spark API name first, then the
        # snake_case fallback, and accepts either a plain (non-callable)
        # attribute or a zero-argument callable for each.
        info = MagicMock(spec=["executor_id", "host", "total_cores", "max_memory"])
        info.executor_id = "7"
        info.host = "host-z"
        info.total_cores = 6
        info.max_memory = 12_000
        (record,) = executor_records_from_executor_summaries([info])
        self.assertEqual(record.executor_id, "7")
        self.assertEqual(record.host, "host-z")
        self.assertEqual(record.total_cores, 6)
        self.assertEqual(record.max_memory_bytes, 12_000)

    def test_derives_host_from_host_port_when_host_is_absent(self):
        # ExecutorSummary.hostPort() is the only host-bearing field the
        # real JVM object actually exposes ("host:port"); _executor_host
        # must split off the port rather than ever observing it verbatim.
        info = MagicMock(spec=["id", "hostPort", "totalCores", "maxMemory"])
        info.id.return_value = "3"
        info.hostPort.return_value = "host-q:41231"
        info.totalCores.return_value = 2
        info.maxMemory.return_value = 4_000
        (record,) = executor_records_from_executor_summaries([info])
        self.assertEqual(record.host, "host-q")

    def test_derives_host_from_host_port_without_a_colon(self):
        info = MagicMock(spec=["id", "hostPort", "totalCores", "maxMemory"])
        info.id.return_value = "3"
        info.hostPort.return_value = "host-no-port"
        info.totalCores.return_value = 2
        info.maxMemory.return_value = 4_000
        (record,) = executor_records_from_executor_summaries([info])
        self.assertEqual(record.host, "host-no-port")

    def test_derives_host_by_splitting_off_only_the_last_colon_segment(self):
        # rsplit(":", 1) must split on the *last* colon only, keeping any
        # earlier colons (e.g. an IPv6-style address) joined to the host.
        # This also distinguishes it from an unlimited-maxsplit or a
        # left-anchored split()/greater-maxsplit mutant, all of which would
        # produce a different result for a value with two colons.
        info = MagicMock(spec=["id", "hostPort", "totalCores", "maxMemory"])
        info.id.return_value = "3"
        info.hostPort.return_value = "10.0.0.5:34521:extra"
        info.totalCores.return_value = 2
        info.maxMemory.return_value = 4_000
        (record,) = executor_records_from_executor_summaries([info])
        self.assertEqual(record.host, "10.0.0.5:34521")

    def test_falls_back_to_snake_case_host_port_when_camel_case_is_absent(self):
        # _executor_host's own fallback chain tries "hostPort" first, then
        # "host_port"; a summary exposing *only* the snake_case name must
        # still resolve correctly.
        info = MagicMock(spec=["id", "host_port", "totalCores", "maxMemory"])
        info.id.return_value = "3"
        info.host_port = "host-s:9000"
        info.totalCores.return_value = 2
        info.maxMemory.return_value = 4_000
        (record,) = executor_records_from_executor_summaries([info])
        self.assertEqual(record.host, "host-s")

    def test_fails_closed_on_missing_attributes(self):
        info = MagicMock(spec=["id"])
        info.id.return_value = "1"
        with self.assertRaises(ExecutorInventoryError):
            executor_records_from_executor_summaries([info])

    def test_falls_back_to_host_port_when_host_exists_but_py4j_rejects_the_call(self):
        # Live regression: the real JVM ExecutorSummary has no ``host()``
        # method, only ``hostPort()``. Py4J's JavaObject.__getattr__ builds
        # a callable proxy for *any* attribute name lazily, so hasattr(obj,
        # "host") is always true on the real object and only the actual
        # call raises ``py4j.protocol.Py4JError: Method host([]) does not
        # exist``. A plain class (not a spec-restricted MagicMock) models
        # that real "hasattr lies, call fails" shape; MagicMock(spec=...)
        # cannot model it because it raises AttributeError from getattr
        # itself, which does not exercise the invocation-time fallback.
        from py4j.protocol import Py4JError

        class _RealShapedSummary:
            def id(self):
                return "9"

            def host(self):
                raise Py4JError(
                    "An error occurred while calling o439.host. Trace:\n"
                    "py4j.Py4JException: Method host([]) does not exist\n"
                )

            def hostPort(self):
                return "real-host:7077"

            def totalCores(self):
                return 4

            def maxMemory(self):
                return 16_000

        (record,) = executor_records_from_executor_summaries([_RealShapedSummary()])
        self.assertEqual(record.executor_id, "9")
        self.assertEqual(record.host, "real-host")
        self.assertEqual(record.total_cores, 4)
        self.assertEqual(record.max_memory_bytes, 16_000)

    def test_field_error_names_every_attribute_tried(self):
        from people_counter.fabric_executor_inventory import _field

        with self.assertRaises(ExecutorInventoryError) as ctx:
            _field(object(), "foo", "bar")
        self.assertIn("foo", str(ctx.exception))
        self.assertIn("bar", str(ctx.exception))

    def test_field_calls_a_callable_value_but_returns_a_plain_value_verbatim(self):
        from people_counter.fabric_executor_inventory import _field

        callable_holder = MagicMock(spec=["x"])
        callable_holder.x.return_value = "called"
        self.assertEqual(_field(callable_holder, "x"), "called")

        class _Plain:
            y = "plain"

        self.assertEqual(_field(_Plain(), "y"), "plain")

    def test_field_falls_through_to_the_next_candidate_when_a_py4j_error_is_raised(
        self,
    ):
        # Mirrors the real JVM shape directly at the _field level (as
        # opposed to the end-to-end test above): a non-restricted object
        # exposes a callable "first" name whose *call* fails with
        # Py4JError, not whose attribute access fails. _field must catch
        # that invocation-time error and try the next candidate rather than
        # letting it propagate or treating a successful getattr as proof
        # the field is usable.
        from py4j.protocol import Py4JError
        from people_counter.fabric_executor_inventory import _field

        class _Fallthrough:
            def first(self):
                raise Py4JError("Method first([]) does not exist")

            def second(self):
                return "second-value"

        self.assertEqual(_field(_Fallthrough(), "first", "second"), "second-value")

    def test_field_reraises_a_py4j_error_when_every_candidate_fails(self):
        from py4j.protocol import Py4JError
        from people_counter.fabric_executor_inventory import _field

        class _AllMissing:
            def first(self):
                raise Py4JError("Method first([]) does not exist")

            def second(self):
                raise Py4JError("Method second([]) does not exist")

        with self.assertRaises(ExecutorInventoryError):
            _field(_AllMissing(), "first", "second")

    def test_py4j_invocation_error_types_is_empty_when_py4j_is_unimportable(self):
        # When py4j isn't installed (e.g. a local non-Spark environment),
        # the helper must degrade gracefully to "catch nothing" rather than
        # raising ImportError itself, so _field's plain AttributeError-only
        # behavior (matching the test-mock environment) is preserved.
        import builtins

        from people_counter.fabric_executor_inventory import (
            _py4j_invocation_error_types,
        )

        real_import = builtins.__import__

        def _blocked_import(name, *args, **kwargs):
            if name == "py4j.protocol" or name.startswith("py4j"):
                raise ImportError(f"simulated missing module: {name}")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = _blocked_import
        try:
            self.assertEqual(_py4j_invocation_error_types(), ())
        finally:
            builtins.__import__ = real_import


class DefaultExecutorSummariesTests(unittest.TestCase):
    def test_drains_the_java_iterator_via_has_next_and_next(self):
        from people_counter.fabric_executor_inventory import (
            _default_executor_summaries,
        )

        # hasNext's call budget is tracked independently of next() so that
        # a mutant which never calls next() (e.g. appending None instead)
        # still terminates deterministically with a wrong, assertable
        # result rather than looping forever (a real Java iterator's
        # hasNext() wouldn't behave this way, but it keeps this test fast
        # and conclusive either way).
        remaining = ["summary-a", "summary-b"]
        call_budget = [len(remaining)]

        def has_next():
            if call_budget[0] <= 0:
                return False
            call_budget[0] -= 1
            return True

        java_iterator = MagicMock()
        java_iterator.hasNext.side_effect = has_next
        java_iterator.next.side_effect = lambda: remaining.pop(0)
        seq = MagicMock()
        seq.iterator.return_value = java_iterator
        status_store = MagicMock()
        status_store.executorList.return_value = seq
        spark_session = MagicMock()
        spark_session.sparkContext._jsc.sc.return_value.statusStore.return_value = (
            status_store
        )

        result = _default_executor_summaries(spark_session)

        self.assertEqual(result, ["summary-a", "summary-b"])
        status_store.executorList.assert_called_once_with(True)

    def test_returns_an_empty_list_when_the_iterator_is_immediately_exhausted(self):
        from people_counter.fabric_executor_inventory import (
            _default_executor_summaries,
        )

        java_iterator = MagicMock()
        java_iterator.hasNext.return_value = False
        seq = MagicMock()
        seq.iterator.return_value = java_iterator
        status_store = MagicMock()
        status_store.executorList.return_value = seq
        spark_session = MagicMock()
        spark_session.sparkContext._jsc.sc.return_value.statusStore.return_value = (
            status_store
        )

        self.assertEqual(_default_executor_summaries(spark_session), [])


class DiscoverActiveExecutorsTests(unittest.TestCase):
    def _spark(self, sequence):
        spark = MagicMock()
        spark.fake_executor_summaries = MagicMock(side_effect=sequence)
        return spark

    def test_waits_for_stability_before_returning(self):
        unstable = [_info("1", "h1", 4, 8_000)]
        stable = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]
        spark = self._spark([unstable, stable, stable, stable, stable])
        clock = {"t": 0.0}
        sleeps = []

        def now():
            return clock["t"]

        def sleep(seconds):
            sleeps.append(seconds)
            clock["t"] += seconds

        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=2,
            stability_polls=3,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=sleep,
            now=now,
        )
        self.assertEqual(len(records), 2)
        self.assertGreaterEqual(len(sleeps), 3)

    def test_fails_closed_when_deadline_elapses(self):
        flapping = [
            [_info("1", "h1", 4, 8_000)],
            [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)],
        ]

        def infinite():
            while True:
                yield from flapping

        spark = self._spark(infinite())
        clock = {"t": 0.0}

        def now():
            return clock["t"]

        def sleep(seconds):
            clock["t"] += 10.0

        with self.assertRaises(ExecutorInventoryError):
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=2,
                stability_polls=3,
                poll_interval_seconds=1.0,
                deadline_seconds=5.0,
                sleep=sleep,
                now=now,
            )

    def test_tolerates_a_transient_invalid_reading_during_executor_registration(
        self,
    ):
        # Live regression: a newly-registering executor can transiently
        # report maxMemory=0 before its BlockManager finishes registering
        # (live-verified on the deployed Fabric Spark 4.1.1 runtime:
        # "executor '5' reports non-positive max memory"). A single invalid
        # poll must not crash discovery; it must be treated as "not yet
        # stable" and retried rather than propagating the validation error.
        transient_invalid = [_info("5", "h5", 4, 0)]
        stable = [_info("5", "h5", 4, 16_000)]
        spark = self._spark(
            [transient_invalid, stable, stable, stable, stable]
        )
        clock = {"t": 0.0}

        def now():
            return clock["t"]

        def sleep(seconds):
            clock["t"] += seconds

        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=1,
            stability_polls=3,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=sleep,
            now=now,
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].max_memory_bytes, 16_000)

    def test_fails_closed_with_the_most_recent_validation_error_chained(self):
        # If every poll until the deadline raises ExecutorInventoryError
        # (never a single valid reading), the final error must still fail
        # closed, and must chain the most recent validation failure so an
        # operator can see *why* the inventory never stabilized rather than
        # only that it timed out.
        always_invalid = [_info("5", "h5", 4, 0)]

        def infinite():
            while True:
                yield always_invalid

        spark = self._spark(infinite())
        clock = {"t": 0.0}

        def now():
            return clock["t"]

        def sleep(seconds):
            clock["t"] += 10.0

        with self.assertRaises(ExecutorInventoryError) as ctx:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=1,
                stability_polls=2,
                poll_interval_seconds=1.0,
                deadline_seconds=5.0,
                sleep=sleep,
                now=now,
            )
        self.assertIn(
            "most recent poll failed validation", str(ctx.exception)
        )
        # The appended last-error detail must augment, not replace, the
        # base "did not reach ... stable executor(s)" deadline message
        # (distinguishes `message += f"..."` from a `message = f"..."`
        # mutant that would discard the base message entirely).
        self.assertIn("did not reach 1 stable", str(ctx.exception))
        self.assertIn("within 5s", str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, ExecutorInventoryError)
        self.assertIn(
            "non-positive max memory", str(ctx.exception.__cause__)
        )

    def test_a_later_valid_poll_clears_a_prior_invalid_polls_error_from_the_deadline_message(
        self,
    ):
        # If the most recent poll before the deadline succeeded (even if
        # not yet stable), the final deadline message must reflect that
        # most-recent success, not a stale error from an earlier poll.
        invalid_then_valid_forever = [_info("5", "h5", 4, 0)]
        valid_but_never_stable = [
            _info("5", "h5", 4, 16_000),
            _info("6", "h6", 4, 16_000),
        ]

        def infinite():
            yield invalid_then_valid_forever
            while True:
                yield valid_but_never_stable

        spark = self._spark(infinite())
        clock = {"t": 0.0}

        def now():
            return clock["t"]

        def sleep(seconds):
            clock["t"] += 10.0

        with self.assertRaises(ExecutorInventoryError) as ctx:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=5,
                stability_polls=2,
                poll_interval_seconds=1.0,
                deadline_seconds=5.0,
                sleep=sleep,
                now=now,
            )
        self.assertNotIn(
            "most recent poll failed validation", str(ctx.exception)
        )
        self.assertIsNone(ctx.exception.__cause__)

    def test_rejects_invalid_arguments(self):
        spark = self._spark([[]])
        with self.assertRaises(ExecutorInventoryError):
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=0,
            )
        with self.assertRaises(ExecutorInventoryError):
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=1,
                stability_polls=1,
            )

    def test_accepts_boundary_minimum_executors_and_stability_polls(self):
        # minimum_executors=1 and stability_polls=2 are the smallest legal
        # values (`< 1` / `< 2`), not `<= 1` / `<= 2` or `< 2` / `< 3`.
        stable = [_info("1", "h1", 4, 8_000)]
        spark = self._spark([stable, stable, stable])
        clock = {"t": 0.0}
        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=1,
            stability_polls=2,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
            now=lambda: clock["t"],
        )
        self.assertEqual(len(records), 1)

    def test_deadline_validation_guard_is_le_zero_not_le_one(self):
        # The upfront argument guard is `deadline_seconds <= 0`, not
        # `<= 1`: a deadline strictly between 0 and 1 (exclusive-inclusive)
        # must be accepted by the upfront check and only fail, if ever, via
        # the loop's own elapsed-time comparison -- not rejected before the
        # executor inventory is even polled.
        never_stable_a = [_info("1", "h1", 4, 8_000)]
        never_stable_b = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]

        def infinite():
            while True:
                yield never_stable_a
                yield never_stable_b

        spark = self._spark(infinite())
        clock = {"t": 0.0}
        with self.assertRaises(ExecutorInventoryError):
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=2,
                stability_polls=3,
                poll_interval_seconds=1.0,
                deadline_seconds=0.5,
                sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
                now=lambda: clock["t"],
            )
        # The real guard lets this call reach the live executor-summary
        # provider at least once; the `<= 1` mutant would reject
        # deadline_seconds=0.5 before ever calling it. The exact `spark`
        # argument must be forwarded unchanged, not e.g. `None`.
        spark.fake_executor_summaries.assert_called_with(spark)

    def test_rejects_zero_poll_interval_and_zero_deadline_independently(self):
        # The guard is `poll_interval_seconds <= 0 OR deadline_seconds <= 0`;
        # each one individually being exactly zero must fail closed via the
        # upfront argument guard itself (exact message), even when the
        # other argument is valid. A loose `assertRaises` alone would also
        # pass for a `deadline_seconds < 0` mutant, since deadline_seconds
        # exactly zero would then fall through into the loop and still
        # raise -- just with the wrong (loop timeout) message.
        spark = self._spark([[]])
        with self.assertRaises(ExecutorInventoryError) as error:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=1,
                poll_interval_seconds=0.0,
                deadline_seconds=60.0,
            )
        self.assertEqual(
            str(error.exception),
            "poll_interval_seconds and deadline_seconds must be positive",
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=1,
                poll_interval_seconds=1.0,
                deadline_seconds=0.0,
            )
        self.assertEqual(
            str(error.exception),
            "poll_interval_seconds and deadline_seconds must be positive",
        )

    def test_requires_both_matching_resources_and_minimum_count_for_stability(self):
        # A poll that repeats the identical (but too-small) executor set
        # must never count toward stability, even though `current ==
        # previous` holds -- this rules out the `and` -> `or` mutation on
        # the stability condition.
        too_small = [_info("1", "h1", 4, 8_000)]
        enough = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]
        spark = self._spark(
            [too_small, too_small, enough, enough, enough, enough]
        )
        clock = {"t": 0.0}
        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=2,
            stability_polls=3,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
            now=lambda: clock["t"],
        )
        self.assertEqual(len(records), 2)

    def test_requires_exact_consecutive_stable_poll_count(self):
        # stable_count must advance by exactly 1 per matching poll (not 2),
        # and must reset to 0 (not 1) after an instability -- otherwise
        # fewer than `stability_polls` genuinely-stable polls could satisfy
        # the gate.
        stable = [_info("1", "h1", 4, 8_000)]
        unstable = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]
        # Two stable polls, then one unstable poll (resets the counter),
        # then three more stable polls: must take exactly 3 more to settle.
        spark = self._spark([stable, stable, unstable, stable, stable, stable, stable])
        clock = {"t": 0.0}
        poll_count = {"n": 0}
        tracker_calls = []

        def sleep(seconds):
            clock["t"] += seconds
            tracker_calls.append(clock["t"])

        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=1,
            stability_polls=3,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=sleep,
            now=lambda: clock["t"],
        )
        self.assertEqual(len(records), 1)
        # 6 sleeps precede the 7th (returning) poll: 2 stable + 1 unstable
        # (reset) + 3 to re-settle requires exactly 6 intervening sleeps.
        self.assertEqual(len(tracker_calls), 6)

    def test_default_stability_polls_is_exactly_three_not_four(self):
        # With exactly 4 identical polls queued and no explicit
        # `stability_polls`, the default must settle after the 4th poll
        # (1 baseline + 3 matching); a default of 4 would require a 5th
        # poll that is not queued, raising StopIteration from the mock.
        stable = [_info("1", "h1", 4, 8_000)]
        spark = self._spark([stable, stable, stable, stable])
        clock = {"t": 0.0}
        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=1,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
            now=lambda: clock["t"],
        )
        self.assertEqual(len(records), 1)

    def test_default_poll_interval_seconds_is_exactly_two(self):
        stable = [_info("1", "h1", 4, 8_000)]
        spark = self._spark([stable, stable, stable])
        clock = {"t": 0.0}
        sleeps = []
        discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=1,
            stability_polls=2,
            deadline_seconds=60.0,
            sleep=lambda s: (sleeps.append(s), clock.__setitem__("t", clock["t"] + s)),
            now=lambda: clock["t"],
        )
        self.assertTrue(sleeps)
        self.assertTrue(all(s == 2.0 for s in sleeps))

    def test_default_deadline_seconds_is_exactly_one_hundred_twenty(self):
        # Never-stable executor sets force the deadline branch; with 1.0s
        # sleeps and no explicit `deadline_seconds`, the raise must occur
        # after exactly 120 sleeps (120.0s elapsed), not 121.
        flapping_a = [_info("1", "h1", 4, 8_000)]
        flapping_b = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]

        def infinite():
            while True:
                yield flapping_a
                yield flapping_b

        spark = self._spark(infinite())
        clock = {"t": 0.0}
        sleeps = []
        with self.assertRaises(ExecutorInventoryError):
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=2,
                stability_polls=3,
                poll_interval_seconds=1.0,
                sleep=lambda s: (sleeps.append(s), clock.__setitem__("t", clock["t"] + s)),
                now=lambda: clock["t"],
            )
        self.assertEqual(len(sleeps), 120)

    def test_deadline_uses_elapsed_time_not_now_plus_start(self):
        # A monotonic clock need not start at zero. The deadline check must
        # be `now() - start >= deadline_seconds` (elapsed time); `now() +
        # start` would exceed even a generous deadline immediately whenever
        # the clock's starting value is large, failing closed with no
        # elapsed time at all.
        stable = [_info("1", "h1", 4, 8_000)]
        spark = self._spark([stable, stable, stable])
        clock = {"t": 1_000.0}
        records = discover_active_executors(
            spark,
            executor_summaries=spark.fake_executor_summaries,
            minimum_executors=1,
            stability_polls=2,
            poll_interval_seconds=1.0,
            deadline_seconds=60.0,
            sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
            now=lambda: clock["t"],
        )
        self.assertEqual(len(records), 1)

    def test_deadline_error_message_reports_minimum_deadline_and_last_observed(
        self,
    ) -> None:
        flapping_a = [_info("1", "h1", 4, 8_000)]
        flapping_b = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]

        def infinite():
            while True:
                yield flapping_a
                yield flapping_b

        spark = self._spark(infinite())
        clock = {"t": 0.0}
        with self.assertRaises(ExecutorInventoryError) as error:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=2,
                stability_polls=3,
                poll_interval_seconds=5.0,
                deadline_seconds=5.0,
                sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
                now=lambda: clock["t"],
            )
        message = str(error.exception)
        self.assertIn("did not reach 2 stable", message)
        self.assertIn("within 5s", message)
        self.assertIn("executor(s):", message)

    def test_rejects_invalid_arguments_with_the_exact_messages(self):
        spark = self._spark([[]])
        with self.assertRaises(ExecutorInventoryError) as error:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=0,
            )
        self.assertEqual(
            str(error.exception), "minimum_executors must be a positive integer"
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=1,
                stability_polls=1,
            )
        self.assertEqual(
            str(error.exception), "stability_polls must be at least two"
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=1,
                poll_interval_seconds=0.0,
                deadline_seconds=1.0,
            )
        self.assertEqual(
            str(error.exception),
            "poll_interval_seconds and deadline_seconds must be positive",
        )

    def test_deadline_boundary_is_inclusive_of_equality(self):
        # `now() - start >= deadline_seconds` must fail closed the instant
        # elapsed time reaches the deadline, not only once it is exceeded.
        flapping_a = [_info("1", "h1", 4, 8_000)]
        flapping_b = [_info("1", "h1", 4, 8_000), _info("2", "h2", 4, 8_000)]

        def infinite():
            while True:
                yield flapping_a
                yield flapping_b

        spark = self._spark(infinite())
        clock = {"t": 0.0}
        with self.assertRaises(ExecutorInventoryError):
            discover_active_executors(
                spark,
                executor_summaries=spark.fake_executor_summaries,
                minimum_executors=2,
                stability_polls=3,
                poll_interval_seconds=5.0,
                deadline_seconds=5.0,
                sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
                now=lambda: clock["t"],
            )


class SlotAndMemoryTests(unittest.TestCase):
    def _executors(self):
        return (
            ExecutorRecord("1", "h1", 4, 8_000_000_000),
            ExecutorRecord("2", "h2", 4, 8_000_000_000),
        )

    def test_compute_slots_floors_per_executor(self):
        self.assertEqual(compute_slots(self._executors(), task_cpus=1), 8)
        self.assertEqual(compute_slots(self._executors(), task_cpus=4), 2)
        self.assertEqual(compute_slots(self._executors(), task_cpus=3), 2)

    def test_compute_slots_rejects_empty_or_bad_task_cpus(self):
        with self.assertRaises(ExecutorInventoryError):
            compute_slots((), task_cpus=1)
        with self.assertRaises(ExecutorInventoryError):
            compute_slots(self._executors(), task_cpus=0)

    def test_compute_slots_rejects_with_the_exact_messages(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            compute_slots(self._executors(), task_cpus=0)
        self.assertEqual(str(error.exception), "task_cpus must be a positive integer")
        with self.assertRaises(ExecutorInventoryError) as error:
            compute_slots((), task_cpus=1)
        self.assertEqual(str(error.exception), "executors must not be empty")

    def test_compute_memory_cap_uses_minimum_usable_executor(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1_000_000_000),
            ExecutorRecord("2", "h2", 4, 2_000_000_000),
        )
        cap = compute_memory_cap(executors, headroom=0.2, peak_rss_bytes=100_000_000)
        # usable_min = 800_000_000 -> 8 slots per executor * 2 executors
        self.assertEqual(cap, 16)

    def test_compute_memory_cap_rejects_rss_too_large(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        with self.assertRaises(ExecutorInventoryError):
            compute_memory_cap(executors, headroom=0.2, peak_rss_bytes=10_000_000)

    def test_compute_memory_cap_rejects_bad_headroom(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        with self.assertRaises(ExecutorInventoryError):
            compute_memory_cap(executors, headroom=1.0, peak_rss_bytes=1)

    def test_compute_memory_cap_accepts_headroom_boundary_of_zero(self):
        # headroom range is `0.0 <= headroom < 1.0`: zero itself is legal.
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        cap = compute_memory_cap(executors, headroom=0.0, peak_rss_bytes=100_000)
        self.assertEqual(cap, 10)

    def test_compute_memory_cap_rejects_bad_peak_rss_type_independent_of_value(self):
        # The guard is `type(x) is not int OR x < 1`; a non-int value that
        # happens to be >= 1 (e.g. a float) must still be rejected.
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        with self.assertRaises(ExecutorInventoryError):
            compute_memory_cap(executors, headroom=0.2, peak_rss_bytes=100_000.0)

    def test_compute_memory_cap_accepts_peak_rss_boundary_of_one(self):
        executors = (ExecutorRecord("1", "h1", 4, 10),)
        cap = compute_memory_cap(executors, headroom=0.0, peak_rss_bytes=1)
        self.assertEqual(cap, 10)

    def test_compute_memory_cap_floors_the_division_rather_than_truncating_float(self):
        # usable_min // peak_rss_bytes must be an exact integer floor
        # division, not `/` (true division) truncated via int().
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        cap = compute_memory_cap(executors, headroom=0.0, peak_rss_bytes=300_000)
        # 1_000_000 // 300_000 == 3 (not 3.33...)
        self.assertEqual(cap, 3)

    def test_compute_memory_cap_accepts_per_executor_slots_boundary_of_one(self):
        # The guard is `per_executor_slots < 1`, not `<= 1` or `< 2`: a
        # single-executor-slot result must be accepted, not rejected.
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        cap = compute_memory_cap(executors, headroom=0.0, peak_rss_bytes=1_000_000)
        self.assertEqual(cap, 1)

    def test_compute_memory_cap_rejects_with_the_exact_messages(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            compute_memory_cap((), headroom=0.2, peak_rss_bytes=1)
        self.assertEqual(str(error.exception), "executors must not be empty")
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000),)
        with self.assertRaises(ExecutorInventoryError) as error:
            compute_memory_cap(executors, headroom=1.0, peak_rss_bytes=1)
        self.assertEqual(str(error.exception), "headroom must be within [0, 1)")
        with self.assertRaises(ExecutorInventoryError) as error:
            compute_memory_cap(executors, headroom=0.2, peak_rss_bytes=0)
        self.assertEqual(
            str(error.exception), "peak_rss_bytes must be a positive integer"
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            compute_memory_cap(executors, headroom=0.2, peak_rss_bytes=10_000_000)
        self.assertEqual(
            str(error.exception),
            "measured peak RSS does not fit the minimum usable executor memory",
        )


class PlanTaskWidthTests(unittest.TestCase):
    def test_takes_minimum_of_all_limits(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1_000_000_000),
            ExecutorRecord("2", "h2", 4, 1_000_000_000),
        )
        placement = plan_task_width(
            executors,
            task_cpus=1,
            operator_cap=3,
            peak_rss_bytes=100_000_000,
            headroom=0.2,
        )
        self.assertIsInstance(placement, TaskWidthPlacement)
        self.assertEqual(placement.slots, 8)
        self.assertEqual(placement.operator_cap, 3)
        self.assertEqual(placement.planned_task_count, 3)

    def test_without_rss_omits_memory_cap(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        placement = plan_task_width(executors, task_cpus=2)
        self.assertIsNone(placement.memory_cap)
        self.assertEqual(placement.planned_task_count, 2)

    def test_rejects_bad_operator_cap(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        with self.assertRaises(ExecutorInventoryError):
            plan_task_width(executors, task_cpus=1, operator_cap=0)

    def test_rejects_non_int_operator_cap_regardless_of_value(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        with self.assertRaises(ExecutorInventoryError):
            plan_task_width(executors, task_cpus=1, operator_cap=3.0)

    def test_accepts_operator_cap_boundary_of_one(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        placement = plan_task_width(executors, task_cpus=1, operator_cap=1)
        self.assertEqual(placement.planned_task_count, 1)

    def test_default_headroom_is_applied_when_peak_rss_bytes_given(self):
        # Must actually compute and surface a memory cap (not silently stay
        # None) when peak_rss_bytes is supplied, using the 0.20 default
        # headroom.
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        placement = plan_task_width(executors, task_cpus=1, peak_rss_bytes=100_000_000)
        # usable_min = 800_000_000 -> 8 slots per executor * 1 executor
        self.assertEqual(placement.memory_cap, 8)

    def test_result_preserves_the_exact_task_cpus_and_executors_given(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        placement = plan_task_width(executors, task_cpus=2)
        self.assertEqual(placement.task_cpus, 2)
        self.assertEqual(placement.executors, tuple(executors))

    def test_rejects_with_the_exact_messages(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        with self.assertRaises(ExecutorInventoryError) as error:
            plan_task_width(executors, task_cpus=1, operator_cap=0)
        self.assertEqual(str(error.exception), "operator_cap must be a positive integer")
        with self.assertRaises(ExecutorInventoryError) as error:
            plan_task_width(executors, task_cpus=8)
        self.assertEqual(str(error.exception), "planned task width collapsed to zero slots")


class AssertMatchesReviewedProfileTests(unittest.TestCase):
    def test_passes_when_all_executors_match(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1),
            ExecutorRecord("2", "h2", 4, 1),
        )
        assert_matches_reviewed_profile(
            executors, expected_executor_cores=4, expected_task_cpus=1
        )

    def test_fails_closed_on_mismatch(self):
        executors = (ExecutorRecord("1", "h1", 8, 1),)
        with self.assertRaises(ExecutorInventoryError):
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=4, expected_task_cpus=1
            )

    def test_rejects_task_cpus_exceeding_cores(self):
        executors = (ExecutorRecord("1", "h1", 4, 1),)
        with self.assertRaises(ExecutorInventoryError):
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=4, expected_task_cpus=8
            )

    def test_rejects_non_int_expected_executor_cores_regardless_of_value(self):
        executors = (ExecutorRecord("1", "h1", 4, 1),)
        with self.assertRaises(ExecutorInventoryError):
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=4.0, expected_task_cpus=1
            )

    def test_rejects_non_int_expected_task_cpus_regardless_of_value(self):
        executors = (ExecutorRecord("1", "h1", 4, 1),)
        with self.assertRaises(ExecutorInventoryError):
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=4, expected_task_cpus=1.0
            )

    def test_accepts_task_cpus_equal_to_executor_cores(self):
        executors = (ExecutorRecord("1", "h1", 4, 1),)
        assert_matches_reviewed_profile(
            executors, expected_executor_cores=4, expected_task_cpus=4
        )

    def test_rejects_with_the_exact_messages(self):
        executors = (ExecutorRecord("1", "h1", 4, 1),)
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=0, expected_task_cpus=1
            )
        self.assertEqual(
            str(error.exception), "expected_executor_cores must be a positive integer"
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=4, expected_task_cpus=0
            )
        self.assertEqual(
            str(error.exception), "expected_task_cpus must be a positive integer"
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_matches_reviewed_profile(
                executors, expected_executor_cores=4, expected_task_cpus=8
            )
        self.assertEqual(
            str(error.exception), "expected_task_cpus cannot exceed executor cores"
        )
        mismatched = (ExecutorRecord("1", "h1", 8, 1),)
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_matches_reviewed_profile(
                mismatched, expected_executor_cores=4, expected_task_cpus=1
            )
        self.assertEqual(
            str(error.exception),
            "observed executor cores do not match the reviewed profile "
            "(expected 4): [('1', 8)]",
        )


class PerExecutorMemorySlotsTests(unittest.TestCase):
    def test_returns_floored_usable_memory_divided_by_peak_rss(self):
        executor = ExecutorRecord("1", "h1", 4, 1_000_000_000)
        slots = per_executor_memory_slots(
            executor, headroom=0.2, peak_rss_bytes=100_000_000
        )
        # usable = 800_000_000 -> 8 slots
        self.assertEqual(slots, 8)

    def test_rejects_bad_headroom(self):
        executor = ExecutorRecord("1", "h1", 4, 1_000_000_000)
        with self.assertRaises(ExecutorInventoryError) as error:
            per_executor_memory_slots(executor, headroom=1.0, peak_rss_bytes=1)
        self.assertEqual(str(error.exception), "headroom must be within [0, 1)")

    def test_rejects_non_positive_peak_rss(self):
        executor = ExecutorRecord("1", "h1", 4, 1_000_000_000)
        with self.assertRaises(ExecutorInventoryError) as error:
            per_executor_memory_slots(executor, headroom=0.2, peak_rss_bytes=0)
        self.assertEqual(
            str(error.exception), "peak_rss_bytes must be a positive integer"
        )

    def test_accepts_the_smallest_positive_peak_rss_boundary(self):
        # peak_rss_bytes=1 is the smallest value the guard must accept
        # (strictly less than 1 is rejected, not less-than-or-equal/less-
        # than-2).
        executor = ExecutorRecord("1", "h1", 4, 1_000_000_000)
        slots = per_executor_memory_slots(
            executor, headroom=0.0, peak_rss_bytes=1
        )
        self.assertEqual(slots, 1_000_000_000)

    def test_distinguishes_per_executor_values_under_heterogeneous_memory(self):
        big = ExecutorRecord("1", "h1", 4, 1_000_000_000)
        small = ExecutorRecord("2", "h2", 4, 100_000_000)
        peak = 50_000_000
        self.assertEqual(
            per_executor_memory_slots(big, headroom=0.0, peak_rss_bytes=peak), 20
        )
        self.assertEqual(
            per_executor_memory_slots(small, headroom=0.0, peak_rss_bytes=peak), 2
        )


class AssertExecutorPlacementSafeTests(unittest.TestCase):
    def test_passes_for_homogeneous_executors_within_budget(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1_000_000_000),
            ExecutorRecord("2", "h2", 4, 1_000_000_000),
        )
        safe = assert_executor_placement_safe(
            executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.2
        )
        # cores ceiling = 4 per executor; memory slots = 8 per executor ->
        # the cores ceiling is the binding (lower) safe value.
        self.assertEqual(safe, (4, 4))

    def test_fails_closed_when_one_heterogeneous_executor_cannot_fit_its_own_memory(
        self,
    ):
        # A single big-core, low-memory executor could legally receive its
        # own full cores-based task count from Spark's scheduler alone,
        # regardless of any other executor's memory headroom.
        executors = (
            ExecutorRecord("1", "h1", 16, 500_000_000),
            ExecutorRecord("2", "h2", 4, 100_000_000_000),
        )
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_executor_placement_safe(
                executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.0
            )
        self.assertEqual(
            str(error.exception),
            "task_cpus=1 is not placement-safe: Spark's own cores-based "
            "scheduling ceiling exceeds the measured memory-safe concurrency "
            "on executor(s) [('1', 'cores_ceiling=16', 'memory_safe=5')]; "
            "widen task_cpus (a reviewed width of 1/2/4) or reduce executor "
            "memory pressure before admitting this concurrency",
        )

    def test_passes_for_heterogeneous_executors_when_each_fits_its_own_memory(self):
        executors = (
            ExecutorRecord("1", "h1", 16, 100_000_000_000),
            ExecutorRecord("2", "h2", 4, 1_000_000_000),
        )
        safe = assert_executor_placement_safe(
            executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.0
        )
        self.assertEqual(safe, (16, 4))

    def test_passes_at_the_exact_one_memory_slot_boundary(self):
        # usable = 130 * 0.8 = 104 -> floor(104 / 100) = 1 memory slot exactly;
        # cores ceiling is also 1 -> safe at the boundary (not "< 1").
        executors = (ExecutorRecord("1", "h1", 1, 130),)
        safe = assert_executor_placement_safe(
            executors, task_cpus=1, peak_rss_bytes=100, headroom=0.2
        )
        self.assertEqual(safe, (1,))

    def test_passes_when_cores_ceiling_exactly_equals_memory_slots(self):
        # usable = 325 * 0.8 = 260 -> floor(260 / 100) = 2 memory slots,
        # exactly equal to the 2-core-per-task_cpus=1 cores ceiling -> safe
        # (binding equality is not "unsafe", only a strict excess is).
        executors = (ExecutorRecord("1", "h1", 2, 325),)
        safe = assert_executor_placement_safe(
            executors, task_cpus=1, peak_rss_bytes=100, headroom=0.2
        )
        self.assertEqual(safe, (2,))

    def test_rejects_non_positive_task_cpus(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_executor_placement_safe(
                executors, task_cpus=0, peak_rss_bytes=1_000
            )
        self.assertEqual(
            str(error.exception), "task_cpus must be a positive integer"
        )

    def test_rejects_empty_executors(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_executor_placement_safe((), task_cpus=1, peak_rss_bytes=1_000)
        self.assertEqual(str(error.exception), "executors must not be empty")

    def test_per_executor_safe_tasks_are_integers_not_floats(self):
        # `total_cores // task_cpus` (floor division) must stay integral;
        # a regression to true division would silently smuggle floats into
        # downstream slot-count arithmetic and summation.
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        safe = assert_executor_placement_safe(
            executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.2
        )
        self.assertIsInstance(safe[0], int)
        self.assertNotIsInstance(safe[0], float)

    def test_fails_closed_when_task_cpus_is_wider_than_an_executors_own_cores(self):
        executors = (ExecutorRecord("1", "h1", 2, 1_000_000_000),)
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_executor_placement_safe(
                executors, task_cpus=4, peak_rss_bytes=1_000
            )
        self.assertIn("cannot host one task", str(error.exception))

    def test_fails_closed_when_measured_rss_exceeds_an_executors_usable_memory(self):
        executors = (ExecutorRecord("1", "h1", 4, 1_000_000_000),)
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_executor_placement_safe(
                executors, task_cpus=1, peak_rss_bytes=2_000_000_000
            )
        self.assertIn("does not fit its usable memory", str(error.exception))


class ConservativeTaskCpusForUnmeasuredRssTests(unittest.TestCase):
    def test_returns_the_widest_observed_core_count(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1),
            ExecutorRecord("2", "h2", 8, 1),
        )
        self.assertEqual(conservative_task_cpus_for_unmeasured_rss(executors), 8)

    def test_returns_the_single_executors_core_count_when_homogeneous(self):
        executors = (
            ExecutorRecord("1", "h1", 2, 1),
            ExecutorRecord("2", "h2", 2, 1),
        )
        self.assertEqual(conservative_task_cpus_for_unmeasured_rss(executors), 2)

    def test_rejects_empty_executors(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            conservative_task_cpus_for_unmeasured_rss(())
        self.assertEqual(str(error.exception), "executors must not be empty")


class PlanTaskWidthReviewedWidthsTests(unittest.TestCase):
    """P1-A: widths 1/2/4 across homogeneous/heterogeneous inventories."""

    def test_width_one_homogeneous_sums_per_executor_slots(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1_000_000_000),
            ExecutorRecord("2", "h2", 4, 1_000_000_000),
        )
        placement = plan_task_width(
            executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.2
        )
        self.assertEqual(placement.planned_task_count, 8)
        self.assertEqual(placement.per_executor_safe_tasks, (4, 4))

    def test_width_two_homogeneous_sums_per_executor_slots(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1_000_000_000),
            ExecutorRecord("2", "h2", 4, 1_000_000_000),
        )
        placement = plan_task_width(
            executors, task_cpus=2, peak_rss_bytes=100_000_000, headroom=0.2
        )
        # cores ceiling per executor = 2; memory slots per executor = 8 ->
        # cores ceiling binds: 2 + 2 = 4 total.
        self.assertEqual(placement.planned_task_count, 4)

    def test_width_four_homogeneous_sums_per_executor_slots(self):
        executors = (
            ExecutorRecord("1", "h1", 4, 1_000_000_000),
            ExecutorRecord("2", "h2", 4, 1_000_000_000),
        )
        placement = plan_task_width(
            executors, task_cpus=4, peak_rss_bytes=100_000_000, headroom=0.2
        )
        self.assertEqual(placement.planned_task_count, 2)

    def test_heterogeneous_inventory_plans_from_summed_safe_slots_not_executor_count(
        self,
    ):
        # Five eight-core executors (40 one-CPU slots) must plan far more
        # than five physical partitions once memory proves it safe -- the
        # exact headline scenario the rubber-duck review flagged.
        executors = tuple(
            ExecutorRecord(str(i), f"h{i}", 8, 10_000_000_000) for i in range(5)
        )
        placement = plan_task_width(
            executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.2
        )
        self.assertEqual(placement.planned_task_count, 40)

    def test_unknown_rss_can_be_bounded_to_one_task_per_executor_via_operator_cap(
        self,
    ):
        # Reviewed unknown-RSS policy: when RSS has not been measured,
        # callers that must guarantee no more than one task per executor
        # (without forcing `task_cpus` outside the reviewed 1/2/4 widths)
        # bound the total via `operator_cap=len(executors)`.
        executors = tuple(
            ExecutorRecord(str(i), f"h{i}", 8, 10_000_000_000) for i in range(5)
        )
        placement = plan_task_width(
            executors, task_cpus=1, operator_cap=len(executors)
        )
        self.assertIsNone(placement.memory_cap)
        self.assertEqual(placement.planned_task_count, 5)
        # Memory validation was skipped entirely (no RSS was supplied), so
        # the per-executor safe-task field must stay at its empty default,
        # not a falsy placeholder that looks the same in a boolean context.
        self.assertEqual(placement.per_executor_safe_tasks, ())

    def test_headroom_is_forwarded_to_the_placement_safety_check(self):
        # cores ceiling = 2; with headroom=0.0 usable memory yields exactly
        # 2 memory slots (safe); the function's own default headroom=0.20
        # would instead yield 1 memory slot and incorrectly fail closed.
        # This proves the caller-supplied `headroom` reaches the safety
        # check rather than silently falling back to the default.
        executors = (ExecutorRecord("1", "h1", 2, 240_000_000),)
        placement = plan_task_width(
            executors, task_cpus=1, peak_rss_bytes=100_000_000, headroom=0.0
        )
        self.assertEqual(placement.per_executor_safe_tasks, (2,))


class AssertMinimumVideosPerGroupTests(unittest.TestCase):
    def test_passes_when_workload_cannot_fill_every_group(self):
        assert_minimum_videos_per_group(
            [1, 1, 5], total_items=7, minimum_videos_per_group=4
        )

    def test_fails_closed_when_workload_permits_but_a_group_is_small(self):
        with self.assertRaises(ExecutorInventoryError):
            assert_minimum_videos_per_group(
                [1, 7], total_items=8, minimum_videos_per_group=4
            )

    def test_passes_when_every_group_meets_minimum(self):
        assert_minimum_videos_per_group(
            [4, 4], total_items=8, minimum_videos_per_group=4
        )

    def test_empty_groups_are_a_noop(self):
        assert_minimum_videos_per_group([], total_items=0, minimum_videos_per_group=4)

    def test_rejects_bad_minimum(self):
        with self.assertRaises(ExecutorInventoryError):
            assert_minimum_videos_per_group([1], total_items=1, minimum_videos_per_group=0)

    def test_rejects_bad_minimum_with_the_exact_message(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_minimum_videos_per_group([1], total_items=1, minimum_videos_per_group=0)
        self.assertEqual(
            str(error.exception), "minimum_videos_per_group must be a positive integer"
        )

    def test_fails_closed_with_the_exact_message(self):
        with self.assertRaises(ExecutorInventoryError) as error:
            assert_minimum_videos_per_group(
                [1, 7], total_items=8, minimum_videos_per_group=4
            )
        self.assertEqual(
            str(error.exception),
            "workload of 8 item(s) across 2 group(s) permits >= 4 videos "
            "per group, but observed group sizes [1, 7]",
        )

    def test_accepts_minimum_videos_per_group_boundary_of_one(self):
        assert_minimum_videos_per_group([1], total_items=1, minimum_videos_per_group=1)

    def test_single_group_is_not_treated_as_the_empty_case(self):
        # The early-return guard is `group_count == 0`, not `== 1`: a
        # single group that is below the minimum, when the workload could
        # have filled it, must still raise.
        with self.assertRaises(ExecutorInventoryError):
            assert_minimum_videos_per_group(
                [1], total_items=4, minimum_videos_per_group=4
            )


if __name__ == "__main__":
    unittest.main()

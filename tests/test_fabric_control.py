import importlib.util
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from contextvars import Context
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier, Condition, Event, get_ident
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call
from uuid import UUID

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "src/people_counter/fabric_control.py"
# Avoid the SDK package initializer and its unrelated computer-vision dependencies.
if "people_counter.fabric_control" in sys.modules:
    control = sys.modules["people_counter.fabric_control"]
else:
    SPEC = importlib.util.spec_from_file_location("people_counter.fabric_control", MODULE_PATH)
    assert SPEC is not None and SPEC.loader is not None
    control = importlib.util.module_from_spec(SPEC)
    sys.modules[SPEC.name] = control
    SPEC.loader.exec_module(control)


class Expression:
    def __init__(self, evaluate):
        self.evaluate = evaluate

    def __eq__(self, value):
        return Expression(lambda row: self.evaluate(row) == value)

    def __and__(self, other):
        return Expression(lambda row: self.evaluate(row) and other.evaluate(row))

    def isNull(self):
        return Expression(lambda row: self.evaluate(row) is None)

    def cast(self, data_type):
        assert data_type in ("string", "timestamp")
        return self


class Functions:
    @staticmethod
    def col(name):
        return Expression(lambda row: row[name])

    @staticmethod
    def lit(value):
        return Expression(lambda row: value)

    @staticmethod
    def current_timestamp():
        return Functions.lit(datetime(2026, 9, 23, tzinfo=timezone.utc))


class DeltaError(Exception):
    def __init__(self, error_class="DELTA_CONCURRENT_WRITE"):
        self.error_class = error_class

    def getErrorClass(self):
        return self.error_class


class Frame:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *names):
        assert names == ("lock_name", "owner_id", "acquired_at")
        return self

    def limit(self, count):
        assert count == 2
        self.rows = self.rows[:count]
        return self

    def collect(self):
        return [SimpleNamespace(**row) for row in self.rows]


class FakeEnvironment:
    def __init__(self, monkeypatch):
        self.rows = [{"lock_name": "global", "owner_id": None, "acquired_at": None}]
        self.spark = SimpleNamespace(
            catalog=SimpleNamespace(refreshTable=Mock()),
            table=self.table,
        )
        self.now = 0.0
        self.sleeps = []
        self.updates = []
        self.targets = []
        self.before_update = None
        self.on_sleep = None
        self.action_error = None
        self.read_error = None
        self.read_count = 0
        self.jitter = Mock(return_value=0.1)
        self.monkeypatch = monkeypatch
        monkeypatch.setattr(control, "_delta_table", self.delta_table)
        monkeypatch.setattr(control, "_spark_functions", lambda: Functions)
        monkeypatch.setattr(control, "_acquisition_error_types", lambda: (DeltaError,))
        monkeypatch.setattr(control.time, "monotonic", lambda: self.now)
        monkeypatch.setattr(control.time, "sleep", self.sleep)
        monkeypatch.setattr(control.random, "uniform", self.jitter)

    @property
    def row(self):
        return self.rows[0]

    def table(self, name):
        assert name == "locks"
        self.read_count += 1
        assert self.read_count < 200, "unbounded polling at the frozen deadline"
        if self.read_error is not None:
            raise self.read_error
        return Frame([dict(row) for row in self.rows])

    def delta_table(self, spark, name):
        assert spark is self.spark
        if name == "locks":
            return SimpleNamespace(update=self.update)
        assert self.row["owner_id"] is not None, "constructed outside the lock"
        target = Target(self, name)
        self.targets.append(target)
        return target

    def update(self, *, condition, set):
        self.updates.append((condition, set))
        if self.before_update is not None:
            self.before_update()
        for row in self.rows:
            if condition.evaluate(row):
                row.update({key: value.evaluate(row) for key, value in set.items()})

    def sleep(self, seconds):
        assert seconds > 0
        self.sleeps.append(seconds)
        self.now += seconds
        if self.on_sleep is not None:
            self.on_sleep()

    def writer(self, timeout=1):
        return control.ControlWriter(self.spark, "locks", timeout_seconds=timeout)


class Target:
    def __init__(self, environment, name):
        self.environment = environment
        self.name = name
        self.calls = []
        self.owner = environment.row["owner_id"]
        self.used = False

    def __getattr__(self, name):
        def call(*args, **kwargs):
            assert self.environment.row["owner_id"] == self.owner
            assert not self.used, "discarded the builder returned by a chain method"
            self.used = True
            self.calls.append((name, args, kwargs))
            if name in ("execute", "update", "delete"):
                if self.environment.action_error is not None:
                    raise self.environment.action_error
                return None
            builder = Target(self.environment, self.name)
            builder.calls = self.calls
            return builder

        return call


@pytest.fixture
def environment(monkeypatch):
    return FakeEnvironment(monkeypatch)


def test_run_returns_result_and_releases_with_a_unique_owner(environment, caplog):
    writer = environment.writer()
    owners = []
    result = object()

    def operation():
        owners.append(environment.row["owner_id"])
        assert environment.row["acquired_at"] == Functions.current_timestamp().evaluate({})
        return result

    assert writer.run(operation) is result
    assert writer.run(operation) is result
    assert len(set(owners)) == 2
    assert all(UUID(owner).version == 4 for owner in owners)
    assert environment.row == {
        "lock_name": "global", "owner_id": None, "acquired_at": None,
    }
    assert len(environment.updates) == 4
    environment.spark.catalog.refreshTable.assert_called_with("locks")
    assert not caplog.records


def test_three_same_date_writers_serialize_contending_callbacks(monkeypatch):
    spark = object()
    state = {"lock_name": "global", "owner_id": None, "acquired_at": None}
    changed = Condition()
    initial_snapshots = Barrier(3)
    readers = set()
    thread_indexes = {}
    acquisition_attempts = []
    polling = set()
    poll_rounds = [0, 0, 0]
    entered = []
    active = 0
    max_active = 0
    resume_polling = [Event() for _ in range(3)]
    finish_callback = [Event() for _ in range(3)]
    release_committed = [Event() for _ in range(3)]
    allow_release_readback = [Event() for _ in range(3)]
    writers = [
        control.ControlWriter(spark, "locks", timeout_seconds=10)
        for _ in range(3)
    ]

    def read_lock(writer):
        assert writer in writers
        with changed:
            snapshot = control._LockState(state["owner_id"], state["acquired_at"])
            first_read = get_ident() not in readers
            readers.add(get_ident())
        if first_read:
            # Every contender observes the unowned row before any CAS can run.
            initial_snapshots.wait(timeout=5)
        return snapshot

    def atomic_update(*, condition, set):
        with changed:
            values = {key: value.evaluate(state) for key, value in set.items()}
            if values["owner_id"] is not None:
                acquisition_attempts.append(
                    (thread_indexes[get_ident()], values["owner_id"])
                )
            if condition.evaluate(state):
                state.update(values)
            releasing = values["owner_id"] is None
            index = thread_indexes[get_ident()]
        if releasing:
            release_committed[index].set()
            assert allow_release_readback[index].wait(timeout=5), "readback was not resumed"

    def delta_table(session, name):
        assert session is spark
        assert name == "locks"
        return SimpleNamespace(update=atomic_update)

    def controlled_poll(seconds):
        assert 0 < seconds <= 0.25
        with changed:
            index = thread_indexes[get_ident()]
            poll_rounds[index] += 1
            polling.add(index)
            changed.notify_all()
        assert resume_polling[index].wait(timeout=5), "contender was not resumed"
        resume_polling[index].clear()
        with changed:
            polling.remove(index)
            changed.notify_all()

    def operation(index):
        nonlocal active, max_active
        with changed:
            active += 1
            max_active = max(max_active, active)
            entered.append(index)
            changed.notify_all()
        try:
            assert finish_callback[index].wait(timeout=5), "callback was not released"
            return ("2026-09-23", index)
        finally:
            with changed:
                active -= 1
                changed.notify_all()

    def run_writer(index):
        with changed:
            thread_indexes[get_ident()] = index
        return writers[index].run(lambda: operation(index))

    monkeypatch.setattr(control.ControlWriter, "_read_lock", read_lock)
    monkeypatch.setattr(control, "_delta_table", delta_table)
    monkeypatch.setattr(control, "_spark_functions", lambda: Functions)
    monkeypatch.setattr(control, "_acquisition_error_types", lambda: (DeltaError,))
    monkeypatch.setattr(
        control,
        "time",
        SimpleNamespace(monotonic=control.time.monotonic, sleep=controlled_poll),
    )

    assert len({id(writer) for writer in writers}) == 3
    results = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(run_writer, index) for index in range(3)]
        try:
            for completed in range(3):
                with changed:
                    assert changed.wait_for(
                        lambda: len(entered) == completed + 1
                        and len(polling) == 2 - completed
                        and all(poll_rounds[index] == completed + 1 for index in polling),
                        timeout=5,
                    ), "writers did not reach the expected contention phase"
                    assert active == max_active == 1
                    current = entered[-1]
                    contenders = tuple(polling)
                    assert {index for index, _ in acquisition_attempts} == {0, 1, 2}
                finish_callback[current].set()
                assert release_committed[current].wait(timeout=5)
                for index in contenders:
                    resume_polling[index].set()
                if contenders:
                    with changed:
                        assert changed.wait_for(
                            lambda: len(entered) == completed + 2
                            and len(polling) == 1 - completed
                            and all(
                                poll_rounds[index] == completed + 2 for index in polling
                            ),
                            timeout=5,
                        ), "successor did not acquire before the previous owner's readback"
                        assert state["owner_id"] is not None
                allow_release_readback[current].set()
                results.append(futures[current].result(timeout=5))
        finally:
            for event in (*finish_callback, *resume_polling, *allow_release_readback):
                event.set()

    assert max_active == 1
    assert active == 0
    assert sorted(entered) == [0, 1, 2]
    assert sorted(results) == [("2026-09-23", index) for index in range(3)]
    assert state == {"lock_name": "global", "owner_id": None, "acquired_at": None}


def test_default_timeout_stops_acquisition_at_ten_minutes(environment, monkeypatch):
    environment.row["owner_id"] = "busy"
    monkeypatch.setattr(
        control.time, "monotonic", Mock(side_effect=[0, 600, 600, 601])
    )
    with pytest.raises(TimeoutError):
        control.ControlWriter(environment.spark, "locks").run(Mock())
    assert not environment.sleeps
    assert environment.read_count == 1


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("-inf"), float("nan")])
def test_invalid_timeout_is_rejected(environment, timeout):
    with pytest.raises(ValueError, match="finite and greater than zero"):
        environment.writer(timeout)


def test_empty_table_name_is_rejected(environment):
    with pytest.raises(ValueError, match="nonempty"):
        control.ControlWriter(environment.spark, " ")


@pytest.mark.parametrize("rows", [
    [],
    [{"lock_name": "other", "owner_id": None, "acquired_at": None}],
    [{"lock_name": "global", "owner_id": None, "acquired_at": None}] * 2,
    [
        {"lock_name": "global", "owner_id": None, "acquired_at": None},
        {"lock_name": "other", "owner_id": None, "acquired_at": None},
    ],
])
def test_missing_or_duplicate_seed_fails_without_writing(environment, rows):
    environment.rows = rows
    operation = Mock()
    with pytest.raises(control.ControlLockError, match="exactly one pre-seeded"):
        environment.writer().run(operation)
    operation.assert_not_called()
    assert not environment.updates


def test_missing_table_propagates_without_seeding(environment):
    failure = LookupError("table not found")
    environment.read_error = failure
    with pytest.raises(LookupError) as caught:
        environment.writer().run(Mock())
    assert caught.value is failure
    assert not environment.updates


def test_competing_acquisition_checks_cas_result_before_callback(environment):
    winning_owner = "competing-worker"
    calls = []

    def another_worker_wins():
        environment.before_update = None
        environment.row.update(owner_id=winning_owner, acquired_at="old")

    def release_competitor():
        calls.append(environment.row["owner_id"])
        environment.row.update(owner_id=None, acquired_at=None)
        environment.on_sleep = None

    environment.before_update = another_worker_wins
    environment.on_sleep = release_competitor
    assert environment.writer().run(lambda: calls.append("operation")) is None
    assert calls == [winning_owner, "operation"]
    assert len(environment.sleeps) == 1
    assert len(environment.updates) == 3


def test_independent_writer_cannot_run_while_another_callback_owns_lock(environment):
    first = environment.writer(timeout=1)
    second = environment.writer(timeout=0.2)
    contender = Mock()

    def operation():
        owner = environment.row["owner_id"]
        with pytest.raises(TimeoutError):
            Context().run(second.run, contender)
        assert environment.row["owner_id"] == owner
        return "first completed"

    assert first.run(operation) == "first completed"
    contender.assert_not_called()
    assert second.run(lambda: "second completed") == "second completed"
    assert len(environment.updates) == 4


def test_known_occ_is_retried_only_during_acquisition(environment):
    def conflict_once():
        environment.before_update = None
        raise DeltaError("DELTA_CONCURRENT_APPEND")

    environment.before_update = conflict_once
    assert environment.writer().run(lambda: "done") == "done"
    assert len(environment.sleeps) == 1
    assert len(environment.updates) == 3


def test_repeated_occ_times_out_with_bounded_polling(environment):
    environment.before_update = Mock(side_effect=DeltaError())
    operation = Mock()
    with pytest.raises(TimeoutError, match="attempted owner_id="):
        environment.writer(timeout=0.25).run(operation)
    operation.assert_not_called()
    assert environment.now == pytest.approx(0.25)
    assert environment.sleeps == pytest.approx([0.1, 0.1, 0.05])
    assert len(environment.updates) == 3
    assert environment.jitter.call_args_list == [call(0.05, 0.25)] * 3


def test_deadline_reached_during_failed_cas_does_not_sleep_or_retry(environment):
    attempts = []

    def slow_conflict():
        attempts.append("attempt")
        assert len(attempts) == 1, "retried an acquisition past its deadline"
        environment.now = 1
        raise DeltaError()

    environment.before_update = slow_conflict
    with pytest.raises(TimeoutError):
        environment.writer(timeout=1).run(Mock())
    assert not environment.sleeps
    assert len(environment.updates) == 1


@pytest.mark.parametrize("failure", [
    RuntimeError("concurrent conflict; not a Delta OCC error"),
    DeltaError("SOME_CONCURRENT_CONFLICT"),
    DeltaError(None),
])
def test_unknown_acquisition_errors_are_not_retried(environment, failure):
    environment.before_update = Mock(side_effect=failure)
    with pytest.raises(type(failure)) as caught:
        environment.writer().run(Mock())
    assert caught.value is failure
    assert len(environment.updates) == 1
    assert not environment.sleeps


def test_orphan_lock_never_expires_and_reports_its_owner(environment):
    environment.row.update(
        owner_id="orphan-token",
        acquired_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
    )
    operation = Mock()
    with pytest.raises(TimeoutError, match="observed owner_id='orphan-token'.*No automatic takeover"):
        environment.writer(timeout=0.15).run(operation)
    operation.assert_not_called()
    assert environment.row["owner_id"] == "orphan-token"
    assert not environment.updates
    assert environment.now == pytest.approx(0.15)


@pytest.mark.parametrize("failure", [
    ValueError("operation failed"),
    DeltaError("DELTA_CONCURRENT_WRITE"),
    KeyboardInterrupt(),
])
def test_failed_operation_retains_lock_and_original_exception(environment, caplog, failure):
    caplog.set_level(logging.ERROR, logger=control.__name__)
    operation = Mock(side_effect=failure)
    with pytest.raises(type(failure)) as caught:
        environment.writer().run(operation)
    assert caught.value is failure
    operation.assert_called_once_with()
    assert len(environment.updates) == 1
    owner = environment.row["owner_id"]
    assert UUID(owner).version == 4
    assert f"owner_id={owner}" in caplog.text
    assert "lock_table=locks" in caplog.text
    assert "callback_completed=False" in caplog.text
    assert "confirm this writer cannot commit" in caplog.text


def test_ambiguous_acquisition_keeps_committed_owner(environment, caplog):
    def commit_then_fail():
        condition, values = environment.updates[-1]
        assert condition.evaluate(environment.row)
        environment.row.update({
            key: expression.evaluate(environment.row) for key, expression in values.items()
        })
        raise RuntimeError("network disconnected after commit")

    environment.before_update = commit_then_fail
    operation = Mock()
    with pytest.raises(RuntimeError, match="network disconnected"):
        environment.writer().run(operation)
    operation.assert_not_called()
    assert environment.row["owner_id"] in caplog.text
    assert len(environment.updates) == 1


@pytest.mark.parametrize("replacement", [None, "different-owner"])
def test_release_rejects_ownership_mismatch_without_clearing(environment, replacement):
    def lose_ownership():
        environment.row["owner_id"] = replacement

    with pytest.raises(control.ControlLockError, match="Cannot release.*expected owner_id="):
        environment.writer().run(lose_ownership)
    assert environment.row["owner_id"] == replacement
    assert len(environment.updates) == 1


def test_release_cas_does_not_clear_an_owner_that_changes_after_read(environment, caplog):
    def operation():
        def change_owner():
            environment.row["owner_id"] = "successor"
        environment.before_update = change_owner

    environment.writer().run(operation)
    assert environment.row["owner_id"] == "successor"
    assert len(environment.updates) == 2
    assert not caplog.records


def test_successful_release_accepts_successor_before_readback(environment, caplog):
    original_update = environment.update
    successor = environment.writer()

    def update_and_handoff(*, condition, set):
        original_update(condition=condition, set=set)
        if environment.row["owner_id"] is None:
            successor._claim("successor-token")

    environment.update = update_and_handoff
    assert environment.writer().run(lambda: "completed") == "completed"
    assert environment.row["owner_id"] == "successor-token"
    assert environment.row["acquired_at"] is not None
    assert len(environment.updates) == 3
    assert not caplog.records


def test_release_fails_when_same_owner_is_still_present(environment):
    original_update = environment.update
    owner = []

    def unchanged_release(*, condition, set):
        prior = dict(environment.row)
        original_update(condition=condition, set=set)
        if environment.row["owner_id"] is None:
            environment.row.update(prior)

    environment.update = unchanged_release
    with pytest.raises(control.ControlLockError, match="could not be verified"):
        environment.writer().run(lambda: owner.append(environment.row["owner_id"]))
    assert environment.row["owner_id"] == owner[0]
    assert len(environment.updates) == 2


def test_release_rejects_missing_acquisition_timestamp_before_cas(environment):
    def invalidate_timestamp():
        environment.row["acquired_at"] = None

    with pytest.raises(control.ControlLockError, match="Cannot release.*acquired_at=None"):
        environment.writer().run(invalidate_timestamp)
    assert environment.row["owner_id"] is not None
    assert len(environment.updates) == 1


@pytest.mark.parametrize("rows", [
    [],
    [{"lock_name": "global", "owner_id": None, "acquired_at": None}] * 2,
    [{"lock_name": "other", "owner_id": None, "acquired_at": None}],
    [{"lock_name": "global", "owner_id": "successor", "acquired_at": None}],
])
def test_release_rejects_missing_duplicate_or_inconsistent_readback(environment, rows):
    original_update = environment.update

    def corrupt_readback(*, condition, set):
        original_update(condition=condition, set=set)
        if environment.row["owner_id"] is None:
            environment.rows = [dict(row) for row in rows]

    environment.update = corrupt_readback
    with pytest.raises(control.ControlLockError, match="exactly one|could not be verified"):
        environment.writer().run(lambda: None)
    assert len(environment.updates) == 2


@pytest.mark.parametrize("failure", [RuntimeError("release response lost"), DeltaError()])
def test_ambiguous_release_error_propagates_even_if_successor_acquired(
    environment, caplog, failure
):
    original_update = environment.update
    successor = environment.writer()

    def ambiguous_release(*, condition, set):
        original_update(condition=condition, set=set)
        if environment.row["owner_id"] is None:
            successor._claim("successor-token")
            raise failure

    environment.update = ambiguous_release
    with pytest.raises(type(failure)) as caught:
        environment.writer().run(lambda: "completed")
    assert caught.value is failure
    assert environment.row["owner_id"] == "successor-token"
    assert len(environment.updates) == 3
    assert not environment.sleeps
    assert "callback_completed=True" in caplog.text


def test_release_errors_are_not_retried(environment, caplog):
    failure = DeltaError()

    def operation():
        environment.before_update = Mock(side_effect=failure)

    with pytest.raises(DeltaError) as caught:
        environment.writer().run(operation)
    assert caught.value is failure
    assert environment.row["owner_id"] is not None
    assert "callback_completed=True" in caplog.text
    assert not environment.sleeps
    assert len(environment.updates) == 2


def test_release_requires_timestamp_to_be_cleared(environment):
    original_update = environment.update

    def incomplete_clear(*, condition, set):
        original_update(condition=condition, set=set)
        if environment.row["owner_id"] is None:
            environment.row["acquired_at"] = "leftover"

    environment.update = incomplete_clear
    with pytest.raises(control.ControlLockError, match="acquired_at='leftover'"):
        environment.writer().run(lambda: None)


@pytest.mark.parametrize("different_writer", [False, True])
def test_nested_run_is_rejected_and_outer_failure_retains_lock(environment, different_writer):
    writer = environment.writer()
    inner = environment.writer() if different_writer else writer
    with pytest.raises(RuntimeError, match="Nested ControlWriter.run"):
        writer.run(lambda: inner.run(Mock()))
    assert len(environment.updates) == 1
    assert environment.row["owner_id"] is not None
    environment.row.update(owner_id=None, acquired_at=None)
    assert writer.run(lambda: "context reset") == "context reset"


def test_full_builder_is_reconstructed_only_under_lock_for_every_execution(environment):
    writer = environment.writer()
    source = object()
    condition = object()
    values = {"value": "s.value"}
    builder = (
        writer.tables.forName(environment.spark, "control_table")
        .alias("t")
        .merge(source, condition)
        .whenMatchedUpdate("s.update", set=values)
        .whenMatchedUpdateAll("s.all")
        .whenNotMatchedInsertAll("s.insert_all")
        .whenNotMatchedInsert("s.insert", values=values)
        .whenMatchedDelete("s.delete")
        .whenNotMatchedBySourceUpdate("t.orphan", set=values)
        .whenNotMatchedBySourceDelete("t.delete")
    )
    assert not environment.targets
    assert not environment.updates
    builder.execute()
    builder.execute()
    assert len(environment.targets) == 2
    first, second = environment.targets
    assert first is not second
    assert first.owner != second.owner
    assert first.name == second.name == "control_table"
    assert first.calls == second.calls == [
        ("alias", ("t",), {}),
        ("merge", (source, condition), {}),
        ("whenMatchedUpdate", (), {"condition": "s.update", "set": values}),
        ("whenMatchedUpdateAll", (), {"condition": "s.all"}),
        ("whenNotMatchedInsertAll", (), {"condition": "s.insert_all"}),
        ("whenNotMatchedInsert", (), {"condition": "s.insert", "values": values}),
        ("whenMatchedDelete", (), {"condition": "s.delete"}),
        ("whenNotMatchedBySourceUpdate", (), {"condition": "t.orphan", "set": values}),
        ("whenNotMatchedBySourceDelete", (), {"condition": "t.delete"}),
        ("execute", (), {}),
    ]
    assert environment.row["owner_id"] is None


def test_builder_branches_do_not_mutate_an_existing_recipe(environment):
    table = environment.writer().tables.forName(environment.spark, "target")
    aliased = table.alias("t")
    table.delete("id = 1")
    aliased.update(condition="id = 2", set={"value": "3"})
    assert environment.targets[0].calls == [
        ("delete", (), {"condition": "id = 1"}),
    ]
    assert environment.targets[1].calls == [
        ("alias", ("t",), {}),
        ("update", (), {"condition": "id = 2", "set": {"value": "3"}}),
    ]


def test_builder_optional_conditions_are_forwarded(environment):
    table = environment.writer().tables.forName(environment.spark, "target")
    table.merge(object(), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    table.update(set={"value": "1"})
    table.delete()
    assert environment.targets[0].calls[1:3] == [
        ("whenMatchedUpdateAll", (), {"condition": None}),
        ("whenNotMatchedInsertAll", (), {"condition": None}),
    ]
    assert environment.targets[1].calls == [
        ("update", (), {"condition": None, "set": {"value": "1"}}),
    ]
    assert environment.targets[2].calls == [("delete", (), {"condition": None})]


@pytest.mark.parametrize("method", ["execute", "update", "delete"])
def test_facade_action_failure_retains_lock(environment, method):
    environment.action_error = ValueError("target action failed")
    table = environment.writer().tables.forName(environment.spark, "target")
    with pytest.raises(ValueError, match="target action failed"):
        getattr(table, method)()
    assert environment.row["owner_id"] is not None
    assert len(environment.updates) == 1


@pytest.mark.parametrize("name", control._OCC_NAMES)
def test_python_delta_occ_types_are_recognized(name):
    error_type = type(name, (Exception,), {"__module__": "delta.exceptions"})
    assert control._is_delta_occ(error_type())


@pytest.mark.parametrize("error_class", sorted(control._OCC_ERROR_CLASSES))
def test_structured_delta_occ_error_classes_are_recognized(error_class):
    assert control._is_delta_occ(DeltaError(error_class))


@pytest.mark.parametrize("qualified_name", sorted(control._OCC_JAVA_CLASSES))
def test_java_delta_occ_types_are_recognized(qualified_name):
    error = Exception("message does not matter")
    error.java_exception = SimpleNamespace(
        getClass=lambda: SimpleNamespace(getName=lambda: qualified_name)
    )
    assert control._is_delta_occ(error)


@pytest.mark.parametrize("error", [
    Exception("DELTA_CONCURRENT_APPEND conflict"),
    DeltaError("DELTA_CONCURRENT_APPEND_UNRELATED"),
    type("ConcurrentWriteException", (Exception,), {"__module__": "other"})(),
    type("UnrelatedException", (Exception,), {"__module__": "delta.exceptions"})(),
])
def test_conflict_lookalikes_are_not_recognized(error):
    assert not control._is_delta_occ(error)


def test_unrelated_java_error_is_not_recognized():
    error = Exception("concurrent conflict")
    error.java_exception = SimpleNamespace(
        getClass=lambda: SimpleNamespace(getName=lambda: "other.ConcurrentWriteException")
    )
    assert not control._is_delta_occ(error)


def test_module_import_does_not_import_fabric_dependencies(monkeypatch):
    original_import = __import__
    attempted = []

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] in ("pyspark", "delta", "py4j"):
            attempted.append(name)
            raise AssertionError(f"Unexpected Fabric import: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", guarded_import)
    spec = importlib.util.spec_from_file_location("fabric_control_import_probe", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    assert not attempted


def test_lazy_boundaries_resolve_fabric_dependencies_only_when_called(monkeypatch):
    delta = ModuleType("delta")
    tables = ModuleType("delta.tables")
    exceptions = ModuleType("delta.exceptions")
    tables.DeltaTable = SimpleNamespace(forName=Mock(return_value="table"))
    classes = []
    for name in control._OCC_NAMES:
        cls = type(name, (Exception,), {})
        setattr(exceptions, name, cls)
        classes.append(cls)
    delta.exceptions = exceptions
    pyspark = ModuleType("pyspark")
    sql = ModuleType("pyspark.sql")
    sql.functions = object()
    errors = ModuleType("pyspark.errors")
    errors.PySparkException = type("PySparkException", (Exception,), {})
    py4j = ModuleType("py4j")
    protocol = ModuleType("py4j.protocol")
    protocol.Py4JJavaError = type("Py4JJavaError", (Exception,), {})
    for module in (delta, tables, exceptions, pyspark, sql, errors, py4j, protocol):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    spark = object()
    assert control._delta_table(spark, "table_name") == "table"
    tables.DeltaTable.forName.assert_called_once_with(spark, "table_name")
    assert control._spark_functions() is sql.functions
    assert control._acquisition_error_types() == (
        *classes, protocol.Py4JJavaError, errors.PySparkException,
    )

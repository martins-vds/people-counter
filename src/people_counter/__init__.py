"""Typed video person-counting pipelines and command-line interface."""

from people_counter import fabric_control, fabric_events
from people_counter.api import (
    LineCountRecordDict,
    PipelineConfig,
    PipelineRuntime,
    TelemetryRecord,
    line_count_records,
    load_runtime,
    run,
    run_with_runtime,
    telemetry_records,
)
from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.cpu_runtime import (
    ThreadBudget,
    calculate_thread_budget,
    configure_cpu_runtime,
)
from people_counter.fabric_control import ControlLockError, ControlWriter
from people_counter.fabric_events import (
    LeaseLostError,
    WorkerEventClient,
    process_worker_events,
)
from people_counter.models import RunResult
from people_counter.runtime import RuntimeCompatibilityError

__all__ = [
    "ControlLockError",
    "ControlWriter",
    "LeaseLostError",
    "LineCountRecordDict",
    "PipelineConfig",
    "PipelineRuntime",
    "RFDetrBotsortConfig",
    "RTDetrOsnetConfig",
    "RunResult",
    "RuntimeCompatibilityError",
    "TelemetryRecord",
    "ThreadBudget",
    "WorkerEventClient",
    "calculate_thread_budget",
    "configure_cpu_runtime",
    "fabric_control",
    "fabric_events",
    "line_count_records",
    "load_runtime",
    "process_worker_events",
    "run",
    "run_with_runtime",
    "telemetry_records",
]
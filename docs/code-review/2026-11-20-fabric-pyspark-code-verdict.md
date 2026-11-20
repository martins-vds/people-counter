# PySpark Performance Review: Fabric SJD Runtime
# review date: 2026-11-20

# Quick verdict

| Hypothesis | Severity | Confidence | Reason |
|---|---:|---:|---|
| One exact-compatibility Python UDF remains in the Gold hot path. | MEDIUM | High | Four former row UDFs were removed. `_parse_committed_records` remains per published work because replacing it with Spark `to_json` would change the authenticated canonical JSON bytes and SHA-256 values. It is distributed, but still serializes one grouped work record through Python. |
| Main process and control paths are Spark-distributed and bounded. | LOW | High | Process work uses `mapPartitions`, receipts are bounded, attempt records stream, normal control mutations use filtered Delta merge, fallback assets do not cross the driver as bytes, and retention removes obsolete unpublished state. |

# Code smells detected

| Smell | Severity | Exact references |
|---|---:|---|
| Resolved: full control-table Python read/overwrite. | LOW | [`FabricControlStoreImpl`](../../src/people_counter/fabric_control_store.py:288) bootstraps typed tables, filters normal reads, and performs keyed Delta merge/readback. |
| Resolved: full attempt-output collection and driver reconstruction. | LOW | [`sjd_process.py`](../../src/people_counter/sjd_process.py:1502) collects only bounded receipt metadata; record readers and validation use `toLocalIterator` and incremental hashes. |
| Resolved: cluster-wide probe thread counts. | LOW | [`fabric_sjd_runtime.py`](../../src/people_counter/fabric_sjd_runtime.py:147) uses the minimum per-executor safe slot count. Identical signed probe evidence is reused. |
| Resolved: serial fallback asset-byte staging on the driver. | LOW | [`fabric_sjd_runtime.py`](../../src/people_counter/fabric_sjd_runtime.py) now publishes content-addressed aliases and deduplicates them; executors read and cache assets, while the driver handles metadata only. |
| Resolved: one path read and Delta union per committed attempt. | LOW | Process publication maintains partitioned `committed_records`; [`fabric_gold.py`](../../src/people_counter/fabric_gold.py:348) joins that shared table directly. Existing committed attempts require an explicit rerun/backfill before Gold. |
| Resolved: four Gold Python UDF boundaries. | LOW | Checkpoint versions and operational projections now use Spark JSON/array/struct expressions. Focused plans show no Python evaluation for those paths. |
| Residual compatibility boundary: one UDF authenticates committed output. | MEDIUM | [`_parse_committed_records`](../../src/people_counter/fabric_gold.py:2539) validates columns, computes byte-exact canonical SHA-256, and builds the historical `CommittedOutput` JSON shape. It is one call per published work, not one call per raw telemetry row. |
| Residual bounded compatibility APIs. | LOW | Compatibility methods such as `committed_outputs()` and `read_records()` can still return Python lists. Native Fabric process/Gold paths use shared Delta tables and streaming contracts. |
| Resolved: no explicit retention policy. | LOW | [`fabric_retention.py`](../../src/people_counter/fabric_retention.py) enforces dry-run default, 7-day minimum age, bounded deletion counts, and preserves active leases, published outputs, provenance, and Delta history. |

# Recommendations

| Priority | Severity | Recommendation |
|---:|---:|---|
| 1 | MEDIUM | Keep `_parse_committed_records` only behind an explicit compatibility contract. Add a second native schema version before removing it, with byte-identical digest fixtures for old attempts. |
| 2 | LOW | Monitor Python task time and driver GC for the remaining authenticated committed-record UDF. This is one bounded task per work, not a per-record raw-data Python path. |
| 3 | LOW | Run retention as an operator dry run first, then apply it on an approved schedule. Keep Delta VACUUM disabled. |
| 4 | LOW | Capture Spark UI metrics for live Fabric: task-duration distribution, spill, shuffle, executor RSS, Python worker count, and driver GC. |

# Distributed correctness / parallelism checks

| Check | Severity | Result |
|---|---:|---|
| Main inference | LOW | Uses Spark `mapPartitions`; no RDD-to-DataFrame round trip and no Python thread/process pool runs inference. |
| Control state | LOW | Normal transaction reads are filtered and writes use Delta merge. Full global reads are limited to reconciliation and singleton lock checks. |
| Process output | LOW | Receipt `collect` is partition-bounded. Attempt records stream through `toLocalIterator`; publication writes a shared partitioned Delta table. |
| Gold joins and aggregation | LOW | Dates, fact rows, operations, dimensions, validation, and checkpoint hashes use Spark-native DataFrame/SQL expressions. |
| Exact committed-output authentication | MEDIUM | One Python UDF remains per grouped work. It is required to preserve historical canonical JSON bytes and output digest compatibility. |
| Fallback localization | LOW | Driver handles only envelope metadata and alias registration. Executors read, hash, and cache deduplicated content-addressed files. |
| Retention | LOW | Dry-run by default; active and published data is protected; deletion counts and age are bounded; no VACUUM is performed. |
| Spill and skew | MEDIUM | No live Spark UI evidence was available. Static review cannot confirm spill, skew, executor memory, or driver GC. |

## Immediate code changes completed

1. Added shared `committed_records`, partitioned by `batch_id`, with idempotent process publication.
2. Removed Gold per-attempt path collection and union planning.
3. Removed four of five Python UDF boundaries.
4. Added streaming attempt reads and incremental validation.
5. Added distributed, content-addressed fallback localization.
6. Added bounded, dry-run-by-default retention.
7. Added tests for executor-safe probe concurrency, ONNX profile selection, and shared-table fallback behavior.

## Evidence required for production sign-off

1. One representative Fabric run with Spark UI stage metrics and driver/executor RSS.
2. One retention dry-run and operator-approved apply run on a non-production environment.
3. Historical committed-attempt backfill completion before enabling shared-table-only Gold reads.

## Validation

| Check | Result |
|---|---|
| Integrated Fabric suite | 434 passed and 97 subtests; one Spark test failed only because `/tmp` was full. After deleting generated mutation/Spark caches, that exact test passed with 4 subtests. Combined verified result: 435 tests and 101 subtests passed. |
| CRAP | Focused changed-module coverage and CRAP runs completed during each track. Latest broad CRAP pass before the final schema table reported 426 passing tests; the final integration reached 434 passing before the disk-only failure. |
| Mutation | Control `_replace*`: 59 killed, 0 survived. Probe gaps received explicit tests. Broader process/Gold mutation runs were incomplete because of runtime and cache/stats limits. |
| Spark-native plans | Gold checkpoint, operational projection, and native aggregation tests found no `BatchEvalPython`; one deliberate compatibility UDF remains. |

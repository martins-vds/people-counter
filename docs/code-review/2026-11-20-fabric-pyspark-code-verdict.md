# PySpark Performance Review: Fabric SJD Runtime
# review date: 2026-11-20

# Quick verdict

| Hypothesis | Severity | Confidence | Reason |
|---|---:|---:|---|
| The reviewed Fabric runtime is Spark-distributed and bounded. | LOW | High | Inference uses `mapPartitions`; control writes use filtered Delta merges; attempt output streams; fallback assets are content-addressed; Gold has no Python UDF or per-attempt Delta union. |
| Live spill, skew, and memory behavior remain unverified. | MEDIUM | High | Static plans prove no known driver/Python bottleneck in the reviewed paths, but they do not prove real executor RSS, shuffle spill, task skew, or driver GC behavior. |

# Code smells detected

| Smell | Severity | Exact references |
|---|---:|---|
| Resolved: full control-table Python read/overwrite. | LOW | [`FabricControlStoreImpl`](../../src/people_counter/fabric_control_store.py:288) filters transactional reads and performs keyed Delta merge/readback. |
| Resolved: full attempt-output collection and reconstruction. | LOW | [`sjd_process.py`](../../src/people_counter/sjd_process.py:1502) collects only bounded receipt metadata; record processing streams. |
| Resolved: cluster-wide probe thread count. | LOW | [`fabric_sjd_runtime.py`](../../src/people_counter/fabric_sjd_runtime.py) uses the minimum per-executor safe slot count and reuses signed decisions. |
| Resolved: driver-byte fallback localization. | LOW | Executors read and cache deduplicated content-addressed aliases; the driver handles metadata only. |
| Resolved: per-attempt Delta unions in Gold. | LOW | Process publishes a shared `committed_records` table partitioned by `batch_id`; Gold reads it directly. |
| Resolved: final Gold Python UDF. | LOW | Publication now materializes canonical, versioned `output_json` in `committed_records`. [`FabricCommittedSource.committed_output_frame`](../../src/people_counter/fabric_gold.py:293) validates and parses that JSON with Spark-native expressions. |
| Residual: compatibility APIs can return Python lists. | LOW | `committed_outputs()` and attempt `read_records()` retain list contracts for compatibility. Native Fabric process and Gold paths avoid those APIs. |
| Resolved: no retention policy. | LOW | Retention is dry-run by default, bounded by age/count, and preserves active, published, and provenance data. |

# Recommendations

| Priority | Severity | Recommendation |
|---:|---:|---|
| 1 | LOW | Keep schema version `1` immutable. A future output shape must create a new version and prove byte/digest compatibility for backfill. |
| 2 | LOW | Before production cutover, complete committed-attempt backfill so every visible `committed_records` row has non-null `output_schema_version` and `output_json`. |
| 3 | LOW | Run retention in dry-run mode, review the plan, then apply with operator approval. Keep Delta VACUUM disabled. |
| 4 | MEDIUM | Capture live Spark UI task duration, spill, shuffle, executor RSS, Python worker count, and driver GC for one representative production batch. |

# Distributed correctness / parallelism checks

| Check | Severity | Result |
|---|---:|---|
| Inference | LOW | Spark `mapPartitions`; no Python thread/process pool, pandas UDF, or per-row executor serialization. |
| Control | LOW | Filtered reads, keyed Delta merge, bounded exact readback; global reads are limited to reconciliation and singleton lock checks. |
| Process publication | LOW | Bounded receipts; streaming record reads; idempotent shared-table merge with materialized output JSON. |
| Gold | LOW | No `F.udf` or `BatchEvalPython`. Shared-table join, validation, JSON parsing, and aggregation stay in Spark. |
| Fallback localization | LOW | Driver publishes deduplicated aliases; executors perform asset reads and hashing. |
| Retention | LOW | Age/count bounded, fail-closed, dry-run first, no VACUUM. |
| Spill and skew | MEDIUM | Not measured. Live Spark evidence is still required. |

## Immediate code changes completed

1. Added versioned `output_json` to `committed_records`.
2. Materialized exact canonical output once during process validation/publication.
3. Backfilled null materialization through idempotent process publication.
4. Removed the final Gold Python UDF and its `BatchEvalPython` plan.
5. Added shared-table schema migration and fail-closed output validation.

## Evidence required for production sign-off

1. Complete historical committed-attempt backfill and verify zero null `output_json` values.
2. Capture one representative live Fabric Spark UI run and driver/executor RSS.
3. Run one retention dry-run and review the deletion plan.

## Validation

| Check | Result |
|---|---|
| Focused process/Gold/control suite | 198 tests passed, 101 subtests passed. |
| CRAP | Process coverage 94%; Gold coverage 88%. The changed scopes introduced no top-20 CRAP regression; unrelated uncovered repository functions still exceed threshold 30. |
| Native Spark plan | Tests assert no `BatchEvalPython` in materialized Gold plans. Source contains no `F.udf`. |
| Mutation | Scoped run was attempted. Initial stats required optional `torch`; narrowed stats ran but exceeded the practical mutation window, so no new mutation result is claimed. |

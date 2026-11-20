# PySpark Performance Review: Fabric Gold
# review date:2026-11-20
# Quick verdict: checkpoint and operational Python UDFs

| Quick verdict | Severity score | Reason |
| --- | --- | --- |
| Four of five remaining Python UDF boundaries were removed. The committed-record parser remains for byte-exact digest compatibility. | HIGH | Checkpoint versions now use versioned Spark-native `to_json` canonicalization; operational work and attempt projections use native JSON, array, struct, join, and cast expressions. |

# code smells detected

| Code smell detected | Severity score | Reference |
| --- | --- | --- |
| Row-wise Python serialization previously ran for table checkpoint versions. | HIGH | Former `_frame_version` lambda UDF and `_frame_checksum` `_row_canonical` UDF. |
| Row-wise Python UDFs previously materialized operational projections. | HIGH | Former `_operational_work_json` and `_operational_attempt_json` UDFs. |
| Committed-record parsing retains Python execution. | HIGH | `FabricCommittedSource.committed_output_frame`: `F.udf(_parse_committed_records, "string")`. |
| The non-shared-table fallback can still collect committed pointer paths to the driver. | MEDIUM | `FabricCommittedSource._record_frame` fallback before the `committed_records` table branch. |

# recommendations

| Priority | Severity score | Recommendation |
| --- | --- | --- |
| 1 | HIGH | Keep checkpoint digests under the explicit `spark-json-v2:` version prefix. An unversioned checkpoint digest must not be compared with the new Spark-native representation. |
| 2 | HIGH | Keep operational projections native and retain the parity test against `_operational_rows`. |
| 3 | HIGH | Use the shared `committed_records` table in production to avoid pointer collection and independent Delta-source unions. |
| 4 | MEDIUM | Replace `_parse_committed_records` only after a committed-output schema version proves byte-identical digest behavior. |
| 5 | LOW | Capture production Spark UI evidence before tuning shuffle, skew, or memory settings. |

# Distributed correctness / parallelism checks

| Pattern | Severity score | Result |
| --- | --- | --- |
| Checkpoint canonicalization | LOW | Native `to_json`, `array_sort`, `collect_list`, and `sha2` execute in Spark. |
| Operational work projection | LOW | No `BatchEvalPython`; parity is checked against the historical Python projection. |
| Operational attempt projection | LOW | No `BatchEvalPython`; `records_json`, committed run JSON, and joined work metadata are projected with Spark expressions. |
| Committed record parser | HIGH | One retained Python UDF remains, after the grouped record shuffle, for canonical digest authentication and dynamic JSON validation. |
| Shared-record-table path | LOW | Records join to visible work pointers in Spark. |
| Adapter fallback | MEDIUM | The old adapter path still collects pointer rows and reads one attempt path per pointer on the driver. |
| Python thread/process pools or RDD Python loops | LOW | None introduced. |

# retained UDF

| Retained UDF | Severity score | Concrete technical blocker |
| --- | --- | --- |
| `_parse_committed_records` | HIGH | `output_sha256` authenticates `json.dumps` bytes with sorted keys, ASCII escaping, compact separators, arbitrary nested fields, and Python timestamp/datetime normalization. Spark `to_json` cannot be assumed byte-equivalent for this dynamic input contract; replacing it would invalidate existing committed outputs. A new explicit committed-output schema version is required before migration. |

# validation

| Check | Result |
| --- | --- |
| Focused UDF/parity tests | 3 passed. |
| Integrated native aggregation tests | 2 passed with the package available to Spark workers. |
| Static diagnostics | No Pylance diagnostics in the changed production and test files. |
| CRAP | The focused tests passed without coverage; CRAP could not collect coverage because NumPy failed to reload while pytest-cov initialized in this environment. No CRAP score is claimed. |
| Mutation testing | Incomplete: mutant generation did not finish after the bounded run and was stopped; no mutation status is claimed. |

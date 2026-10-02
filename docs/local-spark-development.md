# Local Spark development

This slice runs one client-deploy-mode Spark application per claimed batch, not
one application per video. The driver alone owns the SQLite queue. Whole videos
are distributed with `mapPartitions`; each executor process reuses the existing
`SdkRuntimeProcessor` cache in SDK mode. Results first land in immutable,
batch-attempt-scoped local Delta staging.

## Runtime pins

- Base image:
  `apache/spark:4.1.1-scala2.13-java21-python3-ubuntu@sha256:0d4eebff1893f01a24ba1a45c3ee12705264ea5030aef9155547f7062ede0387`
- Apache Spark 4.1.1, Scala 2.13, and Java 21 come from that image.
- The official image contains Python 3.10. The project image deliberately
  installs managed CPython 3.13.9 for both the driver and executors.
- PySpark 4.1.1 and `delta-spark` 4.2.0 are exact uv locks. Delta 4.2.0 declares
  `pyspark>=4.0.1,<=4.1.1`.
- `io.delta:delta-spark_2.13:4.2.0` and its pinned Maven dependency graph are
  resolved during image build and copied into `/opt/spark/jars`. Runtime never
  uses `--packages`.
- FFmpeg, the image's native libraries, the project, and the CPU SDK dependency
  extra are baked into the same image used by master, both workers, client, and
  optional history server.

The build records the source release in
`PEOPLE_COUNTER_RELEASE_DIGEST`. The driver logs Python, Java, Spark, Scala,
Delta, release, and image identity. The built image ID must be passed through
`PEOPLE_COUNTER_IMAGE_DIGEST` after a local build, as shown below.

## Architecture and boundaries

`ContentAddressedStore` confines paths to one root, verifies SHA-256, and
publishes fsynced temporary files atomically. Existing equal content is
idempotent; different bytes at the same immutable address are a conflict.

`SQLiteQueue` uses WAL, `synchronous=FULL`, a bounded busy timeout, and
`BEGIN IMMEDIATE`. It implements idempotent enqueue, bounded PeekLock claims,
fenced renewal/completion, abandon and delayed retry, lease-expiry recovery,
delivery limits, dead lettering, and all-or-nothing minimum claim cardinality.
All writes run through one serialized control actor with bounded startup,
operation, and shutdown waits. Only `spark-client` mounts
`/var/lib/people-counter/control`; workers cannot open the database.

The claimed-work manifest contains JSON primitive values and is stored by
content hash. The Spark job verifies the manifest digest, verifies each source
hash, processes whole videos, emits executor host/ID and runtime identity, then
writes Delta with `errorifexists` under:

```text
/data/output/delta-staging/<batch-id>/<batch-attempt-id>
```

The driver renews all queue fences while Spark blocks. Every staged row carries
its batch, batch attempt, manifest digest, work attempt, and expected staging
path. The driver validates those identities, exactly one terminal per claim,
terminal-only executor placement, and failed terminals before atomically
completing successes or abandoning failures. Expired locks are recovered on the
next claim or by the explicit `recover` command.

Spark event logs are written to `/data/output/spark-events`. Python control logs
are one JSON object per line and carry correlation IDs. Spark application names
and configuration include the same correlation ID.

## Exact local commands

All published UIs bind only to loopback. Port 7077 is internal and is never
published.

Prepare writable output and configuration:

```bash
cp .env.example .env
mkdir -p local-data/output offline-models
chmod 0777 local-data/output
export PEOPLE_COUNTER_RELEASE_DIGEST="$(git rev-parse HEAD)"
```

Build the one shared image and record its immutable local ID:

```bash
docker compose build
export PEOPLE_COUNTER_IMAGE_DIGEST="$(
  docker image inspect people-counter-spark:4.1.1-delta4.2.0 \
    --format '{{.Id}}'
)"
```

Start master, two one-core workers, and the durable client:

```bash
docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 spark-client
```

Show service health and queue status:

```bash
docker compose ps
docker compose exec -T spark-client people-counter-local status
```

Seed two immutable claims and run the fast no-model smoke:

```bash
docker compose exec -T spark-client people-counter-local seed \
  --video /data/samples/three_people_walking.mp4 \
  --idempotency-prefix smoke-001 \
  --copies 2
docker compose exec -T spark-client people-counter-local submit \
  --mode probe \
  --max-items 2 \
  --minimum-workers 2
```

The submit JSON must show two distinct `executor_identities`. Probe mode still
uses the production `process_video_partition` loop but substitutes a
deterministic `RunResult`; it performs no model, PyPI, or Maven download.

Run SDK mode only after offline models are present under `offline-models/`:

```bash
docker compose exec -T spark-client people-counter-local seed \
  --video /data/samples/three_people_walking.mp4 \
  --idempotency-prefix sdk-001 \
  --copies 2 \
  --models-dir /data/models
docker compose exec -T spark-client people-counter-local submit \
  --mode sdk \
  --max-items 4
```

Ordinary submit defaults to one required worker. SDK batches use bounded
partitions (at most two whole videos per partition), so compatible videos can
reuse one executor-local `SdkRuntimeProcessor` cache while each video remains
sequential. Use `--minimum-workers 2` only for an explicit placement canary;
with two items that deliberately creates two physical partitions.

Start the optional history server and inspect logs:

```bash
docker compose --profile history up -d --wait spark-history
docker compose logs --since=10m spark-client spark-master spark-worker-1 spark-worker-2
```

UI addresses are `http://127.0.0.1:8080` for the master,
`http://127.0.0.1:8081` and `:8082` for workers, `:4040` for an active driver,
and `http://127.0.0.1:18080` for history.

Stop only this Compose project:

```bash
docker compose down
```

Reset the local control volume, worker scratch, event logs, and staging:

```bash
docker compose down --volumes
rm -rf \
  local-data/output/delta-staging \
  local-data/output/local-content \
  local-data/output/spark-events
```

The reset is intentionally explicit and scoped to `local-data/output`.

## Fast inner loop

Pure-Python contracts do not need Docker:

```bash
uv run pytest -q \
  tests/test_local_storage.py \
  tests/test_local_queue.py \
  tests/test_local_spark.py \
  tests/test_local_control.py \
  tests/test_local_cli.py
```

The marked Compose integration test builds a uniquely tagged current-source
image, verifies its release label and staged release identity, and proves exact
placement on `spark-worker-1` and `spark-worker-2` before and after an idle
worker restart:

```bash
RUN_COMPOSE_TESTS=1 uv run pytest -q -m compose tests/test_local_compose.py
```

## Failure injection

Interrupt one executor during a probe:

```bash
docker compose exec -T spark-client people-counter-local seed \
  --video /data/samples/three_people_walking.mp4 \
  --idempotency-prefix worker-loss \
  --copies 2 \
  --probe-delay-seconds 20
docker compose exec -T spark-client people-counter-local submit \
  --max-items 2 --minimum-workers 2 &
docker compose restart spark-worker-2
wait
```

Spark may retry the task on the remaining worker. Placement validation still
requires two final identities, so a one-worker result cannot be committed.

Exercise process reopen and lease recovery by first starting a long probe,
waiting until its claim is observably locked, then stopping the client that
owns the active driver. After the 10-second lease expires, reopen the client,
recover the lock, and submit the ready work again:

```bash
docker compose exec -T spark-client people-counter-local seed \
  --video /data/samples/three_people_walking.mp4 \
  --idempotency-prefix client-loss \
  --copies 1 \
  --probe-delay-seconds 30
docker compose exec -T spark-client people-counter-local submit \
  --max-items 1 \
  --lock-seconds 10 \
  --heartbeat-seconds 2 \
  >local-data/client-loss-submit.log 2>&1 &
submit_pid=$!
until docker compose exec -T spark-client people-counter-local status \
  --state locked | python -c \
  'import json,sys; raise SystemExit(not json.load(sys.stdin)["messages"])'
do
  sleep 0.5
done
docker compose stop spark-client
wait "${submit_pid}" || true
sleep 11
docker compose start spark-client
docker compose exec -T spark-client people-counter-local recover
docker compose exec -T spark-client people-counter-local submit \
  --max-items 1 \
  --lock-seconds 10 \
  --heartbeat-seconds 2
docker compose exec -T spark-client people-counter-local status
```

Attempt directories are immutable. A retry gets a new batch attempt instead of
overwriting ambiguous staging. The idle worker restart in the Compose smoke
proves post-restart placement availability; it does not prove in-flight worker
or driver crash recovery. The recipe above is bounded process-reopen evidence,
but an interrupted local Spark application can still leave executor work or an
immutable incomplete staging attempt behind.

## Limitations and later adapter seam

- This is single-host development infrastructure, not a highly available
  control plane.
- The Compose smoke does not inject an in-flight executor failure. Spark task
  retry and cleanup after executor loss remain outside this deterministic
  smoke.
- Local filesystem Delta is not OneLake and does not prove OneLake commit,
  identity, URI, or concurrency behavior.
- Spark uses client deploy mode because SQLite and fencing stay with the durable
  client container.
- There is no metastore, Service Bus emulator, gold-table implementation,
  garbage collector, or remote upload API.
- The queue and content store are local adapters. A later Fabric/Service Bus
  adapter can supply the same primitive claimed-work manifest to
  `process_claimed_batch`.
- Do **not** claim Fabric portability until a Spark Job Definition canary passes
  with the same runtime digest, manifest, executor behavior, Delta staging
  contract, and OneLake paths.

#!/usr/bin/env bash
set -euo pipefail

role="${1:-client}"
mkdir -p /data/output/spark-events

case "${role}" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080
    ;;
  worker)
    : "${SPARK_WORKER_WEBUI_PORT:?SPARK_WORKER_WEBUI_PORT is required}"
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
      --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
      spark://spark-master:7077
    ;;
  history)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
    ;;
  client)
    exec sleep infinity
    ;;
  *)
    exec "$@"
    ;;
esac

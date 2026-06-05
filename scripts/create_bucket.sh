#!/usr/bin/env bash
# Provision the Delta Lake bucket in LocalStack. Run once (idempotent).
# delta-rs does not create buckets — the bucket must exist before the first write.
set -euo pipefail

BUCKET="${DELTA_BUCKET:-ohlcv}"

docker exec localstack awslocal s3 mb "s3://${BUCKET}" 2>/dev/null \
  && echo "created s3://${BUCKET}" \
  || echo "s3://${BUCKET} already exists"

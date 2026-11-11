#!/usr/bin/env bash
# Starts RustFS (an S3-compatible server) on port 9000 and creates the test bucket.
# Credentials are throwaway values for a local container.
set -euo pipefail
docker run -d --name kv0-s3 -p 9000:9000 \
  -e RUSTFS_ACCESS_KEY=minioadmin -e RUSTFS_SECRET_KEY=minioadmin rustfs/rustfs:latest >/dev/null
for _ in $(seq 1 30); do
  curl -fsS http://127.0.0.1:9000/health >/dev/null 2>&1 && break
  sleep 1
done
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
  aws --endpoint-url http://127.0.0.1:9000 s3 mb "s3://${KV0_TEST_S3_BUCKET:-kv0-test}"

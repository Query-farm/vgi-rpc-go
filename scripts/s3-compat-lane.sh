#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# The vgirpc/s3 S3-compatibility lane: runs TestS3Compat* against RustFS over
# plain http (path style) and over TLS (a throwaway self-signed certificate),
# with static keys and with an STS AssumeRole session token. Fails if any of
# those tests is skipped or missing, so a lane that silently lost its server
# cannot pass.
#
# Needs docker and openssl. Ports: S3_COMPAT_HTTP_PORT (39591) and
# S3_COMPAT_TLS_PORT (39593).
set -euo pipefail

IMAGE=${RUSTFS_IMAGE:-ghcr.io/rustfs/rustfs@sha256:29c02251c085cb04edce556304a9ec0f8fba0c40300266cf4f3d953783fe2450}
HTTP_PORT=${S3_COMPAT_HTTP_PORT:-39591}
TLS_PORT=${S3_COMPAT_TLS_PORT:-39593}
ACCESS=vgis3compat
SECRET=vgis3compatsecret
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
NAME=vgi-s3compat-$$

cleanup() {
  docker rm -f "$NAME-http" "$NAME-tls" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
  -keyout "$WORK/rustfs_key.pem" -out "$WORK/rustfs_cert.pem" 2>/dev/null
chmod 755 "$WORK"
chmod 644 "$WORK"/*.pem

common=(-e RUSTFS_ACCESS_KEY=$ACCESS -e RUSTFS_SECRET_KEY=$SECRET -e RUSTFS_CONSOLE_ENABLE=false)
docker run -d --name "$NAME-http" -p "127.0.0.1:$HTTP_PORT:9000" "${common[@]}" "$IMAGE" >/dev/null
docker run -d --name "$NAME-tls" -p "127.0.0.1:$TLS_PORT:9000" -v "$WORK:/certs:ro" \
  -e RUSTFS_TLS_PATH=/certs "${common[@]}" "$IMAGE" >/dev/null

for i in $(seq 1 60); do
  if curl -fs -o /dev/null "http://127.0.0.1:$HTTP_PORT/health" &&
     curl -fsk -o /dev/null "https://127.0.0.1:$TLS_PORT/health"; then
    break
  fi
  [ "$i" = 60 ] && { docker logs "$NAME-http" | tail -20; docker logs "$NAME-tls" | tail -20; echo "RustFS did not start"; exit 1; }
  sleep 1
done

status=0
for ep in "http://127.0.0.1:$HTTP_PORT" "https://127.0.0.1:$TLS_PORT"; do
  echo "== vgirpc/s3 against RustFS at $ep"
  if ! out=$(cd "$ROOT/vgirpc/s3" && AWS_CA_BUNDLE="$WORK/rustfs_cert.pem" SSL_CERT_FILE="$WORK/rustfs_cert.pem" \
      VGI_S3_TEST_ENDPOINT="$ep" VGI_S3_TEST_BUCKET=vgi-s3compat \
      VGI_S3_TEST_ACCESS_KEY=$ACCESS VGI_S3_TEST_SECRET_KEY=$SECRET \
      go test -run 'TestS3Compat' -count=1 -v . 2>&1); then
    echo "$out"
    status=1
    continue
  fi
  echo "$out" | grep -E -- '^(--- |ok )'
  if echo "$out" | grep -q -- '--- SKIP'; then
    echo "FAIL: S3-compatibility tests were skipped"
    status=1
  fi
  passed=$(echo "$out" | grep -c -- '--- PASS: TestS3Compat' || true)
  if [ "$passed" -lt 5 ]; then
    echo "FAIL: only $passed S3-compatibility tests passed (want 5)"
    status=1
  fi
done
exit $status

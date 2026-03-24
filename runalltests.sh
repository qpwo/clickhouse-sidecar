#!/usr/bin/env bash
set -euo pipefail

# Clean old state
rm -rf example-db stress-py-db stress-node-db poc-db-* debug-db
pkill -9 clickhouse || true

export PYTHONPATH="$(pwd)/python"

echo "=== Python Example ==="
python3 python/example/main.py

echo -e "\n=== Python Stress Test ==="
python3 python/stresstest/stress.py

echo -e "\n=== Python PoC: Zombie Lease (False Positive) ==="
python3 python/stresstest/poc_false_positive.py

echo -e "\n=== Python PoC: Premature Shutdown (False Negative) ==="
python3 python/stresstest/poc_false_negative.py

echo -e "\n=== Setup Node Library ==="
(cd node/clickhouse-sidecar && npm i @clickhouse/client)

echo -e "\n=== Node.js Example ==="
(cd node/example && npm i && npm run start) || { cat example-db/daemon.log 2>/dev/null; exit 1; }

echo -e "\n=== Node.js Stress Test ==="
(cd node/stresstest && npm i && npm run start) || { cat stress-node-db/daemon.log 2>/dev/null; exit 1; }

echo -e "\n=== Node.js PoC: Zombie Lease (False Positive) ==="
(cd node/stresstest && node --expose-gc poc-false-positive.mjs) || { cat poc-db-1/daemon.log 2>/dev/null; exit 1; }

echo -e "\n=== Node.js PoC: Premature Shutdown (False Negative) ==="
(cd node/stresstest && node poc-false-negative.mjs) || { cat poc-db-2/daemon.log 2>/dev/null; exit 1; }

echo -e "\nAll tests completed successfully."

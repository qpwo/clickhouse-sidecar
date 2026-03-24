#!/usr/bin/env bash
set -euo pipefail

# Clean old state
rm -rf example-db stress-py-db stress-node-db poc-db-* debug-db
pkill -9 clickhouse || true

export PYTHONPATH="$(pwd)/py-lib"

echo "=== Python Example ==="
python3 py-example/main.py

echo -e "\n=== Python Stress Test ==="
python3 py-stress/stress.py

echo -e "\n=== Python PoC: Zombie Lease (False Positive) ==="
python3 py-stress/poc_false_positive.py

echo -e "\n=== Python PoC: Premature Shutdown (False Negative) ==="
python3 py-stress/poc_false_negative.py

echo -e "\n=== Setup Node Environment ==="
npm i @clickhouse/client tsx @types/node typescript

echo -e "\n=== Node.js Example ==="
(cd js-example && npx tsx basic.ts) || { cat example-db/daemon.log 2>/dev/null; exit 1; }

echo -e "\n=== Node.js Stress Test ==="
(cd js-stress && node test.mjs) || { cat ../stress-node-db/daemon.log 2>/dev/null; exit 1; }

echo -e "\n=== Node.js PoC: Zombie Lease (False Positive) ==="
(cd js-stress && node --expose-gc poc-false-positive.mjs) || { cat ../poc-db-1/daemon.log 2>/dev/null; exit 1; }

echo -e "\n=== Node.js PoC: Premature Shutdown (False Negative) ==="
(cd js-stress && node poc-false-negative.mjs) || { cat ../poc-db-2/daemon.log 2>/dev/null; exit 1; }

echo -e "\nAll tests completed successfully."

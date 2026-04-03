#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="$(pwd)/python-pkg"

echo "=== install python dep ==="
pip3 install clickhouse-connect

echo
echo "=== install node deps ==="
npm i

echo
echo "=== node sidecar e2e ==="
node test_node_e2e.mjs

echo
echo "=== python sidecar e2e ==="
python3 test_python_e2e.py

echo
echo "=== tiny fullstack app e2e ==="
node test_fullstack_app.mjs

echo
echo "all tests passed"

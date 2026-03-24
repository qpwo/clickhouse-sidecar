#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="$(pwd)"

echo "=== Running Basic Test ==="
python3 tests/test_basic.py

echo -e "\n=== Running Stress Test ==="
python3 tests/test_stress.py

echo -e "\n=== Running PoC: Zombie Lease (False Positive) ==="
python3 tests/poc_false_positive.py

echo -e "\n=== Running PoC: Premature Shutdown (False Negative) ==="
python3 tests/poc_false_negative.py

echo -e "\nAll tests completed successfully."

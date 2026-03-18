#!/usr/bin/env bash
set -euo pipefail

echo "=== Running Stress Test ==="
node stresstest/test.mjs

echo -e "\n=== Running PoC: Zombie Lease (False Positive) ==="
node --expose-gc stresstest/poc-false-positive.mjs

echo -e "\n=== Running PoC: Premature Shutdown (False Negative) ==="
node --expose-gc stresstest/poc-false-negative.mjs

echo -e "\nAll tests completed successfully."

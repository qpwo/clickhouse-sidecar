#!/usr/bin/env bash
set -euo pipefail

echo "=== Running Stress Test ==="
node stresstest/test.mjs

echo -e "\n=== Running PoC: False Positive ==="
node --expose-gc stresstest/poc-false-positive.mjs

echo -e "\n=== Running PoC: False Negative ==="
node --expose-gc stresstest/poc-false-negative.mjs

echo -e "\n=== Running PoC: Premature Shutdown ==="
node --expose-gc stresstest/poc-premature.mjs

echo -e "\n=== Running PoC: Zombie Lease ==="
node --expose-gc stresstest/poc-zombie.mjs

echo -e "\nAll tests completed."

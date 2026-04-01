#!/bin/bash
# enable-mglru.sh
set -eu -o pipefail

echo 'y' | sudo tee /sys/kernel/mm/lru_gen/enabled > /dev/null

# Check that it was successfully enabled.
# Different kernels may expose different bitmasks (e.g., 0x0001, 0x0007).
lru_val=$(cat /sys/kernel/mm/lru_gen/enabled)
[[ "$lru_val" != "0x0000" ]]

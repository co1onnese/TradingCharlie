#!/bin/bash
set -euo pipefail
source .venv/bin/activate
export USERNAME="charlie"

python3 charlie_tr1_flow.py run \
    --tickers AAPL \
    --as_of_date 2024-06-15 \
    --variation_count 3 \
    --seed 1234

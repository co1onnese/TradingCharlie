#!/bin/bash
# run_pipeline.sh
#
# Run Charlie-TR1-DB pipeline with sample data (AAPL)
#
# Usage:
#   ./scripts/run_pipeline.sh         # Run pipeline normally
#   ./scripts/run_pipeline.sh --clean # Clean all data first, then run pipeline

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check for --clean flag
if [ "${1:-}" == "--clean" ]; then
    echo "Running cleanup before pipeline..."
    "${SCRIPT_DIR}/scripts/clean_all.sh" -y
    echo ""
fi

# Activate virtual environment
source .venv/bin/activate
export USERNAME="charlie"

# Run the pipeline
python3 charlie_tr1_flow.py run \
    --tickers AAPL \
    --as_of_date 2024-06-15 \
    --variation_count 3 \
    --seed 1234

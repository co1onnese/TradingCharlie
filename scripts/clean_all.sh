#!/bin/bash
# clean_all.sh
#
# Complete cleanup script for Charlie-TR1-DB
# Wipes all logs, data, and database to start fresh
#
# Usage:
#   ./scripts/clean_all.sh       # Interactive mode with confirmation
#   ./scripts/clean_all.sh -y    # Auto-confirm (dangerous!)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration (read from environment or use defaults)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_ROOT="${CHARLIE_DATA_ROOT:-/opt/charlie_data}"
METAFLOW_DIR="${PROJECT_ROOT}/.metaflow"
DB_HOST="${CHARLIE_DB_HOST:-localhost}"
DB_PORT="${CHARLIE_DB_PORT:-5432}"
DB_NAME="${CHARLIE_DB_NAME:-charlie}"
DB_USER="${CHARLIE_DB_USER:-charlie}"
DB_PASSWORD="${CHARLIE_DB_PASSWORD:-charliepass}"
INIT_SQL="${SCRIPT_DIR}/init_charlie_db.sql"

# Parse arguments
AUTO_CONFIRM=false
if [ "$1" == "-y" ] || [ "$1" == "--yes" ]; then
    AUTO_CONFIRM=true
fi

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Charlie-TR1-DB Complete Cleanup Script${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Show what will be cleaned
echo -e "${YELLOW}This will clean:${NC}"
echo -e "  ${RED}✗${NC} Metaflow logs:     ${METAFLOW_DIR}"
echo -e "  ${RED}✗${NC} Data directory:    ${DATA_ROOT}"
echo -e "  ${RED}✗${NC} Database schema:   ${DB_NAME}.charlie"
echo ""

if [ "$AUTO_CONFIRM" = false ]; then
    echo -e "${RED}WARNING: This action cannot be undone!${NC}"
    read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirmation
    if [ "$confirmation" != "yes" ]; then
        echo -e "${GREEN}Cleanup cancelled.${NC}"
        exit 0
    fi
fi

echo ""
echo -e "${BLUE}Starting cleanup...${NC}"
echo ""

# ===== 1. Clean Metaflow logs =====
echo -e "${BLUE}[1/3] Cleaning Metaflow logs...${NC}"
if [ -d "$METAFLOW_DIR" ]; then
    SIZE_BEFORE=$(du -sh "$METAFLOW_DIR" 2>/dev/null | cut -f1 || echo "unknown")
    echo "  Removing: ${METAFLOW_DIR} (${SIZE_BEFORE})"
    rm -rf "$METAFLOW_DIR"
    echo -e "  ${GREEN}✓${NC} Metaflow logs cleaned"
else
    echo "  (no .metaflow directory found, skipping)"
fi
echo ""

# ===== 2. Clean data directory =====
echo -e "${BLUE}[2/3] Cleaning data directory...${NC}"
if [ -d "$DATA_ROOT" ]; then
    SIZE_BEFORE=$(du -sh "$DATA_ROOT" 2>/dev/null | cut -f1 || echo "unknown")
    echo "  Removing contents of: ${DATA_ROOT} (${SIZE_BEFORE})"

    # Remove subdirectories but keep the root directory
    find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true

    # Recreate standard subdirectories
    mkdir -p "$DATA_ROOT"/{raw,normalized,assembled,labels,distilled_theses,exports/parquet}

    echo -e "  ${GREEN}✓${NC} Data directory cleaned and recreated"
else
    echo "  Creating data directory: ${DATA_ROOT}"
    mkdir -p "$DATA_ROOT"/{raw,normalized,assembled,labels,distilled_theses,exports/parquet}
    echo -e "  ${GREEN}✓${NC} Data directory created"
fi
echo ""

# ===== 3. Reset database =====
echo -e "${BLUE}[3/3] Resetting database...${NC}"

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo -e "  ${RED}ERROR: psql command not found. Cannot reset database.${NC}"
    exit 1
fi

# Check if init SQL file exists
if [ ! -f "$INIT_SQL" ]; then
    echo -e "  ${RED}ERROR: Init SQL file not found at ${INIT_SQL}${NC}"
    exit 1
fi

# Test database connection
export PGPASSWORD="$DB_PASSWORD"
echo "  Testing database connection..."
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "  ${RED}ERROR: Could not connect to database.${NC}"
    echo "  Connection: ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
    exit 1
fi

# Drop all objects in charlie schema
echo "  Dropping charlie schema..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" > /dev/null 2>&1 <<EOF
DROP SCHEMA IF EXISTS charlie CASCADE;
EOF

# Reinitialize from SQL file
echo "  Reinitializing schema from ${INIT_SQL}..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$INIT_SQL" > /dev/null 2>&1

# Verify
TABLE_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'charlie';" 2>/dev/null | xargs)
VIEW_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'charlie';" 2>/dev/null | xargs)

echo -e "  ${GREEN}✓${NC} Database reset complete (${TABLE_COUNT} tables, ${VIEW_COUNT} views)"
unset PGPASSWORD
echo ""

# ===== Summary =====
echo -e "${BLUE}================================================${NC}"
echo -e "${GREEN}Cleanup completed successfully!${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""
echo "System is now in a clean state. Ready for fresh pipeline run."
echo ""
echo "Next steps:"
echo "  1. Run the pipeline: ./run_test.sh"
echo "  2. Or with a specific ticker: source .venv/bin/activate && python charlie_tr1_flow.py run --tickers AAPL --as_of_date 2024-06-15"
echo ""

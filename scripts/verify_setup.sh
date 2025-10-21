#!/bin/bash
# Verification script for Charlie TR1-DB setup

echo "=========================================="
echo "Charlie TR1-DB Setup Verification"
echo "=========================================="
echo ""

# Check Python
echo "1. Checking Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "   ✓ $PYTHON_VERSION"
else
    echo "   ✗ Python not found"
    exit 1
fi

# Check UV
echo "2. Checking UV package manager..."
if command -v uv &> /dev/null; then
    UV_VERSION=$(uv --version)
    echo "   ✓ $UV_VERSION"
else
    echo "   ⚠ UV not installed (install with: curl -LsSf https://astral.sh/uv/install.sh | sh)"
fi

# Check PostgreSQL
echo "3. Checking PostgreSQL..."
if command -v psql &> /dev/null; then
    PG_VERSION=$(psql --version)
    echo "   ✓ $PG_VERSION"
    
    # Test database connection
    echo "4. Testing database connection..."
    if PGPASSWORD="charliepass" psql -h localhost -U charlie -d charlie -c "SELECT 1" &> /dev/null; then
        echo "   ✓ Database connection successful"
        
        # Count tables
        TABLE_COUNT=$(PGPASSWORD="charliepass" psql -h localhost -U charlie -d charlie -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'charlie';")
        echo "   ✓ Found $TABLE_COUNT tables in charlie schema"
        
        # Count indexes
        INDEX_COUNT=$(PGPASSWORD="charliepass" psql -h localhost -U charlie -d charlie -t -c "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'charlie';")
        echo "   ✓ Found $INDEX_COUNT indexes"
    else
        echo "   ✗ Cannot connect to database"
        echo "   Run: sudo -u postgres psql -d charlie -f /opt/T1/scripts/init_charlie_db.sql"
        exit 1
    fi
else
    echo "   ✗ PostgreSQL not installed"
    exit 1
fi

# Check project files
echo "5. Checking project files..."
FILES=(
    "/opt/T1/pyproject.toml"
    "/opt/T1/.python-version"
    "/opt/T1/charlie_tr1_flow.py"
    "/opt/T1/scripts/init_charlie_db.sql"
    "/opt/T1/README.md"
    "/opt/T1/IMPLEMENTATION_STATUS.md"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "   ✓ $(basename $file)"
    else
        echo "   ✗ Missing: $file"
    fi
done

# Check data directory
echo "6. Checking data directory..."
if [ -d "/opt/charlie_data" ]; then
    echo "   ✓ /opt/charlie_data exists"
else
    echo "   ⚠ /opt/charlie_data not created yet (will be created on first run)"
fi

echo ""
echo "=========================================="
echo "Setup Status: READY"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Install UV: curl -LsSf https://astral.sh/uv/install.sh | sh"
echo "2. Create venv: uv venv"
echo "3. Activate: source .venv/bin/activate"
echo "4. Install deps: uv pip install -e ."
echo "5. Set API keys (see README.md)"
echo "6. Run pipeline: python charlie_tr1_flow.py run --tickers AAPL --as_of_date 2024-06-01"
echo ""

#!/bin/bash
# Setup script to install test dependencies and run tests

set -e

echo "Installing test dependencies..."
pip install -e ".[dev]" || pip3 install -e ".[dev]"

echo ""
echo "Running unit tests..."
pytest -v -m "not integration" --tb=short

echo ""
echo "Test setup complete!"
echo ""
echo "To run tests:"
echo "  All unit tests:        pytest -m 'not integration'"
echo "  With coverage:         pytest -m 'not integration' --cov=src/es_query_gen"
echo "  Integration tests:     pytest -m integration"
echo "  All tests:             pytest"
echo "  Specific file:         pytest tests/test_models.py"

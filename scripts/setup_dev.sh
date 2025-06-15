#!/usr/bin/env bash
# Setup development environment
set -e
python -m venv venv
source venv/bin/activate
pip install uv
uv pip install -r requirements.txt
pip install pre-commit
pre-commit install

echo "Development environment is ready. Activate it with 'source venv/bin/activate'."

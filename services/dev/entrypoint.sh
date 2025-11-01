#!/usr/bin/env bash
set -euo pipefail

# Ensure the project is installed in editable mode
if ! uv pip show chainlit-cassandra-data-layer >/dev/null 2>&1; then
    uv pip install --quiet --editable /workspace
fi

if [ $# -gt 0 ]; then
    exec "$@"
fi

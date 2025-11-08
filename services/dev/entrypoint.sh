#!/usr/bin/env bash
set -e

uv sync --quiet || true

# Run any additional commands passed and replace the shell with the command
if [ $# -gt 0 ]; then
    exec "$@"
fi

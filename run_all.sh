#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python -m pip install -r requirements.txt
python scripts/prepare_data.py
docker compose up -d
python scripts/draw_architecture.py
python scripts/run_pipeline.py --demo
echo "Default run finished. Window reports are in results/results.jsonl"

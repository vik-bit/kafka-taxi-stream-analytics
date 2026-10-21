@echo off
setlocal
cd /d "%~dp0"
python -m pip install -r requirements.txt
python scripts\prepare_data.py
docker compose up -d
python scripts\run_pipeline.py --experiments
echo Experiment table: results\experiment_summary.csv

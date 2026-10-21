"""Small JSON status files the stages use to agree on progress.

Hot files live in the temp directory. The project folder is on OneDrive,
and replacing a file there while another process is reading it raises
PermissionError on Windows.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from common.stats import read_json, write_json


def control_dir(cfg: dict) -> Path:
    path = Path(tempfile.gettempdir()) / "bda_assignment2_control"
    path.mkdir(parents=True, exist_ok=True)
    return path


def runs_dir(cfg: dict) -> Path:
    path = Path(cfg["_root"]) / "results" / "runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def status_path(cfg: dict, role: str) -> Path:
    return control_dir(cfg) / f"{cfg['run_id']}_{role}.json"


def stop_path(cfg: dict) -> Path:
    return control_dir(cfg) / f"{cfg['run_id']}.stop"


def write_status(cfg: dict, role: str, payload: dict) -> None:
    write_json(status_path(cfg, role), payload)


def read_status(cfg: dict, role: str):
    return read_json(status_path(cfg, role))


def stopped(cfg: dict) -> bool:
    return stop_path(cfg).exists()

"""Load the single assignment configuration file."""

from __future__ import annotations

import argparse
import copy
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_config(path: str | Path | None = None) -> dict:
    path = Path(path) if path else ROOT / "config.yaml"
    with open(path, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    cfg["_root"] = str(ROOT)
    cfg["_config_path"] = str(path)
    return cfg


def write_config(cfg: dict, path: str | Path) -> None:
    data = {k: v for k, v in cfg.items() if not k.startswith("_")}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False)


def with_overrides(cfg: dict, **overrides) -> dict:
    merged = copy.deepcopy(cfg)
    merged.update(overrides)
    return merged


def cfg_from_argv() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    return load_config(args.config)

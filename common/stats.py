"""Small numeric helpers shared by the stages and the experiment table."""

from __future__ import annotations

import json
import time
from pathlib import Path


def pct_error(estimate: float, exact: float) -> float | None:
    if exact is None or estimate is None:
        return None
    if exact == 0:
        return 0.0 if estimate == 0 else None
    return abs(estimate - exact) / abs(exact) * 100.0


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def round_or_none(value, digits: int = 4):
    if value is None:
        return None
    return round(float(value), digits)


def write_json(path: str | Path, payload: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2)
    temporary = path.with_suffix(path.suffix + ".tmp")
    for _ in range(8):
        try:
            temporary.write_text(text, encoding="utf-8")
            temporary.replace(path)
            return
        except PermissionError:
            time.sleep(0.05)
    path.write_text(text, encoding="utf-8")


def read_json(path: str | Path):
    path = Path(path)
    if not path.exists():
        return None
    for _ in range(8):
        try:
            text = path.read_text(encoding="utf-8")
            if not text.strip():
                return None
            return json.loads(text)
        except PermissionError:
            time.sleep(0.05)
        except json.JSONDecodeError:
            time.sleep(0.05)
    return None

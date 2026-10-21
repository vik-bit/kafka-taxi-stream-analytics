"""Replay the taxi file into taxi.raw.

This is the only component that reads the dataset. Duplicates keep the same
event_id. Delayed events sit in a holding buffer for 1..max_delay_seconds
of wall-clock time and are then published.
"""

from __future__ import annotations

import csv
import heapq
import random
import time
from collections import Counter
from pathlib import Path

from common.config import cfg_from_argv
from common.control import write_status
from common.kafka_util import json_producer
from common.stats import write_json

ROOT_FIELDS = (
    "event_id",
    "event_time",
    "dropoff_time",
    "pickup_zone",
    "dropoff_zone",
    "trip_distance",
    "fare_amount",
    "passenger_count",
    "payment_type",
)


def _as_int(value):
    if value is None or value == "":
        return None
    return int(float(value))


def _as_float(value):
    if value is None or value == "":
        return None
    return float(value)


def load_rows(cfg: dict) -> list[dict]:
    path = Path(cfg["data_path"])
    if not path.is_absolute():
        path = Path(cfg["_root"]) / path
    rows = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for index, raw in enumerate(reader):
            if cfg.get("max_events") and index >= int(cfg["max_events"]):
                break
            pickup = raw["pickup_datetime"]
            dropoff = raw["dropoff_datetime"]
            pickup_zone = _as_int(raw["PULocationID"])
            dropoff_zone = _as_int(raw["DOLocationID"])
            if not pickup or not dropoff or not pickup_zone or not dropoff_zone:
                continue
            rows.append({
                "event_id": f"2024-01-row-{index:08d}",
                "event_time": pickup.replace(" ", "T")[:19],
                "dropoff_time": dropoff.replace(" ", "T")[:19],
                "pickup_zone": pickup_zone,
                "dropoff_zone": dropoff_zone,
                "trip_distance": _as_float(raw["trip_distance"]),
                "fare_amount": _as_float(raw["fare_amount"]),
                "passenger_count": _as_int(raw.get("passenger_count")),
                "payment_type": _as_int(raw.get("payment_type")),
            })
    return rows


def main() -> None:
    cfg = cfg_from_argv()
    rows = load_rows(cfg)
    if not rows:
        raise SystemExit("producer found no taxi rows; run scripts/prepare_data.py")

    rng = random.Random(int(cfg["random_seed"]))
    rate = float(cfg["producer_rate"])
    interval = 0.0 if rate <= 0 else 1.0 / rate
    dup_rate = float(cfg["duplicate_rate"])
    delay_rate = float(cfg["delayed_event_rate"])
    max_delay = float(cfg["max_delay_seconds"])
    key_mode = cfg.get("partition_key") or "none"

    producer = json_producer(cfg["bootstrap_servers"], int(cfg.get("producer_linger_ms", 5)))
    partitions = Counter()
    pending: list = []
    seq = 0
    sent = 0
    duplicates = 0
    delayed = 0
    started = time.perf_counter()
    next_slot = started

    def stamp_and_send(payload: dict, key):
        nonlocal sent, next_slot
        now = time.perf_counter()
        if interval and now < next_slot:
            time.sleep(next_slot - now)
        payload = dict(payload)
        payload["producer_ts_ms"] = int(time.time() * 1000)
        future = producer.send("taxi.raw", key=key, value=payload)
        future.add_callback(lambda meta, _parts=partitions: _parts.update([meta.partition]))
        sent += 1
        next_slot = (next_slot + interval) if interval else time.perf_counter()
        if sent % 1000 == 0:
            elapsed = max(time.perf_counter() - started, 1e-6)
            write_status(cfg, "producer", {
                "done": False,
                "produced": sent,
                "duplicates": duplicates,
                "delayed": delayed,
                "eps": round(sent / elapsed, 2),
                "target_rate": rate,
            })

    def enqueue(payload: dict, key, delay_s: float, is_duplicate_pair: bool):
        nonlocal seq
        seq += 1
        heapq.heappush(pending, (time.perf_counter() + delay_s, seq, payload, key, is_duplicate_pair))

    def drain_ready() -> None:
        nonlocal duplicates
        now = time.perf_counter()
        while pending and pending[0][0] <= now:
            _release, _seq, payload, key, is_duplicate_pair = heapq.heappop(pending)
            stamp_and_send(payload, key)
            if is_duplicate_pair:
                duplicate = dict(payload)
                duplicate["injected_duplicate"] = True
                stamp_and_send(duplicate, key)
                duplicates += 1

    for row in rows:
        drain_ready()
        payload = {field: row[field] for field in ROOT_FIELDS}
        payload["injected_duplicate"] = False
        key = None if key_mode in ("none", "round_robin", None) else str(row["pickup_zone"])
        delay_s = 0.0
        if max_delay > 0 and rng.random() < delay_rate:
            delay_s = rng.uniform(1.0, max_delay)
            delayed += 1
        make_dup = rng.random() < dup_rate
        if delay_s > 0:
            enqueue(payload, key, delay_s, make_dup)
        else:
            stamp_and_send(payload, key)
            if make_dup:
                duplicate = dict(payload)
                duplicate["injected_duplicate"] = True
                stamp_and_send(duplicate, key)
                duplicates += 1

    rows_done = time.perf_counter()
    sent_when_rows_done = sent
    while pending:
        sleep_for = pending[0][0] - time.perf_counter()
        if sleep_for > 0:
            time.sleep(min(sleep_for, 0.5))
        drain_ready()

    producer.flush()
    producer.close()
    elapsed = max(time.perf_counter() - started, 1e-6)
    total = sum(partitions.values()) or 1
    busiest = max(partitions.values()) if partitions else 0
    summary = {
        "done": True,
        "run_id": cfg["run_id"],
        "produced": sent,
        "source_rows": len(rows),
        "duplicates": duplicates,
        "delayed": delayed,
        "eps": round(sent / elapsed, 2),
        "eps_during_read": round(sent_when_rows_done / max(rows_done - started, 1e-6), 2),
        "target_rate": rate,
        "elapsed_sec": round(elapsed, 3),
        "partition_counts": {str(k): v for k, v in sorted(partitions.items())},
        "partition_skew": round(busiest / total, 4),
        "partition_key": key_mode,
    }
    write_status(cfg, "producer", summary)
    write_json(Path(cfg["_root"]) / "results" / "runs" / f"{cfg['run_id']}_producer.json", summary)
    print(
        f"producer done run={cfg['run_id']} sent={sent} eps={summary['eps']} "
        f"dups={duplicates} delayed={delayed} skew={summary['partition_skew']}"
    )


if __name__ == "__main__":
    main()

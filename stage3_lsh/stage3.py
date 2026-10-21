"""Stage 3: taxi.clean -> taxi.lsh.

Destination sets are built per 60-minute event-time window. MinHash-LSH
proposes candidate pickup-zone pairs; exact Jaccard keeps pairs at or above
the threshold. A second banding of the same 100 hashes is always reported.
"""

from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path

from common.config import cfg_from_argv
from common.control import read_status, stopped, write_status
from common.kafka_util import assigned_lag, json_consumer, json_producer
from common.lsh import evaluate_lsh
from common.stats import round_or_none, write_json
from common.windows import format_epoch, parse_event_time, watermark_time, window_bounds


class ZoneWindow:
    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end
        self.destinations: dict[int, set[int]] = defaultdict(set)
        self.trips: dict[int, int] = defaultdict(int)
        self.seen_ids: set[str] = set()
        self.partitions: dict[int, set[int]] = defaultdict(set)
        self.messages = 0
        self.saw_past_end = False


def _headline(result: dict) -> dict:
    return {
        "candidates": result["candidates"],
        "true_pairs": result["true_pairs"],
        "verified_pairs": result["verified_pairs"],
        "recall": round_or_none(result["recall"], 4),
        "candidate_precision": round_or_none(result["candidate_precision"], 4),
        "candidate_reduction": round_or_none(result["candidate_reduction"], 4),
        "lsh_latency_ms": result["lsh_latency_ms"],
        "exact_latency_ms": result["exact_latency_ms"],
        "active_zones": result["active_zones"],
        "all_possible_pairs": result["all_possible_pairs"],
        "bands": result["bands"],
        "rows": result["rows"],
    }


def main() -> None:
    cfg = cfg_from_argv()
    window_minutes = int(cfg["window_minutes"])
    lateness = int(cfg["allowed_lateness_seconds"])
    producer = json_producer(cfg["bootstrap_servers"], int(cfg.get("producer_linger_ms", 5)))
    consumer = json_consumer(cfg["bootstrap_servers"], "taxi.clean", f"stage3-{cfg['run_id']}")
    windows: dict[int, ZoneWindow] = {}
    closed: set[int] = set()
    max_event = None
    processed = 0
    started = time.perf_counter()
    max_lag = 0

    def emit(window: ZoneWindow, closed_by: str) -> None:
        common = dict(
            destination_sets=window.destinations,
            trip_counts=window.trips,
            min_trips=int(cfg["min_trips_per_zone"]),
            num_hashes=int(cfg["lsh_num_hashes"]),
            threshold=float(cfg["jaccard_threshold"]),
            seed=int(cfg["random_seed"]),
        )
        primary = evaluate_lsh(
            bands=int(cfg["lsh_bands"]),
            rows=int(cfg["lsh_rows_per_band"]),
            **common,
        )
        alternate = evaluate_lsh(
            bands=int(cfg["lsh_alt_bands"]),
            rows=int(cfg["lsh_alt_rows_per_band"]),
            **common,
        )
        split_zones = sum(1 for parts in window.partitions.values() if len(parts) > 1)
        payload = {
            "run_id": cfg["run_id"],
            "window_start": format_epoch(window.start),
            "window_end": format_epoch(window.end),
            "window_events": window.messages,
            "closed_by": closed_by,
            "lsh": _headline(primary),
            "lsh_alt": _headline(alternate),
            "lsh_example_pairs": primary["example_pairs"],
            "repartition": {
                "pickup_zones": len(window.partitions),
                "zones_split_across_partitions": split_zones,
                "merge_required": split_zones > 0,
                "reason": (
                    "Each pickup zone's trips arrived on one partition, so its destination "
                    "set was already complete."
                    if split_zones == 0 else
                    "Round-robin (or a missing zone key) split pickup zones across partitions, "
                    "so Stage 3 had to merge partial destination sets before MinHash. "
                    "That merge is the shuffle a keyed stream would have avoided."
                ),
            },
        }
        producer.send("taxi.lsh", key=payload["window_start"], value=payload)

    def close_ready(force: bool = False) -> None:
        mark = watermark_time(max_event, lateness)
        for start, window in list(windows.items()):
            end_dt = parse_event_time(format_epoch(window.end))
            if max_event is not None and max_event >= end_dt:
                window.saw_past_end = True
            should_close = force or (mark is not None and mark >= end_dt)
            if not should_close:
                continue
            reason = "watermark"
            if force and not (mark and mark >= end_dt):
                reason = "end_of_stream"
            emit(window, reason)
            closed.add(start)
            del windows[start]

    def on_event(event: dict, partition: int) -> None:
        nonlocal max_event, processed
        processed += 1
        moment = parse_event_time(event["event_time"])
        start, end = window_bounds(moment, window_minutes)
        if start in closed:
            return
        if max_event is None or moment > max_event:
            max_event = moment
        window = windows.get(start)
        if window is None:
            window = ZoneWindow(start, end)
            windows[start] = window
        window.messages += 1
        zone = int(event["pickup_zone"])
        window.destinations[zone].add(int(event["dropoff_zone"]))
        window.partitions[zone].add(partition)
        if event["event_id"] not in window.seen_ids:
            window.trips[zone] += 1
            window.seen_ids.add(event["event_id"])
        close_ready(False)

    empty = 0
    saw_assignment = False
    try:
        while not stopped(cfg):
            polled = consumer.poll(timeout_ms=400)
            if consumer.assignment():
                saw_assignment = True
            lag = assigned_lag(consumer)
            max_lag = max(max_lag, lag)
            if not polled:
                empty += 1
                stage1 = read_status(cfg, "stage1") or {}
                if stage1.get("done") and empty >= 3 and saw_assignment and lag == 0:
                    break
                continue
            empty = 0
            batch = []
            for tp, messages in polled.items():
                for message in messages:
                    batch.append((message, tp.partition))
            batch.sort(key=lambda item: item[0].value.get("event_time") or "")
            for message, partition in batch:
                on_event(message.value, partition)
            consumer.commit()
        close_ready(True)
        consumer.commit()
    finally:
        producer.flush()
        producer.close()
        consumer.close()

    elapsed = max(time.perf_counter() - started, 1e-6)
    summary = {
        "done": True,
        "run_id": cfg["run_id"],
        "processed": processed,
        "eps": round(processed / elapsed, 2),
        "max_lag_clean": max_lag,
    }
    write_status(cfg, "stage3", summary)
    write_json(Path(cfg["_root"]) / "results" / "runs" / f"{cfg['run_id']}_stage3.json", summary)
    print(f"stage3 done run={cfg['run_id']} processed={processed} eps={summary['eps']}")


if __name__ == "__main__":
    main()

"""Stage 1: taxi.raw -> taxi.clean.

Validates the event, adds route / duration / slow-trip fields, and marks
Bloom-filter membership. A positive Bloom hit never drops the event.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from common.bloom import BloomFilter
from common.config import cfg_from_argv
from common.control import read_status, stopped, write_status
from common.kafka_util import assigned_lag, json_consumer, json_producer
from common.stats import write_json

def _duration_minutes(event: dict) -> float | None:
    try:
        pickup = datetime.fromisoformat(event["event_time"])
        dropoff = datetime.fromisoformat(event["dropoff_time"])
    except (KeyError, TypeError, ValueError):
        return None
    return (dropoff - pickup).total_seconds() / 60.0


def _valid(event: dict) -> bool:
    if not event.get("event_id") or not event.get("event_time"):
        return False
    if event.get("pickup_zone") in (None, 0) or event.get("dropoff_zone") in (None, 0):
        return False
    return _duration_minutes(event) is not None


def main() -> None:
    cfg = cfg_from_argv()
    consumers_n = max(1, int(cfg["consumers"]))
    bloom = BloomFilter(int(cfg["bloom_expected_items"]), float(cfg["bloom_false_positive_target"]))
    # Diagnostic only: what a per-partition filter would have answered.
    local_blooms = defaultdict(
        lambda: BloomFilter(int(cfg["bloom_expected_items"]), float(cfg["bloom_false_positive_target"]))
    )
    exact_ids: set[str] = set()
    lock = threading.Lock()
    producer = json_producer(cfg["bootstrap_servers"], int(cfg.get("producer_linger_ms", 5)))
    stats = Counter()
    lag_by_thread: dict[int, int] = {}
    assignment: dict[int, list[int]] = {}
    fp_example = None
    started = time.perf_counter()
    first_event_at = None
    stop = threading.Event()

    def handle(event: dict, partition: int) -> None:
        nonlocal fp_example, first_event_at
        if first_event_at is None:
            first_event_at = time.perf_counter()
        if not _valid(event):
            with lock:
                stats["rejected"] += 1
            return
        duration = _duration_minutes(event)
        event_id = event["event_id"]
        with lock:
            actually_seen = event_id in exact_ids
            said_seen = bloom.might_contain(event_id)
            local_said = local_blooms[partition].might_contain(event_id)
            if actually_seen:
                stats["repeat_queries"] += 1
                if not said_seen:
                    stats["false_negatives"] += 1
            else:
                stats["first_queries"] += 1
                if said_seen and fp_example is None:
                    fp_example = {
                        "event_id": event_id,
                        "pickup_zone": event.get("pickup_zone"),
                        "dropoff_zone": event.get("dropoff_zone"),
                        "partition": partition,
                        "note": "Bloom reported seen before this event_id was inserted",
                    }
                    stats["false_positives"] += 1
                elif said_seen:
                    stats["false_positives"] += 1
            if event.get("injected_duplicate") and not local_said:
                stats["partition_local_missed_duplicates"] += 1
            if event.get("injected_duplicate"):
                stats["injected_duplicates"] += 1
            bloom.add(event_id)
            local_blooms[partition].add(event_id)
            exact_ids.add(event_id)
            stats["partition_" + str(partition)] += 1

        clean = dict(event)
        clean.update({
            "route_key": f"{event['pickup_zone']}-{event['dropoff_zone']}",
            "trip_duration_min": round(duration, 4),
            "slow_trip_bit": 1 if duration > 30 else 0,
            "bloom_seen_before": bool(said_seen),
            "actually_seen_before": bool(actually_seen),
            "stage1_ts_ms": int(time.time() * 1000),
            "ingest_partition": partition,
        })
        # Keep the run's key policy. Re-keying here would hide the round-robin experiment.
        key_mode = cfg.get("partition_key") or "none"
        out_key = None if key_mode in ("none", "round_robin") else str(event["pickup_zone"])
        # KafkaProducer is thread-safe. Keeping the send outside the Bloom lock
        # is what lets extra Stage 1 consumers actually run in parallel.
        producer.send("taxi.clean", key=out_key, value=clean)
        with lock:
            stats["published"] += 1

    def loop(thread_index: int) -> None:
        consumer = json_consumer(
            cfg["bootstrap_servers"],
            "taxi.raw",
            f"stage1-{cfg['run_id']}",
        )
        try:
            while not stop.is_set() and not stopped(cfg):
                polled = consumer.poll(timeout_ms=400)
                assignment[thread_index] = sorted(tp.partition for tp in consumer.assignment())
                with lock:
                    lag_by_thread[thread_index] = assigned_lag(consumer)
                    stats["max_lag_raw"] = max(stats["max_lag_raw"], sum(lag_by_thread.values()))
                if not polled:
                    _publish_live()
                    continue
                for _tp, messages in polled.items():
                    for message in messages:
                        handle(message.value, message.partition)
                if stats["published"] - stats.get("_committed", 0) >= int(cfg.get("commit_every", 200)):
                    consumer.commit()
                    with lock:
                        stats["_committed"] = stats["published"]
                _publish_live()
            consumer.commit()
        finally:
            consumer.close()

    def _publish_live() -> None:
        origin = first_event_at or started
        elapsed = max(time.perf_counter() - origin, 1e-6)
        firsts = stats["first_queries"] or 0
        repeats = stats["repeat_queries"] or 0
        write_status(cfg, "stage1", {
            "done": False,
            "published": stats["published"],
            "rejected": stats["rejected"],
            "eps": round(stats["published"] / elapsed, 2),
            "fp_rate": (stats["false_positives"] / firsts) if firsts else 0.0,
            "fn_rate": (stats["false_negatives"] / repeats) if repeats else 0.0,
            "max_lag_raw": stats["max_lag_raw"],
        })

    threads = [threading.Thread(target=loop, args=(i,), daemon=True) for i in range(consumers_n)]
    for thread in threads:
        thread.start()
    # Threads stay in the group until every raw message is published. Exiting
    # one consumer early would rebalance the group and replay a partition.
    assigned_samples = []
    while not stopped(cfg):
        with lock:
            assigned_samples.append(sum(1 for parts in assignment.values() if parts))
        producer_state = read_status(cfg, "producer") or {}
        done = producer_state.get("done")
        target = producer_state.get("produced")
        with lock:
            handled = stats["published"] + stats["rejected"]
        if done and target is not None and handled >= target:
            time.sleep(0.4)
            with lock:
                handled = stats["published"] + stats["rejected"]
            if handled >= target:
                break
        time.sleep(0.2)
    stop.set()
    for thread in threads:
        thread.join()
    producer.flush()
    producer.close()

    elapsed = max(time.perf_counter() - (first_event_at or started), 1e-6)
    firsts = stats["first_queries"] or 0
    repeats = stats["repeat_queries"] or 0
    assigned = {idx: parts for idx, parts in assignment.items() if parts}
    summary = {
        "done": True,
        "run_id": cfg["run_id"],
        "published": stats["published"],
        "rejected": stats["rejected"],
        "eps": round(stats["published"] / elapsed, 2),
        "elapsed_sec": round(elapsed, 3),
        "consumers_started": consumers_n,
        "consumers_assigned": len(assigned),
        "idle_consumers": max(0, consumers_n - len({p for parts in assigned.values() for p in parts})),
        "assignment": {str(k): v for k, v in assigned.items()},
        "max_lag_raw": stats["max_lag_raw"],
        "bloom": {
            "inserted": bloom.inserted,
            "bit_array_size": bloom.bit_array_size,
            "hash_functions": bloom.hash_functions,
            "memory_bytes": bloom.memory_bytes,
            "expected_items": bloom.expected_items,
            "false_positive_target": bloom.false_positive_target,
            "false_positives": stats["false_positives"],
            "false_negatives": stats["false_negatives"],
            "first_queries": firsts,
            "repeat_queries": repeats,
            "fp_rate": (stats["false_positives"] / firsts) if firsts else 0.0,
            "fn_rate": (stats["false_negatives"] / repeats) if repeats else 0.0,
            "false_positive_example": fp_example,
            "partition_local_missed_duplicates": stats["partition_local_missed_duplicates"],
            "injected_duplicates": stats["injected_duplicates"],
            "partition_local_fn_rate": (
                stats["partition_local_missed_duplicates"] / stats["injected_duplicates"]
                if stats["injected_duplicates"] else 0.0
            ),
        },
    }
    # idle_consumers should count threads that never received a partition.
    # The shutdown snapshot is a rebalance, not the steady state. Use the median
    # number of threads that actually held a partition while the run was live.
    if assigned_samples:
        steady_assigned = sorted(assigned_samples)[len(assigned_samples) // 2]
    else:
        steady_assigned = sum(1 for parts in assignment.values() if parts)
    summary["consumers_assigned"] = steady_assigned
    summary["idle_consumers"] = max(0, consumers_n - steady_assigned)
    summary["assignment_at_shutdown"] = {str(k): v for k, v in assignment.items()}
    write_status(cfg, "stage1", summary)
    out = Path(cfg["_root"]) / "results" / "runs" / f"{cfg['run_id']}_stage1.json"
    write_json(out, summary)
    print(
        f"stage1 done run={cfg['run_id']} published={summary['published']} "
        f"eps={summary['eps']} fp={summary['bloom']['fp_rate']:.4f} "
        f"idle_consumers={summary['idle_consumers']}"
    )


if __name__ == "__main__":
    main()

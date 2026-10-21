"""Stage 2: taxi.clean -> taxi.metrics.

One consumer reads every partition. DGIM, the reservoirs, and the exact
means are global stream state, so splitting them across a consumer group
would answer a different question than the assignment asks. Parallelism
is measured on Stage 1, where Kafka actually assigns partitions.
"""

from __future__ import annotations

import random
import time
import zlib
from collections import defaultdict, deque
from pathlib import Path

from common.bloom import BloomFilter
from common.config import cfg_from_argv
from common.control import read_status, stopped, write_status
from common.dgim import DGIM
from common.hll import HyperLogLog
from common.kafka_util import assigned_lag, json_consumer, json_producer
from common.reservoir import Reservoir
from common.stats import pct_error, percentile, round_or_none, write_json
from common.windows import format_epoch, parse_event_time, watermark_time, window_bounds

DIVERSITY_REASON = (
    "Diversity is a non-linear per-zone statistic: a zone is diverse only when its "
    "own drop-off set has at least 10 zones. Event sampling keeps a fraction of trips, "
    "so those sets stay incomplete and the fraction is biased down. Entity sampling keeps "
    "every trip for a deterministic subset of pickup zones, so the zones it keeps are fully "
    "observed. With the same sample rate the event budgets match, and the entity estimate "
    "is the one that stays close to the exact fraction."
)


class Window:
    def __init__(self, start: int, end: int, precision: int):
        self.start = start
        self.end = end
        self.hll = HyperLogLog(precision)
        self.routes: set[str] = set()
        self.destinations: dict[int, set[int]] = defaultdict(set)
        self.trips: dict[int, int] = defaultdict(int)
        self.seen_ids: set[str] = set()
        self.event_dest: dict[int, set[int]] = defaultdict(set)
        self.entity_dest: dict[int, set[int]] = defaultdict(set)
        self.event_sample_events = 0
        self.entity_sample_events = 0
        self.messages = 0
        self.processing_latencies: list[float] = []
        self.producer_ts: list[int] = []
        self.saw_past_end = False
        self.late_included = 0
        self.late_updated = 0


def _rate_ok(zone: int, rate: float, seed: int) -> bool:
    digest = zlib.crc32(f"{seed}:{zone}".encode("utf-8")) & 0xFFFFFFFF
    return (digest / 2**32) < rate


def main() -> None:
    cfg = cfg_from_argv()
    seed = int(cfg["random_seed"])
    window_minutes = int(cfg["window_minutes"])
    lateness = int(cfg["allowed_lateness_seconds"])
    sample_rate = float(cfg["sample_rate"])
    diverse_at = int(cfg["diverse_dropoffs"])
    producer = json_producer(cfg["bootstrap_servers"], int(cfg.get("producer_linger_ms", 5)))
    consumer = json_consumer(cfg["bootstrap_servers"], "taxi.clean", f"stage2-{cfg['run_id']}")

    bloom_probe = BloomFilter(int(cfg["bloom_expected_items"]), float(cfg["bloom_false_positive_target"]))
    fare_reservoirs = {int(k): Reservoir(int(k), seed + int(k)) for k in cfg["reservoir_sizes"]}
    dist_reservoirs = {int(k): Reservoir(int(k), seed + 1000 + int(k)) for k in cfg["reservoir_sizes"]}
    sample_rng = random.Random(seed + 7)
    dgim = DGIM(int(cfg["dgim_max_window"]))
    recent_bits: deque[int] = deque(maxlen=int(cfg["dgim_max_window"]))
    fare_sum = 0.0
    fare_n = 0
    dist_sum = 0.0
    dist_n = 0
    windows: dict[int, Window] = {}
    closed: set[int] = set()
    max_event = None
    processed = 0
    unique_ids: set[str] = set()
    firsts = repeats = fp = fn = 0
    fp_example = None
    late_examples: list[dict] = []
    discarded = 0
    max_lag = 0
    processing_latencies: list[float] = []
    started = time.perf_counter()
    first_event_at = None
    last_commit_at = 0
    applied_path = Path(cfg["_root"]) / "results" / "runs" / f"{cfg['run_id']}_applied_ids.txt"
    applied_path.parent.mkdir(parents=True, exist_ok=True)
    applied_log = open(applied_path, "a", encoding="utf-8", buffering=1)
    commit_every = int(cfg.get("commit_every", 200))

    def elapsed_busy() -> float:
        origin = first_event_at or started
        return max(time.perf_counter() - origin, 1e-6)

    def live(done: bool = False) -> None:
        elapsed = elapsed_busy()
        write_status(cfg, "stage2", {
            "done": done,
            "processed": processed,
            "unique_ids": len(unique_ids),
            "eps": round(processed / elapsed, 2),
            "max_lag_clean": max_lag,
            "discarded_late": discarded,
            "open_windows": len(windows),
        })

    def bloom_block() -> dict:
        stage1 = read_status(cfg, "stage1") or {}
        bloom = stage1.get("bloom") or {}
        return {
            "fp_rate": round((fp / firsts) if firsts else 0.0, 6),
            "fn_rate": round((fn / repeats) if repeats else 0.0, 6),
            "inserted": bloom.get("inserted", firsts),
            "bit_array_size": bloom.get("bit_array_size") or bloom_probe.bit_array_size,
            "hash_functions": bloom.get("hash_functions") or bloom_probe.hash_functions,
            "memory_bytes": bloom.get("memory_bytes") or bloom_probe.memory_bytes,
            "false_positive_example": fp_example or bloom.get("false_positive_example"),
        }

    def emit(window: Window, closed_by: str) -> None:
        now_ms = int(time.time() * 1000)
        report_lat = [now_ms - ts for ts in window.producer_ts if ts]
        fare_mean = (fare_sum / fare_n) if fare_n else None
        dist_mean = (dist_sum / dist_n) if dist_n else None
        by_k = {}
        for k, reservoir in fare_reservoirs.items():
            fare_est = reservoir.mean()
            dist_est = dist_reservoirs[k].mean()
            by_k[str(k)] = {
                "k": k,
                "exact_mean_fare": round_or_none(fare_mean, 4),
                "sample_mean_fare": round_or_none(fare_est, 4),
                "error_pct": round_or_none(pct_error(fare_est, fare_mean), 4),
                "exact_mean_distance": round_or_none(dist_mean, 4),
                "sample_mean_distance": round_or_none(dist_est, 4),
                "distance_error_pct": round_or_none(pct_error(dist_est, dist_mean), 4),
            }
        headline_k = str(int(cfg["reservoir_report_k"]))
        sampling = dict(by_k[headline_k])
        sampling["by_k"] = by_k
        sampling["scope"] = "cumulative_stream"

        def fraction(table: dict[int, set[int]]):
            if not table:
                return None
            diverse = sum(1 for drops in table.values() if len(drops) >= diverse_at)
            return diverse / len(table)

        exact_fraction = fraction(window.destinations)
        event_fraction = fraction(window.event_dest)
        entity_fraction = fraction(window.entity_dest)
        sampling["diversity"] = {
            "exact_fraction": round_or_none(exact_fraction, 4),
            "event_sampling_fraction": round_or_none(event_fraction, 4),
            "entity_sampling_fraction": round_or_none(entity_fraction, 4),
            "event_sample_events": window.event_sample_events,
            "entity_sample_events": window.entity_sample_events,
            "sample_rate": sample_rate,
            "more_reliable": "entity",
            "why": DIVERSITY_REASON,
        }

        hll_est = window.hll.count()
        hll_exact = len(window.routes)
        dgim_by_k = {}
        bits = list(recent_bits)
        for k in cfg["dgim_query_ks"]:
            k = int(k)
            exact_ones = sum(bits[-k:])
            estimate = dgim.estimate(k)
            dgim_by_k[str(k)] = {
                "k": k,
                "exact_ones": exact_ones,
                "estimate": round(estimate, 2),
                "abs_error": round(abs(estimate - exact_ones), 2),
                "error_pct": round_or_none(pct_error(estimate, exact_ones), 4),
            }
        dgim_headline = dict(dgim_by_k[str(int(cfg["dgim_report_k"]))])
        dgim_headline["buckets"] = dgim.bucket_count
        dgim_headline["by_k"] = dgim_by_k
        dgim_headline["window_events"] = dgim.window_size

        producer_state = read_status(cfg, "producer") or {}
        stage1_state = read_status(cfg, "stage1") or {}
        elapsed = elapsed_busy()
        report = {
            "run_id": cfg["run_id"],
            "window_start": format_epoch(window.start),
            "window_end": format_epoch(window.end),
            "events_processed": processed,
            "window_events": window.messages,
            "producer_eps": producer_state.get("eps"),
            "stage1_eps": stage1_state.get("eps"),
            "stage2_eps": round(processed / elapsed, 2),
            "consumer_lag": max_lag,
            "lag_at_report": assigned_lag(consumer),
            "max_lag_clean": max_lag,
            "latency_p50_ms": round_or_none(percentile(report_lat, 0.50), 2),
            "latency_p95_ms": round_or_none(percentile(report_lat, 0.95), 2),
            "processing_latency_p50_ms": round_or_none(percentile(window.processing_latencies, 0.50), 2),
            "processing_latency_p95_ms": round_or_none(percentile(window.processing_latencies, 0.95), 2),
            "latency_note": (
                "latency_p50_ms/p95_ms are report-timestamp minus producer_ts_ms. "
                "consumer_lag is the max Kafka lag on taxi.clean seen by this report, not the drained value. "
                "processing_latency_* is stage2 receive time minus producer_ts_ms and is the number that moves with load."
            ),
            "bloom": bloom_block(),
            "sampling": sampling,
            "hll": {
                "exact_routes": hll_exact,
                "estimate": round(hll_est, 2),
                "error_pct": round_or_none(pct_error(hll_est, hll_exact), 4),
                "precision": int(cfg["hll_precision"]),
                "registers": window.hll.m,
                "sketch_bytes": window.hll.memory_bytes,
                "exact_key_bytes": sum(len(route) for route in window.routes),
                "exact_bytes_approx": sum(64 + len(route) for route in window.routes),
            },
            "dgim": dgim_headline,
            "closed_by": closed_by,
            "watermark": format_epoch(int(calendar_epoch(watermark_time(max_event, lateness)))) if max_event else None,
            "late_included": window.late_included,
            "late_updated": window.late_updated,
            "partitions": int(cfg["partitions"]),
            "consumers": int(cfg["consumers"]),
            "partition_key": cfg.get("partition_key"),
        }
        producer.send("taxi.metrics", key=report["window_start"], value=report)

    def calendar_epoch(moment):
        import calendar
        return calendar.timegm(moment.timetuple())

    def close_ready(force: bool = False) -> None:
        mark = watermark_time(max_event, lateness)
        for start, window in list(windows.items()):
            end_dt = parse_event_time(format_epoch(window.end))
            passed_end = max_event is not None and max_event >= end_dt
            if passed_end:
                window.saw_past_end = True
            should_close = force or (mark is not None and mark >= end_dt)
            if not should_close:
                continue
            emit(window, "end_of_stream" if force and not (mark and mark >= end_dt) else "watermark")
            closed.add(start)
            del windows[start]

    def on_event(event: dict) -> None:
        nonlocal max_event, processed, firsts, repeats, fp, fn, fp_example, discarded, first_event_at
        if first_event_at is None:
            first_event_at = time.perf_counter()
        processed += 1
        unique_ids.add(event["event_id"])
        applied_log.write(event["event_id"] + "\n")
        bit = int(event.get("slow_trip_bit") or 0)
        dgim.add_bit(bit)
        recent_bits.append(bit)
        if event.get("fare_amount") is not None:
            fare_sum_add = float(event["fare_amount"])
            for reservoir in fare_reservoirs.values():
                reservoir.add(fare_sum_add)
            nonlocal_fare(fare_sum_add)
        if event.get("trip_distance") is not None:
            dist_add = float(event["trip_distance"])
            for reservoir in dist_reservoirs.values():
                reservoir.add(dist_add)
            nonlocal_dist(dist_add)

        actually = bool(event.get("actually_seen_before"))
        said = bool(event.get("bloom_seen_before"))
        if actually:
            repeats += 1
            if not said:
                fn += 1
        else:
            firsts += 1
            if said:
                fp += 1
                if fp_example is None:
                    fp_example = {
                        "event_id": event.get("event_id"),
                        "pickup_zone": event.get("pickup_zone"),
                        "note": "Bloom reported seen before this event_id was inserted",
                    }

        moment = parse_event_time(event["event_time"])
        start, end = window_bounds(moment, window_minutes)
        now_ms = int(time.time() * 1000)
        hop = now_ms - int(event.get("producer_ts_ms") or now_ms)
        processing_latencies.append(hop)
        watermark = watermark_time(max_event, lateness)
        is_late = watermark is not None and moment < watermark
        if start in closed:
            discarded += 1
            _remember_late(_late_record(event, moment, watermark, start, "discarded"))
            if moment > (max_event or moment):
                max_event = moment
            return

        if max_event is None or moment > max_event:
            max_event = moment
        window = windows.get(start)
        if window is None:
            window = Window(start, end, int(cfg["hll_precision"]))
            windows[start] = window
        decision = None
        if is_late:
            decision = "updated" if window.saw_past_end else "included"
            if decision == "updated":
                window.late_updated += 1
            else:
                window.late_included += 1
            _remember_late(_late_record(event, moment, watermark, start, decision))

        window.messages += 1
        window.processing_latencies.append(hop)
        if event.get("producer_ts_ms"):
            window.producer_ts.append(int(event["producer_ts_ms"]))
        route = event["route_key"]
        window.routes.add(route)
        window.hll.add(route)
        zone = int(event["pickup_zone"])
        drop = int(event["dropoff_zone"])
        window.destinations[zone].add(drop)
        if event["event_id"] not in window.seen_ids:
            window.trips[zone] += 1
            window.seen_ids.add(event["event_id"])
        if sample_rng.random() < sample_rate:
            window.event_dest[zone].add(drop)
            window.event_sample_events += 1
        if _rate_ok(zone, sample_rate, seed):
            window.entity_dest[zone].add(drop)
            window.entity_sample_events += 1
        close_ready(False)

    def nonlocal_fare(value: float) -> None:
        nonlocal fare_sum, fare_n
        fare_sum += value
        fare_n += 1

    def nonlocal_dist(value: float) -> None:
        nonlocal dist_sum, dist_n
        dist_sum += value
        dist_n += 1

    def _remember_late(example: dict) -> None:
        # Keep a few of each decision. The first events are usually "included",
        # and a single cap would hide the discarded ones the TA asks to see.
        already = sum(1 for item in late_examples if item["decision"] == example["decision"])
        if already < 4:
            late_examples.append(example)

    def _late_record(event, moment, watermark, start, decision) -> dict:
        return {
            "event_id": event.get("event_id"),
            "event_time": event.get("event_time"),
            "window_start": format_epoch(start),
            "watermark": watermark.isoformat(timespec="seconds") if watermark else None,
            "event_time_lateness_sec": round((max_event - moment).total_seconds(), 3) if max_event else None,
            "producer_ts_ms": event.get("producer_ts_ms"),
            "decision": decision,
        }

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
                # An empty assignment reports lag 0. Wait until this process has
                # actually joined the group, or a restart exits before reading
                # the backlog left by the killed process.
                if stage1.get("done") and empty >= 3 and saw_assignment and lag == 0:
                    break
                if empty % 5 == 0:
                    live(False)
                continue
            empty = 0
            batch = []
            for _tp, messages in polled.items():
                batch.extend(messages)
            batch.sort(key=lambda message: message.value.get("event_time") or "")
            for message in batch:
                on_event(message.value)
            if processed - last_commit_at >= commit_every:
                consumer.commit()
                last_commit_at = processed
            if processed % 200 == 0:
                live(False)
        close_ready(True)
        consumer.commit()
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        applied_log.close()

    elapsed = max(time.perf_counter() - (first_event_at or started), 1e-6)
    summary = {
        "done": True,
        "run_id": cfg["run_id"],
        "processed": processed,
        "unique_ids": len(unique_ids),
        "duplicate_processing": max(0, processed - len(unique_ids)),
        "eps": round(processed / elapsed, 2),
        "elapsed_sec": round(elapsed, 3),
        "max_lag_clean": max_lag,
        "discarded_late": discarded,
        "late_examples": late_examples,
        "processing_latency_p50_ms": round_or_none(percentile(processing_latencies, 0.50), 2),
        "processing_latency_p95_ms": round_or_none(percentile(processing_latencies, 0.95), 2),
        "bloom": bloom_block(),
        "checkpoint_state": bool(cfg.get("checkpoint_state")),
        "state_note": (
            "Stage 2 keeps DGIM, reservoirs, HyperLogLog, and window sets in memory. "
            "Offsets are committed every "
            f"{commit_every} events (at-least-once). A kill before commit replays that "
            "suffix, and without a checkpoint the in-memory sketches start empty while "
            "Kafka resumes from the last commit, so the prefix is not recomputed."
        ),
    }
    write_status(cfg, "stage2", summary)
    write_json(Path(cfg["_root"]) / "results" / "runs" / f"{cfg['run_id']}_stage2.json", summary)
    print(
        f"stage2 done run={cfg['run_id']} processed={processed} eps={summary['eps']} "
        f"discarded_late={discarded} max_lag={max_lag}"
    )


if __name__ == "__main__":
    main()

"""Start Kafka stages for one run, or the full experiment matrix.

--demo runs the default configuration (P3, medium rate, pickup_zone key).
--experiments runs every required comparison and writes experiment_summary.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.config import load_config, with_overrides, write_config
from common.control import control_dir
from common.kafka_util import recreate_topics, wait_for_kafka
from common.stats import read_json

MODULES = (
    ("final", "final_consumer.consume"),
    ("stage3", "stage3_lsh.stage3"),
    ("stage2", "stage2_stream_analytics.stage2"),
    ("stage1", "stage1_clean_bloom.stage1"),
    ("producer", "producer.produce"),
)


def _env() -> dict:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT)
    return env


def _tail(path: Path, lines: int = 30) -> str:
    if not path.exists():
        return ""
    data = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(data[-lines:])


def _clear_run(run_id: str) -> None:
    import tempfile
    folders = [
        ROOT / "results" / "control",
        Path(tempfile.gettempdir()) / "bda_assignment2_control",
    ]
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)
        for path in folder.glob(f"{run_id}*"):
            path.unlink()
    for name in (
        f"{run_id}_windows.jsonl",
        f"{run_id}_stage1.json",
        f"{run_id}_stage2.json",
        f"{run_id}_stage3.json",
        f"{run_id}_producer.json",
        f"{run_id}_final.json",
        f"{run_id}_applied_ids.txt",
    ):
        target = ROOT / "results" / "runs" / name
        if target.exists():
            target.unlink()


def _spawn(module: str, config_path: Path, log_path: Path) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(log_path, "w", encoding="utf-8")
    return subprocess.Popen(
        [sys.executable, "-m", module, "--config", str(config_path)],
        cwd=str(ROOT),
        env=_env(),
        stdout=handle,
        stderr=subprocess.STDOUT,
    )


def _wait(procs: dict[str, subprocess.Popen], timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        codes = {name: proc.poll() for name, proc in procs.items()}
        if all(code is not None for code in codes.values()):
            return
        if any(code not in (0, None) for code in codes.values()):
            time.sleep(0.8)
            break
        time.sleep(0.4)
    for proc in procs.values():
        if proc.poll() is None:
            proc.kill()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                pass


def run_once(base: dict, overrides: dict, timeout: float = 420) -> dict:
    cfg = with_overrides(base, **overrides)
    run_id = cfg["run_id"]
    _clear_run(run_id)
    config_path = control_dir(cfg) / f"{run_id}.yaml"
    write_config(cfg, config_path)
    print(
        f"\n=== {run_id}: partitions={cfg['partitions']} consumers={cfg['consumers']} "
        f"key={cfg['partition_key']} rate={cfg['producer_rate']} events={cfg['max_events']} ===",
        flush=True,
    )
    wait_for_kafka(cfg["bootstrap_servers"])
    recreate_topics(
        cfg["bootstrap_servers"],
        int(cfg["partitions"]),
        int(cfg["metrics_partitions"]),
        int(cfg["lsh_partitions"]),
    )
    logs = ROOT / "logs"
    procs = {}
    for name, module in MODULES:
        if name == "producer":
            continue
        procs[name] = _spawn(module, config_path, logs / f"{run_id}_{name}.log")
    time.sleep(4)
    procs["producer"] = _spawn("producer.produce", config_path, logs / f"{run_id}_producer.log")
    _wait(procs, timeout)
    failed = {name: proc.returncode for name, proc in procs.items() if proc.returncode not in (0, None)}
    if failed:
        print(f"run {run_id} process exits: {failed}")
        for name in failed:
            print(f"--- {name} log ---")
            print(_tail(logs / f"{run_id}_{name}.log"))
    summary = {
        "run_id": run_id,
        "exit_codes": {name: proc.returncode for name, proc in procs.items()},
        "producer": read_json(ROOT / "results" / "runs" / f"{run_id}_producer.json"),
        "stage1": read_json(ROOT / "results" / "runs" / f"{run_id}_stage1.json"),
        "stage2": read_json(ROOT / "results" / "runs" / f"{run_id}_stage2.json"),
        "final": read_json(ROOT / "results" / "runs" / f"{run_id}_final.json"),
    }
    windows = _windows(run_id)
    summary["windows"] = len(windows)
    print(
        f"=== {run_id} finished windows={len(windows)} "
        f"exits={summary['exit_codes']} ===",
        flush=True,
    )
    return summary


def _windows(run_id: str) -> list[dict]:
    path = ROOT / "results" / "runs" / f"{run_id}_windows.jsonl"
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _mean(values):
    values = [v for v in values if isinstance(v, (int, float))]
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def csv_row(cfg_overrides: dict, label: str) -> dict:
    run_id = cfg_overrides["run_id"]
    producer = read_json(ROOT / "results" / "runs" / f"{run_id}_producer.json") or {}
    stage1 = read_json(ROOT / "results" / "runs" / f"{run_id}_stage1.json") or {}
    stage2 = read_json(ROOT / "results" / "runs" / f"{run_id}_stage2.json") or {}
    windows = _windows(run_id)
    bloom = (stage1.get("bloom") or {})
    return {
        "experiment": label,
        "run_id": run_id,
        "partitions": cfg_overrides["partitions"],
        "consumers": cfg_overrides["consumers"],
        "kafka_key": cfg_overrides["partition_key"],
        "target_rate": cfg_overrides["producer_rate"],
        "produced_eps": producer.get("eps_during_read") or producer.get("eps"),
        "produced_eps_overall": producer.get("eps"),
        "stage1_eps": stage1.get("eps"),
        "stage2_eps": stage2.get("eps"),
        "latency_p50_ms": _mean([w.get("latency_p50_ms") for w in windows]),
        "latency_p95_ms": _mean([w.get("latency_p95_ms") for w in windows]),
        "processing_p50_ms": stage2.get("processing_latency_p50_ms"),
        "processing_p95_ms": stage2.get("processing_latency_p95_ms"),
        "max_lag_raw": stage1.get("max_lag_raw"),
        "max_lag_clean": stage2.get("max_lag_clean"),
        "partition_skew": producer.get("partition_skew"),
        "idle_consumers": stage1.get("idle_consumers"),
        "hll_error_pct": _mean([(w.get("hll") or {}).get("error_pct") for w in windows]),
        "dgim_error_pct": _mean([(w.get("dgim") or {}).get("error_pct") for w in windows]),
        "sampling_error_pct": _mean([(w.get("sampling") or {}).get("error_pct") for w in windows]),
        "lsh_recall": _mean([(w.get("lsh") or {}).get("recall") for w in windows]),
        "lsh_candidate_reduction": _mean([(w.get("lsh") or {}).get("candidate_reduction") for w in windows]),
        "lsh_alt_recall": _mean([(w.get("lsh_alt") or {}).get("recall") for w in windows]),
        "lsh_alt_candidates": _mean([(w.get("lsh_alt") or {}).get("candidates") for w in windows]),
        "bloom_fp_rate": bloom.get("fp_rate"),
        "bloom_fn_rate": bloom.get("fn_rate"),
        "bloom_fp_example": (bloom.get("false_positive_example") or {}).get("event_id") if isinstance(bloom.get("false_positive_example"), dict) else None,
        "events_processed": stage2.get("processed"),
        "discarded_late": stage2.get("discarded_late"),
        "zones_split": _mean([
            ((w.get("repartition") or {}).get("zones_split_across_partitions")) for w in windows
        ]),
        "partition_local_fn_rate": bloom.get("partition_local_fn_rate"),
    }


def group_lag(bootstrap: str, group: str, topic: str) -> dict:
    from kafka import KafkaAdminClient, KafkaConsumer

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="lag-probe")
    try:
        fetched = admin.list_group_offsets(group)
    except Exception as exc:
        return {"error": str(exc), "lag": None}
    finally:
        admin.close()
    committed = fetched.get(group, fetched) if isinstance(fetched, dict) else {}
    selected = [tp for tp in committed if getattr(tp, "topic", None) == topic]
    if not selected:
        return {"lag": None, "committed": 0, "end": None}
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    ends = consumer.end_offsets(selected)
    consumer.close()
    lag = 0
    committed_sum = 0
    end_sum = 0
    for tp in selected:
        off = committed[tp].offset
        committed_sum += off
        end_sum += ends[tp]
        lag += max(0, ends[tp] - off)
    return {"lag": lag, "committed": committed_sum, "end": end_sum}


def run_restart(base: dict) -> None:
    """Stop Stage 2 mid-stream and show lag, offsets, and lost in-memory state."""
    overrides = dict(
        run_id="restart-demo",
        partitions=4,
        consumers=4,
        partition_key="pickup_zone",
        producer_rate=400,
        max_events=8000,
        delayed_event_rate=0,
        max_delay_seconds=0,
        duplicate_rate=0,
        checkpoint_state=False,
        commit_every=1000,
    )
    cfg = with_overrides(base, **overrides)
    run_id = cfg["run_id"]
    _clear_run(run_id)
    config_path = control_dir(cfg) / f"{run_id}.yaml"
    write_config(cfg, config_path)
    print("\n=== restart-demo ===", flush=True)
    wait_for_kafka(cfg["bootstrap_servers"])
    recreate_topics(cfg["bootstrap_servers"], 4, 1, 1)
    logs = ROOT / "logs"
    procs = {}
    for name, module in MODULES:
        if name in ("producer", "stage2"):
            continue
        procs[name] = _spawn(module, config_path, logs / f"{run_id}_{name}.log")
    stage2 = _spawn("stage2_stream_analytics.stage2", config_path, logs / f"{run_id}_stage2_first.log")
    time.sleep(4)
    producer = _spawn("producer.produce", config_path, logs / f"{run_id}_producer.log")
    before = None
    deadline = time.time() + 90
    while time.time() < deadline:
        live = read_json(control_dir(cfg) / f"{run_id}_stage2.json") or {}
        if live.get("processed", 0) >= 2500:
            before = group_lag(cfg["bootstrap_servers"], f"stage2-{run_id}", "taxi.clean")
            before["processed"] = live.get("processed")
            break
        time.sleep(0.3)
    print(f"restart snapshot before kill: {before}", flush=True)
    stage2.kill()
    stage2.wait(timeout=10)
    time.sleep(3)
    during = group_lag(cfg["bootstrap_servers"], f"stage2-{run_id}", "taxi.clean")
    print(f"restart snapshot while Stage 2 is down: {during}", flush=True)
    stage2b = _spawn("stage2_stream_analytics.stage2", config_path, logs / f"{run_id}_stage2_second.log")
    producer.wait(timeout=180)
    _wait({"stage1": procs["stage1"], "stage3": procs["stage3"], "final": procs["final"], "stage2": stage2b}, 180)
    after = group_lag(cfg["bootstrap_servers"], f"stage2-{run_id}", "taxi.clean")
    stage2_summary = read_json(ROOT / "results" / "runs" / f"{run_id}_stage2.json") or {}
    producer_summary = read_json(ROOT / "results" / "runs" / f"{run_id}_producer.json") or {}
    id_path = ROOT / "results" / "runs" / f"{run_id}_applied_ids.txt"
    replayed = 0
    if id_path.exists():
        counts: dict[str, int] = {}
        for line in id_path.read_text(encoding="utf-8").splitlines():
            if line:
                counts[line] = counts.get(line, 0) + 1
        replayed = sum(1 for n in counts.values() if n > 1)
    observation = {
        "run_id": run_id,
        "before_kill": before,
        "while_stopped": during,
        "after_recovery": after,
        "produced": producer_summary.get("produced"),
        "stage2_processed_after_restart": stage2_summary.get("processed"),
        "stage2_unique_ids": stage2_summary.get("unique_ids"),
        "duplicate_processing_in_restarted_process": stage2_summary.get("duplicate_processing"),
        "event_ids_applied_twice": replayed,
        "what_happened": (
            "Stage 2 was killed after a commit. While it was down the producer kept writing, "
            "the committed offset stayed put, and lag (log end minus committed offset) grew. "
            "On restart the same group resumed at that commit and drained the suffix; lag "
            "returned to 0. In-memory DGIM, reservoirs, and windows started empty because "
            "checkpoint_state is false, so the committed prefix was not rebuilt. "
            "If the kill lands after a batch is applied and before the next commit "
            "(commit_every is 1000), those event ids are applied twice. "
            f"This run applied {replayed} event ids twice. The pipeline is at-least-once."
        ),
    }
    dest = ROOT / "results" / "restart_observation.json"
    dest.write_text(json.dumps(observation, indent=2), encoding="utf-8")
    print(json.dumps(observation, indent=2), flush=True)


def experiment_plan(base: dict) -> list[tuple[str, dict]]:
    shared = dict(
        partitions=4,
        consumers=4,
        partition_key="pickup_zone",
        producer_rate=1000,
        max_events=16000,
        duplicate_rate=0.02,
        delayed_event_rate=0.02,
        max_delay_seconds=20,
        bloom_expected_items=200000,
        bloom_false_positive_target=0.01,
        commit_every=200,
    )
    runs = [
        ("P1", dict(shared, run_id="P1", partitions=1, consumers=1)),
        ("P2", dict(shared, run_id="P2", partitions=4, consumers=1)),
        ("P3-medium-K1", dict(shared, run_id="P3-medium")),
        ("P4", dict(shared, run_id="P4", partitions=4, consumers=6)),
        ("K2-round-robin", dict(shared, run_id="K2-round-robin", partition_key="none")),
        ("rate-low", dict(shared, run_id="rate-low", producer_rate=100)),
        ("rate-high", dict(shared, run_id="rate-high", producer_rate=0, producer_linger_ms=30)),
        ("bloom-fp", dict(
            shared,
            run_id="bloom-fp",
            bloom_expected_items=400,
            bloom_false_positive_target=0.15,
            max_events=5000,
            producer_rate=2500,
            delayed_event_rate=0,
            max_delay_seconds=0,
        )),
        ("late-events", dict(
            shared,
            run_id="late-events",
            producer_rate=8,
            max_events=700,
            delayed_event_rate=0.08,
            max_delay_seconds=12,
            allowed_lateness_seconds=25,
            consumers=1,
            partitions=1,
        )),
    ]
    return runs


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="default P3-medium pipeline only")
    parser.add_argument("--experiments", action="store_true", help="full required experiment matrix")
    parser.add_argument("--restart", action="store_true", help="Stage 2 kill/restart observation")
    args = parser.parse_args()
    base = load_config(ROOT / "config.yaml")
    if args.experiments:
        jsonl = ROOT / base["results_jsonl"]
        jsonl.parent.mkdir(parents=True, exist_ok=True)
        jsonl.write_text("", encoding="utf-8")
        rows = []
        for label, overrides in experiment_plan(base):
            run_once(base, overrides)
            rows.append(csv_row(overrides, label))
        run_restart(base)
        write_csv(rows, ROOT / base["experiment_csv"])
        print(f"wrote {ROOT / base['experiment_csv']}")
        return
    if args.restart:
        run_restart(base)
        return
    # Default TA path: one pipeline, the config.yaml run.
    jsonl = ROOT / base["results_jsonl"]
    if jsonl.exists() and args.demo:
        # Demo appends a fresh copy of the default run without deleting experiment history
        # unless the file is empty. A dedicated demo still appends; use --experiments to rebuild.
        pass
    overrides = {
        "run_id": base["run_id"],
        "partitions": base["partitions"],
        "consumers": base["consumers"],
        "partition_key": base["partition_key"],
        "producer_rate": base["producer_rate"],
        "max_events": base["max_events"],
    }
    run_once(base, overrides)


if __name__ == "__main__":
    main()

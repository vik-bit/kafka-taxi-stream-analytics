"""Join taxi.metrics and taxi.lsh into results.jsonl and a console line."""

from __future__ import annotations

import json
from pathlib import Path

from kafka import KafkaConsumer

from common.config import cfg_from_argv
from common.control import read_status, stopped, write_status
from common.stats import write_json


def _console(row: dict) -> None:
    sampling = row.get("sampling") or {}
    hll = row.get("hll") or {}
    dgim = row.get("dgim") or {}
    lsh = row.get("lsh") or {}
    bloom = row.get("bloom") or {}
    print(
        f"[{row.get('run_id')}] {row.get('window_start')} -> {row.get('window_end')} "
        f"events={row.get('events_processed')} "
        f"fare {sampling.get('exact_mean_fare')} vs {sampling.get('sample_mean_fare')} "
        f"({sampling.get('error_pct')}%) "
        f"HLL {hll.get('exact_routes')} vs {hll.get('estimate')} ({hll.get('error_pct')}%) "
        f"DGIM k={dgim.get('k')} {dgim.get('exact_ones')} vs {dgim.get('estimate')} "
        f"({dgim.get('error_pct')}%) "
        f"LSH recall={lsh.get('recall')} candidates={lsh.get('candidates')} "
        f"reduction={lsh.get('candidate_reduction')} "
        f"bloom_fp={bloom.get('fp_rate')}",
        flush=True,
    )


def main() -> None:
    cfg = cfg_from_argv()
    root = Path(cfg["_root"])
    jsonl_path = root / cfg["results_jsonl"]
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    per_run = root / "results" / "runs" / f"{cfg['run_id']}_windows.jsonl"
    consumer = KafkaConsumer(
        "taxi.metrics",
        "taxi.lsh",
        bootstrap_servers=cfg["bootstrap_servers"],
        group_id=f"final-{cfg['run_id']}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    metrics = {}
    lsh = {}
    emitted = set()
    rows = []

    def try_emit(key: str) -> None:
        if key in emitted or key not in metrics or key not in lsh:
            return
        row = dict(metrics[key])
        row["lsh"] = lsh[key].get("lsh")
        row["lsh_alt"] = lsh[key].get("lsh_alt")
        row["lsh_example_pairs"] = lsh[key].get("lsh_example_pairs")
        row["repartition"] = lsh[key].get("repartition")
        line = json.dumps(row, separators=(",", ":"))
        with open(jsonl_path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        with open(per_run, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        _console(row)
        emitted.add(key)
        rows.append(row)

    empty = 0
    try:
        while not stopped(cfg):
            polled = consumer.poll(timeout_ms=400)
            if not polled:
                empty += 1
                stage2 = read_status(cfg, "stage2") or {}
                stage3 = read_status(cfg, "stage3") or {}
                if stage2.get("done") and stage3.get("done") and empty >= 3:
                    break
                continue
            empty = 0
            for _tp, messages in polled.items():
                for message in messages:
                    body = message.value
                    key = f"{body.get('run_id')}|{body.get('window_start')}"
                    if message.topic == "taxi.metrics":
                        metrics[key] = body
                    else:
                        lsh[key] = body
                    try_emit(key)
            consumer.commit()
    finally:
        consumer.close()

    # A window can miss one side only if a stage crashed. Keep the orphan visible.
    orphans = []
    for key in set(metrics) | set(lsh):
        if key not in emitted:
            orphans.append(key)
    summary = {"done": True, "windows": len(rows), "orphans": orphans, "run_id": cfg["run_id"]}
    write_status(cfg, "final", summary)
    write_json(root / "results" / "runs" / f"{cfg['run_id']}_final.json", {"windows": rows, **summary})
    print(f"final consumer done run={cfg['run_id']} windows={len(rows)} orphans={len(orphans)}", flush=True)


if __name__ == "__main__":
    main()

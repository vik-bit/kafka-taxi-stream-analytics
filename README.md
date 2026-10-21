# Real-Time Taxi Stream Analytics

Kafka pipeline for NYC yellow taxi trips: clean + Bloom filter, reservoir / event / entity sampling, HyperLogLog, DGIM, and MinHash-LSH. Every stage after the producer reads a Kafka topic and writes a Kafka topic. Topic names are the required ones: `taxi.raw`, `taxi.clean`, `taxi.metrics`, `taxi.lsh`.

The processing runtime is a Python consumer-group stream processor (the same model as Kafka Streams: a group per stage, manual offset commits, event-time watermarks, optional keys). DGIM is implemented in this repo. HyperLogLog, Bloom, and MinHash are also implemented here so their parameters are visible. Spark or Flink would not change the Kafka contract; this runtime keeps a parameter change in `config.yaml` to a single rerun on a laptop.

## Machine

- Windows 11 Home (10.0.26200), Docker Desktop
- 12th Gen Intel Core i7-1255U (10 cores, 12 logical processors), 16 GB RAM
- Kafka 7.6.1 runs in one Docker container. Producer and all four stages run on the Windows host and connect to `localhost:9092`.

## Data

`data/taxi_sample.csv` is the busiest 4 hours of NYC TLC yellow trips in January 2024: 31,880 rows from 2024-01-04 16:00 through 19:59. `scripts/prepare_data.py` rebuilds it (and downloads the TLC parquet if it is missing). The producer is the only process that reads the file. Rows with a missing zone or a bad timestamp are dropped. Source columns are renamed to the assignment names. `dropoff_time` is included on `taxi.raw` because trip duration cannot be computed from the pickup time alone.

## Run the default pipeline

From this directory:

```bat
run_all.bat
```

That installs dependencies, prepares the CSV if needed, starts Kafka, creates the topics, and runs the `config.yaml` settings: 4 partitions, 4 consumers, key = `pickup_zone`, 1000 events/s, up to 20,000 trips, 2% duplicates, 2% delayed by 1–20 seconds. Window reports land in `results/results.jsonl` and are printed to the console.

Equivalent shell form: `bash run_all.sh`.

The full comparison table is already in `results/experiment_summary.csv`. Rebuild it (about 15 minutes) with:

```bat
run_experiments.bat
```

Useful knobs in `config.yaml`, then `run_all.bat` again:

| Change the TA asked about | Key |
| --- | --- |
| Producer rate | `producer_rate` (0 means unlimited) |
| Partitions / consumers | `partitions`, `consumers` |
| Key | `partition_key`: `pickup_zone` or `none` |
| Reservoir size in the headline | `reservoir_report_k` (100, 1000, or 10000) |
| DGIM query in the headline | `dgim_report_k` (1000, 5000, or 10000) |
| Bloom size | `bloom_expected_items`, `bloom_false_positive_target` |
| LSH banding | `lsh_bands`, `lsh_rows_per_band` (product must stay 100) |
| Watermark | `allowed_lateness_seconds` |

`results/runs/<run_id>_windows.jsonl` has the same objects as `results.jsonl` for one run. `results/late_event_examples.json` and `results/restart_observation.json` are the Task 5 evidence.

## What each stage does

**Producer.** Replays the CSV in pickup-time order into `taxi.raw` at the configured rate and records the achieved rate. About 2% of events are sent twice with the same `event_id`. About 2% are held 1–20 seconds of wall clock. The baseline key is `pickup_zone`.

**Stage 1** (`taxi.raw` → `taxi.clean`). Checks the zone and timestamps, then adds `route_key`, `trip_duration_min`, `slow_trip_bit` (1 when duration > 30 minutes), `bloom_seen_before`, and `stage1_ts_ms`. The Bloom filter is sized from `bloom_expected_items` and `bloom_false_positive_target`. A hit never drops the event. Ground truth is the set of ids already inserted. Several threads share one consumer group; Kafka will not give a partition to more threads than the topic has.

**Stage 2** (`taxi.clean` → `taxi.metrics`). One consumer, because the sketches are global. Reservoirs at k = 100, 1,000, and 10,000 for fare and distance, plus the exact running means. Per 60-minute event-time window: exact route set vs HyperLogLog (precision 12), and the diversity fraction (a zone is diverse with at least 10 distinct drop-off zones) under event sampling and entity sampling at the same `sample_rate`. DGIM tracks `slow_trip_bit` over the last 10,000 events and is queried at k = 1,000, 5,000, and 10,000. Windows close when the watermark (`max event time - allowed_lateness_seconds`) passes the window end. Offsets are committed every 200 events (at-least-once).

**Stage 3** (`taxi.clean` → `taxi.lsh`). One destination set per pickup zone per hour. Zones with at least 20 trips get a 100-hash MinHash signature. Default banding is 20 bands × 5 rows. The second setting is 10 × 10. Candidates are verified with exact Jaccard at 0.50. All-pairs exact Jaccard on the same zones is the ground truth for recall, candidate precision, and candidate reduction.

**Final consumer.** Joins a metrics message and an LSH message on `run_id` + `window_start`, appends one JSON object to `results/results.jsonl`, and prints one line.

## Experiments (measured on this machine)

Partition runs use the same 16,000-trip prefix and a 1000 events/s target. Achieved rate during the read was 1000/s. The overall average is lower (~463/s) because 2% of events are held up to 20 seconds.

**P1–P4.** Same 16,000 trips. Stage 1 stayed near 445–457 events/s for P1 (1 partition, 1 consumer), P2 (4 partitions, 1 consumer), and P3 (4 and 4). Max lag on `taxi.raw` was 45–556, so one consumer was already keeping up and extra partitions did not raise throughput. P4 starts 6 consumers on 4 partitions: 4 busy and **2 idle**, 446 events/s. A consumer past the partition count is never assigned one. P3 applied about 200 events twice during a rebalance; that is at-least-once, not a different input.

**Key.** `pickup_zone` put 33.5% of messages on the busiest of 4 partitions; round-robin put 25.6%. Throughput was almost the same. With the zone key, every pickup zone's trips stayed on one partition (0 zones split). Round-robin split 81 zones, so Stage 3 had to merge partial destination sets before MinHash. A partition-local Bloom filter, measured as a diagnostic beside the real shared filter, missed 70% of intentional duplicates under round-robin and missed none under the zone key. Same-key locality keeps per-zone and per-id state correct; round-robin evens the load and forces a merge.

**Rate (4 partitions, 4 consumers).**

All three rates use the same 16,000 trips, 4 partitions, and 4 consumers.

| Rate | Produced/s | Stage 2/s | Processing p50 / p95 | Report p50 | Max lag raw | HLL err | DGIM err | LSH recall | Late drops |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Low (target 100) | 100 | 91 | 144 / 390 ms | 49 s | 50 | 2.0% | 31% | 0.72 | 17 |
| Medium (target 1000, P3) | 1000 | 446 | 302 / 1078 ms | 15 s | 556 | 2.1% | 18% | 0.73 | 264 |
| High (unlimited) | 11206 | 721 | 4.5 / 11.0 s | 13 s | 11951 | 1.7% | 27% | 0.66 | 2933 |

The high run is where the producer gets ahead: lag on `taxi.raw` reaches about 12,000 and processing p95 goes to 11 s. HLL error does not get worse. LSH recall drops because 2,933 events miss their hour. A 20-second wall-clock hold is a long jump in event time at this replay speed. The sketch did not get worse; the window lost late trips. Report p50 (window-close time minus `producer_ts_ms`) moves the other way: it is 49 s at the slow rate, because the hour takes longer to finish, and 13 s when the file is pushed through quickly.

## Algorithm results on the default hour (P3, 16:00–17:00)

**Bloom (default).** m = 1,917,012 bits, k = 7, sized for 200,000 ids at 1% false positives. On ~16k inserts the observed false-positive rate and false-negative rate were both 0. The filter is far from full, so that is expected. A separate run (`bloom-fp`, 400 expected ids, target 15%, m = 1,580, k = 3) produced a false-positive rate of 0.805 and false-negative rate 0. Example: `2024-01-row-00000018`, pickup zone 142, drop-off zone 230. The event was not dropped.

**Reservoir (k = 1000), cumulative.** Exact mean fare 19.17 vs sample 20.06 (4.7% error). Exact mean distance 3.21 vs sample 3.36 (4.9%). k = 100 and k = 10,000 are under `sampling.by_k`.

**Diversity, sample rate 0.10.** Exact fraction 0.432. Event sampling 0.352. Entity sampling 0.571. Event sampling is biased down: a 10% trip sample often never reaches 10 distinct drop-offs, so a diverse zone looks ordinary. Entity sampling keeps every trip for about 10% of zones, so the error is variance rather than a broken threshold.

**HyperLogLog, precision 12.** 2,198 exact routes vs estimate 2,229 (1.4% error). The sketch is 4,096 bytes. The exact key store is about 156 KB once per-key overhead is counted, and it grows with the number of routes. The sketch does not.

**DGIM** at the same report. Fewer than 10,000 events had arrived, so k = 10,000 is the whole history so far:

| k | Exact 1s | Estimate | Error |
| ---: | ---: | ---: | ---: |
| 1000 | 141 | 192 | 36% |
| 5000 | 674 | 768 | 14% |
| 10000 | 1024 | 768 | 25% |

Bucket count is on the report. Errors stayed inside DGIM's 50% bound. `dgim.by_k` has all three queries on every window.

**LSH, 53 active zones, 1,378 possible pairs, 281 truly similar.**

| Banding | Candidates | Recall | Candidate precision | Reduction | LSH time | Exact all-pairs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20 × 5 | 360 | 0.765 | 0.597 | 0.739 | 121 ms | 12 ms |
| 10 × 10 | 16 | 0.050 | 0.875 | 0.988 | 129 ms | 12 ms |

10 rows per band only collides sets whose Jaccard is near 1, so recall at the 0.50 cutoff collapses while the few candidates stay precise. 20 × 5 sits near that cutoff. Example pair: zones 43 and 48, exact Jaccard 0.612 (MinHash 0.59). All-pairs was faster here because 53 zones is a small n²; LSH still cut the verified pairs, and it saves CPU only with many more zones. Pairs for this window are in `lsh_example_pairs`.

## Late events and restart

Watermark = max event time seen minus 20 seconds. A late event whose hour is still open is counted (`included`). A late event whose hour has already closed is dropped (`discarded`) and does not enter that window's HLL or LSH sets. It still enters DGIM and the reservoirs, which are defined on the cleaned stream rather than on a closed hour.

From `results/late_event_examples.json` (P3): event `2024-01-row-00000159` at 16:01:17 was **included** in the still-open 16:00 hour. Event `2024-01-row-00003792` at 16:30:05 was **discarded** after the watermark had passed 17:00. No event landed in the short gap between "hour has ended" and "watermark has closed the hour", so this run has no `updated` case.

Restart (`results/restart_observation.json`): Stage 2 was killed with lag 656 and committed offsets summing to 4,085. While it was down the committed offset stayed 4,085 and lag grew to 2,505. After restart lag returned to 0. The new process read only the suffix (4,472 records, sketches started empty). 944 event ids were applied twice, and 557 of those repeats are visible inside the restarted process. Commits are every 1,000 events, so this is at-least-once.

## Limitations

- Default Bloom parameters do not produce a false positive on 16k ids. The `bloom-fp` run is the demonstration.
- At 1,000 events/s, extra partitions and extra consumers do not raise throughput on this laptop. The high-rate run is the one that saturates the pipeline.
- With a few dozen active zones, exact all-pairs Jaccard is cheaper than building MinHash signatures.
- `passenger_count` and `payment_type` are carried on every clean event. They are not part of the graded sketches.

"""Offline checks for Bloom, DGIM, HLL, reservoir, and MinHash-LSH."""

from __future__ import annotations

import random
from collections import deque

from common.bloom import BloomFilter
from common.dgim import DGIM
from common.hll import HyperLogLog
from common.lsh import evaluate_lsh, jaccard
from common.reservoir import Reservoir
from common.stats import pct_error


def _check(name: str, ok: bool, detail: str) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        raise SystemExit(1)


def test_bloom() -> None:
    bloom = BloomFilter(5000, 0.01)
    for i in range(3000):
        bloom.add(f"id-{i}")
    misses = sum(1 for i in range(3000) if not bloom.might_contain(f"id-{i}"))
    false_pos = sum(1 for i in range(3000, 6000) if bloom.might_contain(f"id-{i}"))
    rate = false_pos / 3000
    _check("bloom-fn", misses == 0, f"false negatives={misses}")
    _check("bloom-fp", rate < 0.05, f"observed fp={rate:.4f} m={bloom.m} k={bloom.k}")


def test_dgim() -> None:
    rng = random.Random(7)
    window = 10000
    bits = []
    sketch = DGIM(window)
    recent: deque[int] = deque(maxlen=window)
    worst = 0.0
    for _ in range(25000):
        bit = 1 if rng.random() < 0.2 else 0
        bits.append(bit)
        sketch.add_bit(bit)
        recent.append(bit)
        if len(bits) % 2500 == 0:
            for k in (1000, 5000, 10000):
                exact = sum(list(recent)[-k:])
                est = sketch.estimate(k)
                err = pct_error(est, exact) or 0.0
                worst = max(worst, err)
                if exact and err > 50:
                    _check("dgim", False, f"k={k} exact={exact} est={est:.1f} err={err:.1f}%")
    _check("dgim", True, f"worst error {worst:.2f}% over k=1000/5000/10000, buckets={sketch.bucket_count}")


def test_hll() -> None:
    sketch = HyperLogLog(12)
    exact = set()
    for i in range(8000):
        key = f"route-{i % 5000}"
        exact.add(key)
        sketch.add(key)
    est = sketch.count()
    err = pct_error(est, len(exact))
    _check("hll", err is not None and err < 5, f"exact={len(exact)} est={est:.1f} err={err:.2f}% bytes={sketch.memory_bytes}")


def test_reservoir() -> None:
    rng = random.Random(1)
    values = [rng.random() * 40 for _ in range(20000)]
    exact = sum(values) / len(values)
    reservoir = Reservoir(10000, seed=1)
    for value in values:
        reservoir.add(value)
    err = pct_error(reservoir.mean(), exact)
    _check("reservoir", err is not None and err < 2, f"exact={exact:.3f} sample={reservoir.mean():.3f} err={err:.3f}%")


def test_lsh() -> None:
    # Two clusters of pickup zones that share destinations, plus noise zones.
    sets = {}
    counts = {}
    for zone in range(10):
        sets[zone] = set(range(0, 40)) | {100 + zone}
        counts[zone] = 30
    for zone in range(10, 20):
        sets[zone] = set(range(200, 240)) | {300 + zone}
        counts[zone] = 30
    for zone in range(20, 30):
        sets[zone] = {1000 + zone * 3 + offset for offset in range(15)}
        counts[zone] = 25
    wide = evaluate_lsh(sets, counts, 20, 100, 20, 5, 0.50, seed=3)
    tight = evaluate_lsh(sets, counts, 20, 100, 50, 2, 0.50, seed=3)
    same = jaccard(sets[0], sets[1])
    _check("lsh-jaccard", same > 0.9, f"planted pair jaccard={same:.3f}")
    _check(
        "lsh-recall",
        wide["recall"] is not None and wide["recall"] >= 0.9 and wide["true_pairs"] > 0,
        f"20x5 recall={wide['recall']} candidates={wide['candidates']} true={wide['true_pairs']}",
    )
    _check(
        "lsh-tradeoff",
        tight["candidates"] >= wide["candidates"],
        f"50x2 candidates={tight['candidates']} vs 20x5 candidates={wide['candidates']}",
    )


if __name__ == "__main__":
    test_bloom()
    test_dgim()
    test_hll()
    test_reservoir()
    test_lsh()
    print("all algorithm checks passed")

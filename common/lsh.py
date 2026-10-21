"""MinHash signatures and banded LSH for pickup-zone destination sets.

LSH only proposes candidate pairs. Exact Jaccard decides similarity.
"""

from __future__ import annotations

import random
import time
from itertools import combinations

PRIME = 2_147_483_647


class MinHashLSH:
    def __init__(self, num_hashes: int, bands: int, rows: int, seed: int = 42):
        if bands * rows != num_hashes:
            raise ValueError(f"{bands} bands x {rows} rows must equal {num_hashes} hashes")
        self.num_hashes = num_hashes
        self.bands = bands
        self.rows = rows
        rng = random.Random(seed)
        self.a = [rng.randint(1, PRIME - 1) for _ in range(num_hashes)]
        self.b = [rng.randint(0, PRIME - 1) for _ in range(num_hashes)]

    def signature(self, elements: set[int]) -> list[int]:
        sig = [PRIME] * self.num_hashes
        if not elements:
            return sig
        for element in elements:
            value = int(element) + 1
            for i in range(self.num_hashes):
                hashed = (self.a[i] * value + self.b[i]) % PRIME
                if hashed < sig[i]:
                    sig[i] = hashed
        return sig

    def candidates(self, signatures: dict[int, list[int]]) -> set[tuple[int, int]]:
        buckets: list[dict[tuple, list[int]]] = [dict() for _ in range(self.bands)]
        for zone, sig in signatures.items():
            for band in range(self.bands):
                start = band * self.rows
                chunk = tuple(sig[start:start + self.rows])
                buckets[band].setdefault(chunk, []).append(zone)
        pairs: set[tuple[int, int]] = set()
        for band_buckets in buckets:
            for zones in band_buckets.values():
                if len(zones) < 2:
                    continue
                for left, right in combinations(sorted(zones), 2):
                    pairs.add((left, right))
        return pairs


def jaccard(left: set[int], right: set[int]) -> float:
    if not left and not right:
        return 1.0
    union = len(left | right)
    if union == 0:
        return 0.0
    return len(left & right) / union


def minhash_jaccard_estimate(left: list[int], right: list[int]) -> float:
    if not left:
        return 0.0
    return sum(a == b for a, b in zip(left, right)) / len(left)


def evaluate_lsh(
    destination_sets: dict[int, set[int]],
    trip_counts: dict[int, int],
    min_trips: int,
    num_hashes: int,
    bands: int,
    rows: int,
    threshold: float,
    seed: int,
) -> dict:
    active = {
        zone: drops
        for zone, drops in destination_sets.items()
        if trip_counts.get(zone, 0) >= min_trips and drops
    }
    zones = sorted(active)
    all_possible = len(zones) * (len(zones) - 1) // 2
    lsh = MinHashLSH(num_hashes, bands, rows, seed=seed)
    started = time.perf_counter()
    signatures = {zone: lsh.signature(active[zone]) for zone in zones}
    candidate_pairs = lsh.candidates(signatures) if zones else set()
    verified = []
    for left, right in sorted(candidate_pairs):
        score = jaccard(active[left], active[right])
        if score >= threshold:
            verified.append((left, right, score))
    lsh_seconds = time.perf_counter() - started

    exact_started = time.perf_counter()
    true_pairs = []
    for left, right in combinations(zones, 2):
        score = jaccard(active[left], active[right])
        if score >= threshold:
            true_pairs.append((left, right, score))
    exact_seconds = time.perf_counter() - exact_started

    verified_keys = {(a, b) for a, b, _ in verified}
    true_keys = {(a, b) for a, b, _ in true_pairs}
    hits = verified_keys & true_keys
    recall = (len(hits) / len(true_keys)) if true_keys else None
    candidate_precision = (
        len(candidate_pairs & true_keys) / len(candidate_pairs) if candidate_pairs else None
    )
    reduction = (1.0 - (len(candidate_pairs) / all_possible)) if all_possible else None

    examples = []
    for left, right, score in verified[:5]:
        examples.append({
            "pickup_a": left,
            "pickup_b": right,
            "exact_jaccard": round(score, 4),
            "minhash_estimate": round(
                minhash_jaccard_estimate(signatures[left], signatures[right]), 4
            ),
            "destinations_a": len(active[left]),
            "destinations_b": len(active[right]),
        })

    return {
        "bands": bands,
        "rows": rows,
        "num_hashes": num_hashes,
        "active_zones": len(zones),
        "all_possible_pairs": all_possible,
        "candidates": len(candidate_pairs),
        "true_pairs": len(true_keys),
        "verified_pairs": len(verified_keys),
        "recall": recall,
        "candidate_precision": candidate_precision,
        "candidate_reduction": reduction,
        "lsh_latency_ms": round(lsh_seconds * 1000, 3),
        "exact_latency_ms": round(exact_seconds * 1000, 3),
        "example_pairs": examples,
    }

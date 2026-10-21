"""Algorithm R reservoir sampling for linear means (fare, distance)."""

from __future__ import annotations

import random


class Reservoir:
    def __init__(self, k: int, seed: int):
        self.k = int(k)
        self.sample: list[float] = []
        self.seen = 0
        self._rng = random.Random(seed)

    def add(self, value: float) -> None:
        self.seen += 1
        if len(self.sample) < self.k:
            self.sample.append(float(value))
            return
        slot = self._rng.randrange(self.seen)
        if slot < self.k:
            self.sample[slot] = float(value)

    def mean(self) -> float | None:
        if not self.sample:
            return None
        return sum(self.sample) / len(self.sample)

"""Bloom filter sized from the assignment's n and false-positive target.

Check-then-insert is the caller's job. This class never deletes, so a
well-formed filter has a false-negative rate of zero.
"""

from __future__ import annotations

import hashlib
import math


class BloomFilter:
    def __init__(self, expected_items: int, false_positive_target: float):
        if expected_items < 1:
            raise ValueError("expected_items must be positive")
        if not 0 < false_positive_target < 1:
            raise ValueError("false_positive_target must be in (0, 1)")
        ln2 = math.log(2)
        self.expected_items = int(expected_items)
        self.false_positive_target = float(false_positive_target)
        self.m = max(8, int(math.ceil(-(expected_items * math.log(false_positive_target)) / (ln2 ** 2))))
        self.k = max(1, int(round((self.m / expected_items) * ln2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.inserted = 0

    def _indexes(self, key: str):
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:16], "little") or 1
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def _get(self, index: int) -> int:
        return (self.bits[index // 8] >> (index % 8)) & 1

    def _set(self, index: int) -> None:
        self.bits[index // 8] |= 1 << (index % 8)

    def might_contain(self, key: str) -> bool:
        return all(self._get(index) for index in self._indexes(key))

    def add(self, key: str) -> None:
        for index in self._indexes(key):
            self._set(index)
        self.inserted += 1

    @property
    def bit_array_size(self) -> int:
        return self.m

    @property
    def hash_functions(self) -> int:
        return self.k

    @property
    def memory_bytes(self) -> int:
        return len(self.bits)

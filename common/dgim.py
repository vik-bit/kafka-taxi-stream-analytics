"""DGIM (Datar-Gionis-Indyk-Motwani) for the most recent N binary events.

Implemented by the group, following the standard bucket-merge rules:
at most two buckets of each size, timestamp is the most recent bit in
the bucket, and a query counts the oldest overlapping bucket as half.
"""

from __future__ import annotations


class DGIM:
    def __init__(self, window_size: int = 10000):
        if window_size < 1:
            raise ValueError("window_size must be positive")
        self.window_size = int(window_size)
        self.t = 0
        # Oldest bucket first. Each bucket is [timestamp, size].
        self.buckets: list[list[int]] = []

    def add_bit(self, bit: int) -> None:
        self.t += 1
        if int(bit):
            self.buckets.append([self.t, 1])
            self._merge()
        self._expire()

    def _expire(self) -> None:
        limit = self.t - self.window_size
        while self.buckets and self.buckets[0][0] <= limit:
            self.buckets.pop(0)

    def _merge(self) -> None:
        # Cascade: three buckets of one size become one bucket of double size.
        changed = True
        while changed:
            changed = False
            sizes = {}
            for index, (_ts, size) in enumerate(self.buckets):
                sizes.setdefault(size, []).append(index)
            for size, indexes in sizes.items():
                if len(indexes) <= 2:
                    continue
                older, newer = indexes[0], indexes[1]
                timestamp = self.buckets[newer][0]
                del self.buckets[newer]
                del self.buckets[older]
                self.buckets.insert(older, [timestamp, size * 2])
                changed = True
                break

    def estimate(self, k: int) -> float:
        """Estimated number of 1s among the most recent k bits."""
        if self.t == 0 or k <= 0:
            return 0.0
        k = min(int(k), self.window_size, self.t)
        boundary = self.t - k
        inside = [bucket for bucket in self.buckets if bucket[0] > boundary]
        if not inside:
            return 0.0
        total = float(sum(bucket[1] for bucket in inside))
        # The oldest overlapping bucket is only partly inside the query.
        total -= inside[0][1] / 2.0
        return max(0.0, total)

    @property
    def bucket_count(self) -> int:
        return len(self.buckets)

    def exact_recent(self, bits: list[int], k: int) -> int:
        return sum(bits[-k:])

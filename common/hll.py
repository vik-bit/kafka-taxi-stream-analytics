"""HyperLogLog for distinct route keys. Precision comes from config."""

from __future__ import annotations

import hashlib
import math


class HyperLogLog:
    def __init__(self, precision: int = 12):
        if not 4 <= precision <= 18:
            raise ValueError("precision must be between 4 and 18")
        self.precision = precision
        self.m = 1 << precision
        self.registers = [0] * self.m
        if self.m == 16:
            self.alpha = 0.673
        elif self.m == 32:
            self.alpha = 0.697
        elif self.m == 64:
            self.alpha = 0.709
        else:
            self.alpha = 0.7213 / (1.0 + 1.079 / self.m)

    def _hash(self, key: str) -> int:
        return int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest()[:8], "big")

    def add(self, key: str) -> None:
        hashed = self._hash(key)
        index = hashed >> (64 - self.precision)
        remainder = hashed & ((1 << (64 - self.precision)) - 1)
        width = 64 - self.precision
        if remainder == 0:
            rho = width + 1
        else:
            rho = width - remainder.bit_length() + 1
        if rho > self.registers[index]:
            self.registers[index] = rho

    def count(self) -> float:
        indicator = sum(2.0 ** (-register) for register in self.registers)
        estimate = self.alpha * self.m * self.m / indicator
        if estimate <= 2.5 * self.m:
            zeros = self.registers.count(0)
            if zeros:
                estimate = self.m * math.log(self.m / zeros)
        elif estimate > (1 << 32) / 30.0:
            estimate = -(1 << 32) * math.log(1.0 - estimate / (1 << 32))
        return estimate

    @property
    def memory_bytes(self) -> int:
        # Registers stay under 64, so one byte each is enough.
        return self.m

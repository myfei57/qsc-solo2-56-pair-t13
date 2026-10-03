"""乱序到达的限时、有界重排缓冲。

两路采集源经不同链路到达，同一门控事件的两份报告常有几十到几百毫秒错位。缓冲按
「归一后的基准序号」把同一件事的多路报告归到一个槽位：槽内来齐预期来源就立即
冲刷；一直来不齐就挂着，由监控在 ``wait_s`` 超时后冲刷（用已到的票按策略判，拿
不准走安全侧）。

缓冲必须有界：现场异常（来源停摆或序号飞掉）会让槽位越积越多，因此设
``capacity`` 上限。满了以后不是静丢，而是把最旧的槽位按超时强制冲刷并标记
``buffer_overflow``，让这一票走正常判定（多半落到 pending／reject 被门控拦住），
绝不静默放行。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .classify import OUT_OF_ORDER


@dataclass
class BufferedObservation:
    """缓冲里一份到达的原始报告及其归一结果。"""

    source: str
    raw: int
    normalized: int
    tick: int
    delta: int
    kind: str
    received_at: float
    wraps: int = 0
    ext: int = 0

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "raw": self.raw,
            "normalized": self.normalized,
            "tick": self.tick,
            "delta": self.delta,
            "kind": self.kind,
            "received_at": self.received_at,
            "wraps": self.wraps,
            "ext": self.ext,
        }


@dataclass
class _Slot:
    normalized: int
    opened_at: float
    observations: dict[str, BufferedObservation] = field(default_factory=dict)
    flushed: bool = False

    def age(self, now: float) -> float:
        return max(0.0, now - self.opened_at)


class ReorderBuffer:
    """按归一序号归集多路报告的重排缓冲。"""

    def __init__(self, wait_s: float = 2.0, capacity: int = 256) -> None:
        if wait_s <= 0:
            raise ValueError("乱序缓冲等待时间必须为正")
        if capacity <= 0:
            raise ValueError("乱序缓冲容量必须为正")
        self._wait_s = float(wait_s)
        self._capacity = int(capacity)
        self._slots: dict[int, _Slot] = {}

    @property
    def wait_s(self) -> float:
        return self._wait_s

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._slots)

    def add(
        self,
        normalized: int,
        observation: BufferedObservation,
        *,
        now: float,
        expected_sources: int,
    ) -> dict:
        """归入一份报告，返回该槽位是否可冲刷、原因及可能被挤出的旧槽位。"""

        evicted: tuple[int, "_Slot"] | None = None
        if normalized not in self._slots and len(self._slots) >= self._capacity:
            evicted = self.overflow_one()
        slot = self._slots.get(normalized)
        if slot is None:
            slot = _Slot(normalized=normalized, opened_at=now)
            self._slots[normalized] = slot
        # 同一来源对同一序号的重复送达：保留首达，后到记为 duplicate。
        if observation.source in slot.observations:
            observation.kind = "duplicate"
        slot.observations.setdefault(observation.source, observation)
        readiness = self._readiness(slot, now, expected_sources)
        readiness["evicted"] = evicted
        return readiness

    def _readiness(
        self, slot: _Slot, now: float, expected_sources: int
    ) -> dict:
        if len(slot.observations) >= expected_sources:
            return {"ready": True, "reason": "sources_complete"}
        if slot.age(now) >= self._wait_s:
            return {"ready": True, "reason": "wait_timeout"}
        return {"ready": False, "reason": OUT_OF_ORDER}

    def take(self, normalized: int) -> _Slot | None:
        """取出并移除一个槽位。"""

        return self._slots.pop(normalized, None)

    def due(self, now: float) -> list[int]:
        """返回已到等待时限、应当冲刷的归一序号（升序）。"""

        return sorted(
            key
            for key, slot in self._slots.items()
            if slot.age(now) >= self._wait_s
        )

    def oldest_key(self) -> int | None:
        if not self._slots:
            return None
        return min(self._slots, key=lambda key: (self._slots[key].opened_at, key))

    def flush_due(self, now: float) -> list[tuple[int, str, _Slot]]:
        """取出全部已超时的槽位，附带冲刷原因。"""

        flushed: list[tuple[int, str, _Slot]] = []
        for key in self.due(now):
            slot = self._slots.pop(key, None)
            if slot is not None:
                flushed.append((key, "wait_timeout", slot))
        return flushed

    def overflow_one(self) -> tuple[int, _Slot] | None:
        """强制取出最旧槽位（缓冲溢出时用）。"""

        key = self.oldest_key()
        if key is None:
            return None
        return key, self._slots.pop(key)

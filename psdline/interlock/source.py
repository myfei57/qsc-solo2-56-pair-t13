"""单源序号链。

每个采集源各自上报 16 位序号，先在这条链上展开成单调的绝对轴：模运算增量落在
``(0, 半圈)`` 内算前进（跨模数边界时累积圈数），增量为 0 是重复上报，增量达到或
超过半圈说明序号倒退。倒退可能是源重启，也可能是旧数据重放，链本身不裁决，只把
``backwards`` 观测交上去，由对齐器结合基准线区分。
"""

from __future__ import annotations

from dataclasses import dataclass

from .verdict import MODULUS, verdict_of

FIRST = "first"
ADVANCED = "advanced"
DUPLICATE = "duplicate"
BACKWARDS = "backwards"

RECENT_LIMIT = 64


@dataclass(frozen=True)
class SourceObservation:
    """一次单源观测的结果。"""

    source: str
    raw: int
    absolute: int
    wraps: int
    epoch: int
    kind: str
    delta: int

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "raw": self.raw,
            "absolute": self.absolute,
            "wraps": self.wraps,
            "epoch": self.epoch,
            "kind": self.kind,
            "delta": self.delta,
        }


class SourceChain:
    """一个采集源的序号链（展开到绝对轴）。"""

    def __init__(self, name: str) -> None:
        self._name = str(name)
        self._raw: int | None = None
        self._wraps = 0
        self._epoch = 0
        self._recent: list[int] = []
        self._reports = 0
        self._duplicates = 0
        self._resets = 0
        self._last_at = 0.0

    @property
    def name(self) -> str:
        """返回源名。"""

        return self._name

    @property
    def raw(self) -> int | None:
        """返回最近一次接受的原始序号（未上报过为 ``None``）。"""

        return self._raw

    @property
    def wraps(self) -> int:
        """返回本纪元内累计的回绕圈数。"""

        return self._wraps

    @property
    def epoch(self) -> int:
        """返回当前纪元（每次确认重启后递增）。"""

        return self._epoch

    @property
    def reports(self) -> int:
        """返回累计上报次数。"""

        return self._reports

    @property
    def duplicates(self) -> int:
        """返回累计重复上报次数。"""

        return self._duplicates

    @property
    def resets(self) -> int:
        """返回累计重锚次数。"""

        return self._resets

    @property
    def last_at(self) -> float:
        """返回最近一次上报的时间（未上报过为 0）。"""

        return self._last_at

    @property
    def absolute(self) -> int:
        """返回当前绝对轴位置（未上报过为 -1）。"""

        if self._raw is None:
            return -1
        return self._wraps * MODULUS + self._raw

    def observe(self, raw: int, at: float) -> SourceObservation:
        """观测一次上报；链状态只在确认前进时推进。"""

        raw = int(raw) % MODULUS
        self._reports += 1
        self._last_at = float(at)
        if self._raw is None:
            self._raw = raw
            self._remember(raw)
            return self._observation(raw, FIRST, 0)
        verdict = verdict_of(self._raw, raw)
        if verdict.reason == "replay":
            self._duplicates += 1
            return self._observation(raw, DUPLICATE, 0)
        if verdict.advanced:
            if raw < self._raw:
                self._wraps += 1
            self._raw = raw
            self._remember(raw)
            return self._observation(raw, ADVANCED, verdict.delta)
        if raw in self._recent:
            # 倒退到最近见过的序号：旧数据重放，不动链。
            self._duplicates += 1
            return self._observation(raw, DUPLICATE, verdict.delta)
        # 倒退到没见过的位置：可能是源重启，保持链不动，交给对齐器裁决。
        return self._observation(raw, BACKWARDS, verdict.delta)

    def rebase(self, raw: int, at: float) -> SourceObservation:
        """确认源重启后按新纪元重锚：圈数清零，从当前序号重新展开。"""

        raw = int(raw) % MODULUS
        self._epoch += 1
        self._wraps = 0
        self._raw = raw
        self._recent = [raw]
        self._resets += 1
        self._last_at = float(at)
        return self._observation(raw, FIRST, 0)

    def snapshot(self) -> dict:
        """导出可落盘的链状态。"""

        return {
            "raw": self._raw,
            "wraps": self._wraps,
            "epoch": self._epoch,
            "recent": list(self._recent),
            "reports": self._reports,
            "duplicates": self._duplicates,
            "resets": self._resets,
            "last_at": self._last_at,
        }

    @classmethod
    def restore(cls, name: str, payload: dict) -> "SourceChain":
        """从落盘状态还原一条链。"""

        chain = cls(name)
        raw = payload.get("raw")
        chain._raw = None if raw is None else int(raw) % MODULUS
        chain._wraps = int(payload.get("wraps", 0))
        chain._epoch = int(payload.get("epoch", 0))
        chain._recent = [int(item) % MODULUS for item in payload.get("recent", [])]
        chain._recent = chain._recent[-RECENT_LIMIT:]
        chain._reports = int(payload.get("reports", 0))
        chain._duplicates = int(payload.get("duplicates", 0))
        chain._resets = int(payload.get("resets", 0))
        chain._last_at = float(payload.get("last_at", 0.0))
        return chain

    def _observation(self, raw: int, kind: str, delta: int) -> SourceObservation:
        return SourceObservation(
            self._name, raw, self.absolute, self._wraps, self._epoch, kind, delta
        )

    def _remember(self, raw: int) -> None:
        self._recent.append(raw)
        if len(self._recent) > RECENT_LIMIT:
            del self._recent[: len(self._recent) - RECENT_LIMIT]

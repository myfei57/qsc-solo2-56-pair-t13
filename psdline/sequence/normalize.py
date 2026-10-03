"""把多路采集源的原始序号归一到同一条基准。

现场只有一条门控事件流，但两路采集器各自维护一个 16 位计数视图：起始相位不同、
各自回绕、还可能丢几个现场事件。因此不能拿 ``raw`` 直接比，也不能假定两路首报的
``raw`` 相等。归一分两步：

- :class:`SourceCalibration` 是一路的相位校准 ``zero``：把 ``raw`` 换算成该视图
  自己的零基、跨周期展开值 ``ext``（见 :meth:`SourceCalibration.extend`），回绕被
  ``wraps`` 展开，不会再在 65535 → 0 处跌落；
- :class:`SourceTrack` 在此基础上再记一个「该视图零基值相对统一基准的偏移
  ``offset``」。偏移在两路首个能配对的事件上学到一次，之后该源每条都能换算成统一
  基准刻度 ``base_value``，两路就能按同一个数对齐到同一件物理事件。

每一路相对自己上一条的前进／重放／回绕用模增量独立检测；跨来源的一致性在统一基准
上比较。
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MODULUS = 65536
HALF = DEFAULT_MODULUS // 2


def modular_delta(previous: int, current: int, modulus: int = DEFAULT_MODULUS) -> int:
    """返回 ``current`` 相对 ``previous`` 的模增量（``0 .. modulus-1``）。"""

    return (int(current) - int(previous)) % int(modulus)


@dataclass(frozen=True)
class SourceCalibration:
    """一路来源的相位校准：零基刻度 ``tick = (raw - zero) mod modulus``。"""

    source: str
    zero: int
    modulus: int = DEFAULT_MODULUS

    def tick(self, raw: int) -> int:
        """把原始序号换算成一个周期内的零基刻度。"""

        return (int(raw) - int(self.zero)) % self.modulus

    def extend(self, raw: int, wraps: int) -> int:
        """把原始序号跨周期展开成该视图自己的零基单调整数。"""

        return int(wraps) * self.modulus + self.tick(raw)

    def as_dict(self) -> dict:
        return {"source": self.source, "zero": self.zero, "modulus": self.modulus}

    @classmethod
    def from_dict(cls, payload: dict) -> "SourceCalibration":
        modulus = int(payload.get("modulus", DEFAULT_MODULUS))
        return cls(
            source=str(payload.get("source", "")),
            zero=int(payload.get("zero", 0)) % modulus,
            modulus=modulus,
        )


@dataclass
class SourceTrack:
    """一路来源被持续观测后的归一进度（可序列化，供重启恢复）。"""

    source: str
    modulus: int = DEFAULT_MODULUS
    zero: int = 0
    offset: int | None = None
    wraps: int = 0
    last_raw: int | None = None
    last_ext: int | None = None
    last_base: int | None = None
    reports: int = 0

    @property
    def calibration(self) -> SourceCalibration:
        return SourceCalibration(self.source, self.zero, self.modulus)

    @property
    def started(self) -> bool:
        """该来源是否已经见过首报。"""

        return self.last_raw is not None

    @property
    def aligned(self) -> bool:
        """该来源是否已经学到统一基准偏移。"""

        return self.offset is not None

    def start(self, raw: int) -> None:
        """首报：确定该视图自己的零相位。"""

        self.zero = int(raw) % self.modulus
        self.wraps = 0
        self.last_raw = int(raw) % self.modulus
        self.last_ext = 0

    def align_to(self, base_value: int, ext: int) -> None:
        """用一次配对事件学到偏移：``base = ext + offset``。"""

        self.offset = int(base_value) - int(ext)

    def project(self, raw: int) -> tuple[int, int, int, int]:
        """把一条原始序号投影成 ``(wraps, ext, base_value, delta)``，不推进状态。

        ``delta`` 是该视图相对上一条原始序号的模增量，用于独立判前进／重放／回绕。
        尚未学到偏移时 ``base_value`` 先用 ``ext`` 占位，由监控在配对后回填偏移。
        """

        raw = int(raw) % self.modulus
        assert self.last_raw is not None
        delta = modular_delta(self.last_raw, raw, self.modulus)
        wraps = self.wraps
        # 前进一小步（delta 落在前半圈）且原始计数器回绕到更小的值，说明跨过了一个
        # 周期边界。用原始 raw 判，而不是用相对 zero 的 tick：首报若恰在周期末，
        # tick 会被 zero 旋到 0 附近，反而看不出回绕。
        if 0 < delta < self.modulus // 2 and raw < self.last_raw:
            wraps += 1
        # 展开成沿计数器累计的步数：跨掉的整圈加上本圈内 raw 相对 zero 的零基有向差。
        # 不做模运算，这样 zero=65535 时 65535->0 为 wraps 0->1，展开 = 65536-65535 = 1。
        ext = wraps * self.modulus + raw - self.zero
        tick = self.calibration.tick(raw)
        base_value = ext + self.offset if self.offset is not None else ext
        return wraps, ext, base_value, delta

    def commit(
        self, raw: int, wraps: int, ext: int, base_value: int
    ) -> None:
        """确认一条事件后落定该视图的高水位。"""

        self.last_raw = int(raw) % self.modulus
        self.wraps = int(wraps)
        self.last_ext = ext
        self.last_base = base_value
        self.reports += 1

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "modulus": self.modulus,
            "zero": self.zero,
            "offset": self.offset,
            "wraps": self.wraps,
            "last_raw": self.last_raw,
            "last_ext": self.last_ext,
            "last_base": self.last_base,
            "reports": self.reports,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "SourceTrack":
        def _int_or_none(key: str) -> int | None:
            value = payload.get(key)
            return None if value is None else int(value)

        return cls(
            source=str(payload.get("source", "")),
            modulus=int(payload.get("modulus", DEFAULT_MODULUS)),
            zero=int(payload.get("zero", 0)),
            offset=_int_or_none("offset"),
            wraps=int(payload.get("wraps", 0)),
            last_raw=_int_or_none("last_raw"),
            last_ext=_int_or_none("last_ext"),
            last_base=_int_or_none("last_base"),
            reports=int(payload.get("reports", 0)),
        )


def normalize_sequence(raw: int, calibration: SourceCalibration) -> int:
    """按给定相位把一路原始序号归一到一个周期内的零基刻度。"""

    return calibration.tick(raw)

"""门控事件序号：多采集来源对齐与前进／回绕／重放判定。

现场有两路（可扩展为多路）独立采集源，各自维护自己的 16 位序号计数器，计数相位
不一致、上报会乱序、还夹着重复。本子包把两路序号先归一到同一条单调基准，再做
判定，并把每次判定结果落到一份不可变流水里，供判定、审计与门控读取同一份事实：

- :mod:`psdline.sequence.normalize` 把单个来源的原始序号换算成单调基准；
- :mod:`psdline.sequence.classify` 只做纯函数判定（前进／重放／回绕／乱序）；
- :mod:`psdline.sequence.policy` 汇总多来源，给出前进或拦截结论（两种取舍）；
- :mod:`psdline.sequence.buffer` 是乱序到达的限时、有界重排缓冲；
- :mod:`psdline.sequence.monitor` 串起以上各件，负责持久化、恢复与并发。

门控安全侧在「误判代价」上选择保守：拿不准就判不前进（``rejected``），宁可拦停
人工复核，也不放进一个可能被重放或回绕的序号。
"""

from __future__ import annotations

from .buffer import ReorderBuffer, BufferedObservation
from .classify import (
    ADVANCED,
    DUPLICATE,
    OUT_OF_ORDER,
    REPLAY,
    ROLLED_BACK,
    classify_advance,
)
from .monitor import Observation, SequenceMonitor, SequenceResult
from .normalize import SourceCalibration, modular_delta, normalize_sequence
from .policy import PRIMARY, QUORUM, decide

__all__ = [
    "ADVANCED",
    "DUPLICATE",
    "OUT_OF_ORDER",
    "PRIMARY",
    "QUORUM",
    "REPLAY",
    "ROLLED_BACK",
    "BufferedObservation",
    "Observation",
    "ReorderBuffer",
    "SequenceMonitor",
    "SequenceResult",
    "SourceCalibration",
    "classify_advance",
    "decide",
    "modular_delta",
    "normalize_sequence",
]

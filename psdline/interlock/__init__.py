"""门控联锁。

联锁负责三件事：故障置位时把对应门区锁在关闭位、按条件解除锁定、以及把多路
采集源上报的门控事件序号归到同一条基准轴上判定前进、回绕与重放（单链的模运算
判定是每条源链内部的基本功）。锁定状态不写在联锁自己的文档里，而是落到站台门
上，保证只有一份事实；序号判定结果也只有一份，判定、审计与门控共读。
"""

from __future__ import annotations

from .align import (
    ADVANCED,
    DEGRADED,
    GAP,
    LATE,
    PENDING,
    REPLAY,
    RESET,
    WRAPAROUND,
    AlignedVerdict,
    SequenceAligner,
)
from .arm import arm_key, InterlockArmer
from .clear import release_ready, releasable
from .interlock import ACTIVE, CLEAR, FaultRegistry
from .restore import restore_lockouts
from .source import SourceChain, SourceObservation
from .status import interlock_status
from .verdict import MODULUS, SequenceVerdict, next_sequence, verdict_of

__all__ = [
    "ACTIVE",
    "ADVANCED",
    "CLEAR",
    "DEGRADED",
    "GAP",
    "LATE",
    "MODULUS",
    "PENDING",
    "REPLAY",
    "RESET",
    "WRAPAROUND",
    "AlignedVerdict",
    "FaultRegistry",
    "InterlockArmer",
    "SequenceAligner",
    "SequenceVerdict",
    "SourceChain",
    "SourceObservation",
    "arm_key",
    "interlock_status",
    "next_sequence",
    "release_ready",
    "releasable",
    "restore_lockouts",
    "verdict_of",
]

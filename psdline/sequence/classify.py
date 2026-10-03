"""前进／回绕／重放／重复的纯函数判定。

判定建立在已经归一的单调整数上，不碰任何状态，方便单独验证：

- ``advanced``    相对上一条严格前进（含跨 16 位周期，归一后自然变大）；
- ``duplicate``   与上一条完全相同：同一条上报被送达两次，幂等忽略，不推进门控；
- ``replay``      落在更早的位置：一个已经过去的序号在前进之后被重新抛出，拒绝；
- ``rolled_back`` 回退距离超过半圈：非预期的序号大倒退，拒绝并告警；
- ``out_of_order`` 只是轻微迟到、仍在重排窗口能补回的范围内，交给缓冲而不立刻拒绝。
"""

from __future__ import annotations

ADVANCED = "advanced"
DUPLICATE = "duplicate"
REPLAY = "replay"
ROLLED_BACK = "rolled_back"
OUT_OF_ORDER = "out_of_order"

DEAD_KINDS = (REPLAY, ROLLED_BACK)


def modular_delta(previous: int, current: int, modulus: int) -> int:
    """模增量，``0..modulus-1``。"""

    return (int(current) - int(previous)) % modulus


def classify_advance(previous: int, current: int, modulus: int) -> tuple[str, int]:
    """按模增量判一条相对上一条的变化，返回（类别，模增量）。"""

    delta = modular_delta(previous, current, modulus)
    half = modulus // 2
    if delta == 0:
        return DUPLICATE, delta
    if delta >= half:
        return ROLLED_BACK, delta
    return ADVANCED, delta


def classify_position(previous_normalized: int | None, current_normalized: int) -> tuple[str, int]:
    """按单调基准上的绝对位置判新旧，返回（类别，带符号差）。

    归一后序号应当只增不减，因此可以直接比大小，不必再猜模：相等是重复送达，
    变小是把已经过去的序号重新抛出（重放）。
    """

    if previous_normalized is None:
        return ADVANCED, 0
    gap = int(current_normalized) - int(previous_normalized)
    if gap == 0:
        return DUPLICATE, gap
    if gap < 0:
        return REPLAY, gap
    return ADVANCED, gap

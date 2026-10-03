"""把多路来源对同一序号的判定汇总成一个前进／拦截结论。

两种取舍对应不同的误判代价，现场按需要配置：

- ``primary``：以主源为准做单调校验。主源说前进就放行，主源重放／回绕就拦截；
  其它来源只做旁证（矛盾时在 ``warnings`` 里留痕，审计可见，但不单独拦停）。
  优点是主源可靠时延迟最低，代价是主源单点故障会误判。

- ``quorum``：多数一致才算前进。要求「已上报的来源中，判前进的严格过半」，且没有
  任何一路报出致命的重放／回绕。单路迟到时先缓冲等待，不拿半票放行；两路对同一
  序号一前进一矛盾时，前进票不超过半数 → 判 ``quorum_pending`` 继续等待或最终拒绝。
  优点是能挡住单源被重放，代价是可用性下降、延迟增大。

两种策略的安全侧一致：拿不准（矛盾、未达多数、主源缺失）都不前进，交给人工复核。
"""

from __future__ import annotations

from .classify import ADVANCED, DEAD_KINDS, DUPLICATE, OUT_OF_ORDER

PRIMARY = "primary"
QUORUM = "quorum"
POLICIES = (PRIMARY, QUORUM)

ACCEPT = "accept"
REJECT = "reject"
PENDING = "pending"


def decide(
    classifications: dict[str, str],
    *,
    policy: str,
    primary_source: str,
    expected_sources: int = 2,
) -> dict:
    """汇总各来源类别，给出 ``accept`` / ``reject`` / ``pending`` 结论。

    ``classifications`` 只包含当前已经就同一序号给出意见的来源；缺失来源由
    ``expected_sources`` 体现，决定 ``quorum`` 下是否还有等待的意义。
    """

    votes = list(classifications.values())
    advance_votes = [name for name, kind in classifications.items() if kind == ADVANCED]
    dead = {
        name: kind for name, kind in classifications.items() if kind in DEAD_KINDS
    }
    reported = len(classifications)

    if policy == PRIMARY:
        primary_kind = classifications.get(primary_source)
        warnings: list[str] = []
        if primary_kind is None:
            return {
                "decision": PENDING,
                "reason": "primary_missing",
                "advance_votes": advance_votes,
                "dead": dead,
                "warnings": warnings,
            }
        if primary_kind == ADVANCED:
            for name, kind in classifications.items():
                if name != primary_source and kind in DEAD_KINDS:
                    warnings.append(f"secondary_{kind}:{name}")
            return {
                "decision": ACCEPT,
                "reason": "primary_advanced",
                "advance_votes": advance_votes,
                "dead": dead,
                "warnings": warnings,
            }
        if primary_kind == DUPLICATE:
            return {
                "decision": ACCEPT,
                "reason": "duplicate",
                "advance_votes": advance_votes,
                "dead": dead,
                "warnings": warnings,
            }
        return {
            "decision": REJECT,
            "reason": primary_kind,
            "advance_votes": advance_votes,
            "dead": dead,
            "warnings": warnings,
        }

    if policy != QUORUM:
        raise ValueError(f"未知序号判定策略: {policy}")

    quorum = expected_sources // 2 + 1
    if dead:
        return {
            "decision": REJECT,
            "reason": "quorum_dead",
            "advance_votes": advance_votes,
            "dead": dead,
            "warnings": [],
        }
    if len(advance_votes) >= quorum:
        return {
            "decision": ACCEPT,
            "reason": "quorum_advanced",
            "advance_votes": advance_votes,
            "dead": dead,
            "warnings": [],
        }
    if reported < expected_sources:
        return {
            "decision": PENDING,
            "reason": "quorum_pending",
            "advance_votes": advance_votes,
            "dead": dead,
            "warnings": [],
        }
    # 来源都到齐了但仍未过半：彼此矛盾或停在乱序，按安全侧拒绝。
    return {
        "decision": REJECT,
        "reason": "quorum_split" if OUT_OF_ORDER in votes or advance_votes else "quorum_unmet",
        "advance_votes": advance_votes,
        "dead": dead,
        "warnings": [],
    }

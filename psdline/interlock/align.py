"""多源序号对齐与判定。

两路采集源各报各的 16 位序号，错位、重复、乱序都先在这里归到同一条基准轴上，
再判定每一次前进是 ``advanced``（同周期前进）、``wraparound``（跨周期回绕前进）、
``replay``（重复上报）、``late``（迟到）、``gap``（缓冲超时后确认的缺口）还是
``degraded``（证据不足的降级提交）。乱序到达的序号先进入缓冲，等一个判定窗口再
落锤；提交严格按刻度顺序，历史只追加不改写。

基准轴取主源（``primary``）的展开绝对轴；其余源靠常数偏移归并上来。偏移只在
双方都静默（最新上报都超过 ``pair_window_s``，没有在途事件）时观测——两源对
同一事件的上报谁先谁后不一定，但在双方都安静下来时，各自的最新值必然对应同一
个事件；连续 ``offset_confirm`` 次静默观测一致才锁定或重估偏移。
提交策略二选一：

``primary``   以主源为准做单调校验，副源只佐证、对表，并在主源静默超时后接管
              （可用性优先；副源持续超前会记分歧告警）；
``quorum``    多数源（两路时即双源）都报出同一刻度才算干净前进，否则缓冲超时
              后降级提交（安全性优先；单源掉线时门控默认不再采信）。

判定结果（:class:`AlignedVerdict`）只有一份：上报方拿到的返回值、审计流水与门控
读取的都是它。提交先落 ``sequence`` 流水再更新头文档，崩溃后从流水前滚恢复。
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass

from ..clock import Clock
from ..errors import RailError
from ..store import JsonlJournal, JsonStore
from .source import BACKWARDS, DUPLICATE, SourceChain
from .verdict import MODULUS

# 判定类别
ADVANCED = "advanced"
WRAPAROUND = "wraparound"
REPLAY = "replay"
LATE = "late"
GAP = "gap"
DEGRADED = "degraded"
PENDING = "pending"
RESET = "reset"

# 缓冲原因
WAIT_GAP = "gap"  # 前进幅度超过可信上限，等乱序补报
WAIT_EVIDENCE = "evidence"  # 策略要求的佐证还没到齐
WAIT_DIVERGE = "diverge"  # 非参考源超前于水位，等参考源表态
WAIT_ORDERED = "ordered"  # 排在更早的缓冲项之后，不许超车

HEAD_DOC = "sequence/head"
SEEN_WINDOW = 8192


@dataclass(frozen=True)
class AlignedVerdict:
    """一次对齐判定的结果；判定、审计与门控读的都是它。"""

    tick: int
    cycle: int
    kind: str
    accepted: bool
    committed: bool
    degraded: bool
    sources: dict
    skipped: int
    at: float
    reason: str

    def as_dict(self) -> dict:
        return {
            "tick": self.tick,
            "cycle": self.cycle,
            "kind": self.kind,
            "accepted": self.accepted,
            "committed": self.committed,
            "degraded": self.degraded,
            "sources": dict(self.sources),
            "skipped": self.skipped,
            "at": self.at,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "AlignedVerdict":
        return cls(
            tick=int(payload.get("tick", -1)),
            cycle=int(payload.get("cycle", -1)),
            kind=str(payload.get("kind", "")),
            accepted=bool(payload.get("accepted", False)),
            committed=bool(payload.get("committed", False)),
            degraded=bool(payload.get("degraded", False)),
            sources={str(key): int(value) for key, value in dict(payload.get("sources", {})).items()},
            skipped=int(payload.get("skipped", 0)),
            at=float(payload.get("at", 0.0)),
            reason=str(payload.get("reason", "")),
        )


@dataclass
class _Pending:
    """一条已归到基准轴、等待最终判定的上报。"""

    tick: int
    reporters: dict
    at: float
    deadline: float
    reason: str


@dataclass
class _Unpaired:
    """一条还没归到基准轴上的上报（源偏移未知）。"""

    source: str
    raw: int
    absolute: int
    at: float
    deadline: float


class SequenceAligner:
    """把多路序号归到同一基准轴并给出唯一判定。"""

    def __init__(
        self,
        store: JsonStore,
        clock: Clock,
        journal: JsonlJournal,
        *,
        sources: tuple[str, ...] = ("alpha", "beta"),
        primary: str = "alpha",
        commit_policy: str = "primary",
        hold_s: float = 0.5,
        pair_window_s: float = 1.0,
        source_timeout_s: float = 5.0,
        max_forward_step: int = 64,
        offset_confirm: int = 3,
        pending_limit: int = 256,
        degraded_accept: bool = False,
    ) -> None:
        self._sources = tuple(str(item) for item in sources)
        self._primary = str(primary)
        if self._primary not in self._sources:
            raise RailError(f"主源不在采集源清单里: {primary}", component="interlock")
        if commit_policy not in ("primary", "quorum"):
            raise RailError(f"未知的提交策略: {commit_policy}", component="interlock")
        if len(self._sources) < 2:
            raise RailError("序号采集源至少需要两路", component="interlock")
        self._store = store
        self._clock = clock
        self._journal = journal
        self._commit_policy = commit_policy
        self._hold_s = float(hold_s)
        self._pair_window_s = float(pair_window_s)
        self._source_timeout_s = float(source_timeout_s)
        self._max_forward_step = int(max_forward_step)
        self._offset_confirm = max(1, int(offset_confirm))
        self._pending_limit = max(1, int(pending_limit))
        self._degraded_accept = bool(degraded_accept)
        self._majority = len(self._sources) // 2 + 1

        self._lock = threading.RLock()
        self._chains = {name: SourceChain(name) for name in self._sources}
        self._offsets: dict[str, int | None] = {name: None for name in self._sources}
        self._offsets[self._primary] = 0
        self._candidates: dict[str, list[int]] = {name: [] for name in self._sources}
        self._horizon: int | None = None
        self._reference = self._primary
        self._pending: list[_Pending] = []
        self._unpaired: list[_Unpaired] = []
        self._seen: set[int] = set()
        self._seen_order: deque[int] = deque()
        self._last_pair: dict[str, tuple] = {}
        self._last_verdict: AlignedVerdict | None = None
        self._counters: dict[str, int] = {}
        self._restore()

    # ---- 上报与判定 -----------------------------------------------------

    def report(self, source: str, raw: int, at: float | None = None) -> dict:
        """接入一路序号上报，返回本次结论与由此促成的全部判定。"""

        source = str(source)
        with self._lock:
            if source not in self._chains:
                raise RailError(f"未登记的序号源: {source}", component="interlock")
            moment = self._clock.now() if at is None else float(at)
            outcome: dict = {"verdict": None, "committed": [], "rejected": [], "events": []}
            chain = self._chains[source]
            obs = chain.observe(int(raw), moment)
            self._refresh_reference(moment, outcome["events"])
            if obs.kind == BACKWARDS:
                obs = chain.rebase(int(raw), moment)
                self._offsets[source] = None
                self._candidates[source] = []
                outcome["events"].append(
                    {
                        "event": "source_reset",
                        "source": source,
                        "raw": obs.raw,
                        "epoch": obs.epoch,
                        "at": moment,
                    }
                )
                outcome["verdict"] = self._verdict(
                    -1, RESET, False, False, {source: obs.raw}, 0, moment, "源序号倒退，已按新纪元重锚"
                )
                self._drain(moment, outcome)
                self._persist_head()
                return outcome
            if obs.kind == DUPLICATE:
                offset = self._offsets[source]
                tick = obs.absolute + offset if offset is not None else -1
                outcome["verdict"] = self._verdict(
                    tick, REPLAY, False, False, {source: obs.raw}, 0, moment, "同源重复上报"
                )
                self._counters[REPLAY] = self._counters.get(REPLAY, 0) + 1
                outcome["rejected"].append(outcome["verdict"])
                self._drain(moment, outcome)
                self._persist_head()
                return outcome
            self._settle_pairing(moment, outcome)
            self._adopt_baseline(source, moment, outcome)
            offset = self._offsets[source]
            if offset is None:
                self._park_unpaired(source, obs.raw, obs.absolute, moment, outcome)
                outcome["verdict"] = self._verdict(
                    -1, PENDING, False, False, {source: obs.raw}, 0, moment, "源偏移未定，上报已入配对缓冲"
                )
                self._drain(moment, outcome)
                self._persist_head()
                return outcome
            tick = obs.absolute + offset
            outcome["verdict"] = self._route(source, obs.raw, tick, moment, outcome)
            self._drain(moment, outcome)
            self._settle_verdict(outcome)
            self._persist_head()
            return outcome

    def flush(self, at: float | None = None) -> dict:
        """把缓冲里到期的上报按刻度顺序判定掉（由控制循环或下一次上报驱动）。"""

        with self._lock:
            moment = self._clock.now() if at is None else float(at)
            outcome: dict = {"verdict": None, "committed": [], "rejected": [], "events": []}
            self._refresh_reference(moment, outcome["events"])
            self._settle_pairing(moment, outcome)
            for item in list(self._unpaired):
                if moment >= item.deadline:
                    self._unpaired.remove(item)
                    self._counters["unpaired_dropped"] = self._counters.get("unpaired_dropped", 0) + 1
                    outcome["events"].append(
                        {
                            "event": "unpaired_expired",
                            "source": item.source,
                            "raw": item.raw,
                            "at": moment,
                        }
                    )
            self._drain(moment, outcome)
            self._persist_head()
            return outcome

    def rebase(self, at: float | None = None) -> dict:
        """双源同时重启后，把参考源当前位置人工锚定回已提交水位。"""

        with self._lock:
            moment = self._clock.now() if at is None else float(at)
            if self._horizon is None:
                raise RailError("还没有已提交水位，无需重锚", component="interlock")
            chain = self._chains[self._reference]
            if chain.raw is None:
                raise RailError("参考源还没有任何上报，无法重锚", component="interlock")
            self._offsets[self._reference] = self._horizon - chain.absolute
            self._candidates[self._reference] = []
            events = [
                {
                    "event": "sequence_rebase",
                    "reference": self._reference,
                    "horizon": self._horizon,
                    "offset": self._offsets[self._reference],
                    "at": moment,
                }
            ]
            self._persist_head()
            return {"events": events, "status": self.status(moment)}

    # ---- 查询 -----------------------------------------------------------

    @property
    def pending_count(self) -> int:
        """返回缓冲中等待判定的上报条数。"""

        with self._lock:
            return len(self._pending)

    def gate(self, at: float | None = None) -> dict:
        """门控读取的序号连续性结论（与判定、审计同一份结果）。"""

        with self._lock:
            moment = self._clock.now() if at is None else float(at)
            last = self._last_verdict
            if last is None:
                return {
                    "allow": False,
                    "kind": "",
                    "tick": None,
                    "degraded": self._degraded_now(moment),
                    "reason": "还没有任何已提交的判定",
                }
            return {
                "allow": last.accepted,
                "kind": last.kind,
                "tick": last.tick,
                "degraded": self._degraded_now(moment),
                "reason": last.reason,
            }

    def status(self, at: float | None = None) -> dict:
        """返回对齐状态全量（水位、偏移、各源健康、缓冲与计数）。"""

        with self._lock:
            moment = self._clock.now() if at is None else float(at)
            return {
                "horizon": self._horizon,
                "cycle": None if self._horizon is None else self._horizon // MODULUS,
                "reference": self._reference,
                "primary": self._primary,
                "commit_policy": self._commit_policy,
                "majority": self._majority,
                "degraded": self._degraded_now(moment),
                "pending": len(self._pending),
                "unpaired": len(self._unpaired),
                "offsets": {name: self._offsets[name] for name in self._sources},
                "sources": {
                    name: self._source_status(name, moment) for name in self._sources
                },
                "counters": dict(sorted(self._counters.items())),
                "last_verdict": self._last_verdict.as_dict() if self._last_verdict else None,
                "gate": self.gate(moment),
            }

    # ---- 路由与缓冲 -----------------------------------------------------

    def _route(self, source: str, raw: int, tick: int, moment: float, outcome: dict) -> AlignedVerdict:
        if tick < 0:
            verdict = self._verdict(tick, LATE, False, False, {source: raw}, 0, moment, "刻度落在基准原点之前")
            self._counters[LATE] = self._counters.get(LATE, 0) + 1
            outcome["rejected"].append(verdict)
            return verdict
        pending = self._find_pending(tick)
        if pending is not None:
            pending.reporters[source] = raw
            self._counters["corroborated"] = self._counters.get("corroborated", 0) + 1
            return self._verdict(tick, PENDING, False, False, {source: raw}, 0, moment, "刻度已在缓冲，已补佐证")
        horizon = self._horizon
        if horizon is not None and tick <= horizon:
            if tick in self._seen:
                verdict = self._verdict(tick, REPLAY, False, False, {source: raw}, 0, moment, "刻度已提交，按重复归并")
            else:
                verdict = self._verdict(tick, LATE, False, False, {source: raw}, 0, moment, "刻度早于水位且从未提交")
            self._counters[verdict.kind] = self._counters.get(verdict.kind, 0) + 1
            outcome["rejected"].append(verdict)
            return verdict
        if horizon is None:
            if self._commit_policy == "primary" and source != self._reference:
                return self._park(source, raw, tick, moment, WAIT_DIVERGE, outcome)
            if not self._evidence_met({source}):
                return self._park(source, raw, tick, moment, WAIT_EVIDENCE, outcome, moment + self._pair_window_s)
            verdict = self._commit(tick, ADVANCED, True, {source: raw}, 0, moment, "基准锚定")
            outcome["committed"].append(verdict)
            return verdict
        if self._commit_policy == "primary" and source != self._reference:
            return self._park(source, raw, tick, moment, WAIT_DIVERGE, outcome)
        if tick - horizon > self._max_forward_step:
            return self._park(source, raw, tick, moment, WAIT_GAP, outcome)
        if not self._evidence_met({source}):
            return self._park(source, raw, tick, moment, WAIT_EVIDENCE, outcome, moment + self._pair_window_s)
        if any(
            entry.tick < tick and entry.reason != WAIT_DIVERGE for entry in self._pending
        ):
            # 前面还有提交候选没判完：不许超车（分歧项不是提交候选，不阻塞）。
            return self._park(source, raw, tick, moment, WAIT_ORDERED, outcome)
        kind = WRAPAROUND if tick // MODULUS > horizon // MODULUS else ADVANCED
        verdict = self._commit(tick, kind, True, {source: raw}, tick - horizon - 1, moment, "")
        outcome["committed"].append(verdict)
        return verdict

    def _park(
        self,
        source: str,
        raw: int,
        tick: int,
        moment: float,
        reason: str,
        outcome: dict,
        deadline: float | None = None,
    ) -> AlignedVerdict:
        while len(self._pending) >= self._pending_limit:
            # 缓冲满了：立即判定最老的一条，腾出位置。
            self._expire(self._pending.pop(0), moment, outcome)
        entry = _Pending(tick, {source: raw}, moment, deadline or moment + self._hold_s, reason)
        self._pending.append(entry)
        self._pending.sort(key=lambda item: (item.tick, item.at))
        return self._verdict(tick, PENDING, False, False, {source: raw}, 0, moment, f"已入乱序缓冲（{reason}）")

    def _park_unpaired(self, source: str, raw: int, absolute: int, moment: float, outcome: dict) -> None:
        while len(self._unpaired) >= self._pending_limit:
            dropped = self._unpaired.pop(0)
            self._counters["unpaired_dropped"] = self._counters.get("unpaired_dropped", 0) + 1
            outcome["events"].append(
                {"event": "unpaired_expired", "source": dropped.source, "raw": dropped.raw, "at": moment}
            )
        self._unpaired.append(
            _Unpaired(source, raw, absolute, moment, moment + self._source_timeout_s)
        )

    def _drain(self, moment: float, outcome: dict) -> None:
        while True:
            progressed = False
            for entry in list(self._pending):
                horizon = self._horizon
                if horizon is not None and entry.tick <= horizon:
                    self._pending.remove(entry)
                    kind = REPLAY if entry.tick in self._seen else LATE
                    self._counters[kind] = self._counters.get(kind, 0) + 1
                    outcome["rejected"].append(
                        self._verdict(
                            entry.tick,
                            kind,
                            False,
                            False,
                            dict(entry.reporters),
                            0,
                            moment,
                            "缓冲归位：刻度已提交" if kind == REPLAY else "缓冲归位：刻度已过时",
                        )
                    )
                    progressed = True
                    continue
                if self._committable(entry):
                    self._pending.remove(entry)
                    self._commit_pending(entry, moment, outcome)
                    progressed = True
                    continue
                if moment >= entry.deadline:
                    self._pending.remove(entry)
                    self._expire(entry, moment, outcome)
                    progressed = True
                    continue
                if entry.reason == WAIT_DIVERGE:
                    continue  # 分歧项不是提交候选，不阻塞后面的有序提交
                break  # 提交候选未到判定时机；后面的不许超车
            if not progressed:
                break

    def _committable(self, entry: _Pending) -> bool:
        horizon = self._horizon
        if horizon is None:
            return entry.reason != WAIT_DIVERGE and self._evidence_met(set(entry.reporters))
        if entry.tick - horizon > self._max_forward_step:
            return False
        if (
            self._commit_policy == "primary"
            and entry.reason == WAIT_DIVERGE
            and self._reference not in entry.reporters
        ):
            return False
        return self._evidence_met(set(entry.reporters))

    def _commit_pending(self, entry: _Pending, moment: float, outcome: dict) -> None:
        horizon = self._horizon
        if horizon is None:
            verdict = self._commit(entry.tick, ADVANCED, True, entry.reporters, 0, moment, "基准锚定")
        else:
            kind = WRAPAROUND if entry.tick // MODULUS > horizon // MODULUS else ADVANCED
            verdict = self._commit(
                entry.tick, kind, True, entry.reporters, entry.tick - horizon - 1, moment, ""
            )
        outcome["committed"].append(verdict)

    def _expire(self, entry: _Pending, moment: float, outcome: dict) -> None:
        horizon = self._horizon
        if horizon is not None and entry.tick <= horizon:
            kind = REPLAY if entry.tick in self._seen else LATE
            self._counters[kind] = self._counters.get(kind, 0) + 1
            outcome["rejected"].append(
                self._verdict(entry.tick, kind, False, False, dict(entry.reporters), 0, moment, "缓冲归位")
            )
            return
        reason = entry.reason if entry.reason != WAIT_ORDERED else self._classify(entry)
        if reason == WAIT_GAP:
            base = self._horizon if self._horizon is not None else entry.tick - 1
            outcome["committed"].append(
                self._commit(
                    entry.tick,
                    GAP,
                    False,
                    entry.reporters,
                    max(0, entry.tick - base - 1),
                    moment,
                    "缺口确认：缓冲期内未等到补报",
                )
            )
            return
        if reason == WAIT_EVIDENCE:
            outcome["committed"].append(
                self._commit(
                    entry.tick,
                    DEGRADED,
                    self._degraded_accept,
                    entry.reporters,
                    0,
                    moment,
                    "佐证未到齐，降级提交",
                )
            )
            return
        if reason == WAIT_DIVERGE:
            self._counters["diverged"] = self._counters.get("diverged", 0) + 1
            outcome["events"].append(
                {
                    "event": "source_diverged",
                    "tick": entry.tick,
                    "reporters": dict(entry.reporters),
                    "at": moment,
                }
            )
            return
        self._commit_pending(entry, moment, outcome)  # 排队排到了，自身没有问题

    def _classify(self, entry: _Pending) -> str:
        horizon = self._horizon
        if horizon is not None and entry.tick - horizon > self._max_forward_step:
            return WAIT_GAP
        if self._commit_policy == "primary" and self._reference not in entry.reporters:
            return WAIT_DIVERGE
        if not self._evidence_met(set(entry.reporters)):
            return WAIT_EVIDENCE
        return WAIT_ORDERED

    def _find_pending(self, tick: int) -> _Pending | None:
        for entry in self._pending:
            if entry.tick == tick:
                return entry
        return None

    # ---- 提交 -----------------------------------------------------------

    def _commit(
        self,
        tick: int,
        kind: str,
        accepted: bool,
        reporters: dict,
        skipped: int,
        moment: float,
        reason: str,
    ) -> AlignedVerdict:
        verdict = AlignedVerdict(
            tick=tick,
            cycle=tick // MODULUS,
            kind=kind,
            accepted=accepted,
            committed=True,
            degraded=self._degraded_now(moment) or kind == DEGRADED,
            sources=dict(reporters),
            skipped=skipped,
            at=moment,
            reason=reason or kind,
        )
        # 先落流水再改状态：历史只追加，崩溃后从流水前滚。
        self._journal.append({"type": "commit", **verdict.as_dict()})
        self._horizon = tick
        self._seen.add(tick)
        self._seen_order.append(tick)
        while len(self._seen_order) > SEEN_WINDOW:
            self._seen.discard(self._seen_order.popleft())
        self._last_verdict = verdict
        self._counters[kind] = self._counters.get(kind, 0) + 1
        return verdict

    def _verdict(
        self,
        tick: int,
        kind: str,
        accepted: bool,
        committed: bool,
        reporters: dict,
        skipped: int,
        moment: float,
        reason: str,
    ) -> AlignedVerdict:
        return AlignedVerdict(
            tick=tick,
            cycle=tick // MODULUS if tick >= 0 else -1,
            kind=kind,
            accepted=accepted,
            committed=committed,
            degraded=False,
            sources=dict(reporters),
            skipped=skipped,
            at=moment,
            reason=reason,
        )

    def _settle_verdict(self, outcome: dict) -> None:
        """本次上报若先缓冲、同调用内又被判定，把结论换成最终判定。"""

        verdict = outcome["verdict"]
        if verdict is None or verdict.kind != PENDING:
            return
        for item in outcome["committed"] + outcome["rejected"]:
            if item.tick == verdict.tick:
                outcome["verdict"] = item
                return

    # ---- 偏移配对 -------------------------------------------------------

    def _anchor(self, exclude: str) -> str | None:
        candidates = [
            name
            for name in self._sources
            if name != exclude
            and self._offsets[name] is not None
            and self._chains[name].raw is not None
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda name: self._chains[name].last_at)

    def _settle_pairing(self, moment: float, outcome: dict) -> None:
        """在双源都静默时观测偏移，连续一致才锁定或重估。

        上报到达有先有后，途中取到的"最新值"可能差着在途事件；只有双方都安静
        下来（各自最新上报都早于 ``pair_window_s``），两源的最新值才必然对应同
        一个事件。同一组观测只计一次，避免长时间静默被重复计数。
        """

        for name in self._sources:
            chain = self._chains[name]
            if chain.raw is None:
                continue
            if name == self._reference and self._offsets[name] is not None:
                continue  # 参考源的轴就是基准本身，不参与对表
            anchor = self._anchor(name)
            if anchor is None:
                continue
            if moment - chain.last_at < self._pair_window_s:
                continue
            if moment - self._chains[anchor].last_at < self._pair_window_s:
                continue
            pair = (self._chains[anchor].absolute, chain.absolute)
            if self._last_pair.get(name) == pair:
                continue
            self._last_pair[name] = pair
            candidate = (self._chains[anchor].absolute + self._offsets[anchor]) - chain.absolute  # type: ignore[operator]
            current = self._offsets[name]
            if current is not None and candidate == current:
                self._candidates[name] = []
                continue
            history = self._candidates[name]
            history.append(candidate)
            del history[: -self._offset_confirm]
            if len(history) < self._offset_confirm or len(set(history)) != 1:
                continue
            self._candidates[name] = []
            if current is None:
                self._offsets[name] = candidate
                outcome["events"].append(
                    {"event": "offset_locked", "source": name, "offset": candidate, "at": moment}
                )
            else:
                shift = candidate - current
                self._offsets[name] = candidate
                outcome["events"].append(
                    {
                        "event": "offset_resynced",
                        "source": name,
                        "old": current,
                        "new": candidate,
                        "at": moment,
                    }
                )
                for entry in self._pending:
                    if name in entry.reporters:
                        entry.tick += shift
                self._pending.sort(key=lambda item: (item.tick, item.at))
            self._route_unpaired(name, moment, outcome)

    def _adopt_baseline(self, source: str, moment: float, outcome: dict) -> None:
        """主源从未上报且已 failover 时，由接管源的轴定义基准。"""

        if (
            self._offsets[source] is None
            and source == self._reference
            and self._horizon is None
        ):
            self._offsets[source] = 0
            outcome["events"].append({"event": "baseline_adopted", "source": source, "at": moment})
            self._route_unpaired(source, moment, outcome)

    def _route_unpaired(self, source: str, moment: float, outcome: dict) -> None:
        rest = []
        for item in self._unpaired:
            if item.source != source:
                rest.append(item)
                continue
            self._route(item.source, item.raw, item.absolute + self._offsets[source], moment, outcome)  # type: ignore[operator]
        self._unpaired = rest

    # ---- 参考源与证据 ---------------------------------------------------

    def _silent(self, name: str, moment: float) -> bool:
        last_at = self._chains[name].last_at
        return last_at == 0 or moment - last_at > self._source_timeout_s

    def _degraded_now(self, moment: float) -> bool:
        return any(self._silent(name, moment) for name in self._sources)

    def _evidence_met(self, reporters: set) -> bool:
        if self._commit_policy == "quorum":
            return len(reporters) >= self._majority
        return self._reference in reporters

    def _refresh_reference(self, moment: float, events: list) -> None:
        reference = self._primary
        if self._silent(self._primary, moment):
            candidates = [
                name
                for name in self._sources
                if name != self._primary and not self._silent(name, moment)
            ]
            if candidates:
                reference = max(candidates, key=lambda name: self._chains[name].last_at)
        if reference == self._primary and self._reference != self._primary:
            # 主源追不上水位时暂不交还，等它越过水位再 failback。
            offset = self._offsets[self._primary]
            aligned = None if offset is None else self._chains[self._primary].absolute + offset
            if self._horizon is not None and (aligned is None or aligned <= self._horizon):
                reference = self._reference
        if reference != self._reference:
            events.append(
                {
                    "event": "failback" if reference == self._primary else "failover",
                    "from": self._reference,
                    "to": reference,
                    "at": moment,
                }
            )
            self._reference = reference

    # ---- 状态与恢复 -----------------------------------------------------

    def _source_status(self, name: str, moment: float) -> dict:
        chain = self._chains[name]
        return {
            "raw": chain.raw,
            "absolute": chain.absolute if chain.raw is not None else None,
            "wraps": chain.wraps,
            "epoch": chain.epoch,
            "reports": chain.reports,
            "duplicates": chain.duplicates,
            "resets": chain.resets,
            "last_at": chain.last_at,
            "silent": self._silent(name, moment),
            "offset": self._offsets[name],
        }

    def _persist_head(self) -> None:
        self._store.write(
            HEAD_DOC,
            {
                "horizon": self._horizon,
                "reference": self._reference,
                "offsets": self._offsets,
                "chains": {name: chain.snapshot() for name, chain in self._chains.items()},
                "pending": [
                    {
                        "tick": entry.tick,
                        "reporters": entry.reporters,
                        "at": entry.at,
                        "deadline": entry.deadline,
                        "reason": entry.reason,
                    }
                    for entry in self._pending
                ],
                "unpaired": [
                    {
                        "source": item.source,
                        "raw": item.raw,
                        "absolute": item.absolute,
                        "at": item.at,
                        "deadline": item.deadline,
                    }
                    for item in self._unpaired
                ],
                "counters": self._counters,
                "journal_count": self._journal.count(),
                "last_verdict": self._last_verdict.as_dict() if self._last_verdict else None,
            },
        )

    def _restore(self) -> None:
        head = self._store.read(HEAD_DOC)
        if isinstance(head, dict):
            horizon = head.get("horizon")
            self._horizon = None if horizon is None else int(horizon)
            reference = str(head.get("reference", self._primary))
            self._reference = reference if reference in self._chains else self._primary
            offsets = head.get("offsets", {})
            if isinstance(offsets, dict):
                for name in self._sources:
                    if name in offsets:
                        value = offsets[name]
                        self._offsets[name] = None if value is None else int(value)
            chains = head.get("chains", {})
            if isinstance(chains, dict):
                for name, payload in chains.items():
                    if name in self._chains and isinstance(payload, dict):
                        self._chains[name] = SourceChain.restore(name, payload)
            self._pending = [
                _Pending(
                    int(item["tick"]),
                    {str(key): int(value) for key, value in dict(item["reporters"]).items()},
                    float(item["at"]),
                    float(item["deadline"]),
                    str(item["reason"]),
                )
                for item in head.get("pending", [])
                if isinstance(item, dict)
            ]
            self._pending.sort(key=lambda item: (item.tick, item.at))
            self._unpaired = [
                _Unpaired(
                    str(item["source"]),
                    int(item["raw"]),
                    int(item["absolute"]),
                    float(item["at"]),
                    float(item["deadline"]),
                )
                for item in head.get("unpaired", [])
                if isinstance(item, dict) and item.get("source") in self._chains
            ]
            counters = head.get("counters", {})
            if isinstance(counters, dict):
                self._counters = {str(key): int(value) for key, value in counters.items()}
            last = head.get("last_verdict")
            if isinstance(last, dict):
                self._last_verdict = AlignedVerdict.from_dict(last)
        # 流水前滚：补上落盘流水里有、头文档没赶上的提交。
        commits = [
            record
            for record in self._journal.read_all()
            if isinstance(record, dict) and record.get("type") == "commit"
        ]
        for record in commits:
            tick = int(record.get("tick", -1))
            if self._horizon is None or tick > self._horizon:
                self._horizon = tick
                self._last_verdict = AlignedVerdict.from_dict(record)
                kind = str(record.get("kind", ""))
                self._counters[kind] = self._counters.get(kind, 0) + 1
        # 重放识别窗口：从流水尾部重建。
        tail = [int(record["tick"]) for record in commits[-SEEN_WINDOW:]]
        self._seen = set(tail)
        self._seen_order = deque(tail)

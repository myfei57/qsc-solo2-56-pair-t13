"""多来源序号监控：对齐、乱序缓冲、判定与不可变落盘。

:class:`SequenceMonitor` 是判定、审计、门控共同读取的唯一事实来源。每条门控事件
按 ``key``（默认线路，可按列车/门区隔离）独立维护：

1. 各路原始报告先经各自的 :class:`~psdline.sequence.normalize.SourceTrack` 归一到
   同一条基准；偏移在两路首个能配对的事件上学一次，之后按统一基准刻度对齐；
2. 每路相对自己上一条做单调检测：重复送达幂等忽略，重放／回绕立即拒绝；前进送进
   :class:`ReorderBuffer`，等齐其它来源或超时再按策略汇总；
3. 判定结论先**追加**到只增不改的 ledger（JSONL，fsync），再更新可重建的 checkpoint。
   因此崩溃恢复时「已追加即已生效」：重放 ledger 即可还原内存，历史永不改写。

门控只读 :meth:`SequenceMonitor.latest` / :meth:`SequenceMonitor.gate_clear`，审计
读同一条 :class:`SequenceResult`（同一 ``event_id``），三处不会各判各的。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from ..clock import Clock, SystemClock
from ..store import JsonStore, JsonlJournal
from .buffer import BufferedObservation, ReorderBuffer
from .classify import (
    ADVANCED,
    DUPLICATE,
    REPLAY,
    ROLLED_BACK,
    classify_position,
)
from .normalize import DEFAULT_MODULUS, SourceTrack
from .policy import ACCEPT, PENDING, PRIMARY, QUORUM, decide

DEFAULT_KEY = "line"


@dataclass(frozen=True)
class Observation:
    """一路采集源上报的一条门控事件序号。"""

    source: str
    raw: int
    key: str = DEFAULT_KEY
    at: float | None = None
    event_ref: str = ""


@dataclass(frozen=True)
class SequenceResult:
    """一次判定的最终结果，判定／审计／门控读同一份。"""

    event_id: str
    seq: int
    key: str
    normalized: int
    decision: str
    reason: str
    policy: str
    observations: dict
    advance_votes: tuple
    dead: dict
    warnings: tuple
    sources: tuple
    accepted: bool
    at: float
    modulus: int

    def as_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "seq": self.seq,
            "key": self.key,
            "normalized": self.normalized,
            "decision": self.decision,
            "reason": self.reason,
            "policy": self.policy,
            "observations": dict(self.observations),
            "advance_votes": list(self.advance_votes),
            "dead": dict(self.dead),
            "warnings": list(self.warnings),
            "sources": list(self.sources),
            "accepted": self.accepted,
            "at": self.at,
            "modulus": self.modulus,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "SequenceResult":
        return cls(
            event_id=str(payload.get("event_id", "")),
            seq=int(payload.get("seq", 0)),
            key=str(payload.get("key", DEFAULT_KEY)),
            normalized=int(payload.get("normalized", 0)),
            decision=str(payload.get("decision", "")),
            reason=str(payload.get("reason", "")),
            policy=str(payload.get("policy", PRIMARY)),
            observations=dict(payload.get("observations", {})),
            advance_votes=tuple(payload.get("advance_votes", ())),
            dead=dict(payload.get("dead", {})),
            warnings=tuple(payload.get("warnings", ())),
            sources=tuple(payload.get("sources", ())),
            accepted=bool(payload.get("accepted", False)),
            at=float(payload.get("at", 0.0)),
            modulus=int(payload.get("modulus", DEFAULT_MODULUS)),
        )


@dataclass
class _KeyState:
    tracks: dict[str, SourceTrack] = field(default_factory=dict)
    watermark: int | None = None
    seen: dict[str, int] = field(default_factory=dict)  # source -> 最近已接收统一基准
    buffer: ReorderBuffer | None = None
    results: list = field(default_factory=list)
    counter: int = 0
    # 尚未学到偏移时先到的首事件（base 未知），等另一路首事件配对学偏移。
    pending_anchor: BufferedObservation | None = None
    next_base: int = 0


class SequenceMonitor:
    """多来源门控序号监控器。"""

    def __init__(
        self,
        store: JsonStore,
        clock: Clock | None = None,
        *,
        policy: str = PRIMARY,
        primary_source: str = "A",
        sources: tuple[str, ...] = ("A", "B"),
        modulus: int = DEFAULT_MODULUS,
        order_wait_s: float = 2.0,
        buffer_capacity: int = 256,
        ledger_topic: str = "sequence",
    ) -> None:
        if policy not in (PRIMARY, QUORUM):
            raise ValueError(f"未知序号判定策略: {policy}")
        if modulus != DEFAULT_MODULUS:
            raise ValueError("序号模数固定为 65536")
        if primary_source not in sources:
            raise ValueError(f"主源 {primary_source!r} 不在来源列表里")
        if order_wait_s <= 0:
            raise ValueError("乱序缓冲等待时间必须为正")
        if buffer_capacity <= 0:
            raise ValueError("乱序缓冲容量必须为正")
        self._store = store
        self._clock = clock or SystemClock()
        self._policy = policy
        self._primary = primary_source
        self._sources = tuple(sources)
        self._modulus = modulus
        self._order_wait_s = float(order_wait_s)
        self._buffer_capacity = int(buffer_capacity)
        self._ledger = JsonlJournal(store.root, ledger_topic, segment_limit=512)
        self._states: dict[str, _KeyState] = {}
        self._locks: dict[str, threading.RLock] = {}
        self._global = threading.RLock()
        self._accepted = 0
        self._rejected = 0
        self._duplicates = 0
        self._recover()

    # ---- 属性 ---------------------------------------------------------

    @property
    def policy(self) -> str:
        return self._policy

    @property
    def primary_source(self) -> str:
        return self._primary

    @property
    def sources(self) -> tuple[str, ...]:
        return self._sources

    @property
    def modulus(self) -> int:
        return self._modulus

    @property
    def order_wait_s(self) -> float:
        return self._order_wait_s

    @property
    def accepted_count(self) -> int:
        return self._accepted

    @property
    def rejected_count(self) -> int:
        return self._rejected

    @property
    def duplicate_count(self) -> int:
        return self._duplicates

    # ---- 上报入口 -----------------------------------------------------

    def report(self, observation: Observation) -> SequenceResult | None:
        """登记一路报告；冲刷出判定时返回结果，仅入缓冲等待时返回 ``None``。"""

        key = observation.key or DEFAULT_KEY
        now = observation.at if observation.at is not None else self._clock.now()
        observation = Observation(
            source=observation.source,
            raw=int(observation.raw),
            key=key,
            at=now,
            event_ref=observation.event_ref,
        )
        with self._lock_for(key):
            state = self._state(key)
            results = self._ingest(state, observation, now)
            return results[0] if results else None

    def report_many(self, observations: list[Observation]) -> list[SequenceResult]:
        """批量上报（并发安全），返回所有被冲刷出的结果。"""

        flushed: list[SequenceResult] = []
        for observation in observations:
            result = self.report(observation)
            if result is not None:
                flushed.append(result)
        return flushed

    def pump(self, key: str = DEFAULT_KEY, now: float | None = None) -> list[SequenceResult]:
        """冲刷该 key 已超时的乱序槽位（控制周期或定时器调用）。"""

        moment = now if now is not None else self._clock.now()
        with self._lock_for(key):
            state = self._states.get(key)
            if state is None:
                return []
            results: list[SequenceResult] = []
            # 首路一直等不到配对来源：超时后按安全侧拒绝，避免无限挂起。
            anchor = state.pending_anchor
            if anchor is not None and moment - anchor.received_at >= self._order_wait_s:
                state.pending_anchor = None
                results.append(
                    self._emit(
                        state,
                        Observation(source=anchor.source, raw=anchor.raw, key=key, at=moment),
                        normalized=0,
                        decision="reject",
                        reason="anchor_timeout",
                        observations={anchor.source: anchor.as_dict()},
                        dead={},
                        warnings=("anchor_timeout",),
                        advance_votes=(),
                        at=moment,
                        advance_highwater=False,
                        commit_sources=(),
                    )
                )
            if state.buffer is None:
                return results
            for _normalized, _reason, slot in state.buffer.flush_due(moment):
                results.append(self._finalize(state, key, slot, moment, forced=True))
            return results

    def pump_all(self, now: float | None = None) -> list[SequenceResult]:
        """冲刷全部 key 的超时槽位。"""

        moment = now if now is not None else self._clock.now()
        with self._global:
            keys = list(self._states)
        flushed: list[SequenceResult] = []
        for key in keys:
            flushed.extend(self.pump(key, moment))
        return flushed

    # ---- 判定内部 -----------------------------------------------------

    def _ingest(self, state: _KeyState, observation: Observation, now: float) -> list[SequenceResult]:
        if observation.source not in self._sources:
            return [
                self._emit(
                    state,
                    observation,
                    normalized=0,
                    decision="reject",
                    reason="unknown_source",
                    observations={
                        observation.source: self._obs_dict(
                            observation, raw=observation.raw, normalized=0, kind="unknown_source"
                        )
                    },
                    dead={observation.source: "unknown_source"},
                    warnings=(),
                    advance_votes=(),
                    at=now,
                    advance_highwater=False,
                    commit_sources=(),
                )
            ]

        track = state.tracks.get(observation.source)
        if track is None:
            track = SourceTrack(source=observation.source, modulus=self._modulus)
            state.tracks[observation.source] = track

        # ---- 该来源的首条：先确定它自己的零相位，偏移待配对学到 --------
        if not track.started:
            track.start(observation.raw)
            buffered = BufferedObservation(
                source=observation.source,
                raw=observation.raw,
                normalized=0,  # 占位：统一基准偏移尚未学到
                tick=0,
                delta=0,
                kind=ADVANCED,
                received_at=now,
            )
            # 已有另一路首事件在等：两者配对，学到偏移并统一落到基准 0。
            waiting = state.pending_anchor
            if waiting is not None and waiting.source != observation.source:
                first_track = state.tracks[waiting.source]
                # 两路首事件对齐到同一个 base=0；track 的高水位由 _emit 统一提交。
                first_track.align_to(0, 0)
                track.align_to(0, 0)
                waiting.normalized = 0
                waiting.wraps = 0
                waiting.ext = 0
                state.pending_anchor = None
                slot_observations = {
                    waiting.source: waiting.as_dict(),
                    observation.source: buffered.as_dict(),
                }
                return [
                    self._emit_complete(
                        state, observation.key, 0, slot_observations, now
                    )
                ]
            # 该来源是第一路，先挂起等配对。
            state.pending_anchor = buffered
            self._checkpoint(observation.key, state)
            return []

        wraps, ext, base_value, delta = track.project(observation.raw)

        # 偏移尚未学到（该来源是后到的第二路，但首事件走的是另一分支；这里兜底）。
        if not track.aligned:
            track.align_to(state.next_base, ext)
            base_value = ext + track.offset  # type: ignore[operator]

        # ---- 每路独立单调检测 -----------------------------------------
        normalized = int(base_value)
        kind = ADVANCED
        if track.last_base is not None:
            kind, _gap = classify_position(track.last_base, normalized)
        if kind == ADVANCED and delta == 0:
            kind = DUPLICATE
        elif kind == ADVANCED and delta >= self._modulus // 2:
            kind = ROLLED_BACK

        buffered = BufferedObservation(
            source=observation.source,
            raw=observation.raw,
            normalized=normalized,
            tick=track.calibration.tick(observation.raw),
            delta=delta,
            kind=kind,
            received_at=now,
            wraps=wraps,
            ext=ext,
        )

        # 同一来源、同一统一基准的重复送达：以已接收序号表为准，幂等忽略不推进。
        if state.seen.get(observation.source) == normalized:
            self._duplicates += 1
            return [
                self._emit(
                    state,
                    observation,
                    normalized=normalized,
                    decision=ACCEPT,
                    reason=DUPLICATE,
                    observations={observation.source: buffered.as_dict()},
                    dead={},
                    warnings=(),
                    advance_votes=(),
                    at=now,
                    advance_highwater=False,
                    commit_sources=(),
                )
            ]

        # 重放／回绕立即拒绝，安全侧优先，不必等另一路。
        if kind in (REPLAY, ROLLED_BACK):
            return [
                self._emit(
                    state,
                    observation,
                    normalized=normalized,
                    decision="reject",
                    reason=kind,
                    observations={observation.source: buffered.as_dict()},
                    dead={observation.source: kind},
                    warnings=(),
                    advance_votes=(),
                    at=now,
                    advance_highwater=False,
                    commit_sources=(),
                )
            ]

        if state.buffer is None:
            state.buffer = ReorderBuffer(self._order_wait_s, self._buffer_capacity)
        flushed: list[SequenceResult] = []
        readiness = state.buffer.add(
            normalized, buffered, now=now, expected_sources=len(self._sources)
        )
        evicted = readiness.get("evicted")
        if evicted is not None:
            _key0, old_slot = evicted
            flushed.append(self._finalize(state, observation.key, old_slot, now, overflow=True, forced=True))
        if readiness["ready"]:
            slot = state.buffer.take(normalized)
            if slot is not None:
                flushed.append(
                    self._finalize(state, observation.key, slot, now, overflow=False)
                )
        return flushed

    def _finalize(
        self,
        state: _KeyState,
        key: str,
        slot,
        now: float,
        *,
        overflow: bool = False,
        forced: bool = False,
    ) -> SequenceResult:
        normalized = int(slot.normalized)
        observations = {name: item.as_dict() for name, item in slot.observations.items()}
        classifications = {name: item.kind for name, item in slot.observations.items()}
        verdict = decide(
            classifications,
            policy=self._policy,
            primary_source=self._primary,
            expected_sources=len(self._sources),
        )
        decision = verdict["decision"]
        reason = verdict["reason"]
        warnings = list(verdict["warnings"])
        if overflow:
            warnings.append("buffer_overflow")

        # 超时／溢出强制冲刷时来源没等齐：缓冲阶段的 pending 在这里落成最终结论。
        # 安全侧不放行：主源缺票或多数未达成都判拒绝，交人工复核而不是门控打开。
        if forced and decision == PENDING:
            decision = "reject"
            reason = "incomplete_timeout" if not overflow else "incomplete_overflow"
        # 溢出是异常信号（缓冲被灌满，说明采集或链路出了问题）：即使策略本会接受，
        # 只要来源没到齐就不放行，避免在旁证缺失时把门打开。
        if overflow and len(slot.observations) < len(self._sources) and decision == ACCEPT:
            decision = "reject"
            reason = "overflow_incomplete"

        # 越过缓冲后再做基准单调性兜底：统一基准不允许落在已提交高水位之后。
        if decision == ACCEPT and state.watermark is not None and normalized < state.watermark:
            decision = "reject"
            reason = "below_watermark"

        representative = next(iter(slot.observations.values()))
        synthetic = Observation(
            source=representative.source, raw=representative.raw, key=key, at=now
        )
        advance = decision == ACCEPT and reason not in (DUPLICATE,)
        return self._emit(
            state,
            synthetic,
            normalized=normalized,
            decision=decision,
            reason=reason,
            observations=observations,
            dead=dict(verdict["dead"]),
            warnings=tuple(warnings),
            advance_votes=tuple(verdict["advance_votes"]),
            at=now,
            advance_highwater=advance,
            commit_sources=tuple(name for name, kind in classifications.items() if kind == ADVANCED),
        )

    def _emit_complete(
        self,
        state: _KeyState,
        key: str,
        normalized: int,
        observations: dict,
        now: float,
    ) -> SequenceResult:
        """两路首事件配对成功，直接给出基准 0 的完整判定。"""

        classifications = {name: ADVANCED for name in observations}
        verdict = decide(
            classifications,
            policy=self._policy,
            primary_source=self._primary,
            expected_sources=len(self._sources),
        )
        synthetic = Observation(source=key, raw=0, key=key, at=now)
        return self._emit(
            state,
            synthetic,
            normalized=0,
            decision=verdict["decision"],
            reason=verdict["reason"],
            observations=observations,
            dead={},
            warnings=tuple(verdict["warnings"]),
            advance_votes=tuple(verdict["advance_votes"]),
            at=now,
            advance_highwater=verdict["decision"] == ACCEPT,
            commit_sources=tuple(observations),
        )

    # ---- 落盘与恢复 ---------------------------------------------------

    def _emit(
        self,
        state: _KeyState,
        observation: Observation,
        *,
        normalized: int,
        decision: str,
        reason: str,
        observations: dict,
        dead: dict,
        warnings: tuple,
        advance_votes: tuple,
        at: float,
        advance_highwater: bool,
        commit_sources: tuple,
    ) -> SequenceResult:
        state.counter += 1
        result = SequenceResult(
            event_id=f"seq-{observation.key}-{state.counter:08d}",
            seq=state.counter,
            key=observation.key,
            normalized=normalized,
            decision=decision,
            reason=reason,
            policy=self._policy,
            observations=observations,
            advance_votes=advance_votes,
            dead=dead,
            warnings=warnings,
            sources=tuple(sorted(observations)),
            accepted=decision == ACCEPT,
            at=at,
            modulus=self._modulus,
        )
        # 先追加不可变流水；一旦追加成功，结论即生效。
        self._ledger.append(result.as_dict())
        state.results.append(result)

        if result.accepted:
            self._accepted += 1
            if advance_highwater:
                for source in commit_sources:
                    item = observations.get(source, {})
                    track = state.tracks.get(source)
                    if track is not None:
                        track.commit(
                            int(item.get("raw", 0)),
                            int(item.get("wraps", track.wraps)),
                            int(item.get("ext", 0)),
                            int(item.get("normalized", normalized)),
                        )
                        state.seen[source] = normalized
                if state.watermark is None or normalized > (state.watermark or -1):
                    state.watermark = normalized
                state.next_base = max(state.next_base, normalized + 1)
        else:
            self._rejected += 1
        self._checkpoint(observation.key, state)
        return result

    def _checkpoint(self, key: str, state: _KeyState) -> None:
        payload = {
            "key": key,
            "watermark": state.watermark,
            "next_base": state.next_base,
            "counter": state.counter,
            "seen": dict(state.seen),
            "tracks": {name: track.as_dict() for name, track in state.tracks.items()},
            "pending_anchor": state.pending_anchor.as_dict()
            if state.pending_anchor is not None
            else None,
        }
        self._store.write(self._checkpoint_id(key), payload)

    def _recover(self) -> None:
        """以不可变 ledger 为事实源重放恢复；checkpoint 只提供来源相位校准。

        即使 checkpoint 滞后或损坏（写它时崩溃），只要 ledger 已追加，结论就生效：
        高水位、去重表、计数与历史全部从 ledger 重算，历史不会被旧 checkpoint 改写。
        """

        # checkpoint 只用来恢复各来源的相位校准与尚未配对的挂起首事件；这些信息在
        # ledger 里只以最终结果出现，无法单独重建。
        for doc_id in self._store.list_ids():
            parts = doc_id.split("/")
            if len(parts) != 3 or parts[0] != "sequence" or parts[2] != "checkpoint":
                continue
            payload = self._store.read(doc_id)
            if not isinstance(payload, dict):
                continue
            key = str(payload.get("key", parts[1]))
            state = self._state(key)
            state.tracks = {
                str(name): SourceTrack.from_dict(raw)
                for name, raw in dict(payload.get("tracks", {})).items()
                if isinstance(raw, dict)
            }
            anchor = payload.get("pending_anchor")
            if isinstance(anchor, dict):
                state.pending_anchor = BufferedObservation(
                    source=str(anchor.get("source", "")),
                    raw=int(anchor.get("raw", 0)),
                    normalized=int(anchor.get("normalized", 0)),
                    tick=int(anchor.get("tick", 0)),
                    delta=int(anchor.get("delta", 0)),
                    kind=str(anchor.get("kind", ADVANCED)),
                    received_at=float(anchor.get("received_at", 0.0)),
                    wraps=int(anchor.get("wraps", 0)),
                    ext=int(anchor.get("ext", 0)),
                )

        # 权威状态全部由只追加的 ledger 重放重建。
        for record in self._ledger.read_all():
            if not isinstance(record, dict):
                continue
            result = SequenceResult.from_dict(record)
            state = self._state(result.key)
            state.results.append(result)
            state.counter = max(state.counter, result.seq)
            if result.accepted:
                self._accepted += 1
                if result.reason != DUPLICATE:
                    if state.watermark is None or result.normalized > state.watermark:
                        state.watermark = result.normalized
                    state.next_base = max(state.next_base, result.normalized + 1)
                    for source, item in result.observations.items():
                        if isinstance(item, dict) and item.get("kind") == ADVANCED:
                            state.seen[source] = result.normalized
                            track = state.tracks.get(source)
                            if track is not None:
                                track.commit(
                                    int(item.get("raw", 0)),
                                    int(item.get("wraps", track.wraps)),
                                    int(item.get("ext", 0)),
                                    int(item.get("normalized", result.normalized)),
                                )
            else:
                self._rejected += 1

    # ---- 查询（门控、审计读同一份结果）-------------------------------

    def latest(self, key: str = DEFAULT_KEY) -> SequenceResult | None:
        with self._lock_for(key):
            state = self._states.get(key)
            return state.results[-1] if state and state.results else None

    def result(self, event_id: str, key: str = DEFAULT_KEY) -> SequenceResult | None:
        with self._lock_for(key):
            state = self._states.get(key)
            if state is None:
                return None
            for item in reversed(state.results):
                if item.event_id == event_id:
                    return item
            return None

    def history(self, key: str = DEFAULT_KEY, limit: int = 50) -> list[SequenceResult]:
        with self._lock_for(key):
            state = self._states.get(key)
            if state is None:
                return []
            return list(state.results[-limit:])

    def watermark(self, key: str = DEFAULT_KEY) -> int | None:
        with self._lock_for(key):
            state = self._states.get(key)
            return state.watermark if state else None

    def gate_clear(self, key: str = DEFAULT_KEY) -> bool:
        """门控放行条件：最近一条判定为接受；无判定时保守不放行。"""

        result = self.latest(key)
        return bool(result and result.accepted)

    def pending(self, key: str = DEFAULT_KEY) -> int:
        with self._lock_for(key):
            state = self._states.get(key)
            count = len(state.buffer) if state and state.buffer else 0
            if state and state.pending_anchor is not None:
                count += 1
            return count

    def status(self) -> dict:
        with self._global:
            keys = list(self._states)
        return {
            "policy": self._policy,
            "primary_source": self._primary,
            "sources": list(self._sources),
            "modulus": self._modulus,
            "order_wait_s": self._order_wait_s,
            "buffer_capacity": self._buffer_capacity,
            "accepted": self.accepted_count,
            "rejected": self.rejected_count,
            "duplicates": self.duplicate_count,
            "keys": {
                key: {
                    "watermark": self.watermark(key),
                    "pending": self.pending(key),
                    "results": len(self.history(key, limit=10**9)),
                    "tracks": sorted(self._states[key].tracks),
                }
                for key in keys
            },
        }

    # ---- 内部 ---------------------------------------------------------

    def _state(self, key: str) -> _KeyState:
        state = self._states.get(key)
        if state is None:
            state = _KeyState()
            self._states[key] = state
        return state

    def _lock_for(self, key: str) -> threading.RLock:
        with self._global:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._locks[key] = lock
            return lock

    @staticmethod
    def _checkpoint_id(key: str) -> str:
        return f"sequence/{key}/checkpoint"

    @staticmethod
    def _obs_dict(observation: Observation, *, raw: int, normalized: int, kind: str) -> dict:
        return {
            "source": observation.source,
            "raw": raw,
            "normalized": normalized,
            "tick": 0,
            "delta": 0,
            "kind": kind,
            "received_at": observation.at if observation.at is not None else 0.0,
        }

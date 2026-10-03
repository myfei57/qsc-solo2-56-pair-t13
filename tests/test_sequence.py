"""多采集来源门控序号：对齐、前进／回绕／重放、乱序缓冲与恢复。"""

from __future__ import annotations

import threading

import pytest

from psdline.clock import ManualClock
from psdline.sequence import (
    Observation,
    ReorderBuffer,
    SequenceMonitor,
    classify_advance,
    modular_delta,
    normalize_sequence,
)
from psdline.sequence.classify import DUPLICATE, REPLAY, ROLLED_BACK
from psdline.sequence.normalize import SourceCalibration, SourceTrack
from psdline.sequence.policy import PRIMARY, QUORUM, decide
from psdline.sequence.policy import ACCEPT, PENDING, REJECT
from psdline.store import JsonStore


# ---- 纯函数 -------------------------------------------------------------


def test_modular_delta_and_classify():
    assert modular_delta(65534, 2) == 4
    assert classify_advance(10, 10, 65536)[0] == DUPLICATE
    assert classify_advance(100, 5, 65536)[0] == ROLLED_BACK
    kind, delta = classify_advance(65534, 2, 65536)
    assert kind == "advanced" and delta == 4


def test_calibration_removes_phase_offset():
    calib_a = SourceCalibration("A", zero=1000)
    calib_b = SourceCalibration("B", zero=5000)
    assert normalize_sequence(1000, calib_a) == 0
    assert normalize_sequence(5000, calib_b) == 0
    assert normalize_sequence(1001, calib_a) == 1
    assert normalize_sequence(5001, calib_b) == 1


def test_source_track_extends_across_wrap():
    track = SourceTrack("A")
    track.start(0)
    track.align_to(0, 0)
    # 视图走到周期末 65535。
    wraps, ext, base, delta = track.project(65535)
    assert wraps == 0 and ext == 65535 and base == 65535 and delta == 65535
    track.commit(65535, wraps, ext, base)
    # 65535 -> 0：跨过周期边界，展开值继续变到 65536，不跌落。
    wraps, ext, base, delta = track.project(0)
    assert wraps == 1 and ext == 65536 and base == 65536 and delta == 1


# ---- 策略 ---------------------------------------------------------------


def test_primary_policy_trusts_primary():
    assert decide({"A": "advanced", "B": "advanced"}, policy=PRIMARY, primary_source="A")["decision"] == ACCEPT
    missing = decide({"B": "advanced"}, policy=PRIMARY, primary_source="A")
    assert missing["decision"] == PENDING and missing["reason"] == "primary_missing"
    rejected = decide({"A": "replay", "B": "advanced"}, policy=PRIMARY, primary_source="A")
    assert rejected["decision"] == REJECT and rejected["reason"] == REPLAY
    # 主源前进、旁证矛盾：放行但留警告。
    warned = decide({"A": "advanced", "B": "replay"}, policy=PRIMARY, primary_source="A")
    assert warned["decision"] == ACCEPT and warned["warnings"]


def test_quorum_requires_majority_and_rejects_split():
    one = decide({"A": "advanced"}, policy=QUORUM, primary_source="A", expected_sources=2)
    assert one["decision"] == PENDING and one["reason"] == "quorum_pending"
    assert decide({"A": "advanced", "B": "advanced"}, policy=QUORUM, primary_source="A")["decision"] == ACCEPT
    split = decide({"A": "advanced", "B": "replay"}, policy=QUORUM, primary_source="A")
    assert split["decision"] == REJECT
    unmet = decide({"A": "out_of_order", "B": "out_of_order"}, policy=QUORUM, primary_source="A")
    assert unmet["decision"] == REJECT


# ---- 缓冲 ---------------------------------------------------------------


def test_reorder_buffer_completes_and_times_out(clock):
    from psdline.sequence.buffer import BufferedObservation

    buf = ReorderBuffer(wait_s=2.0, capacity=4)
    obs = BufferedObservation("A", 0, 0, 0, 1, "advanced", clock.now())
    ready = buf.add(0, obs, now=clock.now(), expected_sources=2)
    assert ready["ready"] is False
    clock.advance(3.0)
    assert buf.due(clock.now()) == [0]
    assert len(buf.flush_due(clock.now())) == 1


def test_reorder_buffer_is_bounded():
    from psdline.sequence.buffer import BufferedObservation

    buf = ReorderBuffer(wait_s=10.0, capacity=2)
    for value in range(3):
        obs = BufferedObservation("A", value, value, 0, 1, "advanced", 0.0)
        ready = buf.add(value, obs, now=0.0, expected_sources=2)
        if value == 2:
            assert ready["evicted"] is not None  # 挤出最旧槽位，不静丢
    assert len(buf) == 2


# ---- 监控：对齐、重复、重放、回绕 ---------------------------------------


def _monitor(tmp_path, clock, **kwargs):
    return SequenceMonitor(JsonStore(str(tmp_path)), clock, **kwargs)


def _pair(monitor, clock, a_raw, b_raw, step=0.1):
    clock.advance(step)
    monitor.report(Observation("A", a_raw, at=clock.now()))
    clock.advance(step)
    return monitor.report(Observation("B", b_raw, at=clock.now()))


def test_two_sources_with_phase_offset_align(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    result = _pair(mon, clock, 1000, 5000)
    assert result.accepted and result.normalized == 0
    result = _pair(mon, clock, 1001, 5001)
    assert result.accepted and result.normalized == 1
    assert mon.watermark() == 1


def test_duplicate_report_is_idempotent(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    _pair(mon, clock, 1000, 5000)
    _pair(mon, clock, 1001, 5001)
    dup = mon.report(Observation("B", 5001, at=clock.now()))
    assert dup.accepted and dup.reason == DUPLICATE
    assert mon.duplicate_count == 1
    assert mon.watermark() == 1  # 重复不推进高水位


def test_replay_is_rejected_immediately(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    _pair(mon, clock, 1000, 5000)
    _pair(mon, clock, 1001, 5001)
    replay = mon.report(Observation("B", 5000, at=clock.now()))
    assert replay.accepted is False and replay.reason == REPLAY
    assert mon.gate_clear() is False


def test_wrap_across_period_advances(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    _pair(mon, clock, 65535, 300)
    assert _pair(mon, clock, 0, 301).normalized == 1
    assert _pair(mon, clock, 1, 302).normalized == 2
    assert mon.watermark() == 2


def test_rolled_back_large_jump_rejected(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    # 两路配对基准 0（A=100, B=100）。
    _pair(mon, clock, 100, 100)
    # A 前进到 101 并由 B 配对提交，高水位推进到 1。
    assert _pair(mon, clock, 101, 101).normalized == 1
    # A 突然抛出 65000：相对已提交的 101，模增量落在后半圈，是回绕（非预期大倒退）。
    back = mon.report(Observation("A", 65000, at=clock.now()))
    assert back is not None and back.reason == ROLLED_BACK and back.accepted is False


# ---- 乱序、超时、部分失败 -----------------------------------------------


def test_out_of_order_arrival_reorders(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    clock.advance(1)
    mon.report(Observation("A", 100, at=clock.now()))
    mon.report(Observation("B", 500, at=clock.now()))
    clock.advance(0.1)
    # B 的下一条先到，先缓冲。
    assert mon.report(Observation("B", 501, at=clock.now())) is None
    assert mon.pending() == 1
    clock.advance(0.1)
    result = mon.report(Observation("A", 101, at=clock.now()))
    assert result.accepted and result.normalized == 1


def test_incomplete_times_out_to_safe_side_both_policies(tmp_path, clock):
    for policy in (PRIMARY, QUORUM):
        clk = ManualClock()
        mon = SequenceMonitor(
            JsonStore(str(tmp_path / policy)), clk, policy=policy, order_wait_s=2.0
        )
        clk.advance(1)
        mon.report(Observation("A", 100, at=clk.now()))
        mon.report(Observation("B", 500, at=clk.now()))
        clk.advance(0.1)
        mon.report(Observation("B", 501, at=clk.now()))  # A 停摆
        assert mon.pump(now=clk.now()) == []
        clk.advance(2.0)
        results = mon.pump(now=clk.now())
        assert len(results) == 1
        assert results[0].accepted is False
        assert results[0].reason in ("incomplete_timeout", "incomplete_overflow")
        assert mon.gate_clear() is False


def test_anchor_timeout_when_second_source_never_reports(tmp_path, clock):
    mon = SequenceMonitor(JsonStore(str(tmp_path)), clock, order_wait_s=2.0)
    assert mon.report(Observation("A", 100, at=clock.now())) is None
    clock.advance(3.0)
    results = mon.pump(now=clock.now())
    assert len(results) == 1 and results[0].reason == "anchor_timeout"


def test_unknown_source_rejected(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    result = mon.report(Observation("Z", 1, at=clock.now()))
    assert result.accepted is False and result.reason == "unknown_source"


# ---- 并发 ---------------------------------------------------------------


def test_concurrent_distinct_keys_do_not_interfere(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    errors: list[Exception] = []

    def drive(key):
        try:
            for value in range(200):
                mon.report(Observation("A", value, key=key, at=clock.now()))
                mon.report(Observation("B", value, key=key, at=clock.now()))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=drive, args=(f"L{i}",)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    for i in range(6):
        assert mon.watermark(f"L{i}") == 199
        accepted = [
            r.normalized
            for r in mon.history(f"L{i}", limit=10**9)
            if r.reason != DUPLICATE
        ]
        assert accepted == list(range(200))


def test_concurrent_same_key_duplicate_streams_stay_monotonic(tmp_path, clock):
    mon = _monitor(tmp_path, clock, order_wait_s=10.0)
    _pair(mon, clock, 0, 0)
    # 多线程重复提交同一条对齐流：重复由幂等去重，另一路配对已先提交则走缓冲配对。
    def drive():
        for value in range(1, 101):
            mon.report(Observation("A", value, at=clock.now()))
            mon.report(Observation("B", value, at=clock.now()))

    threads = [threading.Thread(target=drive) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert mon.watermark() == 100
    accepted = sorted({
        r.normalized for r in mon.history(limit=10**9) if r.reason != DUPLICATE
    })
    assert accepted == list(range(101))


def test_buffer_overflow_evicts_oldest_and_does_not_release_incomplete(tmp_path, clock):
    mon = SequenceMonitor(
        JsonStore(str(tmp_path)), clock, order_wait_s=1000.0, buffer_capacity=4
    )
    _pair(mon, clock, 0, 0)
    forced = []
    for value in range(1, 8):
        clock.advance(0.1)
        result = mon.report(Observation("A", value, at=clock.now()))
        if result is not None:
            forced.append(result)
    # 旁证一直缺失，缓冲灌满后挤出的最旧槽位必须拒绝而不是放行。
    assert forced and all(not r.accepted and r.reason == "overflow_incomplete" for r in forced)
    assert mon.pending() <= 4
    assert mon.gate_clear() is False


# ---- 不可变历史与恢复 ---------------------------------------------------


def test_history_is_immutable_and_recovers(tmp_path, clock):
    store = JsonStore(str(tmp_path))
    mon = SequenceMonitor(store, clock)
    _pair(mon, clock, 65535, 300)
    _pair(mon, clock, 0, 301)
    replay = mon.report(Observation("B", 300, at=clock.now()))
    assert replay.accepted is False
    ledger_before = mon._ledger.read_all()
    accepted_before = mon.accepted_count
    rejected_before = mon.rejected_count

    # 重建：内存状态从 checkpoint + 不可变 ledger 还原。
    restored = SequenceMonitor(JsonStore(str(tmp_path)), clock)
    assert restored.watermark() == mon.watermark()
    assert restored.accepted_count == accepted_before
    assert restored.rejected_count == rejected_before
    assert restored.latest().event_id == mon.latest().event_id
    # ledger 只追加：恢复后条目与之前逐条一致，历史未被改写。
    assert restored._ledger.read_all() == ledger_before
    assert [r.event_id for r in restored.history(limit=10**9)] == [
        r.event_id for r in mon.history(limit=10**9)
    ]
    # 恢复后能继续正确推进（wraps 不丢）。
    clock.advance(0.1)
    assert restored.report(Observation("A", 1, at=clock.now())) is None
    clock.advance(0.1)
    continued = restored.report(Observation("B", 302, at=clock.now()))
    assert continued.accepted and continued.normalized == 2


def test_keys_are_isolated(tmp_path, clock):
    mon = _monitor(tmp_path, clock)
    mon.report(Observation("A", 10, key="L1", at=clock.now()))
    mon.report(Observation("B", 90, key="L1", at=clock.now()))
    mon.report(Observation("A", 10, key="L2", at=clock.now()))
    mon.report(Observation("B", 90, key="L2", at=clock.now()))
    assert mon.watermark("L1") == 0 and mon.watermark("L2") == 0
    assert mon.latest("L1").key == "L1" and mon.latest("L2").key == "L2"

"""多源序号对齐：归并、回绕、重放、乱序缓冲、降级、恢复与并发。"""

from __future__ import annotations

import threading

import pytest

from psdline.clock import ManualClock
from psdline.console.api import Api
from psdline.errors import RailError
from psdline.interlock import SequenceAligner
from psdline.store import JsonlJournal, JsonStore


def _aligner(tmp_path, clock=None, **overrides):
    store = JsonStore(str(tmp_path))
    journal = JsonlJournal(store.root, "sequence", segment_limit=8)
    params = {
        "sources": ("alpha", "beta"),
        "primary": "alpha",
        "commit_policy": "primary",
        "hold_s": 0.5,
        "pair_window_s": 1.0,
        "source_timeout_s": 5.0,
        "max_forward_step": 8,
        "offset_confirm": 2,
    }
    params.update(overrides)
    return SequenceAligner(store, clock or ManualClock(), journal, **params)


def _pair(aligner, alpha_start=100, beta_start=2100, at=1.0, step=2.0, rounds=2):
    """让两源上报同一事件流并按轮静默，把偏移锁定（beta 比 alpha 晚 2000）。"""

    moment = at
    for round_index in range(rounds):
        aligner.report("alpha", alpha_start + round_index, moment)
        aligner.report("beta", beta_start + round_index, moment)
        moment += step
        aligner.flush(moment)
    return moment


def test_two_sources_align_to_one_baseline(tmp_path):
    aligner = _aligner(tmp_path)
    first = aligner.report("alpha", 100, 1.0)["verdict"]
    assert first.committed is True and first.tick == 100
    pending = aligner.report("beta", 2100, 1.0)["verdict"]
    assert pending.kind == "pending"  # 偏移未定，先入配对缓冲
    aligner.report("alpha", 101, 1.0)
    aligner.report("beta", 2101, 1.0)
    aligner.flush(3.0)  # 双方静默：第一轮配对观测
    aligner.report("alpha", 102, 3.0)
    aligner.report("beta", 2102, 3.0)
    aligner.flush(5.0)  # 第二轮：偏移锁定
    status = aligner.status(5.0)
    assert status["offsets"] == {"alpha": 0, "beta": -2000}
    assert status["horizon"] == 102
    # 副源先到的乱序：先缓冲，主源到达后按同一刻度提交
    assert aligner.report("beta", 2103, 5.0)["verdict"].kind == "pending"
    committed = aligner.report("alpha", 103, 5.0)["verdict"]
    assert committed.committed is True and committed.kind == "advanced"
    assert committed.sources == {"alpha": 103, "beta": 2103}
    # 同一事件的副源副本按重复归并
    replay = aligner.report("beta", 2103, 5.0)["verdict"]
    assert replay.kind == "replay"
    assert aligner.status(5.0)["horizon"] == 103


def test_wraparound_commits_as_forward(tmp_path):
    aligner = _aligner(tmp_path)
    aligner.report("alpha", 65534, 1.0)
    verdict = aligner.report("alpha", 2, 1.0)["verdict"]
    assert verdict.kind == "wraparound" and verdict.accepted is True
    assert verdict.tick == 65538 and verdict.cycle == 1
    status = aligner.status(1.0)
    assert status["horizon"] == 65538 and status["cycle"] == 1


def test_duplicate_report_is_replay(tmp_path):
    aligner = _aligner(tmp_path)
    aligner.report("alpha", 10, 1.0)
    verdict = aligner.report("alpha", 10, 1.0)["verdict"]
    assert verdict.kind == "replay" and verdict.committed is False
    status = aligner.status(1.0)
    assert status["horizon"] == 10
    assert status["sources"]["alpha"]["duplicates"] == 1


def test_gap_suspect_waits_then_commits_gap(tmp_path):
    aligner = _aligner(tmp_path, max_forward_step=4)
    aligner.report("alpha", 100, 1.0)
    pending = aligner.report("alpha", 110, 1.0)["verdict"]  # 跳变太大，先缓一缓
    assert pending.kind == "pending"
    assert aligner.status(1.0)["horizon"] == 100  # 水位没动
    assert aligner.flush(1.4)["committed"] == []  # 窗口没过，继续等
    flushed = aligner.flush(1.6)
    assert [item.kind for item in flushed["committed"]] == ["gap"]
    gap = flushed["committed"][0]
    assert gap.accepted is False and gap.skipped == 9 and gap.tick == 110
    assert aligner.status(1.6)["horizon"] == 110
    # 缺口落锤后恢复正常前进
    nxt = aligner.report("alpha", 111, 1.6)["verdict"]
    assert nxt.kind == "advanced" and nxt.accepted is True


def test_straggler_after_gap_is_late(tmp_path):
    aligner = _aligner(tmp_path, max_forward_step=4)
    _pair(aligner)  # 水位 101，偏移锁定
    aligner.report("alpha", 110, 5.0)  # 缺口嫌疑
    aligner.flush(5.6)  # 缺口落锤，水位 110
    late = aligner.report("beta", 2105, 5.6)["verdict"]  # 乱序迟到的补报
    assert late.kind == "late" and late.committed is False
    assert aligner.status(5.6)["horizon"] == 110


def test_quorum_requires_both_sources(tmp_path):
    aligner = _aligner(tmp_path, commit_policy="quorum")
    solo = aligner.report("alpha", 100, 1.0)["verdict"]
    assert solo.kind == "pending"  # 单源不算前进
    aligner.report("beta", 2100, 1.0)
    aligner.report("alpha", 101, 1.0)
    aligner.report("beta", 2101, 1.0)
    aligner.flush(3.0)  # 第一轮配对观测
    aligner.report("alpha", 102, 3.0)
    aligner.report("beta", 2102, 3.0)
    aligner.flush(5.0)  # 第二轮：偏移锁定，双源一致提交
    status = aligner.status(5.0)
    assert status["horizon"] == 102
    assert status["last_verdict"]["degraded"] is False
    # 只有一路佐证时，配对窗口过后降级提交，门控默认不采信
    pending = aligner.report("alpha", 103, 5.0)["verdict"]
    assert pending.kind == "pending"
    flushed = aligner.flush(6.5)
    assert [item.kind for item in flushed["committed"]] == ["degraded"]
    degraded = flushed["committed"][0]
    assert degraded.accepted is False and degraded.degraded is True
    assert aligner.status(6.5)["horizon"] == 103


def test_source_reset_reanchors_without_rewriting_history(tmp_path):
    aligner = _aligner(tmp_path)
    _pair(aligner)  # 水位 101，beta 偏移 -2000
    aligner.report("alpha", 102, 5.0)
    reset = aligner.report("alpha", 5, 6.0)["verdict"]  # 主源重启，序号倒退
    assert reset.kind == "reset"
    status = aligner.status(6.0)
    assert status["horizon"] == 102  # 历史不变
    assert status["offsets"]["alpha"] is None
    # 重启后两源继续报同一事件流，静默观测把主源重新锚定
    aligner.report("alpha", 6, 6.0)
    aligner.report("beta", 2103, 6.0)
    aligner.report("alpha", 7, 6.0)
    aligner.report("beta", 2104, 6.0)
    aligner.flush(8.0)
    aligner.report("alpha", 8, 8.0)
    aligner.report("beta", 2105, 8.0)
    aligner.flush(10.0)
    status = aligner.status(10.0)
    assert status["offsets"]["alpha"] == 97
    assert status["horizon"] == 105
    assert status["sources"]["alpha"]["resets"] == 1
    nxt = aligner.report("alpha", 9, 10.0)["verdict"]
    assert nxt.committed is True and nxt.tick == 106


def test_primary_silence_fails_over_and_back(tmp_path):
    aligner = _aligner(tmp_path, source_timeout_s=5.0)
    _pair(aligner)  # 水位 101，两源最后上报在 3.0
    verdict = aligner.report("beta", 2102, 11.0)["verdict"]  # 主源静默超时，副源接管
    assert verdict.committed is True and verdict.degraded is True
    assert aligner.status(11.0)["reference"] == "beta"
    aligner.report("alpha", 102, 11.0)  # 水位 102：归并为重放
    caught = aligner.report("alpha", 103, 11.0)["verdict"]  # 追上水位，交还参考权
    assert caught.committed is True and caught.kind == "advanced"
    assert aligner.status(11.0)["reference"] == "alpha"


def test_rebase_anchors_stream_back_to_horizon(tmp_path):
    aligner = _aligner(tmp_path)
    _pair(aligner)  # 水位 101
    aligner.report("alpha", 102, 5.0)  # 水位 102
    aligner.report("alpha", 10, 6.0)  # 双源同时重启
    aligner.report("beta", 20, 6.0)
    aligner.report("alpha", 11, 6.0)
    aligner.report("beta", 21, 6.0)
    aligner.flush(8.0)  # 无锚点：配对不会锁定
    assert aligner.status(8.0)["offsets"]["alpha"] is None
    aligner.rebase(9.0)  # 人工锚定：当前流位置 = 已提交水位
    assert aligner.status(9.0)["offsets"]["alpha"] == 102 - 11
    nxt = aligner.report("alpha", 12, 9.0)["verdict"]
    assert nxt.committed is True and nxt.tick == 103


def test_restart_recovers_horizon_and_pending(tmp_path):
    aligner = _aligner(tmp_path, max_forward_step=4)
    aligner.report("alpha", 100, 1.0)
    aligner.report("alpha", 101, 1.0)
    aligner.report("alpha", 110, 1.0)  # 缺口嫌疑，还在缓冲
    restored = _aligner(tmp_path, max_forward_step=4)  # 模拟进程重启
    status = restored.status(1.0)
    assert status["horizon"] == 101 and status["pending"] == 1
    flushed = restored.flush(1.6)  # 缓冲期限是绝对的，重启后照常到期
    assert [item.kind for item in flushed["committed"]] == ["gap"]
    assert restored.status(1.6)["horizon"] == 110
    dup = restored.report("alpha", 110, 1.6)["verdict"]  # 重启前的重复仍判重放
    assert dup.kind == "replay"


def test_journal_rolls_forward_past_stale_head(tmp_path):
    aligner = _aligner(tmp_path)
    aligner.report("alpha", 100, 1.0)
    aligner.report("alpha", 101, 1.0)
    JsonStore(str(tmp_path)).remove("sequence/head")  # 头文档丢失：从流水前滚
    restored = _aligner(tmp_path)
    assert restored.status(1.0)["horizon"] == 101
    assert restored.report("alpha", 102, 1.0)["verdict"].committed is True


def test_commit_history_is_append_only(tmp_path):
    aligner = _aligner(tmp_path)
    for step in range(5):
        aligner.report("alpha", 100 + step, 1.0)
    journal = JsonlJournal(str(tmp_path), "sequence", segment_limit=8)
    ticks = [record["tick"] for record in journal.read_all()]
    assert ticks == [100, 101, 102, 103, 104]  # 只追加、按刻度有序
    aligner.report("alpha", 103, 1.0)  # 重放不改写历史
    assert [record["tick"] for record in journal.read_all()] == ticks


def test_concurrent_reports_from_both_sources(tmp_path):
    aligner = _aligner(tmp_path)
    _pair(aligner)  # 水位 101，偏移锁定

    def feed(source, start):
        for step in range(50):
            aligner.report(source, start + step, 10.0)

    threads = [
        threading.Thread(target=feed, args=("alpha", 102)),
        threading.Thread(target=feed, args=("beta", 2102)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    status = aligner.status(10.0)
    assert status["horizon"] == 151
    assert status["pending"] == 0
    assert status["counters"]["advanced"] == 52  # 配对 2 笔 + 主源 50 笔
    assert status["sources"]["beta"]["reports"] == 52
    # 提交流水只追加、按刻度有序、并发下不重不漏
    journal = JsonlJournal(str(tmp_path), "sequence", segment_limit=8)
    assert [record["tick"] for record in journal.read_all()] == list(range(100, 152))
    # 副源副本按到达先后分别归并为重放、补票佐证或并入提交，三者都可见
    assert status["counters"]["replay"] + status["counters"].get("corroborated", 0) >= 2


def test_unknown_source_is_rejected(tmp_path):
    aligner = _aligner(tmp_path)
    with pytest.raises(RailError):
        aligner.report("gamma", 1, 1.0)


def test_platform_report_audit_gate_share_one_verdict(platform):
    outcome = platform.sequence_report("alpha", 100)
    assert outcome["ok"] is True and outcome["verdict"]["committed"] is True
    assert outcome["gate"]["allow"] is True and outcome["gate"]["tick"] == 100
    assert platform.sequence_gate()["tick"] == 100
    entries = [entry for entry in platform.audit.list() if entry.kind == "sequence_verdict"]
    assert len(entries) == 1
    assert entries[0].detail["verdict"] == outcome["verdict"]  # 审计与判定同一份
    status = platform.sequence_status()
    assert status["horizon"] == 100 and status["gate"]["tick"] == 100


def test_platform_cycle_flushes_pending(platform, clock):
    platform.sequence_report("alpha", 100)
    pending = platform.sequence_report("alpha", 200)  # 跳变超过可信上限，先缓冲
    assert pending["verdict"]["kind"] == "pending"
    clock.advance(1.0)
    platform.cycle(dt=1.0)  # 控制循环驱动缓冲判定
    assert platform.sequence_status()["horizon"] == 200
    gaps = [
        entry
        for entry in platform.audit.list()
        if entry.kind == "sequence_verdict" and entry.detail["verdict"]["kind"] == "gap"
    ]
    assert len(gaps) == 1 and gaps[0].detail["verdict"]["accepted"] is False


def test_console_sequence_endpoints(platform):
    api = Api(platform)
    status, payload = api.handle("POST", "/api/sequence", {}, {"source": "alpha", "sequence": 100})
    assert status == 200 and payload["ok"] is True
    status, payload = api.handle("GET", "/api/sequence", {}, {})
    assert status == 200 and payload["horizon"] == 100
    status, payload = api.handle("POST", "/api/sequence", {}, {"source": "gamma", "sequence": 1})
    assert status == 409

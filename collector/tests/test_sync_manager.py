"""同步管理器关键行为：缺号触发重同步、重复恰好一次、同序号内容冲突、迟到快照不覆盖新盘口。"""
import asyncio

import pytest

from app.config import Settings
from app.orderbook import ConflictError, DedupWindow, GapError, StaleSnapshotError
from app.sync_manager import SyncManager

SYMBOL = "CU2601"


def make_manager(**overrides) -> SyncManager:
    kwargs = dict(symbol=SYMBOL, source_http="http://x", source_ws="ws://x")
    kwargs.update(overrides)
    return SyncManager(Settings(**kwargs))


def install(mgr: SyncManager, gen: int = 1, buffered: list | None = None):
    mgr.generation = gen
    snapshot = {"type": "snapshot", "symbol": SYMBOL, "seq": 100,
                "bids": [[69990, 5]], "asks": [[70010, 7]]}
    asyncio.run(mgr._install_snapshot(snapshot, buffered or [], gen))


def inc(seq, *changes):
    return {"type": "increment", "symbol": SYMBOL, "seq": seq,
            "changes": [dict(zip(("side", "price", "qty"), c)) for c in changes]}


def test_apply_increment_continuous_and_duplicate():
    mgr = make_manager()
    install(mgr)
    assert mgr.synced and mgr.book.seq == 100

    asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 3))))
    assert mgr.book.seq == 101

    # 重复投递（同序号同内容）：跳过，不重复应用
    asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 3))))
    assert mgr.book.seq == 101 and mgr.stats["duplicates_skipped"] == 1
    assert mgr.stats["conflicts"] == 0


def test_gap_raises_and_counts():
    mgr = make_manager()
    install(mgr)
    with pytest.raises(GapError):
        asyncio.run(mgr._apply_increment(inc(105, ("bid", 1, 1))))
    assert mgr.stats["gaps"] == 1


# ---------- 同序号内容冲突 ----------

def test_conflict_same_seq_different_content():
    """已见序号被重发成不同内容：抛冲突、计数、标出序号，冲突帧不落簿。"""
    mgr = make_manager()
    install(mgr)
    asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 3))))

    with pytest.raises(ConflictError) as exc_info:
        asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 9))))
    assert exc_info.value.seq == 101
    assert mgr.stats["conflicts"] == 1
    assert list(mgr.conflict_seqs) == [101]
    assert mgr.book.bids[69980] == 3            # 冲突帧未落簿，仍是原量
    assert mgr.status_view()["conflict_seqs"] == [101]  # /status 标出冲突序号


def test_snapshot_covered_unseen_seq_not_conflict():
    """快照已覆盖、本连接未见过原帧的序号：按重复丢弃，不得据此报冲突。"""
    mgr = make_manager()
    install(mgr)  # 快照 seq=100，窗口为空（未实收任何帧）
    asyncio.run(mgr._apply_increment(inc(100, ("bid", 69990, 999))))
    assert mgr.stats["conflicts"] == 0
    assert mgr.stats["duplicates_skipped"] == 1
    assert mgr.book.bids == {69990: 5}          # 旧量不变


def test_window_shared_between_buffer_and_realtime():
    """在途缓冲应用的帧进入同一窗口：同步后同序号不同内容的实时帧即冲突。"""
    mgr = make_manager()
    install(mgr, buffered=[inc(101, ("bid", 69980, 3))])
    assert mgr.book.seq == 101

    with pytest.raises(ConflictError):
        asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 8))))
    assert mgr.stats["conflicts"] == 1
    # 同内容重发仍是正常重复
    mgr2 = make_manager()
    install(mgr2, buffered=[inc(101, ("bid", 69980, 3))])
    asyncio.run(mgr2._apply_increment(inc(101, ("bid", 69980, 3))))
    assert mgr2.stats["duplicates_skipped"] == 1 and mgr2.stats["conflicts"] == 0


def test_conflict_in_buffered_increments():
    """快照在途缓冲内出现同序号不同内容：安装失败、保持失步、标出冲突序号。"""
    mgr = make_manager()
    mgr.generation = 1
    snapshot = {"type": "snapshot", "symbol": SYMBOL, "seq": 100,
                "bids": [[69990, 5]], "asks": []}
    buffered = [inc(101, ("bid", 69980, 3)), inc(101, ("bid", 69980, 9))]
    with pytest.raises(ConflictError) as exc_info:
        asyncio.run(mgr._install_snapshot(snapshot, buffered, 1))
    assert exc_info.value.seq == 101
    assert mgr.stats["conflicts"] == 1
    assert list(mgr.conflict_seqs) == [101]
    assert mgr.synced is False                  # 未安装，保持失步（/book 503）


def test_window_capacity_limits_conflict_detection():
    """窗口只保留最近 N 条：被淘汰的序号不同内容按重复丢弃，窗口内仍报冲突。"""
    mgr = make_manager(dedup_window=2)
    install(mgr)
    for seq, qty in ((101, 3), (102, 4), (103, 5)):  # 容量 2，seq 101 被淘汰
        asyncio.run(mgr._apply_increment(inc(seq, ("bid", 69980, qty))))

    asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 9))))  # 已淘汰：无法核对
    assert mgr.stats["conflicts"] == 0
    assert mgr.stats["duplicates_skipped"] == 1

    with pytest.raises(ConflictError):          # 103 仍在窗口内：冲突
        asyncio.run(mgr._apply_increment(inc(103, ("bid", 69980, 9))))
    assert mgr.stats["conflicts"] == 1


def test_new_cycle_resets_window():
    """冲突后重连取新快照：旧连接的帧不影响新轮次（窗口随轮次重建）。"""
    mgr = make_manager()
    install(mgr, gen=1)
    asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 3))))
    with pytest.raises(ConflictError):
        asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 9))))
    assert mgr.stats["conflicts"] == 1

    mgr._window = DedupWindow(mgr.settings.dedup_window)  # _run 每轮恢复时重建
    install(mgr, gen=2)  # 新快照 seq=100
    asyncio.run(mgr._apply_increment(inc(101, ("bid", 69980, 9))))  # 旧帧不再记得
    assert mgr.stats["conflicts"] == 1          # 不新增冲突
    assert mgr.book.bids[69980] == 9            # 新轮次正常应用


def test_stale_snapshot_rejected_at_install():
    """重连前发起的快照（旧世代）不得恢复可用状态或覆盖新盘口。"""
    mgr = make_manager()
    install(mgr, gen=2)  # 当前世代 2，盘口已同步

    stale = {"type": "snapshot", "symbol": SYMBOL, "seq": 50,
             "bids": [[1, 1]], "asks": []}
    with pytest.raises(StaleSnapshotError):
        asyncio.run(mgr._install_snapshot(stale, [], gen=1))  # 旧世代的迟到快照

    assert mgr.synced is True                 # 可用状态未被改变
    assert mgr.book.seq == 100                # 新盘口未被覆盖
    assert mgr.book.bids == {69990: 5}
    assert mgr.stats["stale_snapshots"] == 1


def test_stale_snapshot_discarded_at_fetch():
    """HTTP 响应返回时世代已更替 -> 返回 None，不落簿。"""
    mgr = make_manager()

    class FakeResp:
        def raise_for_status(self): pass
        def json(self):
            return {"type": "snapshot", "symbol": SYMBOL, "seq": 1, "bids": [], "asks": []}

    class FakeHttp:
        async def get(self, *a, **k):
            mgr.generation = 9  # 模拟请求在途期间发生了重连
            return FakeResp()

    mgr._http = FakeHttp()
    mgr.generation = 1
    result = asyncio.run(mgr._fetch_snapshot(gen=1))
    assert result is None
    assert mgr.stats["stale_snapshots"] == 1
    assert mgr.synced is False

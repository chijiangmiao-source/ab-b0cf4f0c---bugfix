"""因果一致性引擎单元测试。"""
import itertools

import pytest

from app.engine import (
    Store,
    RELEASED, WAITING,
    REJECTED_PRECONDITION, REJECTED_ID_CONFLICT, REJECTED_SEQ_GAP,
    REJECTED_SEQ_CONFLICT, REJECTED_UNKNOWN_CONSOLE, REJECTED_UNKNOWN_VALVE,
    REJECTED_FUTURE_DEPENDENCY,
)


def make_store(consoles=("A", "B", "C")):
    s = Store()
    view = s.create_round(
        "测试轮次", list(consoles),
        [{"name": "V1", "state": "CLOSED"}, {"name": "V2", "state": "CLOSED"}],
    )
    return s, view["round_id"]


def ev(eid, cid, seq, deps=None, valve="v1", old="CLOSED", new="OPEN"):
    return {
        "event_id": eid, "console_id": cid, "seq": seq, "deps": deps or {},
        "valve_id": valve, "expected_old_state": old, "new_state": new,
    }


def outcomes(res):
    return {r["event_id"]: r["outcome"] for r in res["results"]}


def test_create_round_validation():
    s = Store()
    with pytest.raises(ValueError):
        s.create_round("x", ["仅一个"], [{"name": "V1"}])          # 控制台少于两个
    with pytest.raises(ValueError):
        s.create_round("x", ["a", "b", "c", "d", "e", "f"], [{"name": "V1"}])  # 超过五个
    with pytest.raises(ValueError):
        s.create_round("x", ["a", "b"], [])                        # 没有阀门
    view = s.create_round("ok", ["a", "b"], [{"name": "V1"}])
    assert view["frontier"] == {"c1": 0, "c2": 0}


def test_waiting_then_cascade_release():
    """先提交依赖另一控制台事件而显示等待，补齐前驱后连续放行。"""
    s, rid = make_store()
    # B 的 b1 依赖 A#1（尚未提交）→ 等待
    r = s.submit_batch(rid, [ev("evt-b-0001", "c2", 1, {"c1": 1}, valve="v2")])
    assert outcomes(r) == {"evt-b-0001": WAITING}
    assert "c1#1" in r["results"][0]["reason"]
    # C 的 c1 依赖 B#1（链式）→ 等待
    r = s.submit_batch(rid, [ev("evt-c-0001", "c3", 1, {"c2": 1}, valve="v2",
                                old="OPEN", new="CLOSED")])
    assert outcomes(r) == {"evt-c-0001": WAITING}
    # A 补齐前驱 → 三个事件连续放行
    r = s.submit_batch(rid, [ev("evt-a-0001", "c1", 1, {}, valve="v1")])
    assert [e["event_id"] for e in r["resolved"]] == ["evt-a-0001", "evt-b-0001", "evt-c-0001"]
    assert all(e["outcome"] == RELEASED for e in r["resolved"])
    st = r["state"]
    assert st["frontier"] == {"c1": 1, "c2": 1, "c3": 1}
    assert st["waiting"] == []
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "OPEN", "v2": "CLOSED"}  # v2 经 OPEN 又被 c1 关回


def test_precondition_rejection_advances_frontier():
    """预条件不符须拒绝并推进因果位置，后继不被阻塞。"""
    s, rid = make_store()
    r = s.submit_batch(rid, [ev("evt-a1", "c1", 1, {}, old="OPEN")])  # 实际为 CLOSED
    assert outcomes(r) == {"evt-a1": REJECTED_PRECONDITION}
    assert r["state"]["frontier"]["c1"] == 1                          # 前沿已推进
    # 后继事件正常放行，未被拒绝结果阻塞
    r = s.submit_batch(rid, [ev("evt-a2", "c1", 2, {}, old="CLOSED")])
    assert outcomes(r) == {"evt-a2": RELEASED}
    valves = {v["id"]: v["state"] for v in r["state"]["valves"]}
    assert valves["v1"] == "OPEN"


@pytest.mark.parametrize("first", ["evt-a-1000", "evt-b-1000"])
def test_contention_smaller_id_wins_regardless_of_arrival(first):
    """并发争用同一旧状态：较小事件标识成功，与到达顺序无关。"""
    s, rid = make_store()
    pair = [
        ev("evt-a-1000", "c1", 1, {"c3": 1}),   # 依赖 C#1 → 等待
        ev("evt-b-1000", "c2", 1, {"c3": 1}),   # 依赖 C#1 → 等待
    ]
    # 按参数指定的到达顺序提交（先大后小 / 先小后大各跑一遍）
    pair.sort(key=lambda p: p["event_id"] != first)
    for p in pair:
        r = s.submit_batch(rid, [p])
        assert outcomes(r)[p["event_id"]] == WAITING
    # C 的前驱到达 → 同批放行，按事件标识裁决
    r = s.submit_batch(rid, [ev("evt-c-1000", "c3", 1, {}, valve="v2")])
    got = {e["event_id"]: e["outcome"] for e in r["resolved"]}
    assert got["evt-a-1000"] == RELEASED                 # 较小标识成功
    assert got["evt-b-1000"] == REJECTED_PRECONDITION    # 另一项稳定预条件拒绝
    # 竞争失败方的后继不被拒绝结果阻塞
    r = s.submit_batch(rid, [ev("evt-b-1001", "c2", 2, {}, old="OPEN", new="CLOSED")])
    assert outcomes(r) == {"evt-b-1001": RELEASED}


def test_batch_arbitration_by_event_id():
    """同批可放行项按事件标识稳定裁决。"""
    s, rid = make_store()
    r = s.submit_batch(rid, [
        ev("evt-z-002", "c2", 1, {}),   # 先到达但标识较大
        ev("evt-a-002", "c1", 1, {}),   # 后到达但标识较小
    ])
    got = outcomes(r)
    assert got["evt-a-002"] == RELEASED
    assert got["evt-z-002"] == REJECTED_PRECONDITION


def test_idempotent_replay_returns_stored_conclusion():
    """同一事件重投返回既有结论，且不重复施加状态变更。"""
    s, rid = make_store()
    r = s.submit_batch(rid, [ev("evt-a1", "c1", 1, {})])
    assert outcomes(r) == {"evt-a1": RELEASED}
    # 另一事件把阀门关回 CLOSED
    s.submit_batch(rid, [ev("evt-b1", "c2", 1, {}, old="OPEN", new="CLOSED")])
    # 重投 evt-a1：返回既有 RELEASED 结论，阀门保持 CLOSED（不重复施加）
    r = s.submit_batch(rid, [ev("evt-a1", "c1", 1, {})])
    assert r["results"][0]["outcome"] == RELEASED
    assert r["results"][0]["duplicate"] is True
    valves = {v["id"]: v["state"] for v in r["state"]["valves"]}
    assert valves["v1"] == "CLOSED"


def test_id_reuse_with_different_payload_rejected():
    s, rid = make_store()
    s.submit_batch(rid, [ev("evt-x", "c1", 1, {})])
    r = s.submit_batch(rid, [ev("evt-x", "c1", 1, {}, new="CLOSED")])  # 载荷变化
    assert outcomes(r) == {"evt-x": REJECTED_ID_CONFLICT}
    valves = {v["id"]: v["state"] for v in r["state"]["valves"]}
    assert valves["v1"] == "OPEN"  # 状态未被第二次提交改变


def test_seq_gap_rejected_then_fillable():
    s, rid = make_store()
    r = s.submit_batch(rid, [ev("evt-a3", "c1", 3, {})])
    assert outcomes(r) == {"evt-a3": REJECTED_SEQ_GAP}
    assert r["state"]["frontier"]["c1"] == 0  # 跳号拒绝不推进前沿
    s.submit_batch(rid, [ev("evt-a1", "c1", 1, {})])
    s.submit_batch(rid, [ev("evt-a2", "c1", 2, {}, valve="v2")])
    r = s.submit_batch(rid, [ev("evt-a3", "c1", 3, {}, valve="v2",
                                old="OPEN", new="CLOSED")])  # 补齐后可正常消费
    assert outcomes(r) == {"evt-a3": RELEASED}


def test_unknown_console_rejected():
    s, rid = make_store()
    r = s.submit_batch(rid, [ev("evt-x", "c9", 1, {})])
    assert outcomes(r) == {"evt-x": REJECTED_UNKNOWN_CONSOLE}
    r = s.submit_batch(rid, [ev("evt-y", "c1", 1, {"c9": 1})])
    assert outcomes(r) == {"evt-y": REJECTED_UNKNOWN_CONSOLE}
    assert r["state"]["frontier"] == {"c1": 0, "c2": 0, "c3": 0}


def test_future_dependency_rejected_but_next_in_line_waits():
    s, rid = make_store()
    # 依赖领先前沿一位 → 允许等待（前驱在途）
    r = s.submit_batch(rid, [ev("evt-w", "c1", 1, {"c2": 1})])
    assert outcomes(r) == {"evt-w": WAITING}
    # 依赖远超前沿 → 未来依赖，明确拒绝
    r = s.submit_batch(rid, [ev("evt-f", "c2", 1, {"c3": 5})])
    assert outcomes(r) == {"evt-f": REJECTED_FUTURE_DEPENDENCY}
    # 依赖自身未来序号 → 未来依赖
    r = s.submit_batch(rid, [ev("evt-s", "c2", 1, {"c2": 1})])
    assert outcomes(r) == {"evt-s": REJECTED_FUTURE_DEPENDENCY}
    # 以上拒绝均不改变阀门状态
    valves = {v["id"]: v["state"] for v in r["state"]["valves"]}
    assert valves == {"v1": "CLOSED", "v2": "CLOSED"}


def test_waiting_on_queued_chain_then_release():
    """依赖已在等待队列中的事件链，前驱补齐后逐级放行。"""
    s, rid = make_store()
    s.submit_batch(rid, [ev("evt-b1", "c2", 1, {"c1": 1}, valve="v2")])
    s.submit_batch(rid, [ev("evt-b2", "c2", 2, {}, valve="v2", old="OPEN", new="CLOSED")])
    # B#2 依赖 B#1（在队列中），A#1 到达后全部连续放行
    r = s.submit_batch(rid, [ev("evt-a1", "c1", 1, {})])
    assert [e["event_id"] for e in r["resolved"]] == ["evt-a1", "evt-b1", "evt-b2"]
    assert r["state"]["frontier"] == {"c1": 1, "c2": 2, "c3": 0}


# ----------------------------------------------------------------------
# 离线恢复：四事件因果链经批量接口一次恢复
# ----------------------------------------------------------------------

def offline_recovery_chain():
    """夜班离线恢复链：A 连续两项本地操作，B、C 依次跨控制台依赖。

    A#1 开 V1 → A#2 开 V2 → B#1（依赖 A#2）关 V2 → C#1（依赖 B#1）关 V1。
    """
    return [
        ev("evt-z-a1", "c1", 1, {}, valve="v1", old="CLOSED", new="OPEN"),
        ev("evt-a-a2", "c1", 2, {}, valve="v2", old="CLOSED", new="OPEN"),
        ev("evt-b-b1", "c2", 1, {"c1": 2}, valve="v2", old="OPEN", new="CLOSED"),
        ev("evt-c-c1", "c3", 1, {"c2": 1}, valve="v1", old="OPEN", new="CLOSED"),
    ]


CHAIN_LOG_ORDER = ["evt-z-a1", "evt-a-a2", "evt-b-b1", "evt-c-c1"]


def assert_chain_state(st):
    """前沿 2/1/1；无等待项；日志因果有序；两阀门最终均关闭。"""
    assert st["frontier"] == {"c1": 2, "c2": 1, "c3": 1}
    assert st["waiting"] == []
    assert [e["event_id"] for e in st["log"]] == CHAIN_LOG_ORDER
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "CLOSED", "v2": "CLOSED"}


def assert_chain_converged(res):
    """同批四项全部放行、按因果顺序消费，且最终状态收敛。"""
    got = outcomes(res)
    assert got == {eid: RELEASED for eid in CHAIN_LOG_ORDER}
    assert [e["event_id"] for e in res["resolved"]] == CHAIN_LOG_ORDER
    assert_chain_state(res["state"])


def test_offline_recovery_chain_in_recorded_order():
    """按 A 的本地记录顺序再接 B、C 提交：同批连续裁决，一次恢复完成。"""
    s, rid = make_store()
    res = s.submit_batch(rid, offline_recovery_chain())
    assert_chain_converged(res)


@pytest.mark.parametrize(
    "order", list(itertools.permutations(range(4))),
    ids=lambda o: "-".join(str(i) for i in o),
)
def test_offline_recovery_chain_any_request_order(order):
    """无论离线队列在请求中的排列如何，结论、日志与阀门状态均一致。"""
    s, rid = make_store()
    chain = offline_recovery_chain()
    res = s.submit_batch(rid, [chain[i] for i in order])
    # 响应仍与请求条目逐一对应
    assert [r["event_id"] for r in res["results"]] == [chain[i]["event_id"] for i in order]
    assert_chain_converged(res)


def test_offline_recovery_chain_split_batches_converge():
    """恢复链拆成两批（C 先等待、B 与 A#2 同批）也收敛到同一状态。"""
    s, rid = make_store()
    chain = offline_recovery_chain()
    r = s.submit_batch(rid, [chain[3], chain[0]])   # C 等待 B#1；A#1 放行
    assert outcomes(r) == {"evt-c-c1": WAITING, "evt-z-a1": RELEASED}
    res = s.submit_batch(rid, [chain[2], chain[1]])  # B#1 与其前驱 A#2 同批
    assert outcomes(res) == {"evt-b-b1": RELEASED, "evt-a-a2": RELEASED}
    assert [e["event_id"] for e in res["resolved"]] == [
        "evt-a-a2", "evt-b-b1", "evt-c-c1"]
    assert_chain_state(res["state"])


# ----------------------------------------------------------------------
# 批量边界回归：无效事件 / 重投 / 同阀门竞争 / 槽位冲突 / 真跳号与真未来依赖
# ----------------------------------------------------------------------

def test_batch_mixed_invalid_events_do_not_block_valid_ones():
    """批内无效事件各自明确拒绝，不拖累同批有效事件及其依赖者。"""
    s, rid = make_store()
    res = s.submit_batch(rid, [
        ev("evt-bad-console", "c9", 1, {}),                # 未知控制台
        ev("evt-bad-valve", "c1", 1, {}, valve="v9"),      # 未知阀门
        ev("evt-bad-seq", "c1", 0, {}),                    # 非法本地序号
        ev("evt-ok-1", "c1", 1, {}),                       # 有效
        ev("evt-ok-2", "c2", 1, {"c1": 1}, valve="v2"),    # 有效（依赖同批前驱）
    ])
    got = outcomes(res)
    assert got["evt-bad-console"] == REJECTED_UNKNOWN_CONSOLE
    assert got["evt-bad-valve"] == REJECTED_UNKNOWN_VALVE
    assert got["evt-bad-seq"] == REJECTED_SEQ_GAP
    assert got["evt-ok-1"] == RELEASED
    assert got["evt-ok-2"] == RELEASED
    st = res["state"]
    assert st["frontier"] == {"c1": 1, "c2": 1, "c3": 0}
    assert [w["event_id"] for w in st["waiting"]] == []


def test_batch_replay_and_id_conflict():
    """批内同标识重投返回既有结论且不重复施加；异参复用明确拒绝。"""
    s, rid = make_store()
    s.submit_batch(rid, [ev("evt-done", "c1", 1, {})])     # v1: CLOSED→OPEN
    res = s.submit_batch(rid, [
        ev("evt-done", "c1", 1, {}),                       # 同参重投 → 既有结论
        ev("evt-done", "c1", 1, {}, new="CLOSED"),         # 异参复用 → 冲突
        ev("evt-next", "c1", 2, {}, valve="v2"),           # 有效新事件
    ])
    # 响应与请求条目逐一对应：同参重投、异参冲突、新事件放行
    assert [(r["event_id"], r["outcome"]) for r in res["results"]] == [
        ("evt-done", RELEASED),
        ("evt-done", REJECTED_ID_CONFLICT),
        ("evt-next", RELEASED),
    ]
    assert res["results"][0]["duplicate"] is True
    valves = {v["id"]: v["state"] for v in res["state"]["valves"]}
    assert valves == {"v1": "OPEN", "v2": "OPEN"}          # 重投未重复施加


def test_batch_same_valve_contention_and_seq_slot_conflict():
    """批内同阀门竞争按标识稳定裁决；同槽位冲突明确拒绝且互不影响。"""
    s, rid = make_store()
    res = s.submit_batch(rid, [
        ev("evt-z-race", "c1", 1, {}),                     # 较大标识
        ev("evt-a-race", "c2", 1, {}),                     # 较小标识 → 放行
        ev("evt-slot", "c3", 1, {}, valve="v2"),           # 占据 c3#1 槽位
        ev("evt-slot-dup", "c3", 1, {}, valve="v2"),       # 同槽位 → 冲突
    ])
    got = outcomes(res)
    assert got["evt-a-race"] == RELEASED
    assert got["evt-z-race"] == REJECTED_PRECONDITION
    assert got["evt-slot"] == RELEASED
    assert got["evt-slot-dup"] == REJECTED_SEQ_CONFLICT
    st = res["state"]
    assert st["frontier"] == {"c1": 1, "c2": 1, "c3": 1}
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "OPEN", "v2": "OPEN"}


def test_batch_true_gap_and_future_dependency_still_rejected():
    """批内无前驱的跳号与未来依赖不会被同批事件"洗白"，仍为明确拒绝。"""
    s, rid = make_store()
    res = s.submit_batch(rid, [
        ev("evt-ok", "c1", 1, {}),
        ev("evt-gap", "c2", 3, {}),               # c2 的 #1/#2 不在批内 → 真跳号
        ev("evt-future", "c3", 1, {"c2": 9}),     # 远超任何在途前驱 → 真未来依赖
    ])
    got = outcomes(res)
    assert got["evt-ok"] == RELEASED
    assert got["evt-gap"] == REJECTED_SEQ_GAP
    assert got["evt-future"] == REJECTED_FUTURE_DEPENDENCY
    st = res["state"]
    assert st["frontier"] == {"c1": 1, "c2": 0, "c3": 0}
    assert st["waiting"] == []
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "OPEN", "v2": "CLOSED"}

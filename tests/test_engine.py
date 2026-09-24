"""因果一致性引擎单元测试。"""
import pytest

from app.engine import (
    Store,
    RELEASED, WAITING,
    REJECTED_PRECONDITION, REJECTED_ID_CONFLICT, REJECTED_SEQ_GAP,
    REJECTED_UNKNOWN_CONSOLE, REJECTED_FUTURE_DEPENDENCY,
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

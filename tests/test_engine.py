"""因果一致性引擎单元测试。"""
import itertools

import pytest

from app.engine import (
    Store,
    RELEASED, WAITING,
    REJECTED_PRECONDITION, REJECTED_ID_CONFLICT, REJECTED_SEQ_GAP,
    REJECTED_SEQ_CONFLICT, REJECTED_SEQ_CONSUMED,
    REJECTED_UNKNOWN_CONSOLE, REJECTED_UNKNOWN_VALVE,
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
# 夜班离线恢复：三控制台四事件链（A 连续两项 + B、C 跨控制台依赖）
# ----------------------------------------------------------------------
def night_shift_events():
    """A 已记录两项本地操作，B 依赖 A#2，C 依赖 B#1；阀门初态均为 CLOSED。"""
    return [
        ev("evt-z-a1", "c1", 1, {}, valve="v1", new="OPEN"),          # A#1 开 V1
        ev("evt-a-a2", "c1", 2, {}, valve="v2", new="OPEN"),          # A#2 开 V2
        ev("evt-b-b1", "c2", 1, {"c1": 2}, valve="v2",
           old="OPEN", new="CLOSED"),                                  # B#1 关 V2
        ev("evt-c-c1", "c3", 1, {"c2": 1}, valve="v1",
           old="OPEN", new="CLOSED"),                                  # C#1 关 V1
    ]


def assert_night_shift_consumed(res):
    """整链被一起接收并连续裁决后的统一断言。"""
    got = outcomes(res)
    assert got == {"evt-z-a1": RELEASED, "evt-a-a2": RELEASED,
                   "evt-b-b1": RELEASED, "evt-c-c1": RELEASED}
    st = res["state"]
    assert st["frontier"] == {"c1": 2, "c2": 1, "c3": 1}
    assert st["waiting"] == []
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "CLOSED", "v2": "CLOSED"}  # 先开后关，最终均关闭
    # 事件日志按因果消费顺序：A#1 → A#2 → B#1 → C#1
    assert [e["event_id"] for e in st["log"]] == [
        "evt-z-a1", "evt-a-a2", "evt-b-b1", "evt-c-c1"]


def test_offline_recovery_chain_single_batch():
    """四项离线操作按 A 本地顺序再接 B、C 一并恢复：全部放行。"""
    s, rid = make_store()
    res = s.submit_batch(rid, night_shift_events())
    assert_night_shift_consumed(res)


def test_offline_recovery_chain_all_request_orders():
    """离线队列在请求中的任意排列都得到完全一致的结论、日志与阀门状态。"""
    events = night_shift_events()
    signatures = set()
    for perm in itertools.permutations(range(len(events))):
        s, rid = make_store()
        res = s.submit_batch(rid, [events[i] for i in perm])
        assert_night_shift_consumed(res)
        signatures.add((
            tuple(sorted((r["event_id"], r["outcome"], r["duplicate"])
                         for r in res["results"])),
            tuple(e["event_id"] for e in res["state"]["log"]),
            tuple(sorted(res["state"]["frontier"].items())),
            tuple(sorted((v["id"], v["state"]) for v in res["state"]["valves"])),
            tuple(sorted(w["event_id"] for w in res["state"]["waiting"])),
        ))
    assert len(signatures) == 1


def test_offline_recovery_chain_split_submissions():
    """同一恢复链分批补交：等待项逐步就位，前驱补齐后连续裁决。"""
    s, rid = make_store()
    a1, a2, b1, c1 = night_shift_events()
    # C 先到达：依赖 B#1（在途一位容忍）→ 等待，等待项可见
    r = s.submit_batch(rid, [c1])
    assert outcomes(r) == {"evt-c-c1": WAITING}
    st = r["state"]
    assert [w["event_id"] for w in st["waiting"]] == ["evt-c-c1"]
    assert st["waiting"][0]["unmet"] == {"c2": 1}
    # B 单独到达：依赖的 A#2 尚不存在 → 真正未来依赖，明确拒绝且不入队
    r = s.submit_batch(rid, [b1])
    assert outcomes(r) == {"evt-b-b1": REJECTED_FUTURE_DEPENDENCY}
    assert r["state"]["frontier"] == {"c1": 0, "c2": 0, "c3": 0}
    # 补交 A 的两项：A#1、A#2 连续放行，C 仍等待 B#1
    r = s.submit_batch(rid, [a1, a2])
    assert outcomes(r) == {"evt-z-a1": RELEASED, "evt-a-a2": RELEASED}
    assert [w["event_id"] for w in r["state"]["waiting"]] == ["evt-c-c1"]
    # 重交 B：放行并级联放行 C，恢复链完整闭合
    r = s.submit_batch(rid, [b1])
    assert outcomes(r) == {"evt-b-b1": RELEASED}
    assert [e["event_id"] for e in r["resolved"]] == ["evt-b-b1", "evt-c-c1"]
    st = r["state"]
    assert st["frontier"] == {"c1": 2, "c2": 1, "c3": 1}
    assert st["waiting"] == []
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "CLOSED", "v2": "CLOSED"}
    assert [e["event_id"] for e in st["log"]] == [
        "evt-z-a1", "evt-a-a2", "evt-b-b1", "evt-c-c1"]


# ----------------------------------------------------------------------
# 批量边界回归：无效事件 / 重投 / 同阀门竞争 / 槽位与序号冲突
# ----------------------------------------------------------------------
def test_batch_boundary_mixed_valid_invalid_and_replay():
    """混合批次：合法事件照常裁决，无效事件明确拒绝，均不互相干扰。"""
    s, rid = make_store()
    s.submit_batch(rid, [ev("evt-done", "c1", 1, {})])  # 已消费：c1#1
    res = s.submit_batch(rid, [
        ev("evt-done", "c1", 1, {}),                     # 同标识重投 → 既有结论
        ev("evt-done", "c1", 1, {}, new="CLOSED"),       # 标识复用异载荷 → 冲突
        ev("evt-ghost", "c9", 1, {}),                    # 未知控制台
        ev("evt-novalve", "c2", 1, {}, valve="v9"),      # 未知阀门
        ev("evt-gap", "c2", 3, {}),                      # 真跳号（缺 c2#2）
        ev("evt-far", "c3", 1, {"c2": 9}),               # 真未来依赖
        ev("evt-ok", "c2", 1, {}, valve="v2"),           # 合法 → 放行
    ])
    got = outcomes(res)
    # 同标识重投返回既有结论；同批异载荷复用被明确拒绝（两条结果各自独立）
    replay, conflict = res["results"][0], res["results"][1]
    assert (replay["event_id"], replay["outcome"], replay["duplicate"]) == (
        "evt-done", RELEASED, True)
    assert (conflict["event_id"], conflict["outcome"]) == (
        "evt-done", REJECTED_ID_CONFLICT)
    assert got["evt-ghost"] == REJECTED_UNKNOWN_CONSOLE
    assert got["evt-novalve"] == REJECTED_UNKNOWN_VALVE
    assert got["evt-gap"] == REJECTED_SEQ_GAP
    assert got["evt-far"] == REJECTED_FUTURE_DEPENDENCY
    assert got["evt-ok"] == RELEASED
    st = res["state"]
    assert st["frontier"] == {"c1": 1, "c2": 1, "c3": 0}
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "OPEN", "v2": "OPEN"}


def test_batch_boundary_same_valve_contention_stable_by_event_id():
    """同批同阀门竞争：按事件标识稳定裁决，与请求排列无关。"""
    base = [
        ev("evt-z-open", "c1", 1, {}, valve="v1", new="OPEN"),
        ev("evt-a-open", "c2", 1, {}, valve="v1", new="OPEN"),
    ]
    for order in (base, base[::-1]):
        s, rid = make_store()
        res = s.submit_batch(rid, order)
        got = outcomes(res)
        assert got["evt-a-open"] == RELEASED
        assert got["evt-z-open"] == REJECTED_PRECONDITION
        # 预条件拒绝同样推进因果位置
        assert res["state"]["frontier"] == {"c1": 1, "c2": 1, "c3": 0}
        valves = {v["id"]: v["state"] for v in res["state"]["valves"]}
        assert valves["v1"] == "OPEN"


def test_batch_boundary_seq_conflicts_and_consumed():
    """槽位冲突与序号已消费：同批内与跨批均被明确拒绝且不入队。"""
    s, rid = make_store()
    # 在队等待事件占据 c2#1
    r = s.submit_batch(rid, [ev("evt-wait", "c2", 1, {"c1": 1})])
    assert outcomes(r) == {"evt-wait": WAITING}
    # 同槽位新标识 → 槽位冲突
    r = s.submit_batch(rid, [ev("evt-intruder", "c2", 1, {})])
    assert outcomes(r) == {"evt-intruder": REJECTED_SEQ_CONFLICT}
    # 同批内同槽位两个新事件 → 双方槽位冲突，后继序号随之跳号
    s2, rid2 = make_store()
    res = s2.submit_batch(rid2, [
        ev("evt-x1", "c1", 1, {}),
        ev("evt-x2", "c1", 1, {}, valve="v2"),
        ev("evt-x3", "c1", 2, {}),
    ])
    got = outcomes(res)
    assert got["evt-x1"] == REJECTED_SEQ_CONFLICT
    assert got["evt-x2"] == REJECTED_SEQ_CONFLICT
    assert got["evt-x3"] == REJECTED_SEQ_GAP
    assert res["state"]["frontier"] == {"c1": 0, "c2": 0, "c3": 0}
    # 序号已消费：先消费 c1#1，再以新标识重提同序号
    s3, rid3 = make_store()
    s3.submit_batch(rid3, [ev("evt-first", "c1", 1, {})])
    r = s3.submit_batch(rid3, [ev("evt-second", "c1", 1, {}, valve="v2")])
    assert outcomes(r) == {"evt-second": REJECTED_SEQ_CONSUMED}


def test_batch_boundary_duplicate_id_within_batch():
    """同批同标识：同载荷视为重放（仅消费一次），异载荷整组拒绝。"""
    s, rid = make_store()
    res = s.submit_batch(rid, [
        ev("evt-dup", "c1", 1, {}),
        ev("evt-dup", "c1", 1, {}),
    ])
    assert [(r["outcome"], r["duplicate"]) for r in res["results"]] == [
        (RELEASED, False), (RELEASED, True)]
    assert len(res["state"]["log"]) == 1  # 只消费一次
    s2, rid2 = make_store()
    res = s2.submit_batch(rid2, [
        ev("evt-dup", "c1", 1, {}, new="OPEN"),
        ev("evt-dup", "c1", 1, {}, new="CLOSED"),
    ])
    assert [r["outcome"] for r in res["results"]] == [
        REJECTED_ID_CONFLICT, REJECTED_ID_CONFLICT]
    assert res["state"]["frontier"] == {"c1": 0, "c2": 0, "c3": 0}

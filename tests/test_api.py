"""HTTP API 层测试（FastAPI TestClient）。"""
import pytest

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def make_round(consoles=("A", "B", "C")):
    r = client.post("/rounds", json={
        "name": "API测试轮次",
        "consoles": list(consoles),
        "valves": [{"name": "V1", "state": "CLOSED"}, {"name": "V2", "state": "CLOSED"}],
    })
    assert r.status_code == 201, r.text
    return r.json()["round_id"]


def post_event(rid, **kw):
    payload = {"deps": {}, **kw}
    return client.post(f"/rounds/{rid}/events", json=payload)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_index_page_served():
    r = client.get("/")
    assert r.status_code == 200 and "真空阀" in r.text


def test_round_validation():
    r = client.post("/rounds", json={"name": "x", "consoles": ["仅一个"],
                                     "valves": [{"name": "V1"}]})
    assert r.status_code == 422
    r = client.post("/rounds", json={"name": "x", "consoles": ["a", "b"], "valves": []})
    assert r.status_code == 422


def test_unknown_round_404():
    assert client.get("/rounds/r-nope").status_code == 404
    r = post_event("r-nope", event_id="e1", console_id="c1", seq=1,
                   valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    assert r.status_code == 404


def test_causal_flow_over_http():
    """等待 → 补齐前驱连续放行 → 状态可观察。"""
    rid = make_round()
    # B 依赖 A#1 → 等待
    r = post_event(rid, event_id="evt-b1", console_id="c2", seq=1, deps={"c1": 1},
                   valve_id="v2", expected_old_state="CLOSED", new_state="OPEN")
    assert r.status_code == 200
    assert r.json()["result"]["outcome"] == "WAITING"
    st = client.get(f"/rounds/{rid}").json()
    assert any(w["event_id"] == "evt-b1" for w in st["waiting"])
    # A 补齐前驱 → 两者连续放行
    r = post_event(rid, event_id="evt-a1", console_id="c1", seq=1,
                   valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    body = r.json()
    assert body["result"]["outcome"] == "RELEASED"
    assert [e["event_id"] for e in body["resolved"]] == ["evt-a1", "evt-b1"]
    st = client.get(f"/rounds/{rid}").json()
    assert st["frontier"] == {"c1": 1, "c2": 1, "c3": 0}
    assert st["waiting"] == []
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "OPEN", "v2": "OPEN"}
    # 单事件结论可稳定复查
    e = client.get(f"/rounds/{rid}/events/evt-b1").json()
    assert e["outcome"] == "RELEASED"


def test_rejections_over_http():
    rid = make_round()
    # 跳号
    r = post_event(rid, event_id="e-gap", console_id="c1", seq=5,
                   valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    assert r.json()["result"]["outcome"] == "REJECTED_SEQ_GAP"
    # 未知控制台
    r = post_event(rid, event_id="e-uc", console_id="c9", seq=1,
                   valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    assert r.json()["result"]["outcome"] == "REJECTED_UNKNOWN_CONSOLE"
    # 未来依赖
    r = post_event(rid, event_id="e-fd", console_id="c1", seq=1, deps={"c2": 9},
                   valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    assert r.json()["result"]["outcome"] == "REJECTED_FUTURE_DEPENDENCY"
    # 以上均不改变阀门状态与前沿
    st = client.get(f"/rounds/{rid}").json()
    assert st["frontier"] == {"c1": 0, "c2": 0, "c3": 0}
    assert all(v["state"] == "CLOSED" for v in st["valves"])
    # 标识复用而载荷变化
    post_event(rid, event_id="e-dup", console_id="c1", seq=1,
               valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    r = post_event(rid, event_id="e-dup", console_id="c1", seq=1,
                   valve_id="v1", expected_old_state="CLOSED", new_state="CLOSED")
    assert r.json()["result"]["outcome"] == "REJECTED_ID_CONFLICT"
    # 同一事件重投返回既有结论
    r = post_event(rid, event_id="e-dup", console_id="c1", seq=1,
                   valve_id="v1", expected_old_state="CLOSED", new_state="OPEN")
    res = r.json()["result"]
    assert res["outcome"] == "RELEASED" and res["duplicate"] is True


def test_batch_endpoint():
    rid = make_round()
    r = client.post(f"/rounds/{rid}/events/batch", json={"events": [
        {"event_id": "evt-z-2", "console_id": "c2", "seq": 1, "deps": {},
         "valve_id": "v1", "expected_old_state": "CLOSED", "new_state": "OPEN"},
        {"event_id": "evt-a-2", "console_id": "c1", "seq": 1, "deps": {},
         "valve_id": "v1", "expected_old_state": "CLOSED", "new_state": "OPEN"},
    ]})
    assert r.status_code == 200
    got = {x["event_id"]: x["outcome"] for x in r.json()["results"]}
    assert got == {"evt-a-2": "RELEASED", "evt-z-2": "REJECTED_PRECONDITION"}


def offline_chain():
    """夜班离线恢复链：A#1 开 V1 → A#2 开 V2 → B#1 关 V2 → C#1 关 V1。"""
    return [
        {"event_id": "evt-z-a1", "console_id": "c1", "seq": 1, "deps": {},
         "valve_id": "v1", "expected_old_state": "CLOSED", "new_state": "OPEN"},
        {"event_id": "evt-a-a2", "console_id": "c1", "seq": 2, "deps": {},
         "valve_id": "v2", "expected_old_state": "CLOSED", "new_state": "OPEN"},
        {"event_id": "evt-b-b1", "console_id": "c2", "seq": 1, "deps": {"c1": 2},
         "valve_id": "v2", "expected_old_state": "OPEN", "new_state": "CLOSED"},
        {"event_id": "evt-c-c1", "console_id": "c3", "seq": 1, "deps": {"c2": 1},
         "valve_id": "v1", "expected_old_state": "OPEN", "new_state": "CLOSED"},
    ]


CHAIN_LOG_ORDER = ["evt-z-a1", "evt-a-a2", "evt-b-b1", "evt-c-c1"]


@pytest.mark.parametrize("order", [
    [0, 1, 2, 3],   # A 的本地记录顺序再接 B、C
    [3, 2, 1, 0],   # 完全逆序：依赖全部先于前驱到达
    [2, 0, 3, 1],   # 交叉排列
    [1, 3, 0, 2],   # 同控制台连续操作被拆开
])
def test_offline_recovery_chain_over_http(order):
    """离线恢复链经批量接口：四项全部放行，请求排列不影响结论与状态。"""
    rid = make_round()
    chain = offline_chain()
    r = client.post(f"/rounds/{rid}/events/batch",
                    json={"events": [chain[i] for i in order]})
    assert r.status_code == 200, r.text
    body = r.json()
    # 响应与请求条目逐一对应，且四项全部放行
    assert [x["event_id"] for x in body["results"]] == [chain[i]["event_id"] for i in order]
    assert all(x["outcome"] == "RELEASED" for x in body["results"])
    assert [e["event_id"] for e in body["resolved"]] == CHAIN_LOG_ORDER
    st = client.get(f"/rounds/{rid}").json()
    assert st["frontier"] == {"c1": 2, "c2": 1, "c3": 1}
    assert st["waiting"] == []
    assert [e["event_id"] for e in st["log"]] == CHAIN_LOG_ORDER
    valves = {v["id"]: v["state"] for v in st["valves"]}
    assert valves == {"v1": "CLOSED", "v2": "CLOSED"}
    # 每项结论可稳定复查
    for eid in CHAIN_LOG_ORDER:
        e = client.get(f"/rounds/{rid}/events/{eid}").json()
        assert e["outcome"] == "RELEASED"

"""HTTP API 层测试（FastAPI TestClient）。"""
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

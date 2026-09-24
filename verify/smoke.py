#!/usr/bin/env python3
"""因果场景 HTTP 冒烟验收。

覆盖验收要求：
1. 先提交依赖另一控制台事件而显示等待，补齐前驱后连续放行；
2. 并发控制台争用同一旧状态时较小事件标识成功（与到达顺序无关），
   另一项稳定显示预条件拒绝，且竞争事件的后继不被拒绝结果阻塞；
3. 同一事件重投返回既有结论；标识复用而载荷变化、跳号、未知控制台、
   未来依赖均被明确拒绝且不改阀门状态；
4. 预条件不符拒绝但推进因果位置；
5. 健康入口与业务接口可观察到正确响应；
6. 离线恢复链（同控制台连续操作 + 跨控制台依赖）同批到达即连续裁决，
   请求排列不影响结论、事件日志与最终阀门状态。

退出码：0 = 全部通过；1 = 存在失败项。
"""
import os
import sys
import time

import requests

BASE = os.environ.get("BASE_URL", "http://localhost:8080").rstrip("/")
FAILURES = []


def check(name, cond, detail=""):
    line = f"[{'PASS' if cond else 'FAIL'}] {name}"
    if detail and not cond:
        line += f" :: {detail}"
    print(line, flush=True)
    if not cond:
        FAILURES.append(name)


def wait_health():
    for _ in range(int(os.environ.get("HEALTH_WAIT_SECONDS", "60"))):
        try:
            r = requests.get(BASE + "/health", timeout=2)
            if r.status_code == 200 and r.json().get("status") == "ok":
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def make_round(name, consoles=("控制台A", "控制台B", "控制台C")):
    r = requests.post(BASE + "/rounds", json={
        "name": name,
        "consoles": list(consoles),
        "valves": [{"name": "前端区阀V1", "state": "CLOSED"},
                   {"name": "光束线阀V2", "state": "CLOSED"}],
    }, timeout=5)
    r.raise_for_status()
    return r.json()["round_id"]


def submit(rid, payload):
    r = requests.post(f"{BASE}/rounds/{rid}/events", json=payload, timeout=5)
    r.raise_for_status()
    return r.json()


def ev(eid, cid, seq, deps=None, valve="v1", old="CLOSED", new="OPEN"):
    return {"event_id": eid, "console_id": cid, "seq": seq, "deps": deps or {},
            "valve_id": valve, "expected_old_state": old, "new_state": new}


def state(rid):
    r = requests.get(f"{BASE}/rounds/{rid}", timeout=5)
    r.raise_for_status()
    return r.json()


def valve_states(st):
    return {v["id"]: v["state"] for v in st["valves"]}


def main():
    print(f"== 冒烟验收目标: {BASE} ==", flush=True)

    # ---- 0. 健康入口与页面 ----
    check("健康入口 /health 可用", wait_health())
    if FAILURES:
        print("✗ 健康入口不可达，终止验收", flush=True)
        return 1
    try:
        r = requests.get(BASE + "/", timeout=5)
        check("页面入口 / 返回控制台页面", r.status_code == 200 and "真空阀" in r.text)
        run_scenarios()
    except requests.RequestException as exc:
        check("业务接口连通性", False, str(exc))

    print(flush=True)
    if FAILURES:
        print(f"✗ {len(FAILURES)} 项验收失败：{FAILURES}", flush=True)
        return 1
    print("✓ 全部冒烟验收通过", flush=True)
    return 0


def run_scenarios():

    # ---- 1. 依赖等待 → 补齐前驱 → 连续放行 ----
    rid = make_round("冒烟轮次一")
    r = submit(rid, ev("evt-b-0001", "c2", 1, {"c1": 1}, valve="v2"))
    check("依赖另一控制台事件的事件显示等待",
          r["result"]["outcome"] == "WAITING" and "c1#1" in r["result"]["reason"],
          str(r["result"]))
    st = state(rid)
    check("等待项在状态接口中可见（含未满足依赖）",
          any(w["event_id"] == "evt-b-0001" and w["unmet"] == {"c1": 1}
              for w in st["waiting"]), str(st["waiting"]))
    r = submit(rid, ev("evt-c-0001", "c3", 1, {"c2": 1}, valve="v2", old="OPEN", new="CLOSED"))
    check("链式依赖事件同样显示等待", r["result"]["outcome"] == "WAITING", str(r["result"]))
    r = submit(rid, ev("evt-a-0001", "c1", 1, {}, valve="v1"))
    got = {e["event_id"]: e["outcome"] for e in r["resolved"]}
    check("补齐前驱后连续放行（a→b→c）",
          [e["event_id"] for e in r["resolved"]] == ["evt-a-0001", "evt-b-0001", "evt-c-0001"]
          and all(o == "RELEASED" for o in got.values()), str(got))
    st = state(rid)
    check("连续放行后前沿推进且无等待项",
          st["frontier"] == {"c1": 1, "c2": 1, "c3": 1} and st["waiting"] == [],
          str(st["frontier"]))
    check("阀门状态按因果顺序演化（v1=OPEN, v2=CLOSED）",
          valve_states(st) == {"v1": "OPEN", "v2": "CLOSED"}, str(valve_states(st)))

    # ---- 2. 并发争用同一旧状态：较小事件标识成功（先到达的是较大标识）----
    rid = make_round("冒烟轮次二")
    r = submit(rid, ev("evt-b-1000", "c2", 1, {"c3": 1}))   # 较大标识先到达 → 等待
    check("争用事件一（较大标识）进入等待", r["result"]["outcome"] == "WAITING")
    r = submit(rid, ev("evt-a-1000", "c1", 1, {"c3": 1}))   # 较小标识后到达 → 等待
    check("争用事件二（较小标识）进入等待", r["result"]["outcome"] == "WAITING")
    r = submit(rid, ev("evt-c-1000", "c3", 1, {}, valve="v2"))  # 前驱到达，触发同批裁决
    got = {e["event_id"]: e["outcome"] for e in r["resolved"]}
    check("同批可放行项按事件标识稳定裁决：较小标识放行",
          got.get("evt-a-1000") == "RELEASED", str(got))
    check("较大标识稳定显示预条件拒绝",
          got.get("evt-b-1000") == "REJECTED_PRECONDITION", str(got))
    e = requests.get(f"{BASE}/rounds/{rid}/events/evt-b-1000", timeout=5).json()
    check("拒绝结论可稳定复查且含原因",
          e["outcome"] == "REJECTED_PRECONDITION" and "预条件" in e["reason"], str(e))
    # 竞争失败方的后继不被拒绝结果阻塞
    r = submit(rid, ev("evt-b-1001", "c2", 2, {}, old="OPEN", new="CLOSED"))
    check("竞争事件的后继不被拒绝结果阻塞",
          r["result"]["outcome"] == "RELEASED", str(r["result"]))
    check("后继放行后阀门状态正确（v1=CLOSED）",
          valve_states(state(rid))["v1"] == "CLOSED")

    # ---- 3. 重投 / 标识复用 / 跳号 / 未知控制台 / 未来依赖 ----
    before = valve_states(state(rid))
    r = submit(rid, ev("evt-a-1000", "c1", 1, {"c3": 1}))   # 同一事件重投
    check("同一事件重投返回既有结论（RELEASED, duplicate）",
          r["result"]["outcome"] == "RELEASED" and r["result"]["duplicate"] is True,
          str(r["result"]))
    check("重投不重复施加状态变更", valve_states(state(rid)) == before)
    bad = ev("evt-a-1000", "c1", 1, {"c3": 1}, new="CLOSED")  # 标识复用而载荷变化
    r = submit(rid, bad)
    check("标识复用而载荷变化被明确拒绝",
          r["result"]["outcome"] == "REJECTED_ID_CONFLICT", str(r["result"]))
    r = submit(rid, ev("evt-gap-1", "c1", 5, {}))           # 跳号
    check("跳号被明确拒绝", r["result"]["outcome"] == "REJECTED_SEQ_GAP", str(r["result"]))
    r = submit(rid, ev("evt-uc-1", "c9", 1, {}))            # 未知控制台
    check("未知控制台被明确拒绝",
          r["result"]["outcome"] == "REJECTED_UNKNOWN_CONSOLE", str(r["result"]))
    r = submit(rid, ev("evt-uc-2", "c1", 2, {"c9": 1}))     # 依赖未知控制台
    check("依赖向量含未知控制台被明确拒绝",
          r["result"]["outcome"] == "REJECTED_UNKNOWN_CONSOLE", str(r["result"]))
    r = submit(rid, ev("evt-fd-1", "c1", 2, {"c2": 9}))     # 未来依赖
    check("未来依赖被明确拒绝",
          r["result"]["outcome"] == "REJECTED_FUTURE_DEPENDENCY", str(r["result"]))
    r = submit(rid, ev("evt-fd-2", "c1", 2, {"c1": 2}))     # 依赖自身未来序号
    check("依赖自身未来序号被明确拒绝",
          r["result"]["outcome"] == "REJECTED_FUTURE_DEPENDENCY", str(r["result"]))
    st = state(rid)
    check("上述拒绝均不改变阀门状态、不推进前沿",
          valve_states(st) == before and st["frontier"] == {"c1": 1, "c2": 2, "c3": 1},
          f"{valve_states(st)} {st['frontier']}")

    # ---- 4. 预条件不符：拒绝但推进因果位置 ----
    r = submit(rid, ev("evt-pc-1", "c1", 2, {}, old="OPEN"))  # 实际 v1=CLOSED
    check("预条件不符被拒绝", r["result"]["outcome"] == "REJECTED_PRECONDITION",
          str(r["result"]))
    st = state(rid)
    check("预条件拒绝仍推进因果位置", st["frontier"]["c1"] == 2, str(st["frontier"]))
    r = submit(rid, ev("evt-pc-2", "c1", 3, {}, old="CLOSED", new="OPEN"))
    check("预条件拒绝的后继正常放行", r["result"]["outcome"] == "RELEASED",
          str(r["result"]))

    # ---- 5. 批量接口（离线恢复同批提交）----
    rid = make_round("冒烟轮次三")
    r = requests.post(f"{BASE}/rounds/{rid}/events/batch", json={"events": [
        ev("evt-z-002", "c2", 1, {}),
        ev("evt-a-002", "c1", 1, {}),
    ]}, timeout=5)
    got = {x["event_id"]: x["outcome"] for x in r.json()["results"]}
    check("批量提交同批按事件标识裁决",
          got == {"evt-a-002": "RELEASED", "evt-z-002": "REJECTED_PRECONDITION"}, str(got))

    # ---- 6. 离线恢复链：同控制台连续操作 + 跨控制台依赖同批到达 ----
    # A#1 开 V1 → A#2 开 V2 → B#1（依赖 A#2）关 V2 → C#1（依赖 B#1）关 V1
    chain = [
        ev("evt-z-a1", "c1", 1, {}, valve="v1"),
        ev("evt-a-a2", "c1", 2, {}, valve="v2"),
        ev("evt-b-b1", "c2", 1, {"c1": 2}, valve="v2", old="OPEN", new="CLOSED"),
        ev("evt-c-c1", "c3", 1, {"c2": 1}, valve="v1", old="OPEN", new="CLOSED"),
    ]
    chain_log = [e["event_id"] for e in chain]

    def check_chain(rid, order, label):
        r = requests.post(f"{BASE}/rounds/{rid}/events/batch",
                          json={"events": [chain[i] for i in order]}, timeout=5)
        body = r.json()
        check(f"{label}：响应与请求条目逐一对应",
              [x["event_id"] for x in body["results"]]
              == [chain[i]["event_id"] for i in order], str(body["results"]))
        got = {x["event_id"]: x["outcome"] for x in body["results"]}
        check(f"{label}：四项操作同批全部放行",
              got == {eid: "RELEASED" for eid in chain_log}, str(got))
        check(f"{label}：同批事件按因果顺序连续消费",
              [e["event_id"] for e in body["resolved"]] == chain_log,
              str([e["event_id"] for e in body["resolved"]]))
        st = state(rid)
        check(f"{label}：前沿推进为 A=2,B=1,C=1 且无等待项",
              st["frontier"] == {"c1": 2, "c2": 1, "c3": 1} and st["waiting"] == [],
              f"{st['frontier']} waiting={st['waiting']}")
        check(f"{label}：事件日志按因果顺序记录",
              [e["event_id"] for e in st["log"]] == chain_log,
              str([e["event_id"] for e in st["log"]]))
        check(f"{label}：两阀门最终均为关闭",
              valve_states(st) == {"v1": "CLOSED", "v2": "CLOSED"},
              str(valve_states(st)))

    # 按 A 的本地记录顺序再接 B、C 提交
    check_chain(make_round("冒烟轮次四"), [0, 1, 2, 3], "离线恢复链（记录顺序）")
    # 不同请求排列（依赖先于前驱到达、同控制台连续操作被拆开）结论一致
    check_chain(make_round("冒烟轮次五"), [3, 2, 1, 0], "离线恢复链（完全逆序）")
    check_chain(make_round("冒烟轮次六"), [2, 0, 3, 1], "离线恢复链（交叉排列）")


if __name__ == "__main__":
    sys.exit(main())

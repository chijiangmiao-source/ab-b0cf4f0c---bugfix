"""同步辐射真空阀联锁 —— 因果一致性引擎。

多个控制台可离线提交联锁操作事件，恢复联网后以任意顺序到达；
服务端按因果规则原子消费事件，保证到达顺序不改变应有的因果状态。

核心规则：
1. 每个控制台的事件按本地序号连续消费，frontier[console] 记录已消费的最大序号；
2. 仅当事件序号恰为前沿下一位、且完整依赖向量全部满足时，事件才被原子消费；
3. 同批可放行的事件按事件标识字典序稳定裁决，与网络到达顺序无关；
4. 预条件（期望旧状态）不符的事件同样被消费：拒绝生效但推进因果位置，
   避免后继事件被永久阻塞；
5. 同一事件标识重投返回既有结论；标识复用而载荷变化、跳号、未知控制台、
   未来依赖均被明确拒绝，且不改变阀门状态、不推进因果前沿。
"""
from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass, field

# ---- 事件结论 ----
RELEASED = "RELEASED"                            # 放行（已消费，阀门状态已变更）
WAITING = "WAITING"                              # 等待（依赖未满足或前驱未消费）
REJECTED_PRECONDITION = "REJECTED_PRECONDITION"  # 预条件不符（已消费，推进前沿）
REJECTED_ID_CONFLICT = "REJECTED_ID_CONFLICT"    # 标识复用而载荷变化
REJECTED_SEQ_GAP = "REJECTED_SEQ_GAP"            # 跳号
REJECTED_SEQ_CONFLICT = "REJECTED_SEQ_CONFLICT"  # 序号槽位被其他事件占用
REJECTED_SEQ_CONSUMED = "REJECTED_SEQ_CONSUMED"  # 序号已被消费
REJECTED_UNKNOWN_CONSOLE = "REJECTED_UNKNOWN_CONSOLE"  # 未知控制台
REJECTED_UNKNOWN_VALVE = "REJECTED_UNKNOWN_VALVE"      # 未知阀门
REJECTED_FUTURE_DEPENDENCY = "REJECTED_FUTURE_DEPENDENCY"  # 未来依赖

# 接收阶段可暂缓的拒绝：同批前驱事件可能尚未入队，须迭代至不动点后再定终态
_DEFERRABLE_OUTCOMES = frozenset({REJECTED_SEQ_GAP, REJECTED_FUTURE_DEPENDENCY})

VALVE_STATES = ("OPEN", "CLOSED")


def fingerprint(payload: dict) -> str:
    """事件载荷的稳定指纹，用于同一标识重投时比对载荷是否一致。"""
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


@dataclass
class Event:
    event_id: str
    console_id: str
    seq: int
    deps: dict
    valve_id: str
    expected_old_state: str
    new_state: str
    payload_fingerprint: str
    outcome: str = WAITING
    reason: str = ""
    consumed: bool = False  # 是否已占据因果位置（前沿已越过该事件）


@dataclass
class Round:
    round_id: str
    name: str
    consoles: dict                       # console_id -> 控制台名称
    valves: dict                         # valve_id -> {"name": ..., "state": ...}
    frontier: dict                       # console_id -> 已消费的最大本地序号
    events: dict = field(default_factory=dict)      # event_id -> Event
    seq_index: dict = field(default_factory=dict)   # (console_id, seq) -> event_id
    log: list = field(default_factory=list)         # 已消费事件标识，按消费顺序

    def known_seq(self, console_id: str) -> int:
        """该控制台已被掌握的连续最大序号（已消费 + 连续在队等待）。"""
        s = self.frontier[console_id]
        while (console_id, s + 1) in self.seq_index:
            s += 1
        return s


class Store:
    """轮次与事件的线程安全内存存储；所有变更在单锁下原子完成。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._rounds: dict[str, Round] = {}

    # ------------------------------------------------------------------
    # 轮次
    # ------------------------------------------------------------------
    def create_round(self, name, console_names, valve_specs):
        """值班员建立轮次：二至五个控制台 + 若干阀门。"""
        name = str(name).strip()
        if not name:
            raise ValueError("轮次名称不能为空")
        consoles = [str(c).strip() for c in console_names]
        if not (2 <= len(consoles) <= 5):
            raise ValueError("轮次须包含二至五个控制台")
        if any(not c for c in consoles):
            raise ValueError("控制台名称不能为空")
        if not valve_specs:
            raise ValueError("轮次须包含至少一个阀门")
        with self._lock:
            rid = "r-" + uuid.uuid4().hex[:8]
            console_map = {f"c{i + 1}": nm for i, nm in enumerate(consoles)}
            valves = {}
            for i, spec in enumerate(valve_specs):
                state = spec.get("state", "CLOSED")
                if state not in VALVE_STATES:
                    raise ValueError(f"非法阀门状态 {state}")
                vname = str(spec.get("name", "")).strip()
                if not vname:
                    raise ValueError("阀门名称不能为空")
                valves[f"v{i + 1}"] = {"name": vname, "state": state}
            rd = Round(round_id=rid, name=name, consoles=console_map,
                       valves=valves, frontier={cid: 0 for cid in console_map})
            self._rounds[rid] = rd
            return self._state_view(rd)

    def list_rounds(self):
        with self._lock:
            return [
                {"round_id": rd.round_id, "name": rd.name,
                 "consoles": len(rd.consoles), "valves": len(rd.valves)}
                for rd in self._rounds.values()
            ]

    def get_state(self, round_id):
        with self._lock:
            rd = self._rounds.get(round_id)
            return None if rd is None else self._state_view(rd)

    def get_event(self, round_id, event_id):
        with self._lock:
            rd = self._rounds.get(round_id)
            if rd is None:
                return None
            ev = rd.events.get(event_id)
            return None if ev is None else self._event_view(ev)

    # ------------------------------------------------------------------
    # 事件提交（单个或离线批量）
    # ------------------------------------------------------------------
    def submit_batch(self, round_id, payloads):
        """接收一批事件：迭代校验入队至不动点，随后按事件标识稳定裁决、连续消费。

        离线恢复时，同一控制台的连续操作与跨控制台依赖链会同批到达，
        而请求中的排列未必符合因果顺序。接收阶段因此迭代进行：每轮按
        确定性顺序尝试接收入队，因跳号/未来依赖被暂缓的事件留待下一轮
        重新判定；一轮中若再无新事件入队（不动点），仍被暂缓的跳号与
        未来依赖即为真正的拒绝。结论与请求中的排列无关。

        返回每条事件的结论、本次被消费的事件列表（按消费顺序）及最新状态。
        """
        with self._lock:
            rd = self._rounds.get(round_id)
            if rd is None:
                return None
            items = [None] * len(payloads)
            pending = self._canonical_batch(payloads)
            while pending:
                accepted_any = False
                deferred = []
                for original_index, payload in pending:
                    item = self._accept(rd, payload)
                    if item["event"] is not None:
                        items[original_index] = item
                        # 仅新入队的事件推进接收状态；重投不触发额外迭代
                        accepted_any = accepted_any or not item["duplicate"]
                    elif item["outcome"] in _DEFERRABLE_OUTCOMES:
                        deferred.append((original_index, payload, item))
                    else:
                        items[original_index] = item
                if not deferred:
                    break
                if not accepted_any:
                    # 不动点：接收状态不再变化，暂缓项即为终态拒绝
                    for original_index, _, item in deferred:
                        items[original_index] = item
                    break
                pending = [(original_index, payload)
                           for original_index, payload, _ in deferred]
            resolved = self._drain(rd)
            results = []
            for item in items:
                ev = item.get("event")
                if ev is not None:
                    results.append({
                        "event_id": ev.event_id,
                        "outcome": ev.outcome,
                        "reason": ev.reason,
                        "duplicate": item["duplicate"],
                    })
                else:
                    results.append({
                        "event_id": item["event_id"],
                        "outcome": item["outcome"],
                        "reason": item["reason"],
                        "duplicate": False,
                    })
            return {
                "round_id": round_id,
                "results": results,
                "resolved": [self._event_view(rd.events[eid]) for eid in resolved],
                "state": self._state_view(rd),
            }

    def _canonical_batch(self, payloads):
        """为离线恢复请求建立与网络重放无关的确定性处理顺序。

        接收阶段按该顺序逐轮迭代至不动点，故同一因果闭包的批次无论请求
        中如何排列都得到一致结论；同序号槽位的竞争也由该顺序稳定裁决。
        复制载荷与依赖向量，避免调用方在提交期间继续编辑离线队列，改写已经
        接受事件的指纹。原始位置另行保留，使响应仍与请求中的条目逐一对应。
        """
        entries = []
        for original_index, payload in enumerate(payloads):
            copied = dict(payload)
            copied["deps"] = dict(copied.get("deps") or {})
            entries.append({
                "original_index": original_index,
                "event_id": str(copied.get("event_id", "")),
                "console_id": str(copied.get("console_id", "")),
                "seq": copied.get("seq"),
                "payload": copied,
            })

        entries.sort(key=lambda entry: (
            entry["event_id"], entry["console_id"],
            entry["seq"] if isinstance(entry["seq"], int) else -1,
            entry["original_index"],
        ))
        return [(entry["original_index"], entry["payload"]) for entry in entries]

    # ------------------------------------------------------------------
    # 内部：接收单条事件（校验 / 幂等 / 入队），调用方须持锁
    # ------------------------------------------------------------------
    def _accept(self, rd, p):
        eid = str(p["event_id"])
        fp = fingerprint(p)

        # 同一事件重投：载荷一致返回既有结论；载荷变化拒绝复用
        existing = rd.events.get(eid)
        if existing is not None:
            if existing.payload_fingerprint == fp:
                return {"event": existing, "duplicate": True}
            return {"event": None, "event_id": eid,
                    "outcome": REJECTED_ID_CONFLICT,
                    "reason": f"事件标识 {eid} 已被不同载荷占用，拒绝复用"}

        cid = p.get("console_id")
        seq = p.get("seq")
        deps = p.get("deps") or {}
        valve_id = p.get("valve_id")

        if cid not in rd.consoles:
            return {"event": None, "event_id": eid, "outcome": REJECTED_UNKNOWN_CONSOLE,
                    "reason": f"未知控制台 {cid}"}
        if valve_id not in rd.valves:
            return {"event": None, "event_id": eid, "outcome": REJECTED_UNKNOWN_VALVE,
                    "reason": f"未知阀门 {valve_id}"}
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            return {"event": None, "event_id": eid, "outcome": REJECTED_SEQ_GAP,
                    "reason": f"非法本地序号 {seq!r}"}

        front = rd.frontier[cid]
        if seq <= front:
            return {"event": None, "event_id": eid, "outcome": REJECTED_SEQ_CONSUMED,
                    "reason": f"序号 {seq} 已被消费（{cid} 前沿为 {front}）"}
        known = rd.known_seq(cid)
        if seq > known + 1:
            return {"event": None, "event_id": eid, "outcome": REJECTED_SEQ_GAP,
                    "reason": f"跳号：{cid} 下一序号应为 {known + 1}，实收 {seq}"}
        occupant = rd.seq_index.get((cid, seq))
        if occupant is not None:
            return {"event": None, "event_id": eid, "outcome": REJECTED_SEQ_CONFLICT,
                    "reason": f"序号槽位 {cid}#{seq} 已被事件 {occupant} 占用"}

        # 依赖向量校验：已满足 / 可等待（前驱在途或已在队列）/ 未来依赖（拒绝）
        unmet = []
        for dc in sorted(deps):
            ds = deps[dc]
            if dc not in rd.consoles:
                return {"event": None, "event_id": eid,
                        "outcome": REJECTED_UNKNOWN_CONSOLE,
                        "reason": f"依赖向量包含未知控制台 {dc}"}
            if isinstance(ds, bool) or not isinstance(ds, int) or ds < 0:
                return {"event": None, "event_id": eid,
                        "outcome": REJECTED_FUTURE_DEPENDENCY,
                        "reason": f"非法依赖序号 {dc}#{ds!r}"}
            if dc == cid and ds >= seq:
                return {"event": None, "event_id": eid,
                        "outcome": REJECTED_FUTURE_DEPENDENCY,
                        "reason": f"事件不得依赖自身当前或未来序号 {dc}#{ds}"}
            if ds <= rd.frontier[dc]:
                continue  # 依赖已被消费，满足
            if ds == rd.frontier[dc] + 1 or ds <= rd.known_seq(dc):
                unmet.append(f"{dc}#{ds}")  # 前驱在途或已在等待队列，允许等待
            else:
                return {"event": None, "event_id": eid,
                        "outcome": REJECTED_FUTURE_DEPENDENCY,
                        "reason": f"未来依赖 {dc}#{ds}（{dc} 前沿为 {rd.frontier[dc]}）"}

        ev = Event(
            event_id=eid, console_id=cid, seq=seq,
            deps={k: int(v) for k, v in deps.items()},
            valve_id=valve_id,
            expected_old_state=p["expected_old_state"],
            new_state=p["new_state"],
            payload_fingerprint=fp,
            reason=("等待依赖：" + "、".join(unmet)) if unmet else "等待前驱事件消费",
        )
        rd.events[eid] = ev
        rd.seq_index[(cid, seq)] = eid
        return {"event": ev, "duplicate": False}

    # ------------------------------------------------------------------
    # 内部：连续消费所有可放行事件，同批按事件标识字典序稳定裁决
    # ------------------------------------------------------------------
    def _drain(self, rd):
        resolved = []
        while True:
            ready = [
                ev for ev in rd.events.values()
                if not ev.consumed
                and ev.seq == rd.frontier[ev.console_id] + 1
                and all(rd.frontier[dc] >= ds for dc, ds in ev.deps.items())
            ]
            if not ready:
                return resolved
            ready.sort(key=lambda e: e.event_id)  # 稳定裁决：与到达顺序无关
            ev = ready[0]
            valve = rd.valves[ev.valve_id]
            current = valve["state"]
            if current == ev.expected_old_state:
                valve["state"] = ev.new_state
                ev.outcome = RELEASED
                ev.reason = (f"放行：阀门 {ev.valve_id} "
                             f"{current} → {ev.new_state}")
            else:
                # 预条件不符同样消费并推进因果位置，避免后继永久阻塞
                ev.outcome = REJECTED_PRECONDITION
                ev.reason = (f"预条件拒绝：期望旧状态 {ev.expected_old_state}，"
                             f"实际为 {current}；事件已消费，因果位置已推进")
            ev.consumed = True
            rd.frontier[ev.console_id] = ev.seq
            rd.log.append(ev.event_id)
            resolved.append(ev.event_id)

    # ------------------------------------------------------------------
    # 视图
    # ------------------------------------------------------------------
    def _event_view(self, ev):
        return {
            "event_id": ev.event_id,
            "console_id": ev.console_id,
            "seq": ev.seq,
            "deps": dict(ev.deps),
            "valve_id": ev.valve_id,
            "expected_old_state": ev.expected_old_state,
            "new_state": ev.new_state,
            "outcome": ev.outcome,
            "reason": ev.reason,
        }

    def _state_view(self, rd):
        waiting = [
            {
                "event_id": ev.event_id,
                "console_id": ev.console_id,
                "seq": ev.seq,
                "valve_id": ev.valve_id,
                "reason": ev.reason,
                "unmet": {dc: ds for dc, ds in ev.deps.items()
                          if rd.frontier[dc] < ds},
            }
            for ev in sorted(rd.events.values(), key=lambda e: e.event_id)
            if not ev.consumed
        ]
        return {
            "round_id": rd.round_id,
            "name": rd.name,
            "consoles": [{"id": cid, "name": nm} for cid, nm in rd.consoles.items()],
            "valves": [{"id": vid, "name": v["name"], "state": v["state"]}
                       for vid, v in rd.valves.items()],
            "frontier": dict(rd.frontier),
            "waiting": waiting,
            "log": [self._event_view(rd.events[eid]) for eid in rd.log],
        }

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
        """接收一批事件：整批校验入队，随后按事件标识稳定裁决、连续消费。

        离线批量恢复的关键：接收判定对请求内排列不敏感。同一控制台的连续
        序号与跨控制台依赖会被作为一个整体看待——只要某序号位的前驱（本
        控制台前一序号、依赖向量指向的序号）在“本批 + 已在队”的事件集中
        齐备，它就可以入队等待，而不会因为请求中排在前面而被误判为跳号或
        未来依赖。真正不可能满足的事件（未知控制台/阀门、非法序号、跳号、
        槽位冲突、序号已消费、依赖缺失或未来依赖）才被拒绝。

        返回每条事件的结论（按请求顺序）、本次被消费的事件列表（按消费
        顺序）及最新状态。
        """
        with self._lock:
            rd = self._rounds.get(round_id)
            if rd is None:
                return None

            # 载荷去重与复制，并按事件标识建立与请求排列无关的确定性处理顺序
            entries = []
            for original_index, payload in self._canonical_batch(payloads):
                entries.append({"original_index": original_index, "payload": payload})
            items = [None] * len(entries)

            # 第一轮：幂等 / 标识冲突 / 字段级合法性。结论与请求排列无关，
            # 因而先于因果可达性判定完成。
            candidates = []          # 代表事件 (entry, cid, seq, deps, valve_id, fp)
            new_groups = {}          # 本批新标识 -> 同标识候选列表
            for entry in entries:
                p = entry["payload"]
                eid = str(p["event_id"])
                idx = entry["original_index"]
                fp = fingerprint(p)

                existing = rd.events.get(eid)
                if existing is not None:
                    if existing.payload_fingerprint == fp:
                        items[idx] = {"event": existing, "duplicate": True}
                    else:
                        items[idx] = {
                            "event": None, "event_id": eid,
                            "outcome": REJECTED_ID_CONFLICT,
                            "reason": f"事件标识 {eid} 已被不同载荷占用，拒绝复用",
                        }
                    continue

                cid, seq, deps, valve_id, rejection = self._validate_fields(rd, eid, p)
                if rejection is not None:
                    items[idx] = rejection
                    continue
                cand = (entry, cid, seq, deps, valve_id, fp)
                new_groups.setdefault(eid, []).append(cand)

            # 同一批次内的同标识重投：载荷一致视为重放，仅保留一个代表参与
            # 裁决（entries 已按标识与原始位置排序，代表与请求排列无关）；
            # 载荷不一致则同批标识复用冲突，全部拒绝。
            mirrors = []   # (原始位置, 代表原始位置)
            for eid, group in new_groups.items():
                fingerprints = {c[5] for c in group}
                rep_idx = group[0][0]["original_index"]
                if len(fingerprints) == 1:
                    candidates.append(group[0])
                    for cand in group[1:]:
                        mirrors.append((cand[0]["original_index"], rep_idx))
                else:
                    reason = f"事件标识 {eid} 在同一批次中以不同载荷重复出现，拒绝复用"
                    for cand in group:
                        items[cand[0]["original_index"]] = {
                            "event": None, "event_id": eid,
                            "outcome": REJECTED_ID_CONFLICT, "reason": reason}

            # 第二轮：整批因果可达性（最小不动点），与请求排列无关
            admitted, rejections = self._admit_candidates(rd, candidates)
            for entry, cid, seq, deps, valve_id, fp in admitted:
                ev = Event(
                    event_id=str(entry["payload"]["event_id"]),
                    console_id=cid, seq=seq, deps=deps, valve_id=valve_id,
                    expected_old_state=entry["payload"]["expected_old_state"],
                    new_state=entry["payload"]["new_state"],
                    payload_fingerprint=fp,
                    reason="等待前驱事件消费",
                )
                rd.events[ev.event_id] = ev
                rd.seq_index[(cid, seq)] = ev.event_id
                items[entry["original_index"]] = {"event": ev, "duplicate": False}
            for entry, rejection in rejections:
                items[entry["original_index"]] = rejection

            # 同批同载荷重放镜像代表结论（代表入队则 duplicate=True；
            # 代表自身被拒绝则镜像同一拒绝，不作为重复事件）
            for member_idx, rep_idx in mirrors:
                rep = items[rep_idx]
                if rep.get("event") is not None:
                    items[member_idx] = {"event": rep["event"], "duplicate": True}
                else:
                    items[member_idx] = dict(rep)

            # 入队完成后按既有规则连续消费（同批按事件标识字典序裁决）
            resolved = self._drain(rd)

            # 为仍在等待的事件刷新人类可读原因
            for ev in rd.events.values():
                if not ev.consumed:
                    ev.reason = self._waiting_reason(rd, ev)

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
        """为离线恢复请求建立与网络重放无关的确定性接收顺序。

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
    # 内部：字段级校验（不依赖因果位置，调用方须持锁）
    # ------------------------------------------------------------------
    def _validate_fields(self, rd, eid, p):
        """校验单条新事件的字段合法性。

        返回 (cid, seq, deps, valve_id, rejection)：合法时 rejection 为 None；
        非法时其余字段无意义，rejection 为拒绝结论。
        """
        cid = p.get("console_id")
        seq = p.get("seq")
        raw_deps = p.get("deps") or {}
        valve_id = p.get("valve_id")

        if cid not in rd.consoles:
            return None, None, None, None, {
                "event": None, "event_id": eid,
                "outcome": REJECTED_UNKNOWN_CONSOLE,
                "reason": f"未知控制台 {cid}"}
        if valve_id not in rd.valves:
            return None, None, None, None, {
                "event": None, "event_id": eid,
                "outcome": REJECTED_UNKNOWN_VALVE,
                "reason": f"未知阀门 {valve_id}"}
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            return None, None, None, None, {
                "event": None, "event_id": eid,
                "outcome": REJECTED_SEQ_GAP,
                "reason": f"非法本地序号 {seq!r}"}

        deps = {}
        for dc in sorted(raw_deps):
            ds = raw_deps[dc]
            if dc not in rd.consoles:
                return None, None, None, None, {
                    "event": None, "event_id": eid,
                    "outcome": REJECTED_UNKNOWN_CONSOLE,
                    "reason": f"依赖向量包含未知控制台 {dc}"}
            if isinstance(ds, bool) or not isinstance(ds, int) or ds < 0:
                return None, None, None, None, {
                    "event": None, "event_id": eid,
                    "outcome": REJECTED_FUTURE_DEPENDENCY,
                    "reason": f"非法依赖序号 {dc}#{ds!r}"}
            if dc == cid and ds >= seq:
                return None, None, None, None, {
                    "event": None, "event_id": eid,
                    "outcome": REJECTED_FUTURE_DEPENDENCY,
                    "reason": f"事件不得依赖自身当前或未来序号 {dc}#{ds}"}
            deps[dc] = int(ds)

        return cid, int(seq), deps, valve_id, None

    # ------------------------------------------------------------------
    # 内部：整批因果可达性（最小不动点），调用方须持锁
    # ------------------------------------------------------------------
    def _admit_candidates(self, rd, candidates):
        """以“本批 + 已在队”事件集为整体判定每条候选是否可入队。

        结论与候选的请求排列无关：某事件可入队当且仅当它的每个因果前驱
        （本控制台前一序号、依赖向量指向的序号）都已被消费，或由事件集中
        的某条事件占据。占据者自身也必须可达，因此以最小不动点迭代求闭包；
        闭包外的事件按其缺失原因明确拒绝（跳号 / 槽位冲突 / 序号已消费 /
        未来依赖）。
        """
        # 槽位占用：(控制台, 序号) -> 占据该槽位的候选下标列表。同槽位出现
        # 多个候选在同一集合内是顺序无关的冲突，双方都不能入队。
        slot_holders = {}
        for i, (_entry, cid, seq, _deps, _v, _fp) in enumerate(candidates):
            slot_holders.setdefault((cid, seq), []).append(i)
        slot_owner = {k: v[0] for k, v in slot_holders.items() if len(v) == 1}
        slot_conflicts = {k for k, v in slot_holders.items() if len(v) > 1}

        occupied = set(rd.seq_index) | set(slot_holders)

        reachable = set()
        changed = True
        while changed:
            changed = False
            for i, (_entry, cid, seq, deps, _v, _fp) in enumerate(candidates):
                # 已消费序号 / 已被在队事件占据的槽位 / 同批槽位冲突：
                # 绝不可入队，结论在下方分类阶段给出
                if (i in reachable or (cid, seq) in slot_conflicts
                        or seq <= rd.frontier[cid] or (cid, seq) in rd.seq_index):
                    continue
                ok = True
                # 本控制台序号须连续：前一序号须已消费或由可达事件占据
                if seq > rd.frontier[cid] + 1:
                    prev = seq - 1
                    owner = slot_owner.get((cid, prev))
                    if not ((cid, prev) in rd.seq_index or
                            (owner is not None and owner in reachable)):
                        ok = False
                # 跨控制台依赖须由已消费序号、在途一位（容忍前驱后到）、
                # 已在队事件或可达的本批事件满足
                if ok:
                    for dc, ds in deps.items():
                        if ds <= rd.frontier[dc]:
                            continue
                        owner = slot_owner.get((dc, ds))
                        if (ds == rd.frontier[dc] + 1
                                or (dc, ds) in rd.seq_index
                                or (owner is not None and owner in reachable)):
                            continue
                        ok = False
                        break
                if ok:
                    reachable.add(i)
                    changed = True

        admitted, rejections = [], []
        for i, (entry, cid, seq, deps, valve_id, fp) in enumerate(candidates):
            if i in reachable:
                admitted.append((entry, cid, seq, deps, valve_id, fp))
                continue
            eid = str(entry["payload"]["event_id"])
            rejections.append((entry, self._classify_blocker(
                rd, eid, cid, seq, deps, candidates,
                slot_holders, slot_owner, slot_conflicts, occupied, reachable)))
        return admitted, rejections

    def _classify_blocker(self, rd, eid, cid, seq, deps, candidates,
                          slot_holders, slot_owner, slot_conflicts, occupied,
                          reachable):
        """为不可达候选给出确定的拒绝结论（先本控制台序号，后依赖向量）。"""
        front = rd.frontier[cid]
        if seq <= front:
            return {"event": None, "event_id": eid,
                    "outcome": REJECTED_SEQ_CONSUMED,
                    "reason": f"序号 {seq} 已被消费（{cid} 前沿为 {front}）"}
        if (cid, seq) in rd.seq_index:
            return {"event": None, "event_id": eid,
                    "outcome": REJECTED_SEQ_CONFLICT,
                    "reason": f"序号槽位 {cid}#{seq} 已被事件 "
                              f"{rd.seq_index[(cid, seq)]} 占用"}
        if (cid, seq) in slot_conflicts:
            others = [j for j in slot_holders[(cid, seq)]
                      if str(candidates[j][0]["payload"]["event_id"]) != eid]
            other_id = (str(candidates[others[0]][0]["payload"]["event_id"])
                        if others else "(本批其他事件)")
            return {"event": None, "event_id": eid,
                    "outcome": REJECTED_SEQ_CONFLICT,
                    "reason": f"序号槽位 {cid}#{seq} 在本批中被事件 {other_id} 同时占用"}

        # 本控制台序号连续性（与既有裁决顺序一致：先跳号，后依赖）
        if seq > front + 1:
            for s in range(front + 1, seq):
                if (cid, s) not in occupied:
                    return {"event": None, "event_id": eid,
                            "outcome": REJECTED_SEQ_GAP,
                            "reason": f"跳号：{cid} 缺少序号 {s}，无法消费到 {seq}"}
            # 槽位虽被占据，但占据者同批冲突或自身不可达 → 该序号位不可消费
            prev = seq - 1
            if (cid, prev) in slot_conflicts:
                return {"event": None, "event_id": eid,
                        "outcome": REJECTED_SEQ_GAP,
                        "reason": f"跳号：前驱序号 {cid}#{prev} 槽位冲突，无法消费到 {seq}"}
            owner = slot_owner.get((cid, prev))
            if owner is not None and owner not in reachable:
                blocker = str(candidates[owner][0]["payload"]["event_id"])
                return {"event": None, "event_id": eid,
                        "outcome": REJECTED_SEQ_GAP,
                        "reason": f"跳号：前驱事件 {blocker}（{cid}#{prev}）不可消费，"
                                  f"无法消费到 {seq}"}

        # 阻塞来自依赖向量：取字典序第一个未满足项，结论与请求排列无关
        for dc, ds in deps.items():
            if ds <= rd.frontier[dc]:
                continue
            if ds == rd.frontier[dc] + 1 or (dc, ds) in rd.seq_index:
                continue  # 在途一位或已在队等待：允许等待，不会是阻塞项
            if (dc, ds) in slot_conflicts:
                detail = f"序号 {dc}#{ds} 槽位冲突"
            elif (dc, ds) in slot_holders:
                blocker = str(candidates[slot_owner[(dc, ds)]][0]["payload"]["event_id"])
                detail = f"前驱事件 {blocker} 自身不可达"
            else:
                detail = f"{dc} 前沿为 {rd.frontier[dc]}，且无该序号事件"
            return {"event": None, "event_id": eid,
                    "outcome": REJECTED_FUTURE_DEPENDENCY,
                    "reason": f"未来依赖 {dc}#{ds}（{detail}）"}

        # 理论上不可达候选必落入上述某类；兜底为跳号拒绝
        return {"event": None, "event_id": eid,
                "outcome": REJECTED_SEQ_GAP,
                "reason": f"跳号：{cid} 无法连续消费到序号 {seq}"}

    def _waiting_reason(self, rd, ev):
        """入队后尚未消费事件的等待原因（依赖优先，其次等本控制台前驱）。"""
        unmet = [f"{dc}#{ds}" for dc, ds in sorted(ev.deps.items())
                 if rd.frontier[dc] < ds]
        if unmet:
            return "等待依赖：" + "、".join(unmet)
        return "等待前驱事件消费"

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

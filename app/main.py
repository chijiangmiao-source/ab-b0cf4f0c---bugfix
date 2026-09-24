"""同步辐射真空阀联锁控制台 —— HTTP API。

- GET  /health                          健康入口
- POST /rounds                          值班员建立轮次（二至五个控制台 + 若干阀门）
- GET  /rounds                          轮次列表
- GET  /rounds/{rid}                    轮次状态（阀门、因果前沿、等待项、日志）
- GET  /rounds/{rid}/events/{eid}       单事件结论（稳定可复查）
- POST /rounds/{rid}/events             提交单个联锁事件
- POST /rounds/{rid}/events/batch       离线恢复后批量提交
"""
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Literal

from .engine import Store

app = FastAPI(title="同步辐射真空阀联锁控制台", version="1.0.0")
store = Store()

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
ValveState = Literal["OPEN", "CLOSED"]


class ValveSpec(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    state: ValveState = "CLOSED"


class RoundCreate(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    consoles: List[str] = Field(min_length=2, max_length=5)
    valves: List[ValveSpec] = Field(min_length=1, max_length=32)


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    console_id: str = Field(min_length=1, max_length=64)
    seq: int = Field(ge=1)
    deps: Dict[str, int] = Field(default_factory=dict)
    valve_id: str = Field(min_length=1, max_length=64)
    expected_old_state: ValveState
    new_state: ValveState


class BatchIn(BaseModel):
    events: List[EventIn] = Field(min_length=1, max_length=200)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.post("/rounds", status_code=201)
def create_round(body: RoundCreate):
    try:
        return store.create_round(
            body.name, body.consoles, [v.model_dump() for v in body.valves]
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/rounds")
def list_rounds():
    return {"rounds": store.list_rounds()}


@app.get("/rounds/{round_id}")
def get_round(round_id: str):
    state = store.get_state(round_id)
    if state is None:
        raise HTTPException(status_code=404, detail="轮次不存在")
    return state


@app.get("/rounds/{round_id}/events/{event_id}")
def get_event(round_id: str, event_id: str):
    view = store.get_event(round_id, event_id)
    if view is None:
        raise HTTPException(status_code=404, detail="事件不存在")
    return view


@app.post("/rounds/{round_id}/events")
def submit_event(round_id: str, body: EventIn):
    res = store.submit_batch(round_id, [body.model_dump()])
    if res is None:
        raise HTTPException(status_code=404, detail="轮次不存在")
    return {
        "round_id": round_id,
        "result": res["results"][0],
        "resolved": res["resolved"],
        "state": res["state"],
    }


@app.post("/rounds/{round_id}/events/batch")
def submit_events_batch(round_id: str, body: BatchIn):
    res = store.submit_batch(round_id, [e.model_dump() for e in body.events])
    if res is None:
        raise HTTPException(status_code=404, detail="轮次不存在")
    return res

# 同步辐射真空阀联锁控制台

多个控制台可**离线提交**联锁操作事件，恢复联网后以任意顺序到达；服务端按**因果规则**原子消费事件，
保证到达顺序不改变应有的因果状态。值班员可建立包含**二至五个控制台**和若干阀门的轮次。

## 因果规则

1. 每个控制台的事件按本地序号连续消费，`frontier[console]` 记录已消费的最大序号；
2. 仅当事件序号恰为前沿下一位、且完整依赖向量全部满足时，事件才被原子消费；
3. 同批可放行的事件按**事件标识字典序**稳定裁决，与网络到达顺序无关；
4. 预条件（期望旧状态）不符的事件同样被消费：**拒绝生效但推进因果位置**，避免后继永久阻塞；
5. 同一事件标识重投返回既有结论（幂等）；标识复用而载荷变化、跳号、未知控制台、
   未来依赖均被明确拒绝，且不改变阀门状态、不推进因果前沿。

## 事件结论

| outcome | 含义 |
| --- | --- |
| `RELEASED` | 放行：已消费，阀门状态已变更 |
| `WAITING` | 等待：依赖未满足或前驱未消费（前驱在途容忍一位） |
| `REJECTED_PRECONDITION` | 预条件不符：已消费并推进因果位置 |
| `REJECTED_ID_CONFLICT` | 标识复用而载荷变化（不消费） |
| `REJECTED_SEQ_GAP` / `REJECTED_SEQ_CONFLICT` / `REJECTED_SEQ_CONSUMED` | 跳号 / 槽位冲突 / 序号已消费（不消费） |
| `REJECTED_UNKNOWN_CONSOLE` / `REJECTED_UNKNOWN_VALVE` | 未知控制台 / 阀门（不消费） |
| `REJECTED_FUTURE_DEPENDENCY` | 未来依赖（不消费） |

## API 摘要

- `GET /health` — 健康入口
- `POST /rounds` — 建立轮次 `{name, consoles[2..5], valves:[{name,state}]}`
- `GET /rounds` / `GET /rounds/{rid}` — 列表 / 状态（阀门、因果前沿、等待项、日志）
- `GET /rounds/{rid}/events/{eid}` — 单事件结论（稳定可复查）
- `POST /rounds/{rid}/events` — 提交事件 `{event_id, console_id, seq, deps, valve_id, expected_old_state, new_state}`
- `POST /rounds/{rid}/events/batch` — 离线恢复批量提交 `{events:[...]}`
- `GET /` — 控制台页面（创建轮次、提交事件、查看阀门状态/因果前沿/等待项/放行与拒绝原因）

## 本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 打开 http://localhost:8000/
```

## Docker / Compose

```bash
# 启动服务（宿主端口可配置，默认 8080）
HOST_PORT=8080 docker compose up --build app
# 健康入口：http://localhost:8080/health

# 验收：构建检查 + 代码测试 + 因果场景 HTTP 冒烟，执行后退出并以退出码报告结果
docker compose up --build --exit-code-from verify --abort-on-container-exit verify
echo $?   # 0 = 验收通过
```

本地验收（服务已运行时）：

```bash
BASE_URL=http://localhost:8000 bash verify/verify.sh
```

## 目录结构

```
app/engine.py     因果一致性引擎（前沿推进、稳定裁决、幂等与各类拒绝）
app/main.py       FastAPI 接口层
app/static/       控制台页面
tests/            引擎单元测试与 API 测试
verify/smoke.py   因果场景 HTTP 冒烟（等待→连续放行、竞争裁决、各类拒绝）
verify/verify.sh  验收编排：构建检查 → pytest → 冒烟，退出码报告结果
```

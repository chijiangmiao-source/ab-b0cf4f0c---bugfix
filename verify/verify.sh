#!/usr/bin/env bash
# 验收编排：构建检查 → 代码测试 → 因果场景 HTTP 冒烟。
# 全部通过退出码为 0，任一失败退出码非 0。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== [1/3] 构建检查：编译与导入 =="
python -m compileall -q app tests verify
python -c "import app.main; print('  import app.main OK')"

echo "== [2/3] 代码测试：pytest =="
python -m pytest -q tests

echo "== [3/3] HTTP 冒烟：因果场景验收（BASE_URL=${BASE_URL:-http://localhost:8080}）=="
python verify/smoke.py

echo "== 验收通过：构建检查 / 代码测试 / HTTP 冒烟 全部成功 =="

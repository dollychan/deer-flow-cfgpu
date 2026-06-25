"""沙箱快照链路诊断脚本（shell 驱动 chromium 版）。

用法（在 consumer 宿主机，backend/ 目录下，带项目 venv）::

    # 用内置一行 HTML 跑通链路
    .venv/bin/python scripts/diag_snapshot.py http://<沙箱 base_url>

    # 用一个真实 HTML 文件验渲染（推荐拿 JS 动态渲染的页面，如连连看游戏）
    .venv/bin/python scripts/diag_snapshot.py http://<沙箱 base_url> ./game.html

base_url 从 consumer 日志里找：搜索 "AioSandbox" / "sandbox_url"，或 `docker ps`
看 sandbox-director 容器映射到 host 的端口（容器内是 8080，常映射到 127.0.0.1:8080）。

脚本构造一个真实 AioSandbox，调用与 present_files 完全相同的 `snapshot_html`
（写 HTML 进沙箱 outputs/ → 驱动内置 chromium --headless --virtual-time-budget
--screenshot → download_file 取回）。失败时 fail-open 的真实异常会以 WARNING 打印；
成功则把 PNG 落到 /tmp/diag_snap.png。
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(2)

base_url = sys.argv[1].rstrip("/")
html_file = sys.argv[2] if len(sys.argv) > 2 else None

from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox  # noqa: E402  (after argv guard)

sb = AioSandbox(id="diag", base_url=base_url)

print("\n[1] chromium 版本探测 ...")
print("   ", sb.execute_command("chromium-browser --version 2>&1 | head -1"))

print("\n[2] snapshot_html(...) — 与 present_files 同一路径 ...")
if html_file:
    html = Path(html_file).read_text(encoding="utf-8")
    print(f"    使用真实 HTML 文件 {html_file}（{len(html)} chars）")
else:
    html = "<h1>诊断快照 OK</h1><p>snapshot diagnostic 测试</p>"
    print("    使用内置一行 HTML（仅验链路；要验渲染请传 HTML 文件路径）")
png = sb.snapshot_html(html)

if not png:
    print("\n失败 ❌ —— snapshot_html 返回 None（上方 WARNING 有真实原因）")
    sys.exit(1)

out = "/tmp/diag_snap.png"
with open(out, "wb") as f:
    f.write(png)
print(f"\nOK ✅ 快照可用，已写出 {out}（{len(png)} bytes，header={png[:8]!r}）")

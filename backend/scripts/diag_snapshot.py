"""沙箱快照链路诊断脚本。

用法（在 consumer 宿主机，backend/ 目录下，带项目 venv）::

    .venv/bin/python scripts/diag_snapshot.py http://<沙箱 base_url>

base_url 从 consumer 日志里找：搜索 "AioSandbox" / "sandbox_url"，或 `docker ps`
看 all-in-one-sandbox 容器映射到 host 的端口（容器内是 8080）。

脚本走的就是 present_files 用的 `AioSandbox.snapshot_html` 同一条路径，会把
fail-open 吞掉的真实异常**打印出来**，并在成功时把 PNG 落到 /tmp/diag_snap.png。
"""

import logging
import sys

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(2)

base_url = sys.argv[1].rstrip("/")

from agent_sandbox import Sandbox as Client

client = Client(base_url=base_url, timeout=120)

# 1) 浏览器服务是否在线？
print("\n[1] browser_tabs.list() ...")
tabs = client.browser_tabs.list()
print("    ->", tabs)

# 2) 完整快照序列（与 AioSandbox.snapshot_html 一致）
import base64

html = "<h1>诊断快照 OK</h1><p>snapshot diagnostic</p>"
data_url = "data:text/html;base64," + base64.b64encode(html.encode()).decode()

before = client.browser_tabs.list()
idx = len(before.data) if before and before.data else 0
print(f"\n[2] create tab (new index = {idx}) ...")
print("    ->", client.browser_tabs.create())
print("[3] activate ...", client.browser_tabs.activate(idx))
print("[4] navigate(data:url, wait_until=load) ...")
print("    ->", client.browser_page.navigate(url=data_url, wait_until="load"))
print("[5] screenshot(full_page=True, format=png) ...")
png = b"".join(client.browser_page.screenshot(full_page=True, format="png"))
print(f"    -> {len(png)} bytes, header={png[:8]!r}")
client.browser_tabs.close(idx)

out = "/tmp/diag_snap.png"
with open(out, "wb") as f:
    f.write(png)
print(f"\nOK ✅ 快照可用，已写出 {out}（{len(png)} bytes）")

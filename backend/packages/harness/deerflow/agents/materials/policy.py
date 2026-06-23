"""Materials Capture policy — 准入⊥转存 三态解析（cfgpu-docs/materials.md §4.2, D12）。

工具结果是否记录为 material、以及记录后 ref 形态，由 **per-tool policy 三态**决定：

| policy | 准入 | 转存 | 典型 |
|---|---|---|---|
| ``rehost``   | 是 | 临期 url → fetch+upload → object_key | cfdream generate_*/task_wait/task_status |
| ``register`` | 是 | ref 保持原样（global_url 不落盘）   | image_search（结果可被 id 引用/挑选） |
| ``off``（默认）| 否 | —                                  | 未配置工具 / web_search / bash |

**默认 off**（非 register）：register 把 ref 当稳定 global_url，未知工具回临期 url 会被存成
"稳定" → 过期炸。off 保守、零意外落盘；已知产物显式 rehost、要引用的显式 register。

**解析顺序**：builtin `tool.metadata["materials_capture"]` → cfdream 内置默认（`cfdream_` 前缀 →
rehost）→ 默认 off。cfdream MCP overlay 的 config 驱动版（`materials.capture: {glob:{policy}}`）
待 api-token interceptor 配置层落地后接入；现以 cfdream_ 前缀硬编码默认兜底（与 §4.3 出口签发
同一前缀约定）。**rehost 的准入信号不靠工具名猜**：cfdream 媒体结果自声明顶层 ``artifact: true``
（result_structure.json 权威），非媒体结果（list_models/异步 stub/error）无此标志 → rehost
policy 下也抽不到 url → 自然 no-op。故 cfdream_ 全量给 rehost 安全。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

CapturePolicy = Literal["rehost", "register", "off"]

_CFDREAM_PREFIX = "cfdream_"
_VALID: tuple[CapturePolicy, ...] = ("rehost", "register", "off")


def resolve_capture_policy(tool_name: str, tool_metadata: Mapping[str, Any] | None = None) -> CapturePolicy:
    """解析某工具结果的 Capture policy。builtin metadata 优先，其次 cfdream_ 默认 rehost，否则 off。"""
    if tool_metadata:
        declared = tool_metadata.get("materials_capture")
        if declared in _VALID:
            return declared  # type: ignore[return-value]
    if tool_name.startswith(_CFDREAM_PREFIX):
        return "rehost"
    return "off"

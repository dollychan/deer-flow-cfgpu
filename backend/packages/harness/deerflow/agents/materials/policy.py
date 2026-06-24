"""Materials Capture policy — 准入⊥转存 三态解析 + url 抽取路径解析（cfgpu-docs/materials.md §4.2, D12）。

工具结果是否记录为 material、以及记录后 ref 形态，由 **per-tool policy 三态**决定：

| policy | 准入 | 转存 | 典型 |
|---|---|---|---|
| ``rehost``   | 是 | 临期 url → fetch+upload → object_key | cfdream generate_*/task_wait/task_status |
| ``register`` | 是 | ref 保持原样（global_url 不落盘）   | image_search（结果可被 id 引用/挑选） |
| ``off``（默认）| 否 | —                                  | 未配置工具 / web_search / bash |

**默认 off**（非 register）：register 把 ref 当稳定 global_url，未知工具回临期 url 会被存成
"稳定" → 过期炸。off 保守、零意外落盘；已知产物显式 rehost、要引用的显式 register。

**配置驱动（D13+，cfdream_ 前缀硬编码已退役）**：policy 与 url 抽取路径均来自 ``AgentConfig``，
经 ``lead_agent`` 的 factory 喂进 ``MaterialsMiddleware`` 构造（与 ``tool_visibility`` →
``MessageStreamMiddleware`` 同构）。**解析顺序**：builtin ``tool.metadata`` → 配置 fnmatch
首匹配 → 默认（policy=off / url_path=None）。无任何内置工具名默认 —— 要捕获的工具（含 cfdream
生成系）必须在 ``materials_capture`` 显式配 policy，且在 ``materials_url_path`` 配 url 字段路径，
否则抽不到 url → 自然 no-op。**rehost 与 register 共用同一抽取入口**（``materials_url_path``）：
不再扫自由文本、不靠工具名猜字段——按声明式 JSON 字段路径读 url。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from fnmatch import fnmatch
from typing import Any, Literal

CapturePolicy = Literal["rehost", "register", "off"]

_VALID: tuple[CapturePolicy, ...] = ("rehost", "register", "off")


def resolve_capture_policy(
    tool_name: str,
    tool_metadata: Mapping[str, Any] | None = None,
    capture_patterns: Sequence[tuple[str, str]] | None = None,
) -> CapturePolicy:
    """解析某工具结果的 Capture policy。builtin metadata 优先，其次配置 fnmatch 首匹配，否则 off。

    无内置工具名默认（cfdream_ 前缀硬编码已退役）：要捕获的工具必须在 ``materials_capture``
    显式配 policy，否则一律 off（零意外落盘）。
    """
    if tool_metadata:
        declared = tool_metadata.get("materials_capture")
        if declared in _VALID:
            return declared  # type: ignore[return-value]
    for pattern, policy in capture_patterns or ():
        if policy in _VALID and fnmatch(tool_name, pattern):
            return policy  # type: ignore[return-value]
    return "off"


def resolve_url_path(
    tool_name: str,
    tool_metadata: Mapping[str, Any] | None = None,
    url_path_patterns: Sequence[tuple[str, str]] | None = None,
) -> str | None:
    """解析某工具结果里 url 的 JSON 字段路径（rehost/register 共用抽取入口）。

    builtin metadata 优先，其次配置 fnmatch 首匹配，否则 None（抽不到 → 该结果无产物准入）。
    """
    if tool_metadata:
        declared = tool_metadata.get("materials_url_path")
        if isinstance(declared, str) and declared:
            return declared
    for pattern, path in url_path_patterns or ():
        if isinstance(path, str) and path and fnmatch(tool_name, pattern):
            return path
    return None

"""Materials-aware summarization (cfgpu-docs/materials.md §7, materials-impl-plan.md P5).

§7 一致性契约的「核心新增」：summarization 把历史消息压成纯文本时，素材的 url/描述/意图
会被压糊或压没（关联性断裂）。本子类在 summary prompt 末尾注入**当前素材清单 + id 铁律**，
让摘要本身保留 id 级关联（id 短到压不坏、是注册表主键永久可解析；url 长且会被压烂故禁止入摘要）。

**不改 deerflow 原 middleware**：纯子类化 ``DeerFlowSummarizationMiddleware``，只 override
``_build_summary_prompt`` 追加素材段 + ``before_model``/``abefore_model`` 搬运 materials。

**为何走 ContextVar**：父类 ``_build_summary_prompt(messages_to_summarize)`` 签名不带 state，
而 materials 在 ``state["materials"]``；中间件实例被跨并发 run 缓存复用（见父类 __init__ 注释，
明确禁止 await 窗口内在实例上 stash 状态）。ContextVar 是 task/线程隔离的，``before_model``
入口 set、``_build_summary_prompt`` get、finally reset，并发安全且不泄漏。地基不变：父类
``_maybe_summarize`` 只读写 ``state["messages"]``，materials 注册表 100% 不被压缩触碰（§7①）。
"""

from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING, override

from deerflow.agents.materials.middleware import render_materials_ledger
from deerflow.agents.middlewares.summarization_middleware import DeerFlowSummarizationMiddleware

if TYPE_CHECKING:
    from langchain.agents.middleware.types import AgentState
    from langchain_core.messages import AnyMessage
    from langgraph.runtime import Runtime

    from deerflow.agents.materials.types import Material

# task/线程隔离的 materials 快照：before_model 入口写入、_build_summary_prompt 读取。
# 默认空 dict（无 materials 时 section 为 None，prompt 原样）。
_materials_ctx: contextvars.ContextVar[dict[str, Material]] = contextvars.ContextVar("materials_summary_ctx", default={})


def _build_materials_summary_section(materials: dict[str, Material] | None) -> str | None:
    """素材清单 + id 铁律段（§7③）。空台账→None（不追加）。复用台账渲染器 → **零 url**（I9）。"""
    ledger = render_materials_ledger(materials)
    if ledger is None:
        return None
    return (
        "已知素材清单（压缩后仍须保留 id 级关联）：\n"
        f"{ledger}\n"
        "铁律：摘要中素材一律用 [mN] id 指代，禁止复述其 url/object_key；"
        "保留素材与用户意图、素材间衍生关系的对应（如『用户要求将 m3 转为视频→m4』）。"
    )


class MaterialsSummarizationMiddleware(DeerFlowSummarizationMiddleware):
    """summarization + 素材一致性契约（§7）：摘要 prompt 末尾注入素材清单与 id 铁律。"""

    @property
    @override
    def name(self) -> str:
        """保持父类名作为 LangGraph 节点/update key——前端按 ``DeerFlowSummarizationMiddleware.before_model``
        识别摘要 SSE 事件，子类化不得改 key（否则前端认不出）。仅换实现、不换对外契约名。"""
        return "DeerFlowSummarizationMiddleware"

    @override
    def before_model(self, state: AgentState, runtime: Runtime) -> dict | None:
        token = _materials_ctx.set(state.get("materials") or {})
        try:
            return super().before_model(state, runtime)
        finally:
            _materials_ctx.reset(token)

    @override
    async def abefore_model(self, state: AgentState, runtime: Runtime) -> dict | None:
        token = _materials_ctx.set(state.get("materials") or {})
        try:
            return await super().abefore_model(state, runtime)
        finally:
            _materials_ctx.reset(token)

    @override
    def _build_summary_prompt(self, messages_to_summarize: list[AnyMessage]) -> str | None:
        base = super()._build_summary_prompt(messages_to_summarize)
        if base is None:
            return None
        section = _build_materials_summary_section(_materials_ctx.get())
        return f"{base}\n\n{section}" if section else base

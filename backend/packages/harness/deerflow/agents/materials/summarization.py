"""Materials-aware summarization (cfgpu-docs/materials.md §7, materials-impl-plan.md P5).

§7 一致性契约：summarization 把历史消息压成纯文本时，素材的 url/描述/意图会被压糊或压没
（关联性断裂）。本子类在 summary prompt 末尾注入一段**纯素材指令**（**不再注入素材清单本身**）：
用 id 指代、禁复述 url、只保留用户明确表达过的关系，并**严禁给素材编造判定/反馈**。

为何从「注入清单」改为「纯指令」（否决旧设计）：live ``<materials>`` 台账（MaterialsMiddleware
每轮 ``wrap_model_call`` 重注）已是素材存在与状态的唯一实时 SSOT，id 级关联从不因摘要丢失。旧版
把当前全量台账塞进 summary prompt 让摘要复述——既冗余，又会诱使摘要模型把「上一素材不满意→重试」
的历史模式**投射到刚生成、用户尚未评价的最新素材**上，伪造出「用户要求重新生成」的假指令，driving
agent 丢掉好产物空转（trace 实证）。故只留纯指令，素材清单一律归 live 台账。

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
    """素材一致性**纯指令**段（§7③）。空台账→None（不追加）。

    **不注入素材清单**（区别于旧版）：素材存在/状态归 live ``<materials>`` 台账（SSOT），本段只
    约束摘要模型「怎么提素材」——用 id、禁 url、只留用户明确说过的关系、**严禁编造或泛化判定/反馈**。
    末句显式压过 base prompt 的 ``## ARTIFACTS`` / 逐项判定要求（summary_prompt=null 用 langchain
    默认模板，其结构本会诱导给素材编状态，见 §7）。
    """
    if not materials:
        return None
    return (
        "关于素材：下方 <materials> 台账是素材存在与状态的唯一实时真相，"
        "摘要**不要**输出任何素材/artifacts 清单或对素材逐条描述。\n"
        "引用素材只用 [mN] id（禁止复述其 url/object_key）。\n"
        "只保留对话中**用户明确表达过**的素材↔意图、素材间衍生关系（如『用户要求将 m3 转为视频→m4』）。\n"
        "严禁给任何素材附加『第几次/成功/失败/用户是否满意/需重新生成』之类判定，除非用户对该素材 id "
        "明确说过；绝不把针对某素材的反馈泛化或投射到另一个素材（尤其是最新生成、用户尚未评价的素材）。\n"
        "（若与上文要求输出 ARTIFACTS 清单或逐项判定的指示冲突，以本段为准。）"
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

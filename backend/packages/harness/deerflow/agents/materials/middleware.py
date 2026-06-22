"""Unified materials middleware (cfgpu-docs/materials.md §5, materials-impl-plan.md P1–P4).

§4.1 Ingest / §4.2 Capture / §4.3 Resolve / §6 台账 四个角色不是四条独立策略，而是
**同一个 registry 子系统的四个 hook**（共享 ``materials`` channel + ``stable_ref→id``
索引 + ``resolve_or_register`` 原语，且 Resolve/Capture 都要贴最内工具层）。统一实现为
这一个 ``MaterialsMiddleware``，渐进长大、factory 注册一次：

- ``before_agent``      = Ingest（§4.1）—— **见下方说明，消费侧已承载，故 P1 暂空**。
- ``wrap_tool_call``    = pre ``_resolve_outgate``（§4.3, P2）+ post ``_capture``（§4.2, P3）。
- ``wrap_model_call``   = ``<materials>`` 台账注入（§6, P4）。

洋葱位（§5）：放 MessageStream 内层 [26]，使 ``_resolve_outgate`` 见最终入参、``_capture``
在 emit 前已稳定化。护栏：``_resolve_outgate`` 签发的 presigned 只活在流向 cfgpu 的
``request``，``_capture`` 只读 ``result.artifact`` 不读 ``request`` → 凭证不回灌 content（I9）。

P1 现状（骨架）：上行素材登记发生在**消费侧** ``agent_runner._normalize_messages``——这是
唯一能保证「url 永不进入持久化 HumanMessage / 首个 checkpoint」的位置（before_agent 是图内
节点，其重写晚于首个 checkpoint）。因此本类的 ``before_agent`` 在 P1 无消费侧职责，留作
未来图内 ingest（如 gateway uploaded_files 路径）的挂点。**首个行为 hook 在 P2 落地并接入
factory**；在此之前本类不注册进链，避免一个 no-op 中间件。
"""

from __future__ import annotations

from langchain.agents.middleware import AgentMiddleware


class MaterialsMiddleware(AgentMiddleware):
    """Registry 子系统的统一中间件外壳（P1 骨架；行为 hook 自 P2 起渐进添加）。"""

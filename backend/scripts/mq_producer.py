#!/usr/bin/env python3
"""MQ Test Producer — sends protocol v2.3 messages to $AGENT_TASKS / $AGENT_SIGNALS.

Used to drive and verify Consumer behaviour end-to-end.

Usage (from backend/ directory):
    # List all scenarios
    python scripts/mq_producer.py --list

    # Send a single scenario
    python scripts/mq_producer.py simple
    python scripts/mq_producer.py followup
    python scripts/mq_producer.py ping

    # Dry-run: print envelope JSON without sending
    python scripts/mq_producer.py simple --dry-run

    # Override thread/user IDs (useful for multi-step scenarios)
    python scripts/mq_producer.py hil --thread-id <thread_id> --user-id <user_id>

    # Override connection from CLI (skips config.yaml lookup)
    python scripts/mq_producer.py simple \\
        --endpoint rmq-host:8080 \\
        --username mykey \\
        --password mysecret \\
        --task-topic AGENT_TASKS \\
        --signal-topic AGENT_SIGNALS

Connection is loaded from config.yaml consumer: section by default.
$ENV_VAR references in config.yaml are resolved from the environment.

Scenarios
---------
Task scenarios (→ $AGENT_TASKS):
  simple        基础文本任务，流式输出，默认配置
  thinking      开启 extended thinking 模式
  no_web        禁用 web_search（web group 工具不加载）
  stream_false  stream_events=false，结果包含 final_state
  timeout       timeout_seconds=10，预期触发 AGENT_TIMEOUT error
  followup      同一 thread 快速连发两条 task，第二条进 followup 队列
  reject        同一 thread 快速连发两条 task，第二条 message_mode=reject
  hil           ask=true，预期 agent 在高花费工具前暂停并推送 tool_approval_required
  hil_resume    HIL resume（command 消息），需传 --thread-id 和 --tool-call-id
  multimodal    多模态内容（图片 + 文档 URL）
  custom_agent  指定 agent_name（需本地存在对应 agent 配置）
  multiturn     同一 thread 两轮对话：第一条任务完成后再发第二条

Signal scenarios (→ $AGENT_SIGNALS):
  ping          广播 ping，任意 Consumer 实例回复
  ping_target   定向 ping 指定 instance_id（需传 --instance-id）
  cancel        取消指定 thread 的运行中任务（需传 --thread-id）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any


# ── Envelope builder ──────────────────────────────────────────────────────────

SCHEMA_VERSION = "2.3"


def _now_iso() -> str:
    dt = datetime.now(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _uuid() -> str:
    return str(uuid.uuid4())


def build_envelope(
    *,
    type: str,
    thread_id: str,
    payload: dict,
    message_id: str | None = None,
    agent_name: str = "lead_agent",
    user_id: str | None = None,
    project_id: str | None = None,
) -> dict:
    """Build a protocol v2.3 message envelope."""
    return {
        "schema_version": SCHEMA_VERSION,
        "message_id": message_id or _uuid(),
        "message_seq": 0,
        "timestamp": _now_iso(),
        "type": type,
        "payload": payload,
        "thread_id": thread_id,
        "agent_name": agent_name,
        "user_id": user_id,
        "project_id": project_id,
    }


def task_payload(
    *,
    messages: list[dict] | None = None,
    command: dict | None = None,
    thinking_enabled: bool = False,
    web_search_enabled: bool = True,
    ask: bool = False,
    timeout_seconds: int | None = None,
    message_mode: str = "followup",
    stream_events: bool = True,
    stream_event_types: list[str] | None = None,
    models: dict | None = None,
    subagent_enabled: bool = False,
    reasoning_effort: str | None = None,
) -> dict:
    """Build a task payload dict."""
    cfg: dict[str, Any] = {
        "thinking_enabled": thinking_enabled,
        "web_search_enabled": web_search_enabled,
        "ask": ask,
        "message_mode": message_mode,
    }
    if timeout_seconds is not None:
        cfg["timeout_seconds"] = timeout_seconds
    if models is not None:
        cfg["models"] = models
    if subagent_enabled:
        cfg["subagent_enabled"] = True
    if reasoning_effort is not None:
        cfg["reasoning_effort"] = reasoning_effort

    reply_cfg: dict[str, Any] = {"stream_events": stream_events}
    if stream_event_types is not None:
        reply_cfg["stream_event_types"] = stream_event_types
    else:
        reply_cfg["stream_event_types"] = ["messages", "custom", "values"]

    return {
        "messages": messages,
        "command": command,
        "config": cfg,
        "reply_config": reply_cfg,
    }


def text_message(text: str) -> dict:
    return {"role": "user", "content": [{"type": "text", "text": text}]}


def multimodal_message(text: str, image_url: str | None = None, document_url: str | None = None) -> dict:
    content: list[dict] = [{"type": "text", "text": text}]
    if image_url:
        content.append({"type": "image_url", "url": [image_url]})
    if document_url:
        content.append({"type": "document_url", "url": [document_url]})
    return {"role": "user", "content": content}


# ── Scenario definitions ──────────────────────────────────────────────────────

class Scenario:
    """A named set of envelopes to send."""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    def build(self, args: argparse.Namespace) -> list[tuple[str, dict]]:
        """Return list of (topic, envelope) pairs."""
        raise NotImplementedError


class SimpleTextScenario(Scenario):
    """最简单的文本任务。验证：基础 task 路由、LangGraph 执行、progress/result 回调。"""

    def __init__(self):
        super().__init__("simple", "基础文本任务，流式输出，所有默认配置")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        user_id = args.user_id or "test-user-001"
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("用一句话解释什么是量子纠缠。")],
            ),
            user_id=user_id,
        )
        _print_info(f"thread_id={thread_id}  user_id={user_id}")
        return [(args.task_topic, envelope)]


class ThinkingScenario(Scenario):
    """开启 thinking_enabled，验证 extended thinking 模式下 is_plan_mode=True，TodoMiddleware 激活。"""

    def __init__(self):
        super().__init__("thinking", "开启 extended thinking 模式 (thinking_enabled=true)")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("列出 5 个提高工作效率的方法，并给出优先级排序。")],
                thinking_enabled=True,
            ),
            user_id=args.user_id or "test-user-001",
        )
        _print_info(f"thread_id={thread_id}  thinking_enabled=true → is_plan_mode=true")
        return [(args.task_topic, envelope)]


class NoWebSearchScenario(Scenario):
    """web_search_enabled=false，验证 web group 工具不被加载到 agent toolset。"""

    def __init__(self):
        super().__init__("no_web", "禁用 web search (web_search_enabled=false)")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("介绍一下 Python 的 asyncio 模块，不需要搜索网络。")],
                web_search_enabled=False,
            ),
            user_id=args.user_id or "test-user-001",
        )
        _print_info(f"thread_id={thread_id}  web_search_enabled=false")
        return [(args.task_topic, envelope)]


class StreamFalseScenario(Scenario):
    """stream_events=false，结果 payload 应包含 final_state，验证 serialize_channel_values。"""

    def __init__(self):
        super().__init__("stream_false", "禁用流式 (stream_events=false)，result 含 final_state")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("写一个 hello world 的 Python 程序。")],
                stream_events=False,
            ),
            user_id=args.user_id or "test-user-001",
        )
        _print_info(f"thread_id={thread_id}  stream_events=false → result.final_state 应有值")
        return [(args.task_topic, envelope)]


class TimeoutScenario(Scenario):
    """timeout_seconds=10，agent 跑不完则推送 AGENT_TIMEOUT error。"""

    def __init__(self):
        super().__init__("timeout", "timeout_seconds=10，预期触发 AGENT_TIMEOUT error")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("请写一篇关于人工智能发展史的 5000 字论文，要求引用最新研究文献。")],
                timeout_seconds=10,
                web_search_enabled=True,
            ),
            user_id=args.user_id or "test-user-001",
        )
        _print_info(f"thread_id={thread_id}  timeout_seconds=10 → 预期 error.code=AGENT_TIMEOUT")
        return [(args.task_topic, envelope)]


class FollowupScenario(Scenario):
    """同一 thread 快速连发两条 task。第一条被 claim，第二条进 followup 队列。
    验证：followup 入队 → 第一个 run 完成后 drain → 第二条作为新 run 执行。"""

    def __init__(self):
        super().__init__("followup", "同 thread 两条 task（默认 followup 模式），验证排队与串行执行")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        user_id = args.user_id or "test-user-001"

        msg1 = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("第一个问题：什么是光合作用？")],
            ),
            user_id=user_id,
        )
        msg2 = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("第二个问题（应进入 followup 队列）：光合作用的产物是什么？")],
                message_mode="followup",
            ),
            user_id=user_id,
        )
        _print_info(
            f"thread_id={thread_id}\n"
            f"  msg1 message_id={msg1['message_id']} → 预期被 CLAIMED\n"
            f"  msg2 message_id={msg2['message_id']} → 预期进 followup 队列，第一条完成后执行"
        )
        return [(args.task_topic, msg1), (args.task_topic, msg2)]


class RejectScenario(Scenario):
    """同一 thread 快速连发两条 task，第二条 message_mode=reject。
    验证：第二条应立即收到 AGENT_BUSY error（retriable=true），不入队。"""

    def __init__(self):
        super().__init__("reject", "同 thread 两条 task，第二条 message_mode=reject，预期 AGENT_BUSY error")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        user_id = args.user_id or "test-user-001"

        msg1 = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("请写一段关于海洋的诗歌。")],
            ),
            user_id=user_id,
        )
        msg2 = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("这条消息应被拒绝（message_mode=reject）。")],
                message_mode="reject",
            ),
            user_id=user_id,
        )
        _print_info(
            f"thread_id={thread_id}\n"
            f"  msg1 → 预期被 CLAIMED，正常执行\n"
            f"  msg2 → 预期立即收到 error.code=AGENT_BUSY retriable=true"
        )
        return [(args.task_topic, msg1), (args.task_topic, msg2)]


class HILAskScenario(Scenario):
    """ask=true，agent 在调用 approval_required_tools 前触发 HumanApprovalMiddleware。
    预期推送：progress(tool_approval_required) → result(paused_for_approval)。
    记录 thread_id，使用 hil_resume 场景发送恢复消息。"""

    def __init__(self):
        super().__init__("hil", "ask=true，触发 HIL 工具审批暂停，预期 tool_approval_required + paused_for_approval")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        user_id = args.user_id or "test-user-001"
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("请生成一张赛博朋克风格的城市夜景图片。")],
                ask=True,
            ),
            user_id=user_id,
        )
        _print_info(
            f"thread_id={thread_id}  ask=true\n"
            "  预期：progress {type:tool_approval_required, tool_calls:[...]}\n"
            "       result  {status:paused_for_approval}\n"
            "  后续：python scripts/mq_producer.py hil_resume --thread-id " + thread_id + " --tool-call-id <call_id from tool_approval_required>"
        )
        return [(args.task_topic, envelope)]


class HILResumeScenario(Scenario):
    """发送 HIL resume 消息（command 非空），恢复已暂停的 thread。
    需要 --thread-id（暂停时记录的 thread_id）和 --tool-call-id（审批事件中的 call id）。"""

    def __init__(self):
        super().__init__("hil_resume", "HIL resume（command 消息），恢复暂停的 thread（需 --thread-id --tool-call-id）")

    def build(self, args):
        if not args.thread_id:
            print("[ERROR] hil_resume 需要 --thread-id（来自 hil 场景输出）", file=sys.stderr)
            sys.exit(1)
        if not args.tool_call_id:
            print("[ERROR] hil_resume 需要 --tool-call-id（来自 tool_approval_required 事件中的 tool_calls[].id）", file=sys.stderr)
            sys.exit(1)

        thread_id = args.thread_id
        tool_call_id = args.tool_call_id
        user_id = args.user_id or "test-user-001"

        # 默认：approved，使用 LLM 生成的原始参数
        approval = {"status": "approved"}

        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload={
                "messages": None,
                "command": {
                    "update": {
                        "tool_approvals": {
                            tool_call_id: approval,
                        }
                    }
                },
                "config": {"thinking_enabled": False, "ask": True},
                "reply_config": {
                    "stream_events": True,
                    "stream_event_types": ["messages", "custom", "values"],
                },
            },
            user_id=user_id,
        )
        _print_info(
            f"thread_id={thread_id}  tool_call_id={tool_call_id}\n"
            f"  approved（原始参数）→ 预期 agent 继续执行并推送 result(success)"
        )
        return [(args.task_topic, envelope)]


class HILRejectResumeScenario(Scenario):
    """HIL resume：拒绝工具调用。"""

    def __init__(self):
        super().__init__("hil_reject", "HIL resume：拒绝工具调用（需 --thread-id --tool-call-id）")

    def build(self, args):
        if not args.thread_id:
            print("[ERROR] hil_reject 需要 --thread-id", file=sys.stderr)
            sys.exit(1)
        if not args.tool_call_id:
            print("[ERROR] hil_reject 需要 --tool-call-id", file=sys.stderr)
            sys.exit(1)

        envelope = build_envelope(
            type="task",
            thread_id=args.thread_id,
            payload={
                "messages": None,
                "command": {
                    "update": {
                        "tool_approvals": {
                            args.tool_call_id: {
                                "status": "rejected",
                                "reason": "测试拒绝：暂不生成图片",
                            }
                        }
                    }
                },
                "config": {"thinking_enabled": False, "ask": True},
                "reply_config": {
                    "stream_events": True,
                    "stream_event_types": ["messages", "custom", "values"],
                },
            },
            user_id=args.user_id or "test-user-001",
        )
        _print_info(
            f"thread_id={args.thread_id}  tool_call_id={args.tool_call_id}\n"
            "  rejected → agent 感知拒绝原因，继续执行其他步骤"
        )
        return [(args.task_topic, envelope)]


class MultimodalScenario(Scenario):
    """多模态消息（text + image_url + document_url），验证 _normalize_messages 的 content block 构建。"""

    def __init__(self):
        super().__init__("multimodal", "多模态消息（text + image_url + document_url）")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        # 使用公开可访问的示例 URL
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[
                    multimodal_message(
                        text="请描述这张图片，并解释图中的概念。",
                        image_url="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1f/Dew_on_a_spider_web.jpg/640px-Dew_on_a_spider_web.jpg",
                        document_url="https://www.w3.org/WAI/WCAG21/wcag21.pdf",
                    )
                ],
                web_search_enabled=False,
            ),
            user_id=args.user_id or "test-user-001",
        )
        _print_info(f"thread_id={thread_id}  content=[text, image_url, document_url]")
        return [(args.task_topic, envelope)]


class CustomAgentScenario(Scenario):
    """指定 agent_name，测试自定义 agent 路由（需本地存在对应 agent 配置）。"""

    def __init__(self):
        super().__init__("custom_agent", "指定 agent_name（需传 --agent-name，且本地存在对应配置）")

    def build(self, args):
        if not args.agent_name or args.agent_name == "lead_agent":
            print("[WARN] custom_agent 场景建议通过 --agent-name 指定自定义 agent 名称", file=sys.stderr)
        thread_id = args.thread_id or _uuid()
        envelope = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("你好，请介绍一下你自己的能力和专长。")],
            ),
            agent_name=args.agent_name or "lead_agent",
            user_id=args.user_id or "test-user-001",
        )
        _print_info(f"thread_id={thread_id}  agent_name={args.agent_name or 'lead_agent'}")
        return [(args.task_topic, envelope)]


class MultiturnScenario(Scenario):
    """两轮对话：发第一条，等待完成后（手动延迟）再发第二条，共用 thread_id。
    验证：LangGraph checkpointer 保留上下文，第二轮能引用第一轮内容。"""

    def __init__(self):
        super().__init__("multiturn", "两轮对话（同 thread_id，间隔 5s），验证 checkpointer 上下文保留")

    def build(self, args):
        thread_id = args.thread_id or _uuid()
        user_id = args.user_id or "test-user-001"

        msg1 = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("我叫小明，今年 25 岁。")],
            ),
            user_id=user_id,
        )
        msg2 = build_envelope(
            type="task",
            thread_id=thread_id,
            payload=task_payload(
                messages=[text_message("请回忆一下，我叫什么名字，几岁？（验证多轮上下文）")],
            ),
            user_id=user_id,
        )
        _print_info(
            f"thread_id={thread_id}\n"
            "  msg1 → 等待完成后，msg2 以同 thread_id 发出\n"
            "  msg2 应能引用 msg1 的内容（checkpointer 验证）"
        )
        return [(args.task_topic, msg1), (args.task_topic, msg2, 5.0)]


# ── Signal scenarios ──────────────────────────────────────────────────────────

class PingScenario(Scenario):
    """广播 ping，任意 Consumer 实例回复 pong。验证 instance heartbeat 和 pong 回调。"""

    def __init__(self):
        super().__init__("ping", "广播 ping，验证 Consumer 健康检查回复 pong")

    def build(self, args):
        thread_id = _uuid()  # ping 不需要真实 thread，但信封要求非空
        envelope = build_envelope(
            type="ping",
            thread_id=thread_id,
            payload={},
        )
        _print_info("广播 ping → 任意 Consumer 实例应回复 pong（$AGENT_RESULTS）")
        return [(args.signal_topic, envelope)]


class PingTargetScenario(Scenario):
    """定向 ping 指定 Consumer 实例，验证实例状态查询。"""

    def __init__(self):
        super().__init__("ping_target", "定向 ping 指定 instance（需 --instance-id）")

    def build(self, args):
        if not args.instance_id:
            print("[ERROR] ping_target 需要 --instance-id（格式：hostname-pid）", file=sys.stderr)
            sys.exit(1)
        thread_id = _uuid()
        envelope = build_envelope(
            type="ping",
            thread_id=thread_id,
            payload={"instance_id": args.instance_id},
        )
        _print_info(f"定向 ping → instance_id={args.instance_id}")
        return [(args.signal_topic, envelope)]


class CancelScenario(Scenario):
    """发送 cancel 信号，中止指定 thread 的运行中任务。"""

    def __init__(self):
        super().__init__("cancel", "取消运行中任务（需 --thread-id），预期 result.status=cancelled")

    def build(self, args):
        if not args.thread_id:
            print("[ERROR] cancel 需要 --thread-id（来自正在运行的任务）", file=sys.stderr)
            sys.exit(1)
        envelope = build_envelope(
            type="cancel",
            thread_id=args.thread_id,
            payload={"reason": "user_requested"},
        )
        _print_info(f"thread_id={args.thread_id}  reason=user_requested → 预期 result.status=cancelled")
        return [(args.signal_topic, envelope)]


# ── Registry ──────────────────────────────────────────────────────────────────

SCENARIOS: dict[str, Scenario] = {s.name: s for s in [
    SimpleTextScenario(),
    ThinkingScenario(),
    NoWebSearchScenario(),
    StreamFalseScenario(),
    TimeoutScenario(),
    FollowupScenario(),
    RejectScenario(),
    HILAskScenario(),
    HILResumeScenario(),
    HILRejectResumeScenario(),
    MultimodalScenario(),
    CustomAgentScenario(),
    MultiturnScenario(),
    PingScenario(),
    PingTargetScenario(),
    CancelScenario(),
]}


# ── Output helpers ────────────────────────────────────────────────────────────

def _print_info(msg: str) -> None:
    print(f"\n[INFO] {msg}")


def _print_envelope(topic: str, envelope: dict, index: int = 0) -> None:
    label = f"[MSG {index + 1}] topic={topic}  message_id={envelope['message_id']}"
    print(f"\n{'─' * 60}")
    print(label)
    print(json.dumps(envelope, ensure_ascii=False, indent=2))


# ── RocketMQ send ─────────────────────────────────────────────────────────────

def _send_message(producer: Any, topic: str, body: bytes) -> None:
    from rocketmq import Message

    msg = Message()
    msg.topic = topic
    msg.body = body
    producer.send(msg)


# ── Config loader ─────────────────────────────────────────────────────────────

def _load_consumer_config() -> tuple[str, str, str, str, str]:
    """Return (endpoint, username, password, task_topic, signal_topic) from config.yaml."""
    # Add backend/ to path so deerflow can be imported
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    try:
        from deerflow.config.app_config import get_app_config
        cfg = get_app_config()
        c = cfg.consumer
        return c.endpoint, c.username, c.password, c.task_topic, c.signal_topic
    except Exception as exc:
        print(f"[WARN] Failed to load config.yaml ({exc}); use --endpoint etc. to provide connection", file=sys.stderr)
        return "", "", "", "$AGENT_TASKS", "$AGENT_SIGNALS"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("scenario", nargs="?", help="Scenario name to run (see --list)")
    parser.add_argument("--list", action="store_true", help="List all scenarios and exit")
    parser.add_argument("--dry-run", action="store_true", help="Print envelopes without sending")

    # Connection overrides
    parser.add_argument("--endpoint", default="", help="RocketMQ gRPC endpoint (host:port)")
    parser.add_argument("--username", default="", help="RocketMQ access key")
    parser.add_argument("--password", default="", help="RocketMQ secret key")
    parser.add_argument("--task-topic", default="", dest="task_topic", help="Task topic name")
    parser.add_argument("--signal-topic", default="", dest="signal_topic", help="Signal topic name")

    # Scenario parameters
    parser.add_argument("--thread-id", default="", dest="thread_id", help="Thread ID (generated if omitted)")
    parser.add_argument("--user-id", default="", dest="user_id", help="User ID")
    parser.add_argument("--agent-name", default="lead_agent", dest="agent_name", help="Agent name")
    parser.add_argument("--tool-call-id", default="", dest="tool_call_id", help="Tool call ID for HIL resume")
    parser.add_argument("--instance-id", default="", dest="instance_id", help="Consumer instance ID for ping_target")

    args = parser.parse_args()

    if args.list:
        print(f"\n{'Scenario':<16} Description")
        print("─" * 70)
        for s in SCENARIOS.values():
            print(f"  {s.name:<14} {s.description}")
        print()
        return

    if not args.scenario:
        parser.print_help()
        return

    scenario = SCENARIOS.get(args.scenario)
    if scenario is None:
        print(f"[ERROR] Unknown scenario '{args.scenario}'. Run --list to see options.", file=sys.stderr)
        sys.exit(1)

    # Resolve connection settings: CLI args > config.yaml
    cfg_endpoint, cfg_user, cfg_pass, cfg_task, cfg_signal = _load_consumer_config()
    endpoint = args.endpoint or cfg_endpoint
    username = args.username or cfg_user
    password = args.password or cfg_pass
    args.task_topic = args.task_topic or cfg_task
    args.signal_topic = args.signal_topic or cfg_signal

    # Build messages
    print(f"\n{'═' * 60}")
    print(f"  Scenario : {scenario.name}")
    print(f"  Topic T  : {args.task_topic}")
    print(f"  Topic S  : {args.signal_topic}")
    print(f"  Dry-run  : {args.dry_run}")
    print(f"{'═' * 60}")

    entries = scenario.build(args)  # list of (topic, envelope) or (topic, envelope, delay_s)

    for i, entry in enumerate(entries):
        topic = entry[0]
        envelope = entry[1]
        delay = entry[2] if len(entry) > 2 else 0.0

        _print_envelope(topic, envelope, i)

        if args.dry_run:
            print("[DRY-RUN] Skipping send.")
            continue

        if not endpoint:
            print("[ERROR] No RocketMQ endpoint configured. Use --endpoint or set consumer.endpoint in config.yaml.", file=sys.stderr)
            sys.exit(1)

        if delay > 0 and i > 0:
            print(f"\n[INFO] Waiting {delay:.1f}s before sending next message...")
            time.sleep(delay)

        try:
            from rocketmq import ClientConfiguration, Credentials, Producer

            credentials = Credentials(username, password)
            client_config = ClientConfiguration(endpoint, credentials, request_timeout=10)
            producer = Producer(client_config, (topic,))
            producer.startup()

            body = json.dumps(envelope, ensure_ascii=False).encode()
            _send_message(producer, topic, body)
            print(f"[SENT] message_id={envelope['message_id']} → topic={topic}")

            producer.shutdown()

        except ImportError:
            print("[ERROR] rocketmq package not installed. Install with: pip install rocketmq", file=sys.stderr)
            sys.exit(1)
        except Exception as exc:
            print(f"[ERROR] Failed to send: {exc}", file=sys.stderr)
            sys.exit(1)

    print(f"\n[DONE] {len(entries)} message(s) sent.\n")


if __name__ == "__main__":
    main()

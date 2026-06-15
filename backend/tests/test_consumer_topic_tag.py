"""task_topic_tag wiring — uplink filter + downlink stamping correspondence.

Covers two halves of the same routing tag:
  1. The uplink subscription filter (`_make_task_filter`) uses RocketMQ's native
     TAG filter for an exact single-tag match (not SQL property-filter, which
     requires broker opt-in), and falls back to "*" when no tag is configured.
  2. The downlink producer adapter (`_RocketMQProducerAdapter`) stamps the same
     tag on every published message, so result/progress/error replies route in
     correspondence with the task subscription.
"""

from __future__ import annotations

import pytest

from app.consumer.__main__ import _make_task_filter, _RocketMQProducerAdapter

# ── Uplink filter ───────────────────────────────────────────────────────────


def test_make_task_filter_uses_tag_type_for_exact_match():
    from rocketmq import FilterExpression
    from rocketmq.grpc_protocol import FilterType

    expr = _make_task_filter("my-tag")

    assert isinstance(expr, FilterExpression)
    assert expr.expression == "my-tag"
    # Must be the native TAG filter — SQL property-filter requires broker opt-in.
    assert expr.filter_type == FilterType.TAG


def test_make_task_filter_empty_tag_accepts_all():
    expr = _make_task_filter("")

    assert expr.expression == "*"


# ── Downlink stamping ─────────────────────────────────────────────────────────


class _CapturingProducer:
    """Sync RocketMQ producer stub that records the Message it is asked to send."""

    def __init__(self) -> None:
        self.sent: list = []

    def send(self, msg) -> None:
        self.sent.append(msg)


@pytest.mark.asyncio
async def test_producer_adapter_stamps_tag_on_downlink():
    producer = _CapturingProducer()
    adapter = _RocketMQProducerAdapter(
        producer,
        result_topic="RESULTS",
        tag="my-tag",
        executor=None,
    )

    await adapter.send_async(b"{}", keys="mid-1")

    assert len(producer.sent) == 1
    msg = producer.sent[0]
    assert msg.topic == "RESULTS"
    assert msg.tag == "my-tag"
    assert "mid-1" in msg.keys


@pytest.mark.asyncio
async def test_producer_adapter_no_tag_leaves_tag_unset():
    producer = _CapturingProducer()
    adapter = _RocketMQProducerAdapter(
        producer,
        result_topic="RESULTS",
        executor=None,
    )

    await adapter.send_async(b"{}")

    msg = producer.sent[0]
    assert msg.tag is None


@pytest.mark.asyncio
async def test_uplink_and_downlink_share_one_tag():
    """The filter expression and the stamped downlink tag are the same string."""
    tag = "instance-A"

    expr = _make_task_filter(tag)

    producer = _CapturingProducer()
    adapter = _RocketMQProducerAdapter(producer, result_topic="RESULTS", tag=tag, executor=None)
    await adapter.send_async(b"{}")

    assert expr.expression == producer.sent[0].tag == tag

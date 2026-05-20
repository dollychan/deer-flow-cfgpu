"""Configuration model for the Consumer process."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ConsumerConfig(BaseModel):
    """RocketMQ connection and runtime settings for the Consumer process.

    Config path: ``consumer:`` section in config.yaml.
    All string fields support ``$ENV_VAR`` substitution via AppConfig.resolve_env_variables.

    Example config.yaml snippet::

        consumer:
          endpoint: $ROCKETMQ_ENDPOINT         # e.g. "rmq-cn-xxx.aliyuncs.com:8080"
          username: $ROCKETMQ_USERNAME
          password: $ROCKETMQ_PASSWORD
          task_topic: $AGENT_TASKS
          signal_topic: $AGENT_SIGNALS
          result_topic: $AGENT_RESULTS
          consumer_group: $AGENT_CONSUMER_GROUP
          signal_consumer_group: $AGENT_SIGNAL_CONSUMER_GROUP
          max_concurrent_runs: 10
    """

    # ── MQ connection ─────────────────────────────────────────────────────────
    endpoint: str = Field(default="", description="RocketMQ gRPC endpoint (host:port)")
    username: str = Field(default="", description="RocketMQ access key (ROCKETMQ_USERNAME)")
    password: str = Field(default="", description="RocketMQ secret key (ROCKETMQ_PASSWORD)")

    # ── Topics and consumer groups ────────────────────────────────────────────
    task_topic: str = Field(default="$AGENT_TASKS", description="Incoming topic: task messages only")
    signal_topic: str = Field(default="$AGENT_SIGNALS", description="Incoming topic: cancel / ping control signals")
    result_topic: str = Field(default="$AGENT_RESULTS", description="Outgoing topic: progress / result / error / pong messages")
    consumer_group: str = Field(default="$AGENT_CONSUMER_GROUP", description="RocketMQ consumer group ID for task topic (AGENT_CONSUMER_GROUP)")
    signal_consumer_group: str = Field(default="$AGENT_SIGNAL_CONSUMER_GROUP", description="RocketMQ consumer group ID for signal topic (AGENT_SIGNAL_CONSUMER_GROUP)")
    task_topic_tag: str = Field(default="", description="Optional tag filter on task_topic (empty = accept all)")
    signal_topic_tag: str = Field(default="", description="Optional tag filter on signal_topic (empty = accept all)")

    # ── Runtime limits ────────────────────────────────────────────────────────
    max_concurrent_runs: int = Field(default=10, description="Max simultaneous agent runs per Consumer instance")
    poll_batch_size: int = Field(default=20, description="Max messages fetched per SimpleConsumer.receive() call")
    invisible_duration_seconds: int = Field(default=300, description="RocketMQ message lease (must exceed max agent run time)")
    processed_messages_ttl_days: int = Field(default=7, description="Days to retain processed_messages records before cleanup (0 = disabled)")

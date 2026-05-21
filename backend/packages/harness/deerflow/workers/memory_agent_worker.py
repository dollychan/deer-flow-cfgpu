"""Memory Agent Update Worker.

Single-instance worker that serialises writes to the ``memory_agent`` table.
Because only this worker writes ``MemoryAgentRow`` rows, no optimistic locking
is needed.

In the current deployment the worker is embedded in the same process as the
MLM queue (``MlmUpdateQueue._process_one`` calls ``repo.upsert_agent`` directly)
so this module is only needed when the agent update pathway is separated into a
dedicated MQ consumer (distributed mode).

Typical usage (single-instance consumer process)::

    worker = MemoryAgentWorker()
    await worker.run()
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

from deerflow.persistence.memory.repository import get_memory_repository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Message schema
# ---------------------------------------------------------------------------


@dataclass
class AgentKnowledgeEvent:
    """Payload published to the agent-knowledge MQ topic."""

    agent_name: str
    facts: list[dict]
    summary: str | None = None


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class MemoryAgentWorker:
    """Consume :class:`AgentKnowledgeEvent` messages and upsert ``memory_agent``.

    In the default (embedded) mode the MQ consumer is replaced by an
    ``asyncio.Queue`` so the worker can be tested and used without a real MQ
    broker.  Swap :meth:`_receive` for a real RocketMQ consumer in distributed
    deployments.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[AgentKnowledgeEvent | None] = asyncio.Queue()
        self._running = False

    async def run(self) -> None:
        """Process events until :meth:`stop` is called."""
        self._running = True
        logger.info("MemoryAgentWorker started")
        try:
            while self._running:
                event = await self._receive()
                if event is None:
                    break
                await self._handle(event)
        finally:
            logger.info("MemoryAgentWorker stopped")

    async def stop(self) -> None:
        """Signal the worker to stop after the current event."""
        self._running = False
        await self._queue.put(None)  # unblock `_receive`

    async def publish(self, event: AgentKnowledgeEvent) -> None:
        """Enqueue an event for processing (embedded mode)."""
        await self._queue.put(event)

    async def _receive(self) -> AgentKnowledgeEvent | None:
        """Return the next event from the internal queue."""
        return await self._queue.get()

    async def _handle(self, event: AgentKnowledgeEvent) -> None:
        """Merge the event's facts into the agent's memory row."""
        repo = get_memory_repository()
        if repo is None:
            logger.debug("MemoryAgentWorker: no repository, skipping agent=%s", event.agent_name)
            return

        try:
            ok = await repo.upsert_agent(event.agent_name, event.facts, event.summary)
            if ok:
                logger.info("MemoryAgentWorker: updated agent=%s facts=%d", event.agent_name, len(event.facts))
            else:
                logger.warning("MemoryAgentWorker: upsert_agent returned False for agent=%s", event.agent_name)
        except Exception:
            logger.exception("MemoryAgentWorker: failed to update agent=%s", event.agent_name)

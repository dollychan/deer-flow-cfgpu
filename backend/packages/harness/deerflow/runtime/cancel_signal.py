"""Task-local cooperative-cancel signal (BUG-009).

Why this exists
---------------
cfgpu generate has **no remote cancel API**: once a task runs it always produces a
billed result. A hard ``runner_task.cancel()`` landing mid-poll orphans that remote
task — its ``task_id`` never reaches a checkpoint, so the consumer can neither read
nor reclaim the result it paid for (see ``cfgpu-docs/cancel.md`` §1).

:class:`~deerflow.agents.middlewares.uninterruptible_tool_middleware.UninterruptibleToolMiddleware`
therefore *shields* matched tool calls and, instead of propagating the cancel,
raises a **cooperative** flag so the run can stop cleanly at the next super-step
boundary — after the tool result has been checkpointed.

That flag must be visible across three places that all run inside the **same**
asyncio task of one run:

  - ``AgentRunner._run``                 — creates the Event and installs it here.
  - ``UninterruptibleToolMiddleware``    — sets the Event when it swallows a cancel.
  - ``AgentRunner._execute`` astream loop — reads ``is_set()`` at step boundaries.

A task-local :class:`~contextvars.ContextVar` (the same pattern as
``deerflow.runtime.user_context.set_current_user``) is the cleanest carrier: it needs
no serialization into ``runnable_config`` and does not leak across concurrent runs —
each run is its own asyncio task, and ``asyncio.create_task`` snapshots the context,
so child tasks see the same Event object while sibling runs stay isolated
(``cfgpu-docs/cancel.md`` §3.1 / §4.1.1).

Standalone-safe
---------------
When no Event is installed (the middleware runs outside the consumer integration),
:func:`signal_cooperative_cancel` returns ``False`` and the middleware falls back to
re-raising the cancel — the protected tool still runs to completion (no orphan), but
the cancel is honored rather than silently dropped.
"""

from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Final


@dataclass
class CancelState:
    """Shared per-run cancel carrier: the cooperative Event plus an in-flight count
    of protected (non-interruptible) tools.

    Mutated from several tasks of the **same** run (the run task, its langgraph node
    sub-tasks, and the sibling cancel-watcher). Because it is a single mutable object
    referenced through a task-local :class:`~contextvars.ContextVar` *and* handed to
    the watcher explicitly, every task sees the same live counter/Event — concurrent
    runs stay isolated (each run installs its own object).

    ``protected_in_flight`` is why the watcher can keep the hard cancel **conditional**
    (BUG-009 / ``cfgpu-docs/cancel.md`` §4.3): a hard ``runner_task.cancel()`` tears
    down ``astream`` and discards the *already-emitted* ``tool_result`` of a shielded
    tool that drained. So the watcher hard-cancels only when no protected tool is in
    flight (LLM / bash keep their immediate interrupt); while one is draining it sets
    the Event and lets the run stop cooperatively at the next super-step boundary,
    after the result has been downlinked and checkpointed.
    """

    event: asyncio.Event
    protected_in_flight: int = field(default=0)


_cancel_state: Final[ContextVar[CancelState | None]] = ContextVar("deerflow_cancel_state", default=None)


def install_cancel_event(event: asyncio.Event) -> Token[CancelState | None]:
    """Install *event* (wrapped in a fresh :class:`CancelState`) as this task's signal.

    Returns a reset token to pass to :func:`reset_cancel_event` in a ``finally``
    block. Call once at the top of a run, alongside ``runner_task = current_task()``.
    """
    return _cancel_state.set(CancelState(event=event))


def reset_cancel_event(token: Token[CancelState | None]) -> None:
    """Restore the previous cancel-state context using the install token."""
    _cancel_state.reset(token)


def get_cancel_state() -> CancelState | None:
    """Return the installed :class:`CancelState` for this task, or ``None``.

    Hand this to the cancel-watcher (a sibling task that does not inherit later
    ContextVar mutations) so it reads the same live ``event`` / ``protected_in_flight``.
    """
    return _cancel_state.get()


def signal_cooperative_cancel() -> bool:
    """Set the installed cancel Event, if any.

    Returns ``True`` when an Event was installed and set (cooperative stop will be
    observed at the next super-step boundary), ``False`` when none is installed
    (caller should honor the cancel itself — see module docstring).
    """
    state = _cancel_state.get()
    if state is not None:
        state.event.set()
        return True
    return False


def enter_protected_tool() -> None:
    """Mark that a protected (non-interruptible) tool call has started on this run.

    Brackets the shielded tool execution so the cancel-watcher can see one is in
    flight and withhold the hard cancel. No-op when no carrier is installed.
    """
    state = _cancel_state.get()
    if state is not None:
        state.protected_in_flight += 1


def exit_protected_tool() -> None:
    """Mark that a protected tool call has finished (pair with :func:`enter_protected_tool`)."""
    state = _cancel_state.get()
    if state is not None and state.protected_in_flight > 0:
        state.protected_in_flight -= 1

"""Middleware that repairs stale / cross-object presigned URLs in tool-call args.

Why this exists
---------------
``present_urls`` stores remote file URLs (cfgpu / OSS / TOS / S3 ... presigned
links) verbatim into ``ThreadState.artifacts``. Those URLs carry a per-object,
time-bound signature in their query string (``?Expires=...&Signature=...`` for
Aliyun OSS, ``?X-Tos-Algorithm=...&X-Tos-Signature=...`` for Volcengine TOS, an
``X-Amz-*`` block for S3, ...). The signature is bound to a single object key and
**cannot** be reused on a different path — doing so fails with HTTP 403.

When ``SummarizationMiddleware`` compresses the conversation, tool results get
truncated and may lose those query strings. A model that later builds a download
command (e.g. ``curl``) from the lossy summary then reconstructs URLs — typically
keeping each object's correct *path* but pasting one object's signature onto
several different paths — producing invalid URLs that 403.

``artifacts`` is a dedicated state field that summarization never touches, so it
always holds the correct, full URLs. This middleware intercepts every tool call
via ``wrap_tool_call``, scans the args for URLs, and for any URL whose **object
identity** (scheme + host + path, query ignored) matches an artifact, rewrites it
to that artifact's authoritative full URL. The fix is deterministic and does not
rely on the model getting the signature right.

Design notes
------------
- Matching is **provider-agnostic**: it strips the entire query string and never
  parses ``Expires`` / ``Signature`` / ``X-Tos-*`` / ``X-Amz-*``. The whole URL
  token is replaced, so any presigning scheme works the same way.
- It only rewrites URLs that match a known artifact by path. Unknown / external
  URLs are left untouched (safe by default).
- The residual gap it cannot fix is a URL whose *path* was also corrupted — there
  is no object identity left to match on. The dominant failure mode (signature
  reuse across objects, path intact) is fully covered.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any, override
from urllib.parse import unquote, urlsplit

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

logger = logging.getLogger(__name__)

# URLs carrying presign signatures always contain ``&`` and are therefore quoted
# in shell commands, so a token runs until the next whitespace/quote/angle bracket.
# Backslash is excluded so a trailing line-continuation (``\``) is not absorbed.
_URL_RE = re.compile(r"https?://[^\s\"'<>\\]+")


def _object_key(url: str) -> tuple[str, str, str]:
    """Identity of the object a (possibly presigned) URL points at, query ignored."""
    parts = urlsplit(url)
    return (parts.scheme.lower(), parts.netloc.lower(), unquote(parts.path))


def _build_index(artifacts: list[str]) -> dict[tuple[str, str, str], str]:
    """Map object identity -> authoritative artifact URL.

    Later entries win on the rare chance two artifacts share an object key; in
    practice ``merge_artifacts`` dedupes and object keys are unique per object.
    """
    return {_object_key(url): url for url in artifacts if isinstance(url, str) and url}


def _rewrite_string(text: str, index: dict[tuple[str, str, str], str]) -> str:
    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        target = index.get(_object_key(token))
        return target if target and target != token else token

    return _URL_RE.sub(repl, text)


def _rewrite(value: Any, index: dict[tuple[str, str, str], str]) -> Any:
    """Recursively rewrite URL tokens in any string leaf of a tool-args structure."""
    if isinstance(value, str):
        return _rewrite_string(value, index)
    if isinstance(value, dict):
        return {k: _rewrite(v, index) for k, v in value.items()}
    if isinstance(value, list):
        return [_rewrite(v, index) for v in value]
    if isinstance(value, tuple):
        return tuple(_rewrite(v, index) for v in value)
    return value


class ArtifactUrlGuardMiddleware(AgentMiddleware):
    """Repair presigned URLs in tool-call args against ``ThreadState.artifacts``."""

    def _guarded_request(self, request: ToolCallRequest) -> ToolCallRequest:
        state = getattr(request, "state", None)
        artifacts = state.get("artifacts") if isinstance(state, dict) else None
        if not artifacts:
            return request

        tool_call = request.tool_call
        args = tool_call.get("args")
        if not isinstance(args, (dict, list)):
            return request

        # Fast reject: most tool calls carry no URL at all. Skip the per-artifact
        # ``urlsplit``/``unquote`` index build (cost grows as artifacts accumulate
        # over a run) when the args contain no ``http`` token to repair.
        if "http" not in repr(args):
            return request

        index = _build_index(list(artifacts))
        new_args = _rewrite(args, index)
        if new_args == args:
            return request

        logger.info(
            "ArtifactUrlGuard: repaired stale/cross-object URL(s) in %r args against artifacts",
            tool_call.get("name"),
        )
        return request.override(tool_call={**tool_call, "args": new_args})

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        return handler(self._guarded_request(request))

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        return await handler(self._guarded_request(request))

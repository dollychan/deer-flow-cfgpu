from __future__ import annotations

import logging
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

logger = logging.getLogger(__name__)


@tool("present_urls", parse_docstring=True)
async def present_urls_tool(
    urls: list[str],
    expires_at_list: list[str | None],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Make remote-hosted files (images, videos) accessible to the user as artifacts.

    Call this after generate_image or generate_video to persist the result URLs
    as viewable artifacts in the client interface. The original remote URLs are
    stored directly.

    Args:
        urls: Remote file URLs, e.g. from cfgpu generate_image / generate_video.
        expires_at_list: Expiration timestamp (ISO8601) for each URL, or null if unknown. Must be the same length as urls.
    """
    if len(urls) != len(expires_at_list):
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        "Error: urls and expires_at_list must have the same length",
                        tool_call_id=tool_call_id,
                    )
                ]
            },
        )

    resolved: list[str] = list(urls)
    items: list[dict] = [
        {"ref": url, "kind": "url", "expires_at": expires_str}
        for url, expires_str in zip(urls, expires_at_list)
    ]

    return Command(
        update={
            "artifacts": resolved,
            "messages": [
                ToolMessage(
                    "Successfully presented URLs",
                    tool_call_id=tool_call_id,
                    artifact={"items": items},
                )
            ],
        },
    )


# Client-facing visibility for MessageStreamMiddleware: this tool's output is a
# final deliverable, emitted as an `artifact` event (carrying ToolMessage.artifact).
present_urls_tool.metadata = {"visibility": "internal"}

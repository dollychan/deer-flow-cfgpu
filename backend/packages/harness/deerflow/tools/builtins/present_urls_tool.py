from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.types import Command

from deerflow.tools.types import Runtime

logger = logging.getLogger(__name__)


def _get_thread_id(runtime: Runtime) -> str | None:
    thread_id = runtime.context.get("thread_id") if runtime.context else None
    if thread_id:
        return thread_id
    runtime_config = getattr(runtime, "config", None) or {}
    thread_id = runtime_config.get("configurable", {}).get("thread_id")
    if thread_id:
        return thread_id
    try:
        return get_config().get("configurable", {}).get("thread_id")
    except RuntimeError:
        return None


@tool("present_urls", parse_docstring=True)
async def present_urls_tool(
    runtime: Runtime,
    urls: list[str],
    expires_at_list: list[str | None],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Make remote-hosted files (images, videos) accessible to the user as artifacts.

    Call this after generate_image or generate_video to persist the result URLs
    as viewable artifacts in the client interface.

    When OSS is configured and a URL is about to expire, it will be automatically
    re-uploaded to object storage and replaced with a long-lived presigned URL.
    When OSS is not configured, the original URLs are stored directly.

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

    from deerflow.oss.uploader import get_oss_uploader

    uploader = get_oss_uploader()
    thread_id = _get_thread_id(runtime) or "unknown"

    resolved: list[str] = []
    items: list[dict] = []
    for url, expires_str in zip(urls, expires_at_list):
        if uploader is None:
            resolved.append(url)
            items.append({"ref": url, "kind": "url", "expires_at": expires_str})
            continue
        try:
            expires_at = datetime.fromisoformat(expires_str) if expires_str else None
            final_url = await uploader.handle_remote_url(url, expires_at, thread_id)
            resolved.append(final_url)
            # When re-uploaded to OSS, the original expiry no longer applies and the
            # new presigned expiry is not returned by handle_remote_url, so report null.
            reuploaded = final_url != url
            items.append({"ref": final_url, "kind": "url", "expires_at": None if reuploaded else expires_str})
        except Exception:
            logger.warning("present_urls: failed to handle %s, using original URL", url, exc_info=True)
            resolved.append(url)
            items.append({"ref": url, "kind": "url", "expires_at": expires_str})

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

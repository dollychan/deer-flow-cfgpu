"""Tests for ArtifactUrlGuardMiddleware (deterministic presigned-URL repair)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest

from deerflow.agents.middlewares.artifact_url_guard_middleware import (
    ArtifactUrlGuardMiddleware,
    _build_index,
    _object_key,
    _rewrite,
)

# Aliyun OSS style — same object path, different Expires/Signature.
ACE_GOOD = "https://oss.cfgpu.com/IMAGE/946e1e74/e6927f.png?Expires=1781062593&OSSAccessKeyId=K&Signature=GOOD%3D"
ACE_BAD = "https://oss.cfgpu.com/IMAGE/946e1e74/e6927f.png?Expires=1781074969&OSSAccessKeyId=K&Signature=BORROWED%3D"
NAMI_GOOD = "https://oss.cfgpu.com/IMAGE/7f5669a2/f8b30c.png?Expires=1781074969&OSSAccessKeyId=K&Signature=BORROWED%3D"

# Volcengine TOS style — entirely different query scheme.
TOS_GOOD = "https://ark.tos-cn-beijing.volces.com/seedream/021780_0.jpeg?X-Tos-Algorithm=TOS4-HMAC-SHA256&X-Tos-Expires=86400&X-Tos-Signature=GOOD"
TOS_BAD = "https://ark.tos-cn-beijing.volces.com/seedream/021780_0.jpeg?X-Tos-Algorithm=TOS4-HMAC-SHA256&X-Tos-Expires=86400&X-Tos-Signature=STALE"

EXTERNAL = "https://example.com/some/other/file.png?token=abc"


# ---------------------------------------------------------------------------
# Fake ToolCallRequest mirroring langchain's dataclass + override() semantics
# ---------------------------------------------------------------------------


@dataclass
class FakeRequest:
    tool_call: dict
    state: dict | None = field(default_factory=dict)

    def override(self, **overrides):
        return replace(self, **overrides)


def _request(args, artifacts, name="bash"):
    return FakeRequest(
        tool_call={"name": name, "args": args, "id": "tc_1"},
        state={"artifacts": artifacts} if artifacts is not None else {},
    )


def _capturing_handler():
    box: dict = {}

    def handler(request):
        box["req"] = request
        return "RESULT"

    return handler, box


def _async_capturing_handler():
    box: dict = {}

    async def handler(request):
        box["req"] = request
        return "RESULT"

    return handler, box


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestObjectKey:
    def test_ignores_query_string(self):
        assert _object_key(ACE_GOOD) == _object_key(ACE_BAD)

    def test_distinguishes_different_paths(self):
        assert _object_key(ACE_GOOD) != _object_key(NAMI_GOOD)

    def test_provider_agnostic_for_tos(self):
        assert _object_key(TOS_GOOD) == _object_key(TOS_BAD)


class TestBuildIndex:
    def test_maps_object_key_to_full_url(self):
        index = _build_index([ACE_GOOD, NAMI_GOOD])
        assert index[_object_key(ACE_BAD)] == ACE_GOOD
        assert index[_object_key(NAMI_GOOD)] == NAMI_GOOD

    def test_skips_non_strings(self):
        index = _build_index([ACE_GOOD, None, ""])  # type: ignore[list-item]
        assert len(index) == 1


class TestRewrite:
    def test_replaces_matching_url(self):
        index = _build_index([ACE_GOOD])
        assert _rewrite(ACE_BAD, index) == ACE_GOOD

    def test_leaves_unknown_url_untouched(self):
        index = _build_index([ACE_GOOD])
        assert _rewrite(EXTERNAL, index) == EXTERNAL

    def test_recurses_into_dict_and_list(self):
        index = _build_index([ACE_GOOD])
        value = {"cmd": [f'curl "{ACE_BAD}"', "ls"], "n": 1}
        out = _rewrite(value, index)
        assert ACE_GOOD in out["cmd"][0]
        assert out["n"] == 1


# ---------------------------------------------------------------------------
# wrap_tool_call
# ---------------------------------------------------------------------------


class TestWrapToolCall:
    def test_repairs_signature_reuse_in_bash_command(self):
        """The exact bug: correct path + a borrowed signature -> repaired."""
        mw = ArtifactUrlGuardMiddleware()
        command = f'curl -L -o ace.png "{ACE_BAD}" && curl -L -o nami.png "{NAMI_GOOD}"'
        request = _request({"command": command}, artifacts=[ACE_GOOD, NAMI_GOOD])
        handler, box = _capturing_handler()

        result = mw.wrap_tool_call(request, handler)

        assert result == "RESULT"
        fixed = box["req"].tool_call["args"]["command"]
        assert ACE_GOOD in fixed  # ace repaired to its own URL
        assert ACE_BAD not in fixed  # borrowed signature gone
        assert NAMI_GOOD in fixed  # nami was already correct

    def test_repairs_tos_style_url(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": f'curl "{TOS_BAD}"'}, artifacts=[TOS_GOOD])
        handler, box = _capturing_handler()

        mw.wrap_tool_call(request, handler)

        assert TOS_GOOD in box["req"].tool_call["args"]["command"]

    def test_passthrough_when_url_already_correct(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": f'curl "{ACE_GOOD}"'}, artifacts=[ACE_GOOD])
        handler, box = _capturing_handler()

        mw.wrap_tool_call(request, handler)

        # nothing changed -> original request object passed straight through
        assert box["req"] is request

    def test_passthrough_for_external_url(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": f'curl "{EXTERNAL}"'}, artifacts=[ACE_GOOD])
        handler, box = _capturing_handler()

        mw.wrap_tool_call(request, handler)

        assert box["req"] is request

    def test_passthrough_when_no_artifacts(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": f'curl "{ACE_BAD}"'}, artifacts=[])
        handler, box = _capturing_handler()

        mw.wrap_tool_call(request, handler)

        assert box["req"] is request

    def test_does_not_mutate_original_request(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": f'curl "{ACE_BAD}"'}, artifacts=[ACE_GOOD])
        handler, _ = _capturing_handler()

        mw.wrap_tool_call(request, handler)

        # override() returns a new request; the original args are untouched.
        assert request.tool_call["args"]["command"] == f'curl "{ACE_BAD}"'

    def test_applies_to_any_tool_args_generically(self):
        """Not bash-only: a URL arg on any tool gets repaired."""
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"reference_images": [ACE_BAD]}, artifacts=[ACE_GOOD], name="generate_image")
        handler, box = _capturing_handler()

        mw.wrap_tool_call(request, handler)

        assert box["req"].tool_call["args"]["reference_images"] == [ACE_GOOD]


class TestAwrapToolCall:
    @pytest.mark.asyncio
    async def test_async_repairs_url(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": f'curl "{ACE_BAD}"'}, artifacts=[ACE_GOOD])
        handler, box = _async_capturing_handler()

        result = await mw.awrap_tool_call(request, handler)

        assert result == "RESULT"
        assert ACE_GOOD in box["req"].tool_call["args"]["command"]

    @pytest.mark.asyncio
    async def test_async_passthrough_without_artifacts(self):
        mw = ArtifactUrlGuardMiddleware()
        request = _request({"command": "ls"}, artifacts=None)
        handler, box = _async_capturing_handler()

        await mw.awrap_tool_call(request, handler)

        assert box["req"] is request

"""Regression test: runtime-config skill injection must stay off the event loop.

Anchors the production offload in `RuntimeConfigMiddleware.abefore_agent`, where
`_load_blocks` (which runs `storage.load_skills()` → `os.walk` +
`ExtensionsConfig.from_file`, and `SKILL.md` `read_text`) is dispatched via
`asyncio.to_thread`.

Invoked under the strict Blockbuster context against a real `LocalSkillStorage`
pointed at a tmp directory. If the production `asyncio.to_thread` offload is
removed, Blockbuster raises `BlockingError` and this test fails.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from langchain_core.messages import HumanMessage

pytestmark = pytest.mark.asyncio


def _seed_skill(skills_root: Path) -> None:
    skill = skills_root / "public" / "demo"
    skill.mkdir(parents=True, exist_ok=True)
    (skill / "SKILL.md").write_text(
        "---\nname: demo\ndescription: regression-test skill\n---\n# demo\nWORKFLOW_BODY\n",
        encoding="utf-8",
    )


async def test_abefore_agent_offloads_skill_io(tmp_path: Path) -> None:
    from deerflow.agents.middlewares.runtime_config_middleware import RuntimeConfigMiddleware
    from deerflow.config.skills_config import SkillsConfig

    _seed_skill(tmp_path)

    mw = RuntimeConfigMiddleware(app_config=SimpleNamespace(skills=SkillsConfig(path=str(tmp_path))))
    state = {"messages": [HumanMessage(content="go")]}
    runtime = SimpleNamespace(context={"skills": ["demo"]})

    result = await mw.abefore_agent(state, runtime)

    assert result is not None
    assert "WORKFLOW_BODY" in result["messages"][0].content

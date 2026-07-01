"""Provide a set of APIs to sync the ``model_tool_configs`` and ``skills`` MySQL
tables (shared with the frontend) from the JSON files in this directory.

Data mapping
------------
``model_tool_config.json`` -> table ``model_tool_configs``
    The file is keyed by ``tool_code`` (e.g. ``generate_video``); each value is a
    list of per-model entries. Within one entry::

        adapterId   -> model_tool_configs.adapter_id
        modelId     -> model_tool_configs.model_id
        displayName -> model_tool_configs.display_name
        (whole entry serialized) -> model_tool_configs.config_json

    The operational key is ``(tool_code, model_id)`` (unique per tool group).

``skills.json`` -> table ``skills``
    A flat list of skill objects keyed by ``skill_code`` (unique)::

        skill_code     -> skills.skill_code
        category_code  -> skills.category_code
        category_name  -> skills.category_name   (JSON also tolerates the
                                                  legacy typo ``cateogry_name``)
        name           -> skills.name
        description    -> skills.description

Exposed APIs (importable as functions, and runnable as a CLI):
    1. set_tool_status(tool_code, model_id, enabled)
    2. set_skill_status(skill_code, enabled)
    3. update_tool_config_json(tool_code, model_id)   -- reads model_tool_config.json
    4. update_skill_details(skill_code)               -- reads skills.json
    5. add_new_skills()                               -- reads skills.json
    6. add_new_model_tool_configs()                   -- reads model_tool_config.json

The connection reads ``MYSQL_DB_HOST`` and ``MYSQL_DB_PASSWORD`` from the
environment (required — the script aborts if either is missing); they are loaded
from the repo-root ``.env`` on import. The remaining fields fall back to
defaults: ``DB_USER``, ``DB_DATABASE``, ``DB_PORT``.

Usage (CLI)::
    python consumer_frontend_info_exchange.py tool-status \\
        --tool-code generate_video --model-id wan-video --enable
    python consumer_frontend_info_exchange.py skill-status \\
        --skill-code seedance-video --disable
    python consumer_frontend_info_exchange.py sync-tool-config \\
        --tool-code generate_video --model-id wan-video
    python consumer_frontend_info_exchange.py sync-skill --skill-code seedance-video
    python consumer_frontend_info_exchange.py add-skills
    python consumer_frontend_info_exchange.py add-tools
    python consumer_frontend_info_exchange.py add-skills --add-tools   # both
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv
from mysql.connector import MySQLConnection

logger = logging.getLogger(__name__)

# -- Paths -------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
MODEL_TOOL_CONFIG_PATH = SCRIPT_DIR / "model_tool_config.json"
SKILLS_PATH = SCRIPT_DIR / "skills.json"
# .env lives at the repo root (backend/scripts/consumer -> parents[2]).
REPO_ROOT = SCRIPT_DIR.parents[2]
ENV_PATH = REPO_ROOT / ".env"

# Load .env once on import so env vars are present regardless of CWD. Existing
# real env values win (override=False); the file only fills in the gaps.
load_dotenv(ENV_PATH)

# Non-data top-level keys in model_tool_config.json that are documentation only.
_MODEL_CONFIG_META_KEYS = {"_comment"}


# -- Connection --------------------------------------------------------------
def _require_env(name: str) -> str:
    """Return ``os.environ[name]``, raising a clear error if it is unset/empty."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"Add it to {ENV_PATH} (or export it) before running this script."
        )
    return value


def get_connection() -> MySQLConnection:
    """Open a MySQL connection.

    ``MYSQL_DB_HOST`` and ``MYSQL_DB_PASSWORD`` are **required** (read from the
    environment; the script aborts if either is missing). The remaining fields fall
    back to defaults: ``DB_USER``, ``DB_DATABASE``, ``DB_PORT``.
    """
    return mysql.connector.connect(
        host=_require_env("MYSQL_DB_HOST"),
        user=os.environ.get("DB_USER", "smartml_canvas_daily"),
        password=_require_env("MYSQL_DB_PASSWORD"),
        database=os.environ.get("DB_DATABASE", "smartml_canvas_daily"),
        port=int(os.environ.get("DB_PORT", "3306")),
    )


# -- JSON loaders ------------------------------------------------------------
def _load_model_tool_config() -> dict[str, list[dict[str, Any]]]:
    """Return ``{tool_code: [entry, ...]}`` from model_tool_config.json."""
    with MODEL_TOOL_CONFIG_PATH.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return {
        key: value
        for key, value in raw.items()
        if key not in _MODEL_CONFIG_META_KEYS and isinstance(value, list)
    }


def _load_skills() -> list[dict[str, Any]]:
    """Return the skills list from skills.json (skipping empty entries)."""
    with SKILLS_PATH.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, list):
        raise ValueError(f"{SKILLS_PATH.name}: expected a JSON list at the top level")
    return [s for s in raw if isinstance(s, dict) and s.get("skill_code")]


def _find_model_entry(
    tool_code: str, model_id: str
) -> tuple[dict[str, Any], str] | None:
    """Find the model entry for ``(tool_code, model_id)``.

    Returns ``(entry, tool_code)`` — the entry object and its tool_code key — or
    ``None`` if no entry matches.
    """
    config = _load_model_tool_config()
    entries = config.get(tool_code)
    if not entries:
        return None
    for entry in entries:
        if entry.get("modelId") == model_id:
            return entry, tool_code
    return None


def _find_skill(skill_code: str) -> dict[str, Any] | None:
    """Find the skill entry for ``skill_code`` in skills.json."""
    for skill in _load_skills():
        if skill.get("skill_code") == skill_code:
            return skill
    return None


def _skill_category_name(skill: dict[str, Any]) -> str:
    """Read category_name, tolerating the legacy ``cateogry_name`` typo."""
    return skill.get("category_name") or skill.get("cateogry_name") or ""


# -- API 1: set tool status --------------------------------------------------
def set_tool_status(
    conn: MySQLConnection, tool_code: str, model_id: str, enabled: bool
) -> int:
    """Enable (``enabled=True``) or disable a model_tool_config row.

    Returns the number of rows affected (0 means no matching row).
    """
    status = 1 if enabled else 0
    with conn.cursor() as cur:
        affected = cur.execute(
            "UPDATE model_tool_configs "
            "SET status = %s "
            "WHERE tool_code = %s AND model_id = %s AND is_delete = 0",
            (status, tool_code, model_id),
        )
        rowcount = cur.rowcount
    conn.commit()
    action = "enabled" if enabled else "disabled"
    logger.info(
        "set_tool_status: %s/%s -> %s (affected=%d)", tool_code, model_id, action, rowcount
    )
    return rowcount


# -- API 2: set skill status -------------------------------------------------
def set_skill_status(
    conn: MySQLConnection, skill_code: str, enabled: bool
) -> int:
    """Enable or disable a skill row. Returns rows affected."""
    status = 1 if enabled else 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE skills "
            "SET status = %s "
            "WHERE skill_code = %s AND is_delete = 0",
            (status, skill_code),
        )
        rowcount = cur.rowcount
    conn.commit()
    action = "enabled" if enabled else "disabled"
    logger.info("set_skill_status: %s -> %s (affected=%d)", skill_code, action, rowcount)
    return rowcount


# -- API 3: update tool config_json from file --------------------------------
def update_tool_config_json(
    conn: MySQLConnection, tool_code: str, model_id: str
) -> int:
    """Refresh a model_tool_config row from model_tool_config.json.

    Updates ``config_json`` (the whole entry serialized) and keeps
    ``adapter_id`` / ``display_name`` in sync with that same entry. Returns the
    number of rows affected (0 if the key was not found in the file or the DB).
    """
    found = _find_model_entry(tool_code, model_id)
    if found is None:
        logger.warning(
            "update_tool_config_json: no entry for tool_code=%s model_id=%s in %s",
            tool_code,
            model_id,
            MODEL_TOOL_CONFIG_PATH.name,
        )
        return 0
    entry, _ = found
    config_json = json.dumps(entry, ensure_ascii=False)
    adapter_id = entry.get("adapterId", "")
    display_name = entry.get("displayName", "")

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE model_tool_configs "
            "SET config_json = %s, adapter_id = %s, display_name = %s "
            "WHERE tool_code = %s AND model_id = %s AND is_delete = 0",
            (config_json, adapter_id, display_name, tool_code, model_id),
        )
        rowcount = cur.rowcount
    conn.commit()
    logger.info(
        "update_tool_config_json: refreshed %s/%s (affected=%d)",
        tool_code,
        model_id,
        rowcount,
    )
    return rowcount


# -- API 4: update skill details from file -----------------------------------
def update_skill_details(conn: MySQLConnection, skill_code: str) -> int:
    """Refresh a skill row's details from skills.json.

    Updates ``category_code``, ``category_name``, ``name`` and ``description``.
    Returns rows affected (0 if not found in the file or the DB).
    """
    skill = _find_skill(skill_code)
    if skill is None:
        logger.warning(
            "update_skill_details: no entry for skill_code=%s in %s",
            skill_code,
            SKILLS_PATH.name,
        )
        return 0

    category_code = skill.get("category_code", "")
    category_name = _skill_category_name(skill)
    name = skill.get("name", "")
    description = skill.get("description")

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE skills "
            "SET category_code = %s, category_name = %s, name = %s, description = %s "
            "WHERE skill_code = %s AND is_delete = 0",
            (category_code, category_name, name, description, skill_code),
        )
        rowcount = cur.rowcount
    conn.commit()
    logger.info("update_skill_details: refreshed %s (affected=%d)", skill_code, rowcount)
    return rowcount


# -- API 5: add new skills ---------------------------------------------------
def add_new_skills(conn: MySQLConnection) -> dict[str, list[str]]:
    """Insert every skill in skills.json that is not yet in the table.

    Existence is checked on ``skill_code`` (including soft-deleted rows). Returns
    a report ``{"inserted": [...], "skipped": [...]}``.
    """
    report: dict[str, list[str]] = {"inserted": [], "skipped": []}
    for skill in _load_skills():
        skill_code = skill["skill_code"]
        category_code = skill.get("category_code", "")
        category_name = _skill_category_name(skill)
        name = skill.get("name", "")
        description = skill.get("description")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM skills WHERE skill_code = %s LIMIT 1",
                (skill_code,),
            )
            if cur.fetchone() is not None:
                report["skipped"].append(skill_code)
                continue
            cur.execute(
                "INSERT INTO skills "
                "(skill_code, category_code, category_name, name, description, "
                " category_sort, sort, status, is_delete) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    skill_code,
                    category_code,
                    category_name,
                    name,
                    description,
                    0,  # category_sort
                    0,  # sort
                    1,  # status: enabled
                    0,  # is_delete
                ),
            )
        report["inserted"].append(skill_code)
    conn.commit()
    logger.info(
        "add_new_skills: inserted=%d skipped=%d",
        len(report["inserted"]),
        len(report["skipped"]),
    )
    return report


# -- API 6: add new model_tool_configs ---------------------------------------
def add_new_model_tool_configs(conn: MySQLConnection) -> dict[str, list[str]]:
    """Insert every model config in model_tool_config.json not yet in the table.

    Existence is checked on ``(tool_code, model_id)`` (including soft-deleted
    rows). Returns ``{"inserted": [...], "skipped": [...]}`` keyed by
    ``"tool_code/model_id"``.
    """
    report: dict[str, list[str]] = {"inserted": [], "skipped": []}
    config = _load_model_tool_config()
    for tool_code, entries in config.items():
        for entry in entries:
            model_id = entry.get("modelId")
            if not model_id:
                continue
            adapter_id = entry.get("adapterId", "")
            display_name = entry.get("displayName", "")
            config_json = json.dumps(entry, ensure_ascii=False)
            key = f"{tool_code}/{model_id}"

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM model_tool_configs "
                    "WHERE tool_code = %s AND model_id = %s LIMIT 1",
                    (tool_code, model_id),
                )
                if cur.fetchone() is not None:
                    report["skipped"].append(key)
                    continue
                cur.execute(
                    "INSERT INTO model_tool_configs "
                    "(tool_code, model_id, adapter_id, display_name, status, "
                    " config_json, sort, is_delete) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        tool_code,
                        model_id,
                        adapter_id,
                        display_name,
                        1,  # status: enabled
                        config_json,
                        0,  # sort
                        0,  # is_delete
                    ),
                )
            report["inserted"].append(key)
    conn.commit()
    logger.info(
        "add_new_model_tool_configs: inserted=%d skipped=%d",
        len(report["inserted"]),
        len(report["skipped"]),
    )
    return report


# -- CLI ---------------------------------------------------------------------
def _add_enable_disable(p: argparse.ArgumentParser) -> None:
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--enable", dest="enabled", action="store_true", help="Set status = 1")
    group.add_argument("--disable", dest="enabled", action="store_false", help="Set status = 0")
    p.set_defaults(enabled=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync the model_tool_configs / skills MySQL tables from local JSON files.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("tool-status", help="API 1: enable/disable a model_tool_config")
    p.add_argument("--tool-code", required=True)
    p.add_argument("--model-id", required=True, help="value of model_id (the JSON 'modelId')")
    _add_enable_disable(p)

    p = sub.add_parser("skill-status", help="API 2: enable/disable a skill")
    p.add_argument("--skill-code", required=True)
    _add_enable_disable(p)

    p = sub.add_parser(
        "sync-tool-config", help="API 3: refresh a tool's config_json from model_tool_config.json"
    )
    p.add_argument("--tool-code", required=True)
    p.add_argument("--model-id", required=True)

    p = sub.add_parser("sync-skill", help="API 4: refresh a skill's details from skills.json")
    p.add_argument("--skill-code", required=True)

    sub.add_parser("add-skills", help="API 5: insert new skills from skills.json")
    sub.add_parser(
        "add-tools", help="API 6: insert new model_tool_configs from model_tool_config.json"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = _build_parser().parse_args(argv)
    conn = get_connection()
    try:
        if args.command == "tool-status":
            set_tool_status(conn, args.tool_code, args.model_id, args.enabled)
        elif args.command == "skill-status":
            set_skill_status(conn, args.skill_code, args.enabled)
        elif args.command == "sync-tool-config":
            update_tool_config_json(conn, args.tool_code, args.model_id)
        elif args.command == "sync-skill":
            update_skill_details(conn, args.skill_code)
        elif args.command == "add-skills":
            print(json.dumps(add_new_skills(conn), ensure_ascii=False, indent=2))
        elif args.command == "add-tools":
            print(json.dumps(add_new_model_tool_configs(conn), ensure_ascii=False, indent=2))
        else:  # pragma: no cover - argparse enforces a valid command
            raise AssertionError(f"unhandled command: {args.command}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from typing import Annotated, Any, NotRequired, TypedDict

from langchain.agents import AgentState

from deerflow.agents.materials.types import Material


class SandboxState(TypedDict):
    sandbox_id: NotRequired[str | None]


class ThreadDataState(TypedDict):
    workspace_path: NotRequired[str | None]
    uploads_path: NotRequired[str | None]
    outputs_path: NotRequired[str | None]


class ViewedImageData(TypedDict):
    base64: str
    mime_type: str


def merge_sandbox(existing: SandboxState | None, new: SandboxState | None) -> SandboxState | None:
    """Reducer for sandbox state - accepts idempotent writes only.

    Multiple sandbox tools can initialize lazily in the same graph step and
    emit the same sandbox_id via Command(update=...). LangGraph needs an
    explicit reducer for that shared state key. Different sandbox ids in the
    same thread indicate a lifecycle/isolation bug, so fail closed instead of
    choosing one silently.
    """
    if new is None:
        return existing
    if existing is None:
        return new

    existing_id = existing.get("sandbox_id")
    new_id = new.get("sandbox_id")
    if existing_id == new_id:
        return existing
    raise ValueError(f"Conflicting sandbox state updates: {existing_id!r} != {new_id!r}")


SandboxStateField = Annotated[NotRequired[SandboxState | None], merge_sandbox]


def merge_artifacts(existing: list[str] | None, new: list[str] | None) -> list[str]:
    """Reducer for artifacts list - merges and deduplicates artifacts."""
    if existing is None:
        return new or []
    if new is None:
        return existing
    # Use dict.fromkeys to deduplicate while preserving order
    return list(dict.fromkeys(existing + new))


def _attach_material(existing: Material, new: Material) -> Material:
    """同 id 字段级合并（cfgpu-docs/materials.md §2.1）：新出现的非 None 字段补入现有项，
    不整体替换。ref_type/ref 受约束：

    - ``asset_url`` immutable —— ref_type/ref 不可变，冲突即 raise（fail-closed）。
    - 放行升级 ``global_url -> oss_path``（rehost 后 ref 改写为 object_key）与
      ``local -> oss_path``（stage 上传后 attach object_key，D15/§4.8）。
    - 其余 ref_type 跨值改写视为非法，raise。
    """
    old_rt = existing.get("ref_type")
    new_rt = new.get("ref_type")
    # 合法升级（到达点增强、非冲突）：临期/纯本地 → 持久 oss_path。
    _upgrades = {("global_url", "oss_path"), ("local", "oss_path")}

    if old_rt == "asset_url":
        if new_rt is not None and new_rt != "asset_url":
            raise ValueError(f"asset_url material {existing.get('id')!r} ref_type is immutable: {old_rt!r} -> {new_rt!r}")
        if "ref" in new and new["ref"] != existing.get("ref"):
            raise ValueError(f"asset_url material {existing.get('id')!r} ref is immutable")
    elif new_rt is not None and new_rt != old_rt and (old_rt, new_rt) not in _upgrades:
        raise ValueError(f"illegal ref_type transition for {existing.get('id')!r}: {old_rt!r} -> {new_rt!r}")

    merged: dict[str, Any] = dict(existing)
    for key, value in new.items():
        if value is not None:
            merged[key] = value
    return merged  # type: ignore[return-value]


def merge_materials(existing: dict[str, Material] | None, new: dict[str, Material] | None) -> dict[str, Material]:
    """Reducer for the materials registry — only-growing, field-level attach.

    materials 是唯一进 checkpoint 的增长 registry（SSOT，summarization 物理碰不到）。
    同 id 走 ``_attach_material`` 字段级合并（放行 global_url→oss_path 升级、asset_url
    immutable）。空 dict 视为 no-op（registry 永不清空；D9 淘汰为后续优化）——刻意不沿用
    viewed_images/tool_approvals 的"空=清空"约定，避免误清整份注册表。
    """
    if new is None:
        return existing or {}
    if existing is None:
        return new or {}
    if len(new) == 0:
        return existing
    merged = dict(existing)
    for mid, new_mat in new.items():
        if mid in merged:
            merged[mid] = _attach_material(merged[mid], new_mat)
        else:
            merged[mid] = new_mat
    return merged


def merge_tool_approvals(existing: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any]:
    """Reducer for tool_approvals dict — merges decisions, new keys override existing.

    Used by HumanApprovalMiddleware to persist approval/rejection decisions across
    the interrupt→resume cycle. The client writes decisions via Command.update so
    that after_model can detect them on re-entry and skip re-emitting the SSE event.

    Special case: an explicit empty dict {} clears all decisions (same convention as
    merge_viewed_images). HumanApprovalMiddleware._build_response returns {} here once
    a batch's decisions have been consumed (baked into the AIMessage), reclaiming the
    otherwise unbounded per-thread accumulation. Clearing is safe because the graph is
    strictly serial — one suspend point per thread — so at that apply moment the dict
    holds only the just-consumed batch plus dead historical residue, both reclaimable.
    See cfgpu-docs/human_approval_middleware.md §9.
    """
    if existing is None:
        return new or {}
    if new is None:
        return existing
    if len(new) == 0:
        return {}
    return {**existing, **new}


def merge_viewed_images(existing: dict[str, ViewedImageData] | None, new: dict[str, ViewedImageData] | None) -> dict[str, ViewedImageData]:
    """Reducer for viewed_images dict - merges image dictionaries.

    Special case: If new is an empty dict {}, it clears the existing images.
    This allows middlewares to clear the viewed_images state after processing.
    """
    if existing is None:
        return new or {}
    if new is None:
        return existing
    # Special case: empty dict means clear all viewed images
    if len(new) == 0:
        return {}
    # Merge dictionaries, new values override existing ones for same keys
    return {**existing, **new}


def merge_todos(existing: list | None, new: list | None) -> list | None:
    """Reducer for todos list - keeps the last non-None value.

    Semantics:
    - If `new` is None (node didn't touch todos), preserve `existing`.
    - If `new` is provided (even empty list), it represents an explicit
      update and wins over `existing`.
    """
    if new is None:
        return existing
    return new


class PromotedTools(TypedDict):
    catalog_hash: str
    names: list[str]


def merge_promoted(existing: PromotedTools | None, new: PromotedTools | None) -> PromotedTools | None:
    """Reducer for deferred-tool promotions, scoped by catalog hash.

    - new None/empty -> preserve existing (node didn't touch promotions).
    - catalog_hash changed -> replace wholesale, dropping stale names (prevents a
      persisted bare name from exposing a different tool after catalog drift).
    - same catalog_hash -> union names, dedupe, preserve order.
    """
    if not new:
        return existing
    if existing is None or existing.get("catalog_hash") != new["catalog_hash"]:
        return {
            "catalog_hash": new["catalog_hash"],
            "names": list(dict.fromkeys(new["names"])),
        }
    return {
        "catalog_hash": existing["catalog_hash"],
        "names": list(dict.fromkeys(existing["names"] + new["names"])),
    }


class ThreadState(AgentState):
    sandbox: SandboxStateField
    thread_data: NotRequired[ThreadDataState | None]
    title: NotRequired[str | None]
    artifacts: Annotated[list[str], merge_artifacts]  # P8(D8) 将删除：降为 materials display 投影
    materials: Annotated[dict[str, Material], merge_materials]  # id -> Material；唯一进 checkpoint 的增长 registry(SSOT)
    todos: Annotated[list | None, merge_todos]
    uploaded_files: NotRequired[list[dict] | None]
    viewed_images: Annotated[dict[str, ViewedImageData], merge_viewed_images]  # image_path -> {base64, mime_type}
    tool_approvals: Annotated[dict[str, Any], merge_tool_approvals]  # tool_call_id -> {status, args?, reason?}
    promoted: Annotated[PromotedTools | None, merge_promoted]

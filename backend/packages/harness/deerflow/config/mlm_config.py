"""Configuration for multi-level memory (MLM).

MLM is the DB-backed user / agent / project memory system (see
``cfgpu-docs/multi-level-memory设计.md``). It is intentionally a top-level
config block, independent of the legacy file-based ``memory`` block: an agent
typically runs one or the other (``memory.enabled=false`` + ``mlm.enabled=true``).
"""

from pydantic import BaseModel, Field


class MlmConfig(BaseModel):
    """Configuration for the multi-level memory subsystem."""

    enabled: bool = Field(
        default=False,
        description="Whether to enable multi-level memory (DB-backed extraction and injection). Requires database.backend = sqlite|postgres.",
    )
    model_name: str | None = Field(
        default=None,
        description="Model name to use for MLM knowledge extraction (None = use default model).",
    )
    debounce_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
        description="Seconds to wait before processing queued MLM extractions (debounce).",
    )
    fact_confidence_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum confidence a newly extracted fact must declare to be persisted. "
            "Candidate facts whose explicit confidence is below this value are dropped at merge time. "
            "Facts that omit confidence (e.g. from un-migrated extraction skills) are kept (graceful rollout)."
        ),
    )
    max_facts_per_scope: int = Field(
        default=50,
        ge=1,
        description=(
            "Maximum number of facts retained per memory row (user/project scope or agent). "
            "When a merge would exceed this, facts are sorted by confidence (desc) and the top N are kept. "
            "This is the primary lever bounding how much memory each row can contribute to the prompt."
        ),
    )
    max_injection_facts: int = Field(
        default=15,
        ge=1,
        description=(
            "Maximum number of facts injected into the prompt per memory row. "
            "At injection time facts are ordered by confidence (desc) and only the top N are rendered, "
            "so a high storage cap does not translate into an unbounded prompt footprint."
        ),
    )


# Global configuration instance
_mlm_config: MlmConfig = MlmConfig()


def get_mlm_config() -> MlmConfig:
    """Get the current multi-level memory configuration."""
    return _mlm_config


def set_mlm_config(config: MlmConfig) -> None:
    """Set the multi-level memory configuration."""
    global _mlm_config
    _mlm_config = config


def load_mlm_config_from_dict(config_dict: dict) -> None:
    """Load multi-level memory configuration from a dictionary."""
    global _mlm_config
    _mlm_config = MlmConfig(**config_dict)

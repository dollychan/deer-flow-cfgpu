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

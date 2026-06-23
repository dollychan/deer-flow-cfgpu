"""Configuration for loop detection middleware."""

from enum import Enum

from pydantic import BaseModel, Field, model_validator


class ToolFreqOverride(BaseModel):
    """Per-tool frequency threshold override.

    Can be higher or lower than the global defaults. Commonly used to raise
    thresholds for high-frequency tools like bash in batch workflows (e.g.
    RNA-seq pipelines) without weakening protection on every other tool.
    """

    warn: int = Field(ge=1)
    hard_limit: int = Field(ge=1)

    @model_validator(mode="after")
    def _validate(self) -> "ToolFreqOverride":
        if self.hard_limit < self.warn:
            raise ValueError("hard_limit must be >= warn")
        return self


class ToolKeyMode(str, Enum):
    """How to derive the stable key used for loop-detection hashing.

    full   — hash all args (no false positives for content-rich tools such as
             image/video generators where every call has a unique prompt).
    fields — hash only the listed ``fields``; useful when only certain
             parameters distinguish one call from another.
    """

    full = "full"
    fields = "fields"


class ToolKeyOverride(BaseModel):
    """Per-tool-pattern override for the loop-detection key derivation strategy.

    Keys in ``LoopDetectionConfig.tool_key_overrides`` are fnmatch patterns
    matched against the tool name, e.g. ``"cfdream_generate_*"`` or
    ``"*generate*"``.  The first matching pattern wins.

    Examples (config.yaml)::

        loop_detection:
          tool_key_overrides:
            "cfdream_generate_*":
              mode: full          # hash all args; different prompts → different hashes
            "my_custom_draw":
              mode: fields
              fields: [prompt, style]   # only these fields distinguish calls
    """

    mode: ToolKeyMode = ToolKeyMode.full
    fields: list[str] = Field(default_factory=list, description="Fields to hash when mode=fields")


class LoopDetectionConfig(BaseModel):
    """Configuration for repetitive tool-call loop detection."""

    enabled: bool = Field(
        default=True,
        description="Whether to enable repetitive tool-call loop detection",
    )
    warn_threshold: int = Field(
        default=3,
        ge=1,
        description="Number of identical tool-call sets before injecting a warning",
    )
    hard_limit: int = Field(
        default=5,
        ge=1,
        description="Number of identical tool-call sets before forcing a stop",
    )
    window_size: int = Field(
        default=20,
        ge=1,
        description="Number of recent tool-call sets to track per thread",
    )
    max_tracked_threads: int = Field(
        default=100,
        ge=1,
        description="Maximum number of thread histories to keep in memory",
    )
    tool_freq_warn: int = Field(
        default=30,
        ge=1,
        description="Number of calls to the same tool type before injecting a frequency warning",
    )
    tool_freq_hard_limit: int = Field(
        default=50,
        ge=1,
        description="Number of calls to the same tool type before forcing a stop",
    )
    tool_freq_overrides: dict[str, ToolFreqOverride] = Field(
        default_factory=dict,
        description=("Per-tool overrides for tool_freq_warn / tool_freq_hard_limit, keyed by tool name. Values can be higher or lower than the global defaults. Commonly used to raise thresholds for high-frequency tools like bash."),
    )
    tool_key_overrides: dict[str, ToolKeyOverride] = Field(
        default_factory=dict,
        description=(
            "Per-tool-pattern overrides for loop-detection key derivation, keyed by fnmatch pattern. "
            "Controls which args are hashed when deciding whether two tool calls are 'the same'. "
            "Use mode=full for content-rich tools (image/video generators) so that calls with "
            "different prompts are never collapsed to the same hash."
        ),
    )

    @model_validator(mode="after")
    def validate_thresholds(self) -> "LoopDetectionConfig":
        """Ensure hard stop cannot happen before the warning threshold."""
        if self.hard_limit < self.warn_threshold:
            raise ValueError("hard_limit must be greater than or equal to warn_threshold")
        if self.tool_freq_hard_limit < self.tool_freq_warn:
            raise ValueError("tool_freq_hard_limit must be greater than or equal to tool_freq_warn")
        return self

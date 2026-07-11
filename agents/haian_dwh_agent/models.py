from __future__ import annotations

from dataclasses import dataclass, field, replace

try:
    from enum import StrEnum
except ImportError:  # Python < 3.11 compatibility for the Airflow image.
    from enum import Enum

    class StrEnum(str, Enum):
        __str__ = str.__str__
        __format__ = str.__format__

from typing import Any, Literal


ToolStatus = Literal["ok", "warning", "error", "skipped"]


class AgentGoal(StrEnum):
    DAILY_COMPETITOR_PIPELINE = "daily_competitor_pipeline"
    FULL_SYNC = "full_sync"
    CHECK_HEALTH = "check_health"
    REPAIR_TABLE = "repair_table"


@dataclass(frozen=True)
class ToolResult:
    tool: str
    status: ToolStatus
    summary: str
    data: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    next_recommendations: list[str] = field(default_factory=list)

    @classmethod
    def success(cls, tool: str, summary: str, data: dict[str, Any] | None = None) -> "ToolResult":
        return cls(tool=tool, status="ok", summary=summary, data=data or {})

    @classmethod
    def failure(
        cls,
        tool: str,
        summary: str,
        errors: list[str],
        data: dict[str, Any] | None = None,
    ) -> "ToolResult":
        return cls(tool=tool, status="error", summary=summary, data=data or {}, errors=errors)


@dataclass(frozen=True)
class TableSyncRequest:
    table_name: str
    csv_filename: str | None = None
    dry_run: bool = True


@dataclass(frozen=True)
class AgentRunState:
    goal: AgentGoal
    max_actions: int = 8
    actions_taken: tuple[str, ...] = ()

    @property
    def action_count(self) -> int:
        return len(self.actions_taken)

    @property
    def has_action_budget(self) -> bool:
        return self.action_count < self.max_actions

    def record_action(self, action_name: str) -> "AgentRunState":
        return replace(self, actions_taken=(*self.actions_taken, action_name))

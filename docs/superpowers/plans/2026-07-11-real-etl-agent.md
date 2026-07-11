# Real ETL Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chuyen pipeline ETL hien tai tu automation script tuyen tinh thanh HAIAN DWH Agent co vong Think -> Act -> Observe, dung LLM de quyet dinh buoc tiep theo nhung van giu ETL/BigQuery la deterministic tools co guardrail.

**Architecture:** Airflow tiep tuc la scheduler ben ngoai, nhung chi kich hoat mot agent goal. Ben trong, Google ADK agent doc observation tu cac tool deterministic: scraper, data quality, FK validation, BigQuery sync, data mart, alert. LLM chi lap ke hoach va chon tool; cac tool khong cho phep thao tac pha huy neu khong co human approval.

**Tech Stack:** Python 3.10, Apache Airflow 2.10, Google BigQuery, Google ADK (`google-adk`), Gemini model, pandas, pytest, Docker Compose.

## Global Constraints

- Khong cho LLM tao SQL tuy y de ghi/xoa BigQuery trong MVP.
- Khong xoa/drop bang BigQuery tu agent neu khong co co che human approval ro rang.
- Moi tool phai tra ve structured result gom `status`, `summary`, `data`, `errors`, `next_recommendations`.
- Dry-run phai la default trong test va local verification.
- Airflow chi trigger agent goal; khong lap lai logic quyet dinh trong DAG.
- Credentials, project id, dataset id phai lay tu environment, khong hardcode.
- Cac CLI cu (`etl_agent.py --run-now`, `competitor_scraper_agent.py --run-now`) phai tiep tuc chay de rollback.

---

## Current Architecture Findings

- `agents/etl_agent.py` dang la orchestrator tuyen tinh. `SchemaRegistry.LOAD_ORDER` quy dinh thu tu load cung, `run_full_sync()` lap qua danh sach nay va `_sync_one_table()` read -> DQ -> FK -> upsert.
- `agents/competitor_scraper_agent.py` dieu phoi Booking/Ivivu scraper va tra ve list record, nhung chua tra observation structured cho planner.
- `agents/create_data_marts.py` tao BigQuery views nhung dang hardcode `PROJECT_ID`, `DATASET_ID`, `CREDENTIALS_PATH`.
- `dags/haian_pipeline_dag.py` dang co 2 BashOperator: scraper truoc, ETL sau. Day la automation flow, chua co agent decision loop.
- `agents/test_dq.py` la script manual, chua phai pytest suite.

## Target File Structure

- Create: `agents/haian_dwh_agent/__init__.py` - package marker.
- Create: `agents/haian_dwh_agent/models.py` - dataclass contracts cho tool input/output va run state.
- Create: `agents/haian_dwh_agent/tools.py` - ADK tool functions wrap ETL, scraper, mart, alert.
- Create: `agents/haian_dwh_agent/agent.py` - ADK `root_agent` voi instruction va registered tools.
- Create: `agents/haian_dwh_agent/runner.py` - helper chay agent bang ADK Runner/session service.
- Create: `agents/haian_dwh_agent/cli.py` - CLI moi: `python -m agents.haian_dwh_agent.cli --goal daily_competitor_pipeline`.
- Create: `tests/test_agent_models.py` - unit tests cho contracts.
- Create: `tests/test_agent_tools.py` - unit tests cho deterministic tools bang monkeypatch.
- Create: `tests/test_create_data_marts.py` - unit tests cho config-driven mart creation.
- Modify: `agents/etl_agent.py` - giu CLI cu, expose service-friendly methods khong phu thuoc argparse.
- Modify: `agents/create_data_marts.py` - bo hardcode, nhan config/env, tra structured result.
- Modify: `agents/competitor_scraper_agent.py` - them wrapper result structured, khong thay doi scraper behavior.
- Modify: `agents/requirements.txt` - them `google-adk` va `pytest`.
- Modify: `dags/haian_pipeline_dag.py` - doi DAG trigger sang agent CLI sau khi tests pass.
- Modify: `Dockerfile` - dam bao package moi duoc install qua requirements.
- Modify: `agents/.env.example` - them `AGENT_MODEL`, `AGENT_MAX_ACTIONS`, `AGENT_DEFAULT_DRY_RUN`.

---

### Task 1: Define Agent Contracts

**Files:**
- Create: `agents/haian_dwh_agent/__init__.py`
- Create: `agents/haian_dwh_agent/models.py`
- Create: `tests/test_agent_models.py`

**Interfaces:**
- Produces: `ToolResult`, `TableSyncRequest`, `AgentRunState`, `AgentGoal`
- Consumes: standard library only

- [ ] **Step 1: Create package marker**

Create `agents/haian_dwh_agent/__init__.py`:

```python
"""HAIAN DWH real agent package."""
```

- [ ] **Step 2: Write failing contract tests**

Create `tests/test_agent_models.py`:

```python
from agents.haian_dwh_agent.models import AgentGoal, AgentRunState, ToolResult


def test_tool_result_success_contract():
    result = ToolResult.success(
        tool="check_data_quality",
        summary="No errors",
        data={"table": "fact_booking", "errors": 0},
    )

    assert result.status == "ok"
    assert result.tool == "check_data_quality"
    assert result.data["table"] == "fact_booking"
    assert result.errors == []


def test_tool_result_failure_contract():
    result = ToolResult.failure(
        tool="sync_table_to_bigquery",
        summary="FK violation",
        errors=["missing dim_room_type id=999"],
        data={"table": "fact_booking"},
    )

    assert result.status == "error"
    assert result.errors == ["missing dim_room_type id=999"]
    assert result.data["table"] == "fact_booking"


def test_agent_run_state_limits_actions():
    state = AgentRunState(goal=AgentGoal.DAILY_COMPETITOR_PIPELINE, max_actions=2)
    state = state.record_action("check_data_quality")
    state = state.record_action("sync_table_to_bigquery")

    assert state.action_count == 2
    assert state.has_action_budget is False
```

Run:

```bash
pytest tests/test_agent_models.py -q
```

Expected: FAIL because `agents.haian_dwh_agent.models` does not exist.

- [ ] **Step 3: Implement model contracts**

Create `agents/haian_dwh_agent/models.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
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
```

- [ ] **Step 4: Verify tests pass**

Run:

```bash
pytest tests/test_agent_models.py -q
```

Expected: PASS.

---

### Task 2: Add Deterministic Tool Facade

**Files:**
- Create: `agents/haian_dwh_agent/tools.py`
- Create: `tests/test_agent_tools.py`
- Modify: `agents/competitor_scraper_agent.py`

**Interfaces:**
- Consumes: `ToolResult`
- Produces: tool functions callable by ADK:
  - `run_competitor_scraper(dry_run: bool = True) -> dict`
  - `run_full_sync(dry_run: bool = True) -> dict`
  - `sync_table_to_bigquery(csv_filename: str, dry_run: bool = True) -> dict`
  - `check_data_quality(table_name: str, csv_filename: str) -> dict`

- [ ] **Step 1: Write failing tests for tool wrappers**

Create `tests/test_agent_tools.py`:

```python
from pathlib import Path

import pandas as pd

from agents.haian_dwh_agent import tools


def test_check_data_quality_returns_structured_result(monkeypatch, tmp_path):
    csv_path = tmp_path / "fact_room_revenue_daily.csv"
    pd.DataFrame(
        {
            "rooms_sold": [0],
            "total_rooms_available": [10],
            "gross_room_revenue": [500000],
        }
    ).to_csv(csv_path, index=False)

    monkeypatch.setattr(tools, "CSV_DATA_DIR", tmp_path)

    result = tools.check_data_quality(
        table_name="fact_room_revenue_daily",
        csv_filename="fact_room_revenue_daily.csv",
    )

    assert result["tool"] == "check_data_quality"
    assert result["status"] in {"ok", "warning", "error"}
    assert "errors" in result


def test_run_full_sync_delegates_to_existing_etl_agent(monkeypatch):
    class FakeETLAgent:
        def run_full_sync(self, dry_run):
            assert dry_run is True
            return [{"table": "dim_date", "inserted": 1, "skipped": 0, "errors": []}]

    monkeypatch.setattr(tools, "ETLAgent", FakeETLAgent)

    result = tools.run_full_sync(dry_run=True)

    assert result["tool"] == "run_full_sync"
    assert result["status"] == "ok"
    assert result["data"]["total_inserted"] == 1
```

Run:

```bash
pytest tests/test_agent_tools.py -q
```

Expected: FAIL because `tools.py` does not exist.

- [ ] **Step 2: Implement tool facade**

Create `agents/haian_dwh_agent/tools.py`:

```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

from agents.etl_agent import ETLAgent, SchemaRegistry
from agents.haian_dwh_agent.models import ToolResult


CSV_DATA_DIR = Path(os.getenv("CSV_DATA_DIR", "Data/Raw"))


def _as_dict(result: ToolResult) -> dict[str, Any]:
    return {
        "tool": result.tool,
        "status": result.status,
        "summary": result.summary,
        "data": result.data,
        "errors": result.errors,
        "next_recommendations": result.next_recommendations,
    }


def run_full_sync(dry_run: bool = True) -> dict[str, Any]:
    agent = ETLAgent()
    results = agent.run_full_sync(dry_run=dry_run)
    total_inserted = sum(item.get("inserted", 0) for item in results)
    total_errors = sum(len(item.get("errors", [])) for item in results)
    status = "ok" if total_errors == 0 else "warning"
    return _as_dict(
        ToolResult(
            tool="run_full_sync",
            status=status,
            summary=f"Full sync finished with {total_inserted} inserted rows and {total_errors} errors",
            data={"tables": results, "total_inserted": total_inserted, "total_errors": total_errors},
            errors=[err for item in results for err in item.get("errors", [])],
        )
    )


def sync_table_to_bigquery(csv_filename: str, dry_run: bool = True) -> dict[str, Any]:
    agent = ETLAgent()
    csv_path = agent.csv_dir / csv_filename
    if not csv_path.exists():
        return _as_dict(
            ToolResult.failure(
                tool="sync_table_to_bigquery",
                summary=f"CSV file not found: {csv_filename}",
                errors=[f"CSV file not found: {csv_path}"],
                data={"csv_filename": csv_filename},
            )
        )
    table_name = SchemaRegistry.csv_to_table(csv_path.stem)
    if table_name is None:
        return _as_dict(
            ToolResult.failure(
                tool="sync_table_to_bigquery",
                summary=f"CSV file is not registered: {csv_filename}",
                errors=[f"CSV stem {csv_path.stem} is not in SchemaRegistry.CSV_TO_TABLE"],
                data={"csv_filename": csv_filename},
            )
        )
    result = agent._sync_one_table(table_name, csv_path, dry_run=dry_run)
    status = "ok" if not result.get("errors") else "warning"
    return _as_dict(
        ToolResult(
            tool="sync_table_to_bigquery",
            status=status,
            summary=f"Synced {table_name}: inserted={result.get('inserted', 0)}, skipped={result.get('skipped', 0)}",
            data=result,
            errors=result.get("errors", []),
        )
    )


def check_data_quality(table_name: str, csv_filename: str) -> dict[str, Any]:
    csv_path = CSV_DATA_DIR / csv_filename
    if not csv_path.exists():
        return _as_dict(
            ToolResult.failure(
                tool="check_data_quality",
                summary=f"CSV file not found: {csv_filename}",
                errors=[f"CSV file not found: {csv_path}"],
                data={"table_name": table_name, "csv_filename": csv_filename},
            )
        )
    agent = ETLAgent()
    df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False).dropna(how="all")
    _, errors = agent.apply_business_rules(table_name, df)
    status = "ok" if not errors else "warning"
    return _as_dict(
        ToolResult(
            tool="check_data_quality",
            status=status,
            summary=f"Data quality checked for {table_name}: {len(errors)} errors",
            data={"table_name": table_name, "csv_filename": csv_filename, "error_count": len(errors)},
            errors=errors,
            next_recommendations=["Fix source CSV before upload"] if errors else [],
        )
    )
```

- [ ] **Step 3: Add structured scraper wrapper without changing CLI behavior**

Modify `agents/competitor_scraper_agent.py` by adding this function above `main()`:

```python
def run_all_structured(
    checkin_dates: list[date] | None = None,
    dry_run: bool = False,
    headless: bool = True,
) -> dict:
    records = run_all(checkin_dates=checkin_dates, dry_run=dry_run, headless=headless)
    success_count = sum(1 for record in records if record.get("scrape_status") == "success")
    warning_count = len(records) - success_count
    return {
        "tool": "run_competitor_scraper",
        "status": "ok" if warning_count == 0 else "warning",
        "summary": f"Scraper finished with {len(records)} records",
        "data": {
            "record_count": len(records),
            "success_count": success_count,
            "warning_count": warning_count,
        },
        "errors": [],
        "next_recommendations": [],
    }
```

- [ ] **Step 4: Expose scraper tool in `tools.py`**

Add this function to `agents/haian_dwh_agent/tools.py`:

```python
def run_competitor_scraper(dry_run: bool = True) -> dict[str, Any]:
    from agents.competitor_scraper_agent import HEADLESS, run_all_structured

    result = run_all_structured(dry_run=dry_run, headless=HEADLESS)
    return result
```

- [ ] **Step 5: Verify tool tests pass**

Run:

```bash
pytest tests/test_agent_tools.py -q
```

Expected: PASS.

---

### Task 3: Convert Data Mart Script Into Safe Tool

**Files:**
- Modify: `agents/create_data_marts.py`
- Modify: `agents/haian_dwh_agent/tools.py`
- Create: `tests/test_create_data_marts.py`

**Interfaces:**
- Produces: `create_marts(project_id: str | None = None, dataset_id: str | None = None, credentials_path: str | None = None, dry_run: bool = False) -> dict`
- Consumes: `GCP_PROJECT_ID`, `BQ_DATASET_ID`, `GOOGLE_CREDENTIALS_PATH`

- [ ] **Step 1: Write failing mart config test**

Create `tests/test_create_data_marts.py`:

```python
from agents import create_data_marts


def test_create_marts_uses_env_config_and_supports_dry_run(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("BQ_DATASET_ID", "test_dataset")
    monkeypatch.setenv("GOOGLE_CREDENTIALS_PATH", "/tmp/service-account.json")

    result = create_data_marts.create_marts(dry_run=True)

    assert result["status"] == "ok"
    assert result["data"]["project_id"] == "test-project"
    assert result["data"]["dataset_id"] == "test_dataset"
    assert result["data"]["view_count"] == 4
```

Run:

```bash
pytest tests/test_create_data_marts.py -q
```

Expected: FAIL because current `create_marts()` does not support config injection or dry-run.

- [ ] **Step 2: Refactor `create_data_marts.py` config**

Modify top-level constants:

```python
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "haian-dwh-project")
DATASET_ID = os.getenv("BQ_DATASET_ID", "haian_dwh")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
```

Modify function signature:

```python
def create_marts(
    project_id: str | None = None,
    dataset_id: str | None = None,
    credentials_path: str | None = None,
    dry_run: bool = False,
) -> dict:
```

Inside the function, resolve values:

```python
    project_id = project_id or PROJECT_ID
    dataset_id = dataset_id or DATASET_ID
    credentials_path = credentials_path or CREDENTIALS_PATH
```

Use `project_id` and `dataset_id` inside SQL strings instead of module constants.

If `dry_run` is true, return before creating credentials/client:

```python
    if dry_run:
        return {
            "tool": "create_data_marts",
            "status": "ok",
            "summary": "Dry-run generated mart SQL without executing BigQuery queries",
            "data": {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "view_count": 4,
            },
            "errors": [],
            "next_recommendations": [],
        }
```

At successful end, return the same contract with summary `"Created 4 BigQuery views"`.

- [ ] **Step 3: Add mart tool wrapper**

Add to `agents/haian_dwh_agent/tools.py`:

```python
def create_data_marts(dry_run: bool = True) -> dict[str, Any]:
    from agents.create_data_marts import create_marts

    return create_marts(dry_run=dry_run)
```

- [ ] **Step 4: Verify mart tests**

Run:

```bash
pytest tests/test_create_data_marts.py -q
```

Expected: PASS.

---

### Task 4: Add ADK Agent Definition

**Files:**
- Create: `agents/haian_dwh_agent/agent.py`
- Modify: `agents/requirements.txt`

**Interfaces:**
- Produces: `root_agent`
- Consumes: tool functions from `agents.haian_dwh_agent.tools`

- [ ] **Step 1: Add dependencies**

Modify `agents/requirements.txt`:

```txt
google-adk
pytest>=8.0.0
```

Keep existing dependencies unchanged.

- [ ] **Step 2: Create ADK root agent**

Create `agents/haian_dwh_agent/agent.py`:

```python
import os

from google.adk.agents import Agent

from agents.haian_dwh_agent.tools import (
    check_data_quality,
    create_data_marts,
    run_competitor_scraper,
    run_full_sync,
    sync_table_to_bigquery,
)


MODEL_NAME = os.getenv("AGENT_MODEL", "gemini-flash-latest")


INSTRUCTION = """
You are HAIAN DWH Agent.

Mission:
- Keep BigQuery data for HAIAN DWH correct, complete, and fresh.
- Use deterministic tools for all actions.
- Decide the next action from observations.

Rules:
- Default to dry-run when the caller asks for validation or local testing.
- Never drop, delete, truncate, or overwrite BigQuery tables.
- Do not invent table names. Use registered CSV/table names only.
- If data quality errors are found, stop uploading that table and recommend source CSV fixes.
- If FK errors are found, sync the referenced dimension first when the referenced CSV exists.
- Create data marts only after Silver sync finishes without blocking errors.
- Send alerts when errors require human intervention.
- Keep each run short. If the same tool fails twice, stop and summarize the blocker.
"""


root_agent = Agent(
    model=MODEL_NAME,
    name="haian_dwh_agent",
    description="Agent that plans and operates the HAIAN data warehouse ETL flow.",
    instruction=INSTRUCTION,
    tools=[
        run_competitor_scraper,
        check_data_quality,
        sync_table_to_bigquery,
        run_full_sync,
        create_data_marts,
    ],
)
```

- [ ] **Step 3: Verify import**

Run:

```bash
python -c "from agents.haian_dwh_agent.agent import root_agent; print(root_agent.name)"
```

Expected:

```text
haian_dwh_agent
```

---

### Task 5: Add Controlled Agent Runner CLI

**Files:**
- Create: `agents/haian_dwh_agent/runner.py`
- Create: `agents/haian_dwh_agent/cli.py`
- Modify: `agents/.env.example`

**Interfaces:**
- Produces:
  - `run_agent_goal(goal: str, dry_run: bool = True) -> str`
  - CLI `python -m agents.haian_dwh_agent.cli --goal daily_competitor_pipeline --dry-run`

- [ ] **Step 1: Update environment example**

Add to `agents/.env.example`:

```dotenv
# --- Agent Runtime ---
AGENT_MODEL=gemini-flash-latest
AGENT_MAX_ACTIONS=8
AGENT_DEFAULT_DRY_RUN=true
```

- [ ] **Step 2: Create runner**

Create `agents/haian_dwh_agent/runner.py`:

```python
from __future__ import annotations

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from agents.haian_dwh_agent.agent import root_agent


APP_NAME = "haian_dwh_agent"
USER_ID = "airflow"


async def run_agent_goal(goal: str, dry_run: bool = True) -> str:
    session_service = InMemorySessionService()
    session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID)
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)
    prompt = (
        f"Run goal: {goal}. "
        f"dry_run={str(dry_run).lower()}. "
        "Plan first, call only needed tools, observe results, then summarize final status."
    )
    content = types.Content(role="user", parts=[types.Part(text=prompt)])
    final_text = ""
    async for event in runner.run_async(user_id=USER_ID, session_id=session.id, new_message=content):
        if event.content and event.content.parts:
            text_parts = [part.text for part in event.content.parts if getattr(part, "text", None)]
            if text_parts:
                final_text = "\n".join(text_parts)
    return final_text
```

- [ ] **Step 3: Create CLI**

Create `agents/haian_dwh_agent/cli.py`:

```python
from __future__ import annotations

import argparse
import asyncio
import os

from agents.haian_dwh_agent.runner import run_agent_goal


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def main() -> None:
    parser = argparse.ArgumentParser(description="HAIAN DWH real agent runner")
    parser.add_argument("--goal", default="daily_competitor_pipeline")
    parser.add_argument("--dry-run", action="store_true", default=_env_bool("AGENT_DEFAULT_DRY_RUN", True))
    parser.add_argument("--live", action="store_true", help="Run tools with writes enabled")
    args = parser.parse_args()

    dry_run = False if args.live else args.dry_run
    result = asyncio.run(run_agent_goal(goal=args.goal, dry_run=dry_run))
    print(result)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Verify dry-run CLI import path**

Run:

```bash
python -m agents.haian_dwh_agent.cli --goal check_health --dry-run
```

Expected: agent starts and returns a textual summary. If credentials/model are not configured, the command should fail with a clear authentication/model error before any BigQuery write.

---

### Task 6: Integrate Agent Into Airflow

**Files:**
- Modify: `dags/haian_pipeline_dag.py`

**Interfaces:**
- Consumes: `python -m agents.haian_dwh_agent.cli`
- Produces: one Airflow task for daily agent goal

- [ ] **Step 1: Keep old DAG behavior as rollback comments**

Before modifying the DAG, copy the current two BashOperator commands into comments at the top of the DAG file:

```python
# Rollback commands:
# python /opt/airflow/agents/competitor_scraper_agent.py --run-now
# python /opt/airflow/agents/etl_agent.py --run-now
```

- [ ] **Step 2: Replace two-step automation with one agent task**

Modify `dags/haian_pipeline_dag.py` task section:

```python
    task_run_agent = BashOperator(
        task_id='run_haian_dwh_agent',
        bash_command='cd /opt/airflow && python -m agents.haian_dwh_agent.cli --goal daily_competitor_pipeline --live'
    )
```

Remove the dependency line `task_scrape_data >> task_etl_bq` after confirming rollback comments are present.

- [ ] **Step 3: Verify DAG imports**

Run:

```bash
python -m py_compile dags/haian_pipeline_dag.py
```

Expected: no output and exit code 0.

---

### Task 7: Verification And Rollout

**Files:**
- Modify only if verification exposes issues:
  - `agents/haian_dwh_agent/*.py`
  - `agents/create_data_marts.py`
  - `agents/etl_agent.py`
  - `dags/haian_pipeline_dag.py`

**Interfaces:**
- Consumes: all tasks above
- Produces: production-readiness evidence

- [ ] **Step 1: Run unit tests**

Run:

```bash
pytest tests -q
```

Expected: PASS.

- [ ] **Step 2: Run syntax checks**

Run:

```bash
python -m py_compile agents/etl_agent.py agents/create_data_marts.py agents/competitor_scraper_agent.py dags/haian_pipeline_dag.py
```

Expected: no output and exit code 0.

- [ ] **Step 3: Run agent local dry-run**

Run:

```bash
python -m agents.haian_dwh_agent.cli --goal check_health --dry-run
```

Expected: agent returns a summary without writing BigQuery.

- [ ] **Step 4: Build Docker image**

Run:

```bash
docker compose build
```

Expected: image builds successfully and installs `google-adk`.

- [ ] **Step 5: Validate Airflow container starts**

Run:

```bash
docker compose up airflow-standalone
```

Expected: Airflow webserver starts, DAG `haian_competitor_price_pipeline` is visible, and DAG import has no Python error.

- [ ] **Step 6: First production run with observability**

Trigger DAG manually once from Airflow UI after confirming `.env` has real GCP credentials. Watch logs for:

```text
run_haian_dwh_agent
daily_competitor_pipeline
create_data_marts
```

Expected: scraper and sync execute once, DQ/FK errors stop unsafe uploads, data marts run only after sync has no blocking errors.

---

## Rollback Plan

If ADK runtime, model auth, or Airflow import fails:

1. Revert `dags/haian_pipeline_dag.py` to the two old BashOperator tasks from rollback comments.
2. Keep `agents/haian_dwh_agent/` in the repo for local debugging; it is not called by Airflow after rollback.
3. Run:

```bash
python -m py_compile dags/haian_pipeline_dag.py
```

4. Restart Airflow container.

## Recommended Execution Order

1. Task 1 and Task 2 first, because they are pure local tests and do not touch GCP.
2. Task 3 next, because it removes hardcoded mart config and makes Gold safe for tool calling.
3. Task 4 and Task 5 after deterministic tools are stable.
4. Task 6 only after local ADK import and dry-run work.
5. Task 7 before any merge or production schedule.

## Acceptance Criteria

- `pytest tests -q` passes.
- Existing CLI rollback commands still work.
- Agent CLI can run a dry-run goal.
- DAG calls the agent entrypoint, not separate scraper and ETL scripts.
- `create_data_marts.py` has no hardcoded local Windows credential path.
- Agent tools return structured observations that a planner can reason over.
- Production mode requires explicit `--live`; dry-run remains default locally.

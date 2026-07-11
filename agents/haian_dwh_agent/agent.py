import os
from pathlib import Path

from dotenv import load_dotenv

# Load env before ADK imports — GOOGLE_API_KEY must be set
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv(_PROJECT_ROOT / "agents" / ".env")

from google.adk.agents import Agent

from agents.haian_dwh_agent.tools import (
    check_data_quality,
    create_data_marts,
    run_competitor_scraper,
    run_full_sync,
    sync_table_to_bigquery,
)


MODEL_NAME = os.getenv("AGENT_MODEL", "gemini-2.0-flash")


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

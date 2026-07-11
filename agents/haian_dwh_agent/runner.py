from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env files before any ADK import (ADK reads GOOGLE_API_KEY at import time)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv(_PROJECT_ROOT / "agents" / ".env")

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from agents.haian_dwh_agent.agent import root_agent


APP_NAME = "haian_dwh_agent"
USER_ID = "airflow"


async def run_agent_goal(goal: str, dry_run: bool = True) -> str:
    """Run the agent with a specific goal and return the final text summary."""
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

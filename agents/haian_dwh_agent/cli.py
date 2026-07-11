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

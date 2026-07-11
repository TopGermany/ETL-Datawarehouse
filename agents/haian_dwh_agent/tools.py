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
    """Run the full ETL sync pipeline across all registered CSV tables.

    Loads all CSV files in dependency order (Dim → Fact → Mart) and
    upserts them into BigQuery.
    """
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
    """Sync a single CSV file to its corresponding BigQuery table.

    Performs data quality checks, FK validation, and upsert for the
    specified CSV file only.
    """
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
    """Check data quality of a CSV file against business rules.

    Validates the CSV content without uploading to BigQuery. Reports
    any business rule violations like negative values, overbooking, etc.
    """
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


def run_competitor_scraper(dry_run: bool = True) -> dict[str, Any]:
    """Run the competitor price scraper for Booking.com and Ivivu.

    Scrapes competitor hotel pricing data and saves results to CSV.
    """
    from agents.competitor_scraper_agent import HEADLESS, run_all_structured

    result = run_all_structured(dry_run=dry_run, headless=HEADLESS)
    return result


def create_data_marts(dry_run: bool = True) -> dict[str, Any]:
    """Create or refresh BigQuery data mart views.

    Creates 4 analytical views: cost analysis, profitpar, competitor
    pricing summary, and Hai An vs market price comparison.
    """
    from agents.create_data_marts import create_marts

    return create_marts(dry_run=dry_run)

from pathlib import Path

import pandas as pd
import pytest

from agents.haian_dwh_agent import tools
from agents import etl_agent


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


@pytest.mark.parametrize("content", [b"", b"  \n", b"{not-json"])
def test_bigquery_connection_rejects_invalid_service_account_file(tmp_path, monkeypatch, content):
    credentials_path = tmp_path / "service-account.json"
    credentials_path.write_bytes(content)
    monkeypatch.setattr(etl_agent, "GCP_AVAILABLE", True)
    uploader = etl_agent.BigQueryUploader(
        project_id="test-project",
        dataset_id="test_dataset",
        credentials_path=str(credentials_path),
    )

    with pytest.raises(ValueError, match="Service Account JSON không hợp lệ hoặc đang rỗng"):
        uploader._get_client()

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

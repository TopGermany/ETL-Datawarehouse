import json
import tempfile
import unittest
from pathlib import Path


class FlowStatusServerTests(unittest.TestCase):
    def test_build_status_uses_real_files_and_runner_outputs(self):
        from tools.flow_status_server import build_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Data" / "Raw").mkdir(parents=True)
            (root / "agents" / "haian_dwh_agent").mkdir(parents=True)
            (root / "dags").mkdir()
            (root / "docs").mkdir()

            (root / "Data" / "Raw" / "dim_date.csv").write_text("date_key\n20260711\n", encoding="utf-8")
            (root / "dags" / "haian_pipeline_dag.py").write_text(
                "dag_id='haian_competitor_price_pipeline'\n"
                "bash_command='python -m agents.haian_dwh_agent.cli --goal daily_competitor_pipeline --live'\n",
                encoding="utf-8",
            )
            (root / "agents" / "haian_dwh_agent" / "tools.py").write_text(
                "def run_full_sync(dry_run=True):\n    pass\n\n"
                "def create_data_marts(dry_run=True):\n    pass\n",
                encoding="utf-8",
            )
            (root / "docs" / "current-project-flow.vi.md").write_text(
                "# Flow\nAirflow -> Agent -> BigQuery\n", encoding="utf-8"
            )

            def runner(args, timeout=8):
                command = " ".join(args)
                if "docker compose ps" in command:
                    return {
                        "ok": True,
                        "stdout": json.dumps(
                            {
                                "Name": "haian_airflow",
                                "Service": "airflow-standalone",
                                "State": "running",
                                "Status": "Up 2 minutes",
                                "Publishers": [{"URL": "0.0.0.0", "TargetPort": 8080, "PublishedPort": 8080}],
                            }
                        ),
                        "stderr": "",
                        "returncode": 0,
                    }
                if "airflow dags list" in command:
                    return {
                        "ok": True,
                        "stdout": (
                            "dag_id                          | fileloc | owners   | is_paused\n"
                            "================================+=========+==========+==========\n"
                            "haian_competitor_price_pipeline | dag.py  | Haian_IT | True\n"
                        ),
                        "stderr": "",
                        "returncode": 0,
                    }
                if "docker compose logs" in command:
                    return {
                        "ok": True,
                        "stdout": "haian_airflow | webserver started\nhaian_airflow | DAG parsed\n",
                        "stderr": "",
                        "returncode": 0,
                    }
                return {"ok": False, "stdout": "", "stderr": "unexpected", "returncode": 1}

            status = build_status(command_runner=runner, now=lambda: "2026-07-11T07:00:00Z", root=root)

        self.assertEqual(status["generatedAt"], "2026-07-11T07:00:00Z")
        self.assertEqual(status["runtime"]["container"]["state"], "running")
        self.assertEqual(status["airflow"]["dagId"], "haian_competitor_price_pipeline")
        self.assertTrue(status["airflow"]["isPaused"])
        self.assertEqual(status["csv"]["files"][0]["name"], "dim_date.csv")
        self.assertEqual(status["tools"]["registered"], ["run_full_sync", "create_data_marts"])
        self.assertIn("webserver started", status["logs"]["tail"])
        self.assertEqual(status["flow"]["source"], "docs/current-project-flow.vi.md")

    def test_build_status_reports_unavailable_sources_explicitly(self):
        from tools.flow_status_server import build_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def runner(args, timeout=8):
                return {"ok": False, "stdout": "", "stderr": "docker unavailable", "returncode": 1}

            status = build_status(command_runner=runner, now=lambda: "2026-07-11T07:00:00Z", root=root)

        self.assertEqual(status["runtime"]["container"]["state"], "unavailable")
        self.assertIn("docker unavailable", status["runtime"]["container"]["error"])
        self.assertEqual(status["csv"]["files"], [])
        self.assertEqual(status["tools"]["registered"], [])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]


CommandResult = dict[str, object]
CommandRunner = Callable[[list[str], int], CommandResult]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run_command(args: list[str], timeout: int = 8) -> CommandResult:
    try:
        completed = subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return {
            "ok": completed.returncode == 0,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "returncode": completed.returncode,
        }
    except Exception as exc:  # noqa: BLE001 - surfaced as status payload
        return {"ok": False, "stdout": "", "stderr": str(exc), "returncode": -1}


def _command(runner: CommandRunner | None, args: list[str], timeout: int = 8) -> CommandResult:
    active_runner = runner or run_command
    return active_runner(args, timeout)


def _parse_compose_ps(stdout: str) -> dict[str, object]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    for line in lines:
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        publishers = data.get("Publishers") or []
        ports = []
        for item in publishers:
            if isinstance(item, dict):
                published = item.get("PublishedPort")
                target = item.get("TargetPort")
                if published and target:
                    ports.append(f"{published}:{target}")
        return {
            "name": data.get("Name") or data.get("Name".lower()) or "haian_airflow",
            "service": data.get("Service") or "airflow-standalone",
            "state": str(data.get("State") or "unknown").lower(),
            "status": data.get("Status") or "",
            "ports": ports,
        }
    return {"name": "haian_airflow", "service": "airflow-standalone", "state": "unknown", "status": stdout.strip(), "ports": []}


def _parse_dag_list(stdout: str) -> dict[str, object]:
    dag_id = "haian_competitor_price_pipeline"
    for line in stdout.splitlines():
        if dag_id not in line:
            continue
        parts = [part.strip() for part in line.split("|")]
        paused_raw = parts[-1] if parts else ""
        return {
            "dagId": dag_id,
            "fileloc": parts[1] if len(parts) > 1 else "",
            "owner": parts[2] if len(parts) > 2 else "",
            "isPaused": paused_raw.lower() == "true",
            "source": "airflow dags list",
        }
    return {"dagId": dag_id, "fileloc": "", "owner": "", "isPaused": None, "source": "airflow dags list"}


def _collect_runtime(root: Path, runner: CommandRunner | None) -> dict[str, object]:
    result = _command(runner, ["docker", "compose", "ps", "--format", "json", "airflow-standalone"])
    if not result["ok"]:
        return {
            "container": {
                "name": "haian_airflow",
                "service": "airflow-standalone",
                "state": "unavailable",
                "status": "",
                "ports": [],
                "error": str(result["stderr"]),
            }
        }
    return {"container": _parse_compose_ps(str(result["stdout"]))}


def _collect_airflow(runner: CommandRunner | None) -> dict[str, object]:
    result = _command(
        runner,
        ["docker", "compose", "exec", "-T", "airflow-standalone", "airflow", "dags", "list"],
        timeout=18,
    )
    if not result["ok"]:
        return {
            "dagId": "haian_competitor_price_pipeline",
            "fileloc": "",
            "owner": "",
            "isPaused": None,
            "source": "airflow dags list",
            "error": str(result["stderr"]),
        }
    return _parse_dag_list(str(result["stdout"]))


def _collect_logs(runner: CommandRunner | None) -> dict[str, object]:
    result = _command(runner, ["docker", "compose", "logs", "--tail=80", "airflow-standalone"], timeout=12)
    if not result["ok"]:
        return {"tail": "", "error": str(result["stderr"])}
    return {"tail": str(result["stdout"]).strip(), "error": ""}


def _collect_csv(root: Path) -> dict[str, object]:
    raw_dir = root / "Data" / "Raw"
    files = []
    if raw_dir.exists():
        for path in sorted(raw_dir.glob("*.csv")):
            try:
                with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
                    line_count = max(0, sum(1 for _ in handle) - 1)
            except OSError:
                line_count = None
            stat = path.stat()
            files.append(
                {
                    "name": path.name,
                    "size": stat.st_size,
                    "rowsApprox": line_count,
                    "modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z"),
                }
            )
    return {"directory": str(raw_dir.relative_to(root)) if raw_dir.exists() else "Data/Raw", "count": len(files), "files": files}


def _collect_tools(root: Path) -> dict[str, object]:
    tools_path = root / "agents" / "haian_dwh_agent" / "tools.py"
    if not tools_path.exists():
        return {"source": "agents/haian_dwh_agent/tools.py", "registered": [], "error": "tools.py not found"}
    text = tools_path.read_text(encoding="utf-8", errors="replace")
    names = re.findall(r"^def\s+([a-zA-Z_][a-zA-Z0-9_]*)\(", text, flags=re.MULTILINE)
    registered = [name for name in names if not name.startswith("_")]
    return {"source": "agents/haian_dwh_agent/tools.py", "registered": registered, "error": ""}


def _collect_flow(root: Path) -> dict[str, object]:
    doc_path = root / "docs" / "current-project-flow.vi.md"
    dag_path = root / "dags" / "haian_pipeline_dag.py"
    command = ""
    dag_id = ""
    if dag_path.exists():
        text = dag_path.read_text(encoding="utf-8", errors="replace")
        dag_match = re.search(r"dag_id=['\"]([^'\"]+)['\"]", text)
        command_match = re.search(r"bash_command=['\"]([^'\"]+)['\"]", text)
        dag_id = dag_match.group(1) if dag_match else ""
        command = command_match.group(1) if command_match else ""
    return {
        "source": "docs/current-project-flow.vi.md" if doc_path.exists() else "",
        "dagSource": "dags/haian_pipeline_dag.py" if dag_path.exists() else "",
        "dagId": dag_id,
        "entryCommand": command,
        "nodes": [
            "Airflow DAG",
            "ADK Agent CLI",
            "Google ADK Runner",
            "Deterministic Tools",
            "OTA Scrapers",
            "Bronze CSV",
            "ETL BigQuery Silver",
            "Gold Data Marts",
        ],
    }


def build_status(
    command_runner: CommandRunner | None = None,
    now: Callable[[], str] | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    active_root = root or PROJECT_ROOT
    return {
        "generatedAt": (now or _utc_now)(),
        "project": {
            "name": "ETL-Datawarehouse",
            "root": str(active_root),
        },
        "runtime": _collect_runtime(active_root, command_runner),
        "airflow": _collect_airflow(command_runner),
        "flow": _collect_flow(active_root),
        "csv": _collect_csv(active_root),
        "tools": _collect_tools(active_root),
        "logs": _collect_logs(command_runner),
    }


@dataclass(frozen=True)
class ServerConfig:
    root: Path


class FlowStatusHandler(BaseHTTPRequestHandler):
    config = ServerConfig(root=PROJECT_ROOT)

    def _send_json(self, payload: dict[str, object], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(404, "Not found")
            return
        body = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            self._send_json(build_status(root=self.config.root))
            return
        if parsed.path in {"/", "/docs/flow-dashboard.html"}:
            self._send_file(self.config.root / "docs" / "flow-dashboard.html")
            return
        self.send_error(404, "Not found")

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the live HAIAN DWH flow dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()

    FlowStatusHandler.config = ServerConfig(root=PROJECT_ROOT)
    server = ThreadingHTTPServer((args.host, args.port), FlowStatusHandler)
    print(f"Serving flow dashboard at http://{args.host}:{args.port}/docs/flow-dashboard.html")
    server.serve_forever()


if __name__ == "__main__":
    main()

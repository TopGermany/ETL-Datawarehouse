# Live Flow Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local HTML dashboard that visualizes the currently running ETL/Datawarehouse flow using real runtime and repository state, not mock data.

**Architecture:** A small Python stdlib HTTP server serves `docs/flow-dashboard.html` and exposes `/api/status`. The API gathers live Docker Compose/Airflow state, recent logs, repo flow files, tool definitions, and CSV file metadata. The HTML client only renders JSON from `/api/status`, so the UI never hardcodes fake pipeline status.

**Tech Stack:** Python 3 stdlib, HTML, CSS, vanilla JavaScript, Docker Compose, Airflow CLI inside the running container.

## Global Constraints

- No mock pipeline data in the UI.
- If a live source is unavailable, show an explicit unavailable/error state.
- Do not require paid or external services to load the dashboard.
- Keep Airflow as-is; this dashboard is read-only.
- Use product-dashboard visual style: dense, scannable, restrained, responsive.

---

### Task 1: Status API Contract

**Files:**
- Create: `tests/test_flow_status_server.py`
- Create: `tools/flow_status_server.py`

**Interfaces:**
- Produces: `build_status(command_runner=None, now=None, root=None) -> dict`

- [ ] Write tests for command parsing and status payload shape.
- [ ] Verify the tests fail because `tools.flow_status_server` does not exist.
- [ ] Implement the minimal status collector.
- [ ] Verify tests pass.

### Task 2: Dashboard UI

**Files:**
- Create: `docs/flow-dashboard.html`

**Interfaces:**
- Consumes: `GET /api/status`

- [ ] Build an HTML product dashboard for runtime summary, flow nodes, CSV state, tool inventory, and logs.
- [ ] Render loading and error states.
- [ ] Add refresh control and auto-refresh timestamp.

### Task 3: Runtime Verification

**Files:**
- Use: `tools/flow_status_server.py`
- Use: `docs/flow-dashboard.html`

**Verification:**
- Run `python -m unittest tests/test_flow_status_server.py`.
- Start `python tools/flow_status_server.py --host 127.0.0.1 --port 8090`.
- Verify `curl http://127.0.0.1:8090/api/status` returns JSON.
- Verify dashboard loads in browser without placeholder/mock data.

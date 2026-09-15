# Opportunity Radar — Agent Instructions

This repository is a **read-only market monitoring system**.

## Never add
- automatic order placement or trading execution
- private-key or wallet-signing logic
- APIs requiring trading permission
- position-management or auto-close logic

## Architecture boundaries
1. **Data Source != Monitor** — collectors normalize venue data; they do not decide opportunities.
2. **Monitor != Application** — opportunity logic belongs under `monitors/`, not in `app.py`.
3. **Fast Scan != Slow Historical Work** — monitor evaluation must not perform long DuckDB queries, chart rendering, or Telegram network I/O.

## Engineering rules
- Keep the implementation simple; YAGNI.
- Do not add frameworks or abstractions that are not required by the current task.
- Follow `docs/superpowers/plans/2026-09-15-opportunity-radar-macbook-v1.md`.
- Use tests for deterministic calculations, validation, lifecycle/state logic, and historical alignment.
- Use fixtures plus limited live smoke tests for collectors.
- Work on one approved task at a time.
- If a task requires breaking one of the three architecture boundaries, stop and report the design deviation before proceeding.
- Keep the working tree clean at task checkpoints and run the full non-live test suite.

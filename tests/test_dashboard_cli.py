from __future__ import annotations

import json
import plistlib
import threading
from functools import partial
from pathlib import Path
from urllib.request import urlopen

import pytest

from radar import dashboard
from radar.config import MarketConfig, RadarConfig
from test_dashboard import NOW, make_market, write_heartbeat, write_markets

REPO_ROOT = Path(__file__).resolve().parents[1]
OPERATIONS = REPO_ROOT / "docs" / "operations"


def test_cli_preserves_safe_local_defaults():
    args = dashboard.build_parser().parse_args(["--config", "config.yaml"])
    assert args.host == "127.0.0.1"
    assert args.port == 8787
    assert args.config == Path("config.yaml")
    assert args.data_root == Path("data")
    assert args.runtime_db == Path("runtime/radar.sqlite3")


def test_cli_accepts_explicit_host_port_and_paths(tmp_path):
    args = dashboard.build_parser().parse_args([
        "--host", "localhost", "--port", "0",
        "--config", str(tmp_path / "settings.yaml"),
        "--data-root", str(tmp_path / "history"),
        "--runtime-db", str(tmp_path / "state" / "existing.sqlite3"),
    ])
    assert args.host == "localhost"
    assert args.port == 0
    assert args.config == tmp_path / "settings.yaml"
    assert args.data_root == tmp_path / "history"
    assert args.runtime_db == tmp_path / "state" / "existing.sqlite3"


@pytest.mark.parametrize("explicit_paths", [False, True])
def test_missing_config_fails_before_creating_sources(tmp_path, monkeypatch, explicit_paths):
    monkeypatch.chdir(tmp_path)
    argv = ["--config", "missing.yaml", "--port", "0"]
    if explicit_paths:
        argv += ["--data-root", "history", "--runtime-db", "state/existing.sqlite3"]
    with pytest.raises(FileNotFoundError):
        dashboard.main(argv)
    assert list(tmp_path.iterdir()) == []


def test_cli_port_zero_reads_explicit_sources_and_injects_loopback_host(tmp_path, monkeypatch):
    config_path = tmp_path / "settings.yaml"
    config = RadarConfig(markets=[
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
    ])
    config_path.write_text(json.dumps(config.model_dump()), encoding="utf-8")
    data_root = tmp_path / "history"
    runtime_db = tmp_path / "state" / "existing.sqlite3"
    write_markets(data_root, [make_market()])
    write_heartbeat(runtime_db)
    before = {path.relative_to(tmp_path): path.read_bytes()
              for path in tmp_path.rglob("*") if path.is_file()}
    create_server = dashboard.create_dashboard_server
    serve_forever = dashboard.ThreadingHTTPServer.serve_forever

    def injected_server(service, *, host, port):
        assert host == "localhost"
        assert port == 0
        return create_server(service, host=host, port=port)

    def smoke(server):
        assert server.server_address[0] == "127.0.0.1"
        assert server.server_address[1] > 0
        thread = threading.Thread(target=serve_forever, args=(server,), daemon=True)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_address[1]}/api/status",
                         timeout=10) as response:
                status = json.loads(response.read())
            assert status["overall"]["configured_feeds"] == 1
            assert status["data_as_of"] == NOW.isoformat()
            assert status["sqlite"]["status"] == "healthy"
        finally:
            server.shutdown()
            thread.join(timeout=2)
        raise KeyboardInterrupt

    monkeypatch.setattr(dashboard, "create_dashboard_server", injected_server)
    monkeypatch.setattr(dashboard, "DashboardStatusService",
                        partial(dashboard.DashboardStatusService, clock=lambda: NOW))
    monkeypatch.setattr(dashboard.ThreadingHTTPServer, "serve_forever", smoke)
    assert dashboard.main([
        "--config", str(config_path), "--host", "localhost", "--port", "0",
        "--data-root", str(data_root), "--runtime-db", str(runtime_db),
    ]) == 0
    assert {path.relative_to(tmp_path): path.read_bytes()
            for path in tmp_path.rglob("*") if path.is_file()} == before


def test_launchdaemon_example_has_explicit_identity_paths_and_safe_binding():
    with (OPERATIONS / "com.opportunity-radar.dashboard.plist.example").open("rb") as handle:
        job = plistlib.load(handle)
    assert job["Label"] == "com.opportunity-radar.dashboard"
    assert job["UserName"] == "REPLACE_WITH_NORMAL_USERNAME"
    assert job["KeepAlive"] is True
    assert job["RunAtLoad"] is True
    argv = job["ProgramArguments"]
    assert Path(argv[0]).is_absolute()
    assert argv[1:3] == ["-m", "radar.dashboard"]
    args = dashboard.build_parser().parse_args(argv[3:])
    assert args.host == "REPLACE_WITH_TAILSCALE_IPV4"
    assert args.port == 8787
    paths = [argv[0], args.config, args.data_root, args.runtime_db,
             job["WorkingDirectory"], job["StandardOutPath"], job["StandardErrorPath"]]
    for path in paths:
        assert Path(path).is_absolute()
        assert "REPLACE_WITH_" in str(path)
        assert "~" not in str(path) and "$" not in str(path)
    assert job["StandardOutPath"] != job["StandardErrorPath"]
    assert "EnvironmentVariables" not in job


@pytest.mark.parametrize("required", [
    "LaunchDaemon", "UserName", "WorkingDirectory", "KeepAlive", "RunAtLoad",
    "id -un", "id -u", "tailscale ip -4", "sys.executable", "pwd -P",
    "--config", "--data-root", "--runtime-db", "Tailscale-only", "0.0.0.0",
    "absolute", "separate process", "read-only", "Radar launch",
    "StandardOutPath", "StandardErrorPath", "plutil -lint", "launchctl bootstrap",
    "launchctl bootout", "launchctl print", "tail -F", "/api/status",
    "/api/opportunities", "benchmark", "30 cold", "30 cached",
])
def test_deployment_guide_covers_required_operational_steps(required):
    guide = (OPERATIONS / "opportunity-radar-dashboard.md").read_text(encoding="utf-8")
    assert required in guide


def test_readme_links_dashboard_run_and_deployment_guide():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "python -m radar.dashboard --config" in readme
    assert "docs/operations/opportunity-radar-dashboard.md" in readme
    assert "127.0.0.1:8787" in readme
    assert "Tailscale" in readme
    assert "separate" in readme and "read-only" in readme

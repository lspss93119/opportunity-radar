# Persistent read-only dashboard on trading-mini

The dashboard runs as a separate process, `python -m radar.dashboard`, reading
the existing YAML config, Parquet history, and SQLite runtime database. SQLite
is opened read-only; a missing database is never initialized. Missing config
fails startup before creating data/runtime files. Missing or unreadable sources
produce down/degraded responses. The dashboard does not run collectors, evaluate
monitors, send Telegram alerts, trade, sign transactions, or manage positions.
Parquet owns market history and SQLite owns persisted episodes/alert lifecycle.

Radar remains a separate process. Do not change the Radar launch job, restart
Radar, modify its config/data/runtime paths, or change sampling, fees, thresholds,
storage schemas, or alerts to install this dashboard. Dashboard stdout/stderr
logs must be separate from each other and from Radar logs.

## Local CLI

From the repository with its existing Python environment:

```sh
uv run python -m radar.dashboard --config /absolute/path/to/existing-config.yaml
```

Defaults: `--host 127.0.0.1`, `--port 8787`, `--data-root data`, and
`--runtime-db runtime/radar.sqlite3`. `--config` is required. Relative paths are
relative to the current working directory, not to the config file. Explicit
overrides are the same five arguments; `--port 0` selects an ephemeral port for
local testing. Visit `http://127.0.0.1:8787/`, `/status`, or a scanner pair link.
Ctrl-C closes the standalone dashboard server.

## Discover deployment values at install time

These steps are a future deployment procedure, not a deployment performed by
this commit. First complete controller review and the production benchmark below.

On trading-mini, log in as the actual normal user that owns/runs Radar. Run these
commands in that user's shell, without `sudo`; do not infer a username from the
machine hostname or from this example:

```sh
id -un
id -u
tailscale ip -4
```

The username must be non-root (`id -u` must not return `0`). Confirm its identity
against the running Radar owner with `ps -axo user,pid,command` and inspect the
existing Radar launch configuration read-only to find its actual config/data/
runtime/log paths. If the owner is uncertain, resolve that before installation.
The Tailscale IPv4 must come from trading-mini itself, not the MacBook. If the
CLI is not on PATH, locate the installed Tailscale executable and use its
absolute path for discovery.

Change to the actual installed repository, then discover its absolute path and
the Python executable from the existing project environment:

```sh
pwd -P
uv run python -c 'import sys; print(sys.executable)'
```

Use the printed absolute environment executable, preserving its virtualenv path
even if it is a symlink. Verify that exact executable can import the installed
module: `/absolute/environment/bin/python -c 'import radar.dashboard'`.
Do not assume launchd inherits a login shell's PATH, activated virtualenv, or
environment variables, and do not run `uv sync` from the daemon.

Record all actual values before proceeding:

| Value | Discovery/selection |
| --- | --- |
| `UserName` | Verified normal Radar owner from `id -un`, `id -u`, and `ps` |
| Tailscale IPv4 | `tailscale ip -4` on trading-mini |
| Python executable | Existing environment's `sys.executable`; absolute path |
| Repository / `WorkingDirectory` | `pwd -P` inside the installed repository |
| Config path | Exact existing Radar YAML path; absolute and readable |
| Data root | Exact existing Parquet root; absolute and readable |
| Runtime DB | Exact existing Radar SQLite file; absolute and readable |
| Log directory and stdout/stderr | Operator-selected absolute dashboard-only paths |

Check source files/directories with `test -r`, directory traversal permissions
with `test -x`, and the executable with `test -x`, as that normal user. Use the
existing source files, including any SQLite companion files; do not create,
repair, copy, migrate, chmod, or chown production data to satisfy the dashboard.
Choose a dedicated log directory accessible to that user, whose two filenames
do not overlap any Radar log. Use literal absolute paths for every plist path;
launchd does not expand `~`, `$HOME`, or shell variables in plist values.

## Tailscale-only binding

Keep local development on loopback. For production set `--host` to the discovered
trading-mini Tailscale IPv4 and restrict port `8787` through the tailnet's access
policy to the intended MacBook/users. Do not bind to `0.0.0.0`, `::`, a LAN/public
address, or publish the dashboard with a public proxy/Funnel. There is no built-in
dashboard authentication; tailnet policy controls access.

If the Tailscale address is not available at boot, binding fails and `KeepAlive`
allows launchd to retry once networking becomes available. Confirm this on the
target rather than broadening the listener. If Tailscale changes its address,
rediscover it and update only the dashboard job.

## Prepare and install the LaunchDaemon

Use [the plist example](com.opportunity-radar.dashboard.plist.example). `UserName`
is required even though the LaunchDaemon is loaded in the privileged system
domain. `WorkingDirectory` selects the repository; `KeepAlive` and `RunAtLoad`
keep the dashboard running independently of SSH sessions or user logout.

In the normal user's shell, assign these variables using the discovered values.
Replace every `REPLACE_WITH_...` value; none is a usable deployment default:

```sh
dashboard_user='REPLACE_WITH_NORMAL_USERNAME'
dashboard_ipv4='REPLACE_WITH_TAILSCALE_IPV4'
dashboard_python='/REPLACE_WITH_ABSOLUTE_PYTHON_EXECUTABLE'
dashboard_repo='/REPLACE_WITH_ABSOLUTE_REPO_PATH'
dashboard_config='/REPLACE_WITH_ABSOLUTE_CONFIG_PATH'
dashboard_data='/REPLACE_WITH_ABSOLUTE_DATA_ROOT'
dashboard_runtime='/REPLACE_WITH_ABSOLUTE_RUNTIME_DB_PATH'
dashboard_log_dir='/REPLACE_WITH_ABSOLUTE_DASHBOARD_LOG_DIRECTORY'
dashboard_stdout="$dashboard_log_dir/dashboard.stdout.log"
dashboard_stderr="$dashboard_log_dir/dashboard.stderr.log"
dashboard_stage="$(mktemp -d)"
dashboard_plist="$dashboard_stage/com.opportunity-radar.dashboard.plist"
cp "$dashboard_repo/docs/operations/com.opportunity-radar.dashboard.plist.example" "$dashboard_plist"
```

Substitute with `plutil` so paths containing spaces or XML characters are encoded
correctly. These shell variables are resolved while preparing the file; the final
plist contains literal values only.

```sh
plutil -replace UserName -string "$dashboard_user" "$dashboard_plist"
plutil -replace WorkingDirectory -string "$dashboard_repo" "$dashboard_plist"
plutil -replace ProgramArguments.0 -string "$dashboard_python" "$dashboard_plist"
plutil -replace ProgramArguments.4 -string "$dashboard_ipv4" "$dashboard_plist"
plutil -replace ProgramArguments.8 -string "$dashboard_config" "$dashboard_plist"
plutil -replace ProgramArguments.10 -string "$dashboard_data" "$dashboard_plist"
plutil -replace ProgramArguments.12 -string "$dashboard_runtime" "$dashboard_plist"
plutil -replace StandardOutPath -string "$dashboard_stdout" "$dashboard_plist"
plutil -replace StandardErrorPath -string "$dashboard_stderr" "$dashboard_plist"
plutil -lint "$dashboard_plist"
plutil -p "$dashboard_plist"
```

Review the rendered file: all paths absolute, correct normal `UserName`, exact
Tailscale IPv4, no placeholders, no implicit HOME, and dashboard-only logs. Check
existing job status before installing; do not overwrite an existing dashboard
definition without first reviewing it and stopping that dashboard job.

```sh
sudo launchctl print system/com.opportunity-radar.dashboard
```

For a new installation, a missing-service result is expected. Create only the
chosen dashboard log directory as the normal user, then install the plist with
root ownership and no group/world write permission:

```sh
mkdir -p "$dashboard_log_dir"
sudo install -o root -g wheel -m 0644 "$dashboard_plist" /Library/LaunchDaemons/com.opportunity-radar.dashboard.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.opportunity-radar.dashboard.plist
```

`RunAtLoad` starts the job. If a prior dashboard uninstall left it disabled,
enable only this service with
`sudo launchctl enable system/com.opportunity-radar.dashboard` before bootstrap.
Do not load/change any Radar plist. Do not start a second foreground dashboard
on port 8787 while this job owns the listener.

## Status, logs, and smoke

```sh
sudo launchctl print system/com.opportunity-radar.dashboard
tail -F "$dashboard_stdout" "$dashboard_stderr"
lsof -nP -iTCP:8787 -sTCP:LISTEN
curl --fail --show-error "http://$dashboard_ipv4:8787/api/status"
curl --fail --show-error "http://$dashboard_ipv4:8787/api/opportunities"
```

Inspect launchctl's state/PID/last exit status, verify the listener is on the exact
Tailscale address, and verify the process owner with `ps -o user,pid,command -p PID`
(replace `PID` with the reported dashboard PID). Repeat the two curl requests
from the authorized MacBook through Tailscale and open the scanner/Status UI.
An HTTP 200 alone does not establish source health: inspect JSON `errors`,
`data_as_of`, `overall.status` on status, and scanner `status`. Old samples,
missing sources, and insufficient historical coverage must remain visible.
If startup fails, inspect stderr for missing config, permissions, an unavailable
Tailscale address, or a port conflict. Fix only deployment values after review.
Launchd does not rotate these logs; arrange dashboard log retention separately
as an operations follow-up.

## Uninstall only the dashboard

```sh
sudo launchctl bootout system/com.opportunity-radar.dashboard
sudo rm /Library/LaunchDaemons/com.opportunity-radar.dashboard.plist
sudo launchctl print system/com.opportunity-radar.dashboard
```

The last command should report no such service. Bootout must succeed (or status
must confirm it was already absent) before removing the exact dashboard plist.
Removing the installed plist prevents its next boot startup; the repository
example can recreate it. Preserve dashboard logs and all Radar config, Parquet,
SQLite, jobs, and processes. This procedure removes only the installed dashboard
definition, never Radar data.

## Production benchmark gate — controller follow-up

Local CLI/documentation verification does not establish production performance.
Before persistent deployment, the controller must run a temporary read-only
dashboard against actual trading-mini production sources and record at least
30 cold `/api/opportunities` requests and 30 cached requests, plus `/api/pair`
for `24h`, `7d`, and `all` with complete exact pair identity. Report p50/p95/max
latencies, RSS/CPU at idle, scanner refresh, and pair-chart load. Preserve the
existing cache behavior when identifying actual cache hits/misses; do not label
requests as cached without evidence.

Compare a comparable Radar baseline and dashboard-load interval for scheduler
critical-path timing, missed/duplicate slots, cycle failures, and Parquet failures.
Accept only with zero additional missed/duplicate slots and no material scheduler
regression. If the gate fails, report evidence and stop; cache redesign or a new
optimization framework requires separate review. No remote benchmark, installation,
or Radar lifecycle action was performed in the local Phase 4 implementation.

References: [Apple launchd job documentation](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html),
[Tailscale CLI reference](https://tailscale.com/docs/reference/tailscale-cli), and
the target's `man launchctl` / `man launchd.plist` for its installed macOS version.

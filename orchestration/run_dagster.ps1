# Launch the Dagster UI + daemon (the pipeline observability layer).
#
#   - webserver: asset graph, run timeline, per-step logs, asset-check results
#   - daemon:    schedules, sensors (incl. the alarm + freshness sensors)
#
# Run from the orchestration/ folder:  ./run_dagster.ps1   then open http://localhost:3000
$ErrorActionPreference = "Stop"

# Resolve paths relative to this script so it works from any working directory.
$scriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$dagsterHome = Join-Path $scriptDir "dagster_home"

# Persistent instance (dagster.yaml lives in dagster_home/).
$env:DAGSTER_HOME = $dagsterHome
# base_dir for the LocalComputeLogManager (referenced from dagster.yaml).
$env:DAGSTER_COMPUTE_LOG_DIR = Join-Path $dagsterHome "compute_logs"

Write-Host "DAGSTER_HOME = $env:DAGSTER_HOME"
Write-Host "Starting Dagster UI at http://localhost:3000 (Ctrl+C to stop)..."

# -m loads the code location from the arte_dagster package (definitions.py::defs).
uv run dagster dev -m arte_dagster.definitions

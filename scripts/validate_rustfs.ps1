param(
    [string]$RawCsv = "..\it_vacancies_2024.csv",
    [int]$Attempts = 3,
    [switch]$SkipStart
)
$ErrorActionPreference = "Stop"
$project = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $project
if ($Attempts -lt 3) { throw "Use at least three attempts for submission evidence" }
if (Test-Path -LiteralPath ".env") {
    foreach ($line in Get-Content -LiteralPath ".env") {
        if ($line -match '^(RUSTFS_ACCESS_KEY|RUSTFS_SECRET_KEY|RUSTFS_ENDPOINT|RUSTFS_BUCKET)=(.*)$') {
            [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2].Trim(), "Process")
        }
    }
}
if (-not $env:RUSTFS_ACCESS_KEY -or -not $env:RUSTFS_SECRET_KEY) {
    throw "Set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY, or create a local .env"
}
$evidence = "evidence/rustfs-validation"
New-Item -ItemType Directory -Path $evidence -Force | Out-Null
$log = Join-Path $evidence "commands.log"
"RustFS validation: $(Get-Date -Format o)" | Set-Content -LiteralPath $log -Encoding utf8
function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    ($Program + " " + ($Arguments -join " ")) | Tee-Object -FilePath $log -Append
    & $Program @Arguments 2>&1 | Tee-Object -FilePath $log -Append
    if ($LASTEXITCODE -ne 0) { throw "$Program failed with exit code $LASTEXITCODE" }
}
if (-not $SkipStart) {
    Invoke-Checked "docker" @("compose", "--env-file", ".env", "up", "-d")
}
Invoke-Checked "docker" @("ps", "--filter", "name=rustfs-hw3", "--format", "{{.Names}} {{.Image}} {{.Status}}")
Invoke-Checked "docker" @("inspect", "--format", "{{.Config.Image}} {{.Image}}", "rustfs-hw3")
# Install an official DuckDB extension only if it has not been installed yet.
Invoke-Checked "python" @("-X", "utf8", "-c", "import duckdb; duckdb.connect().execute('INSTALL httpfs')")
Invoke-Checked "python" @("-X", "utf8", "src/s3_io.py", "upload", $RawCsv, "raw/it_vacancies_2024.csv")
Invoke-Checked "python" @("-X", "utf8", "src/s3_io.py", "upload", "data/prepared/devops_vacancies.ndjson", "variants/devops/vacancies.ndjson")
Invoke-Checked "python" @("-X", "utf8", "src/mapreduce_s3.py", "--out-dir", "results/rustfs/mapreduce", "--workers", "4", "--chunk-bytes", "524288", "--summary-out", "$evidence/mapreduce_run.json")
Invoke-Checked "python" @("-X", "utf8", "src/duckdb_counts_s3.py", "--out-dir", "results/rustfs/duckdb_counts", "--threads", "4")
Invoke-Checked "python" @("-X", "utf8", "src/duckdb_analysis.py", "--out-dir", "results/rustfs/duckdb", "--threads", "4", "--summary-out", "$evidence/duckdb_run.json")
Invoke-Checked "python" @("-X", "utf8", "src/duckdb_variant_check.py", "--out", "$evidence/variant_check.json")
Invoke-Checked "python" @("-X", "utf8", "src/verify_results.py", "results/rustfs/mapreduce", "results/rustfs/duckdb_counts", "--out", "$evidence/verification.json")
Invoke-Checked "python" @("-X", "utf8", "src/s3_io.py", "publish-results", "results/rustfs/mapreduce", "--out", "$evidence/published_results.json")
Invoke-Checked "python" @("-X", "utf8", "src/s3_io.py", "list", "--prefix", "results/mapreduce/", "--out", "$evidence/list_objects.json")
Invoke-Checked "python" @("-X", "utf8", "src/mapreduce_s3.py", "--out-dir", "results/rustfs/recovery", "--workers", "4", "--chunk-bytes", "524288", "--inject-failure", "--summary-out", "$evidence/mapper_failure.json")
Invoke-Checked "python" @("-X", "utf8", "src/verify_results.py", "results/rustfs/recovery", "results/rustfs/mapreduce", "--out", "$evidence/recovery_verification.json")
Invoke-Checked "python" @("-X", "utf8", "src/profile_pipeline.py", "--attempts", "$Attempts", "--workers", "4", "--chunk-bytes", "524288", "--out", "$evidence/profiling.json")
Copy-Item -LiteralPath "results/rustfs/duckdb/regions.json" -Destination "$evidence/regions.json"
Copy-Item -LiteralPath "results/rustfs/duckdb/salary_effects.json" -Destination "$evidence/salary_effects.json"
Write-Host "Validation complete. Evidence: $evidence. Git was not modified."

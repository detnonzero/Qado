param(
    [string]$MinerPublicKey,
    [string]$OpenClDevice = "1070",
    [string[]]$Peers = @(
        "116.202.117.5:33333",
        "116.202.117.6:33333",
        "116.202.117.7:33333",
        "212.227.21.183:33333",
        "[2a01:4f8:231:44e1::15]:33333"
    ),
    [double]$DurationHours = 96,
    [int]$P2pPort = 33491,
    [int]$ApiPort = 18121,
    [int]$SampleSeconds = 60,
    [string]$Label = "miner-soak-local",
    [string]$RunRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($MinerPublicKey)) {
    throw "MinerPublicKey is required."
}

$normalizedMinerPublicKey = $MinerPublicKey.Trim().ToLowerInvariant()
if ($normalizedMinerPublicKey.StartsWith("0x")) {
    $normalizedMinerPublicKey = $normalizedMinerPublicKey.Substring(2)
}

if ($normalizedMinerPublicKey.Length -ne 64 -or $normalizedMinerPublicKey -notmatch '^[0-9a-f]{64}$') {
    throw "MinerPublicKey must be a 64-char lowercase hex string."
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptRoot
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
if ([string]::IsNullOrWhiteSpace($RunRoot)) {
    $runRoot = Join-Path $repoRoot "artifacts\miner-soak\$timestamp"
}
else {
    $runRoot = $RunRoot
}

$dataDir = Join-Path $runRoot "data"
$nodeOutLog = Join-Path $runRoot "node.stdout.log"
$nodeErrLog = Join-Path $runRoot "node.stderr.log"
$runnerLog = Join-Path $runRoot "runner.log"
$samplesPath = Join-Path $runRoot "tip-samples.jsonl"
$summaryPath = Join-Path $runRoot "summary.txt"
$buildLog = Join-Path $runRoot "build.log"
$nodeProject = Join-Path $repoRoot "Qado.NodeHost\Qado.NodeHost.csproj"

New-Item -ItemType Directory -Force -Path $runRoot | Out-Null
New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

function Write-RunnerLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date).ToString("o"), $Message
    Add-Content -Path $runnerLog -Value $line
}

function Get-TipSnapshot {
    param([int]$Port)

    try {
        $tip = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/v1/tip" -f $Port) -TimeoutSec 10
        return [pscustomobject]@{
            ok = $true
            height = [string]$tip.height
            hash = [string]$tip.hash
            timestamp_utc = [string]$tip.timestamp_utc
            chainwork = [string]$tip.chainwork
            error = $null
        }
    }
    catch {
        return [pscustomobject]@{
            ok = $false
            height = $null
            hash = $null
            timestamp_utc = $null
            chainwork = $null
            error = $_.Exception.Message
        }
    }
}

Write-RunnerLog ("Preparing miner soak. peers={0} durationHours={1} p2p={2} api={3} device={4} miner={5}" -f ($Peers -join ","), $DurationHours, $P2pPort, $ApiPort, $OpenClDevice, $normalizedMinerPublicKey)

& dotnet build $nodeProject -c Release --nologo | Tee-Object -FilePath $buildLog
if ($LASTEXITCODE -ne 0) {
    throw "dotnet build failed with exit code $LASTEXITCODE"
}

$nodeDll = Get-ChildItem -Path (Join-Path $repoRoot "Qado.NodeHost\bin\Release") -Recurse -Filter "Qado.NodeHost.dll" |
    Sort-Object LastWriteTimeUtc -Descending |
    Select-Object -First 1 -ExpandProperty FullName

if ([string]::IsNullOrWhiteSpace($nodeDll) -or -not (Test-Path $nodeDll)) {
    throw "NodeHost DLL not found under Qado.NodeHost\bin\Release"
}

$nodeArgs = @(
    $nodeDll,
    "--label", $Label,
    "--data-dir", $dataDir,
    "--p2p-port", [string]$P2pPort,
    "--api-port", [string]$ApiPort,
    "--mine-opencl",
    "--miner-public-key", $normalizedMinerPublicKey,
    "--opencl-device", $OpenClDevice,
    "--no-default-seed"
)

foreach ($peer in $Peers) {
    $nodeArgs += "--peer"
    $nodeArgs += $peer
}

$startUtc = (Get-Date).ToUniversalTime()
$plannedEndUtc = $startUtc.AddHours($DurationHours)

$proc = Start-Process -FilePath "dotnet" `
    -ArgumentList $nodeArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $nodeOutLog `
    -RedirectStandardError $nodeErrLog `
    -PassThru

Write-RunnerLog ("NodeHost+Miner started. pid={0} plannedEndUtc={1}" -f $proc.Id, $plannedEndUtc.ToString("o"))

$apiReady = $false
$maxHeight = -1L
$maxHash = ""
$lastSnapshot = $null
$earlyExit = $false

try {
    while ((Get-Date).ToUniversalTime() -lt $plannedEndUtc) {
        Start-Sleep -Seconds $SampleSeconds

        if ($proc.HasExited) {
            $earlyExit = $true
            Write-RunnerLog ("NodeHost+Miner exited early with exitCode={0}" -f $proc.ExitCode)
            break
        }

        $snapshot = Get-TipSnapshot -Port $ApiPort
        $lastSnapshot = $snapshot

        if ($snapshot.ok) {
            $apiReady = $true

            $heightValue = -1L
            [void][long]::TryParse([string]$snapshot.height, [ref]$heightValue)
            if ($heightValue -gt $maxHeight) {
                $maxHeight = $heightValue
                $maxHash = [string]$snapshot.hash
            }
        }

        $sample = [pscustomobject]@{
            recorded_utc = (Get-Date).ToUniversalTime().ToString("o")
            process_id = $proc.Id
            process_alive = (-not $proc.HasExited)
            api_ready = $snapshot.ok
            tip_height = $snapshot.height
            tip_hash = $snapshot.hash
            tip_timestamp_utc = $snapshot.timestamp_utc
            chainwork = $snapshot.chainwork
            error = $snapshot.error
        }

        Add-Content -Path $samplesPath -Value ($sample | ConvertTo-Json -Compress)
        Write-RunnerLog ("Sample recorded. apiReady={0} tipHeight={1}" -f $snapshot.ok, $snapshot.height)
    }
}
finally {
    if (-not $proc.HasExited) {
        Write-RunnerLog "Stopping NodeHost+Miner after soak window."
        Stop-Process -Id $proc.Id -Force
        $proc.WaitForExit()
    }
}

$handshakeCount = 0
$caps255Count = 0
$syncResetCount = 0
$timeoutCount = 0
$adoptCount = 0
$watchdogCount = 0
$commitFailedCount = 0
$openClEnabledCount = 0
$openClTemplateCount = 0
$blockFoundCount = 0
$minedStoredCount = 0
$minedSideCount = 0
$minedRejectedCount = 0

if (Test-Path $nodeOutLog) {
    $handshakeLines = Select-String -Path $nodeOutLog -Pattern "handshake from" -SimpleMatch -ErrorAction SilentlyContinue
    $handshakeCount = @($handshakeLines).Count
    $caps255Count = @($handshakeLines | Where-Object { $_.Line -match "caps=255" }).Count
    $syncResetCount = @(Select-String -Path $nodeOutLog -Pattern "Block sync reset" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $timeoutCount = @(Select-String -Path $nodeOutLog -Pattern "batch timeout" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $adoptCount = @(Select-String -Path $nodeOutLog -Pattern "Adopted tip" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $watchdogCount = @(Select-String -Path $nodeOutLog -Pattern "watchdog revived" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $commitFailedCount = @(Select-String -Path $nodeOutLog -Pattern "commit failed" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $openClEnabledCount = @(Select-String -Path $nodeOutLog -Pattern "OpenCL mining enabled on" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $openClTemplateCount = @(Select-String -Path $nodeOutLog -Pattern "Mining (OpenCL) h=" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $blockFoundCount = @(Select-String -Path $nodeOutLog -Pattern "Block FOUND" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $minedStoredCount = @(Select-String -Path $nodeOutLog -Pattern "Mined + stored:" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $minedSideCount = @(Select-String -Path $nodeOutLog -Pattern "Mined + stored (side/reorg candidate)" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $minedRejectedCount = @(Select-String -Path $nodeOutLog -Pattern "Rejecting mined block" -SimpleMatch -ErrorAction SilentlyContinue).Count
}

$summary = @(
    ("run_root={0}" -f $runRoot)
    ("start_utc={0}" -f $startUtc.ToString("o"))
    ("planned_end_utc={0}" -f $plannedEndUtc.ToString("o"))
    ("finished_utc={0}" -f ((Get-Date).ToUniversalTime().ToString("o")))
    ("duration_hours={0}" -f $DurationHours)
    ("process_id={0}" -f $proc.Id)
    ("exit_code={0}" -f $proc.ExitCode)
    ("early_exit={0}" -f $earlyExit)
    ("api_ready={0}" -f $apiReady)
    ("max_tip_height={0}" -f $maxHeight)
    ("max_tip_hash={0}" -f $maxHash)
    ("last_tip_height={0}" -f $(if ($lastSnapshot) { [string]$lastSnapshot.height } else { "" }))
    ("last_tip_hash={0}" -f $(if ($lastSnapshot) { [string]$lastSnapshot.hash } else { "" }))
    ("handshake_count={0}" -f $handshakeCount)
    ("caps255_handshake_count={0}" -f $caps255Count)
    ("sync_reset_count={0}" -f $syncResetCount)
    ("batch_timeout_count={0}" -f $timeoutCount)
    ("adopted_tip_count={0}" -f $adoptCount)
    ("watchdog_revived_count={0}" -f $watchdogCount)
    ("commit_failed_count={0}" -f $commitFailedCount)
    ("opencl_enabled_count={0}" -f $openClEnabledCount)
    ("opencl_template_count={0}" -f $openClTemplateCount)
    ("mined_block_found_count={0}" -f $blockFoundCount)
    ("mined_stored_canonical_count={0}" -f $minedStoredCount)
    ("mined_stored_side_count={0}" -f $minedSideCount)
    ("mined_rejected_count={0}" -f $minedRejectedCount)
    ("miner_public_key={0}" -f $normalizedMinerPublicKey)
    ("opencl_device_selector={0}" -f $OpenClDevice)
    ("peers={0}" -f ($Peers -join ","))
    ("node_stdout_log={0}" -f $nodeOutLog)
    ("node_stderr_log={0}" -f $nodeErrLog)
    ("runner_log={0}" -f $runnerLog)
    ("tip_samples={0}" -f $samplesPath)
)

Set-Content -Path $summaryPath -Value $summary
Write-RunnerLog ("Summary written to {0}" -f $summaryPath)

Get-Content -Path $summaryPath

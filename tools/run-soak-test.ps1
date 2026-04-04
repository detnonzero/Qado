param(
    [bool]$UseDefaultSeeds = $true,
    [string[]]$Peers = @(),
    [int]$DurationHours = 4,
    [int]$P2pPort = 33443,
    [int]$ApiPort = 18091,
    [int]$SampleSeconds = 60,
    [string]$Label = "soak-local",
    [string]$RunRoot = ""
)

$ErrorActionPreference = "Stop"

if (-not $UseDefaultSeeds -and ($null -eq $Peers -or $Peers.Count -eq 0)) {
    throw "When UseDefaultSeeds is false, at least one bootstrap peer must be provided via -Peers."
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptRoot
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
if ([string]::IsNullOrWhiteSpace($RunRoot)) {
    $runRoot = Join-Path $repoRoot "artifacts\\soak\\$timestamp"
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
$nodeProject = Join-Path $repoRoot "Qado.NodeHost\\Qado.NodeHost.csproj"
$nodeDll = $null

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

Write-RunnerLog ("Preparing soak test. defaultSeeds={0} peers={1} durationHours={2} p2p={3} api={4}" -f $UseDefaultSeeds, ($Peers -join ","), $DurationHours, $P2pPort, $ApiPort)

& dotnet build $nodeProject -c Release --nologo | Tee-Object -FilePath (Join-Path $runRoot "build.log")
if ($LASTEXITCODE -ne 0) {
    throw "dotnet build failed with exit code $LASTEXITCODE"
}

$nodeDll = Get-ChildItem -Path (Join-Path $repoRoot "Qado.NodeHost\\bin\\Release") -Recurse -Filter "Qado.NodeHost.dll" |
    Sort-Object LastWriteTimeUtc -Descending |
    Select-Object -First 1 -ExpandProperty FullName

if ([string]::IsNullOrWhiteSpace($nodeDll) -or -not (Test-Path $nodeDll)) {
    throw "NodeHost DLL not found under Qado.NodeHost\\bin\\Release"
}

$nodeArgs = @(
    $nodeDll,
    "--label", $Label,
    "--data-dir", $dataDir,
    "--p2p-port", [string]$P2pPort,
    "--api-port", [string]$ApiPort
)

if (-not $UseDefaultSeeds) {
    $nodeArgs += "--no-default-seed"
}

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

Write-RunnerLog ("NodeHost started. pid={0} plannedEndUtc={1}" -f $proc.Id, $plannedEndUtc.ToString("o"))

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
            Write-RunnerLog ("NodeHost exited early with exitCode={0}" -f $proc.ExitCode)
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
        Write-RunnerLog "Stopping NodeHost after soak window."
        Stop-Process -Id $proc.Id -Force
        $proc.WaitForExit()
    }
}

$handshakeCount = 0
$ipv4HandshakeCount = 0
$ipv6HandshakeCount = 0
$caps63Count = 0
$caps127Count = 0
$syncResetCount = 0
$timeoutCount = 0
$adoptCount = 0

if (Test-Path $nodeOutLog) {
    $handshakeLines = Select-String -Path $nodeOutLog -Pattern "handshake from" -SimpleMatch -ErrorAction SilentlyContinue
    $handshakeCount = @($handshakeLines).Count
    $ipv4HandshakeCount = @($handshakeLines | Where-Object { $_.Line -match "\(InterNetwork\)" -or $_.Line -match ":[0-9]+\)?" }).Count
    $ipv6HandshakeCount = @($handshakeLines | Where-Object { $_.Line -match "\[" }).Count
    $caps63Count = @($handshakeLines | Where-Object { $_.Line -match "caps=63" }).Count
    $caps127Count = @($handshakeLines | Where-Object { $_.Line -match "caps=127" }).Count
    $syncResetCount = @(Select-String -Path $nodeOutLog -Pattern "Block sync reset" -SimpleMatch -ErrorAction SilentlyContinue).Count
    $timeoutCount = @(Select-String -Path $nodeOutLog -Pattern "timeout" -ErrorAction SilentlyContinue).Count
    $adoptCount = @(Select-String -Path $nodeOutLog -Pattern "Adopted tip" -SimpleMatch -ErrorAction SilentlyContinue).Count
}

$summary = @(
    ("run_root={0}" -f $runRoot)
    ("start_utc={0}" -f $startUtc.ToString("o"))
    ("planned_end_utc={0}" -f $plannedEndUtc.ToString("o"))
    ("finished_utc={0}" -f ((Get-Date).ToUniversalTime().ToString("o")))
    ("duration_hours={0}" -f $DurationHours)
    ("use_default_seeds={0}" -f $UseDefaultSeeds)
    ("process_id={0}" -f $proc.Id)
    ("exit_code={0}" -f $proc.ExitCode)
    ("early_exit={0}" -f $earlyExit)
    ("api_ready={0}" -f $apiReady)
    ("max_tip_height={0}" -f $maxHeight)
    ("max_tip_hash={0}" -f $maxHash)
    ("last_tip_height={0}" -f $(if ($lastSnapshot) { [string]$lastSnapshot.height } else { "" }))
    ("last_tip_hash={0}" -f $(if ($lastSnapshot) { [string]$lastSnapshot.hash } else { "" }))
    ("handshake_count={0}" -f $handshakeCount)
    ("ipv4_handshake_count={0}" -f $ipv4HandshakeCount)
    ("ipv6_handshake_count={0}" -f $ipv6HandshakeCount)
    ("caps63_handshake_count={0}" -f $caps63Count)
    ("caps127_handshake_count={0}" -f $caps127Count)
    ("sync_reset_count={0}" -f $syncResetCount)
    ("timeout_line_count={0}" -f $timeoutCount)
    ("adopted_tip_count={0}" -f $adoptCount)
    ("bootstrap_peers={0}" -f ($Peers -join ","))
    ("node_stdout_log={0}" -f $nodeOutLog)
    ("node_stderr_log={0}" -f $nodeErrLog)
    ("runner_log={0}" -f $runnerLog)
    ("tip_samples={0}" -f $samplesPath)
)

Set-Content -Path $summaryPath -Value $summary
Write-RunnerLog ("Summary written to {0}" -f $summaryPath)

Get-Content -Path $summaryPath

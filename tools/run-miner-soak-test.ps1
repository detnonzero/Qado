param(
    [string]$MinerPublicKey,
    [string]$OpenClDevice = "1070",
    [bool]$UseDefaultSeeds = $true,
    [string[]]$Peers = @(),
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

if (-not $UseDefaultSeeds -and ($null -eq $Peers -or $Peers.Count -eq 0)) {
    throw "When UseDefaultSeeds is false, at least one bootstrap peer must be provided via -Peers."
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

function Get-ReadinessSnapshot {
    param([int]$Port)

    try {
        $readiness = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/v1/readiness" -f $Port) -TimeoutSec 10
        return [pscustomobject]@{
            ok = $true
            ready = [bool]$readiness.ready
            reason = if ($null -ne $readiness.reason -and -not [string]::IsNullOrWhiteSpace([string]$readiness.reason)) { [string]$readiness.reason } else { $null }
            initial_block_sync_active = [bool]$readiness.initial_block_sync_active
            peer_count = [string]$readiness.peer_count
            tip_height = [string]$readiness.tip_height
            tip_hash = [string]$readiness.tip_hash
            timestamp_utc = [string]$readiness.timestamp_utc
            error = $null
        }
    }
    catch {
        return [pscustomobject]@{
            ok = $false
            ready = $null
            reason = $null
            initial_block_sync_active = $null
            peer_count = $null
            tip_height = $null
            tip_hash = $null
            timestamp_utc = $null
            error = $_.Exception.Message
        }
    }
}

Write-RunnerLog ("Preparing miner soak. defaultSeeds={0} peers={1} durationHours={2} p2p={3} api={4} device={5} miner={6}" -f $UseDefaultSeeds, ($Peers -join ","), $DurationHours, $P2pPort, $ApiPort, $OpenClDevice, $normalizedMinerPublicKey)

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
    "--opencl-device", $OpenClDevice
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
        $readiness = Get-ReadinessSnapshot -Port $ApiPort
        $lastSnapshot = $snapshot

        if ($snapshot.ok -or $readiness.ok) {
            $apiReady = $true
        }

        if ($snapshot.ok) {
            $heightValue = -1L
            [void][long]::TryParse([string]$snapshot.height, [ref]$heightValue)
            if ($heightValue -gt $maxHeight) {
                $maxHeight = $heightValue
                $maxHash = [string]$snapshot.hash
            }
        }
        elseif ($readiness.ok) {
            $heightValue = -1L
            [void][long]::TryParse([string]$readiness.tip_height, [ref]$heightValue)
            if ($heightValue -gt $maxHeight) {
                $maxHeight = $heightValue
                $maxHash = [string]$readiness.tip_hash
            }
        }

        $sample = [pscustomobject]@{
            recorded_utc = (Get-Date).ToUniversalTime().ToString("o")
            process_id = $proc.Id
            process_alive = (-not $proc.HasExited)
            api_ready = ($snapshot.ok -or $readiness.ok)
            tip_api_ok = $snapshot.ok
            tip_height = $snapshot.height
            tip_hash = $snapshot.hash
            tip_timestamp_utc = $snapshot.timestamp_utc
            chainwork = $snapshot.chainwork
            readiness_api_ok = $readiness.ok
            readiness_ready = if ($readiness.ok) { [bool]$readiness.ready } else { $null }
            readiness_reason = if ($readiness.ok) { $readiness.reason } else { $null }
            initial_block_sync_active = if ($readiness.ok) { [bool]$readiness.initial_block_sync_active } else { $null }
            peer_count = if ($readiness.ok) { $readiness.peer_count } else { $null }
            readiness_tip_height = if ($readiness.ok) { $readiness.tip_height } else { $null }
            readiness_tip_hash = if ($readiness.ok) { $readiness.tip_hash } else { $null }
            error = $snapshot.error
            readiness_error = $readiness.error
        }

        Add-Content -Path $samplesPath -Value ($sample | ConvertTo-Json -Compress)
        Write-RunnerLog ("Sample recorded. tipOk={0} readinessOk={1} ready={2} tipHeight={3} peerCount={4}" -f $snapshot.ok, $readiness.ok, $sample.readiness_ready, $(if ($snapshot.ok) { $snapshot.height } else { $readiness.tip_height }), $sample.peer_count)
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
$parentPackRequestCount = 0
$parentPackBackupRequestCount = 0
$parentPackRequestUniquePeerCount = 0
$parentPackRequestFailureCount = 0
$liveOrphanBufferedCount = 0
$outOfPlanOrphanBufferedCount = 0
$orphanBufferedCount = 0
$orphanBufferedTotalCount = 0
$outOfPlanMissingBlockResyncCount = 0
$missingCandidatePayloadRecoveryCount = 0
$sampleCount = 0
$apiReadySampleCount = 0
$readinessApiOkSampleCount = 0
$readinessApiErrorSampleCount = 0
$readinessReadySampleCount = 0
$readinessNotReadySampleCount = 0
$peerTipAheadSampleCount = 0
$noConnectedPeersSampleCount = 0
$initialSyncActiveSampleCount = 0
$peerCountLeOneSampleCount = 0
$minPeerCount = [int]::MaxValue
$maxPeerCount = -1
$peerCountSum = 0
$peerCountObservedSamples = 0
$tipChangeCount = 0
$longestTipPlateauSeconds = 0
$longestNotReadyWindowSeconds = 0
$firstTipHeight = ""
$firstTipHash = ""
$uniqueHandshakePeerCount = 0
$ipv4HandshakeCount = 0
$ipv6HandshakeCount = 0
$pexSentCount = 0
$pexReceivedCount = 0
$duplicateSessionDropCount = 0
$duplicatePeerResolvedCount = 0
$inboundPublicClaimRegisteredCount = 0
$inboundPublicClaimIgnoredCount = 0
$canonicalTailPushCount = 0
$blockSyncContinuingCount = 0
$blockSyncContinuingUniquePeerCount = 0

if (Test-Path $nodeOutLog) {
    $allLogLines = Get-Content -Path $nodeOutLog -ErrorAction SilentlyContinue
    $handshakeLines = @($allLogLines | Select-String -Pattern "handshake from" -SimpleMatch)
    $handshakeCount = @($handshakeLines).Count
    $caps255Count = @($handshakeLines | Where-Object { $_.Line -match "caps=255" }).Count
    $ipv6HandshakeCount = @($handshakeLines | Where-Object { $_.Line -match "handshake from \\[" }).Count
    $ipv4HandshakeCount = $handshakeCount - $ipv6HandshakeCount
    $syncResetCount = @($allLogLines | Select-String -Pattern "Block sync reset" -SimpleMatch).Count
    $timeoutCount = @($allLogLines | Select-String -Pattern "batch timeout" -SimpleMatch).Count
    $adoptCount = @($allLogLines | Select-String -Pattern "Adopted tip" -SimpleMatch).Count
    $watchdogCount = @($allLogLines | Select-String -Pattern "watchdog revived" -SimpleMatch).Count
    $commitFailedCount = @($allLogLines | Select-String -Pattern "commit failed" -SimpleMatch).Count
    $openClEnabledCount = @($allLogLines | Select-String -Pattern "OpenCL mining enabled on" -SimpleMatch).Count
    $openClTemplateCount = @($allLogLines | Select-String -Pattern "Mining (OpenCL) h=" -SimpleMatch).Count
    $blockFoundCount = @($allLogLines | Select-String -Pattern "Block FOUND" -SimpleMatch).Count
    $minedStoredCount = @($allLogLines | Select-String -Pattern "Mined + stored:" -SimpleMatch).Count
    $minedSideCount = @($allLogLines | Select-String -Pattern "Mined + stored (side/reorg candidate)" -SimpleMatch).Count
    $minedRejectedCount = @($allLogLines | Select-String -Pattern "Rejecting mined block" -SimpleMatch).Count

    $parentPackRequestLines = @($allLogLines | Select-String -Pattern "requested parent pack" -SimpleMatch)
    $parentPackRequestCount = $parentPackRequestLines.Count
    $parentPackBackupRequestCount = @($parentPackRequestLines | Where-Object { $_.Line -like "*-backup: requested parent pack*" }).Count
    $parentPackRequestFailureCount = @($allLogLines | Select-String -Pattern "parent-pack request failed" -SimpleMatch).Count
    $liveOrphanBufferedCount = @($allLogLines | Select-String -Pattern "Live orphan buffered" -SimpleMatch).Count
    $outOfPlanOrphanBufferedCount = @($allLogLines | Select-String -Pattern "Out-of-plan orphan buffered" -SimpleMatch).Count
    $orphanBufferedCount = @($allLogLines | Select-String -Pattern "Orphan buffered:" -SimpleMatch).Count
    $orphanBufferedTotalCount = $liveOrphanBufferedCount + $outOfPlanOrphanBufferedCount + $orphanBufferedCount
    $outOfPlanMissingBlockResyncCount = @($allLogLines | Select-String -Pattern "block-out-of-plan-missing-block" -SimpleMatch).Count
    $missingCandidatePayloadRecoveryCount = @($allLogLines | Select-String -Pattern "Recovery sync requested for missing candidate payload" -SimpleMatch).Count
    $pexSentCount = @($allLogLines | Select-String -Pattern "] [INFO] [PEX] Sent " -SimpleMatch).Count
    $pexReceivedCount = @($allLogLines | Select-String -Pattern "] [INFO] [PEX] Received " -SimpleMatch).Count
    $duplicateSessionDropCount = @($allLogLines | Select-String -Pattern "duplicate session drop:" -SimpleMatch).Count
    $duplicatePeerResolvedCount = @($allLogLines | Select-String -Pattern "duplicate peer session resolved" -SimpleMatch).Count
    $inboundPublicClaimRegisteredCount = @($allLogLines | Select-String -Pattern "inbound public claim registered" -SimpleMatch).Count
    $inboundPublicClaimIgnoredCount = @($allLogLines | Select-String -Pattern "inbound public claim ignored" -SimpleMatch).Count
    $canonicalTailPushCount = @($allLogLines | Select-String -Pattern "Pushed canonical tail to lagging peer" -SimpleMatch).Count
    $blockSyncContinuingLines = @($allLogLines | Select-String -Pattern "Block sync continuing from" -SimpleMatch)
    $blockSyncContinuingCount = $blockSyncContinuingLines.Count

    $parentPackPeers = @{}
    foreach ($line in $parentPackRequestLines) {
        if ($line.Line -match "requested parent pack .* from (?<endpoint>.+?)(?: \(\+\d+ similar requests suppressed\))?$") {
            $parentPackPeers[$matches.endpoint] = $true
        }
    }

    $parentPackRequestUniquePeerCount = $parentPackPeers.Count

    $handshakePeers = @{}
    foreach ($line in $handshakeLines) {
        if ($line.Line -match "handshake from (?<endpoint>.+?) \\(") {
            $handshakePeers[$matches.endpoint] = $true
        }
    }

    $uniqueHandshakePeerCount = $handshakePeers.Count

    $syncContinuePeers = @{}
    foreach ($line in $blockSyncContinuingLines) {
        if ($line.Line -match " via (?<endpoint>.+?)\\.$") {
            $syncContinuePeers[$matches.endpoint] = $true
        }
    }

    $blockSyncContinuingUniquePeerCount = $syncContinuePeers.Count
}

if (Test-Path $samplesPath) {
    $sampleObjects = @(
        Get-Content -Path $samplesPath -ErrorAction SilentlyContinue |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_ | ConvertFrom-Json }
    )

    $sampleCount = $sampleObjects.Count
    $lastTipHash = $null
    $plateauStartUtc = $null
    $lastTipSeenUtc = $null
    $notReadyWindowStartUtc = $null
    $lastNotReadySeenUtc = $null

    foreach ($sample in $sampleObjects) {
        if ([bool]$sample.api_ready) {
            $apiReadySampleCount++
        }

        $readinessApiOk = [bool]$sample.readiness_api_ok
        if ($readinessApiOk) {
            $readinessApiOkSampleCount++

            if ([bool]$sample.readiness_ready) {
                $readinessReadySampleCount++
            }
            else {
                $readinessNotReadySampleCount++
            }

            if ([bool]$sample.initial_block_sync_active) {
                $initialSyncActiveSampleCount++
            }

            $readinessReason = [string]$sample.readiness_reason
            if ($readinessReason -eq "peer_tip_ahead") {
                $peerTipAheadSampleCount++
            }
            elseif ($readinessReason -eq "no_connected_peers") {
                $noConnectedPeersSampleCount++
            }

            $peerCountValue = 0
            if ($null -ne $sample.peer_count -and [int]::TryParse([string]$sample.peer_count, [ref]$peerCountValue)) {
                $peerCountSum += $peerCountValue
                $peerCountObservedSamples++

                if ($peerCountValue -lt $minPeerCount) {
                    $minPeerCount = $peerCountValue
                }

                if ($peerCountValue -gt $maxPeerCount) {
                    $maxPeerCount = $peerCountValue
                }

                if ($peerCountValue -le 1) {
                    $peerCountLeOneSampleCount++
                }
            }
        }

        $recordedUtc = [DateTimeOffset]::MinValue
        $recordedOk = [DateTimeOffset]::TryParse([string]$sample.recorded_utc, [ref]$recordedUtc)
        if (-not $recordedOk) {
            continue
        }

        if ($readinessApiOk -and -not [bool]$sample.readiness_ready) {
            if ($null -eq $notReadyWindowStartUtc) {
                $notReadyWindowStartUtc = $recordedUtc
            }

            $lastNotReadySeenUtc = $recordedUtc
        }
        elseif ($null -ne $notReadyWindowStartUtc -and $null -ne $lastNotReadySeenUtc) {
            $notReadyWindowSeconds = [int][Math]::Round(($lastNotReadySeenUtc - $notReadyWindowStartUtc).TotalSeconds, 0)
            if ($notReadyWindowSeconds -gt $longestNotReadyWindowSeconds) {
                $longestNotReadyWindowSeconds = $notReadyWindowSeconds
            }

            $notReadyWindowStartUtc = $null
            $lastNotReadySeenUtc = $null
        }

        if (-not [bool]$sample.tip_api_ok -or [string]::IsNullOrWhiteSpace([string]$sample.tip_hash)) {
            continue
        }

        $tipHash = [string]$sample.tip_hash
        if ([string]::IsNullOrWhiteSpace($lastTipHash)) {
            $firstTipHeight = [string]$sample.tip_height
            $firstTipHash = $tipHash
            $lastTipHash = $tipHash
            $plateauStartUtc = $recordedUtc
            $lastTipSeenUtc = $recordedUtc
            continue
        }

        if ($tipHash -eq $lastTipHash) {
            $lastTipSeenUtc = $recordedUtc
            continue
        }

        if ($null -ne $plateauStartUtc -and $null -ne $lastTipSeenUtc) {
            $plateauSeconds = [int][Math]::Round(($lastTipSeenUtc - $plateauStartUtc).TotalSeconds, 0)
            if ($plateauSeconds -gt $longestTipPlateauSeconds) {
                $longestTipPlateauSeconds = $plateauSeconds
            }
        }

        $tipChangeCount++
        $lastTipHash = $tipHash
        $plateauStartUtc = $recordedUtc
        $lastTipSeenUtc = $recordedUtc
    }

    if ($null -ne $plateauStartUtc -and $null -ne $lastTipSeenUtc) {
        $plateauSeconds = [int][Math]::Round(($lastTipSeenUtc - $plateauStartUtc).TotalSeconds, 0)
        if ($plateauSeconds -gt $longestTipPlateauSeconds) {
            $longestTipPlateauSeconds = $plateauSeconds
        }
    }

    if ($null -ne $notReadyWindowStartUtc -and $null -ne $lastNotReadySeenUtc) {
        $notReadyWindowSeconds = [int][Math]::Round(($lastNotReadySeenUtc - $notReadyWindowStartUtc).TotalSeconds, 0)
        if ($notReadyWindowSeconds -gt $longestNotReadyWindowSeconds) {
            $longestNotReadyWindowSeconds = $notReadyWindowSeconds
        }
    }

    $readinessApiErrorSampleCount = $sampleCount - $readinessApiOkSampleCount
}

if ($minPeerCount -eq [int]::MaxValue) {
    $minPeerCount = -1
}

$avgPeerCount = -1
if ($peerCountObservedSamples -gt 0) {
    $avgPeerCount = [Math]::Round($peerCountSum / [double]$peerCountObservedSamples, 2)
}

$tipHeightDelta = ""
if (-not [string]::IsNullOrWhiteSpace($firstTipHeight) -and -not [string]::IsNullOrWhiteSpace($(if ($lastSnapshot) { [string]$lastSnapshot.height } else { "" }))) {
    $firstTipValue = 0L
    $lastTipValue = 0L
    if ([long]::TryParse($firstTipHeight, [ref]$firstTipValue) -and [long]::TryParse($(if ($lastSnapshot) { [string]$lastSnapshot.height } else { "" }), [ref]$lastTipValue)) {
        $tipHeightDelta = [string]($lastTipValue - $firstTipValue)
    }
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
    ("first_tip_height={0}" -f $firstTipHeight)
    ("first_tip_hash={0}" -f $firstTipHash)
    ("max_tip_height={0}" -f $maxHeight)
    ("max_tip_hash={0}" -f $maxHash)
    ("last_tip_height={0}" -f $(if ($lastSnapshot) { [string]$lastSnapshot.height } else { "" }))
    ("last_tip_hash={0}" -f $(if ($lastSnapshot) { [string]$lastSnapshot.hash } else { "" }))
    ("tip_height_delta={0}" -f $tipHeightDelta)
    ("sample_count={0}" -f $sampleCount)
    ("api_ready_sample_count={0}" -f $apiReadySampleCount)
    ("readiness_api_ok_sample_count={0}" -f $readinessApiOkSampleCount)
    ("readiness_api_error_sample_count={0}" -f $readinessApiErrorSampleCount)
    ("readiness_ready_sample_count={0}" -f $readinessReadySampleCount)
    ("readiness_not_ready_sample_count={0}" -f $readinessNotReadySampleCount)
    ("peer_tip_ahead_sample_count={0}" -f $peerTipAheadSampleCount)
    ("no_connected_peers_sample_count={0}" -f $noConnectedPeersSampleCount)
    ("initial_sync_active_sample_count={0}" -f $initialSyncActiveSampleCount)
    ("peer_count_le_one_sample_count={0}" -f $peerCountLeOneSampleCount)
    ("min_peer_count={0}" -f $minPeerCount)
    ("max_peer_count={0}" -f $maxPeerCount)
    ("avg_peer_count={0}" -f $avgPeerCount)
    ("tip_change_count={0}" -f $tipChangeCount)
    ("longest_tip_plateau_seconds={0}" -f $longestTipPlateauSeconds)
    ("longest_not_ready_window_seconds={0}" -f $longestNotReadyWindowSeconds)
    ("handshake_count={0}" -f $handshakeCount)
    ("unique_handshake_peer_count={0}" -f $uniqueHandshakePeerCount)
    ("ipv4_handshake_count={0}" -f $ipv4HandshakeCount)
    ("ipv6_handshake_count={0}" -f $ipv6HandshakeCount)
    ("caps255_handshake_count={0}" -f $caps255Count)
    ("sync_reset_count={0}" -f $syncResetCount)
    ("batch_timeout_count={0}" -f $timeoutCount)
    ("adopted_tip_count={0}" -f $adoptCount)
    ("watchdog_revived_count={0}" -f $watchdogCount)
    ("commit_failed_count={0}" -f $commitFailedCount)
    ("pex_sent_count={0}" -f $pexSentCount)
    ("pex_received_count={0}" -f $pexReceivedCount)
    ("duplicate_session_drop_count={0}" -f $duplicateSessionDropCount)
    ("duplicate_peer_resolved_count={0}" -f $duplicatePeerResolvedCount)
    ("inbound_public_claim_registered_count={0}" -f $inboundPublicClaimRegisteredCount)
    ("inbound_public_claim_ignored_count={0}" -f $inboundPublicClaimIgnoredCount)
    ("canonical_tail_push_count={0}" -f $canonicalTailPushCount)
    ("block_sync_continuing_count={0}" -f $blockSyncContinuingCount)
    ("block_sync_continuing_unique_peer_count={0}" -f $blockSyncContinuingUniquePeerCount)
    ("parent_pack_request_count={0}" -f $parentPackRequestCount)
    ("parent_pack_backup_request_count={0}" -f $parentPackBackupRequestCount)
    ("parent_pack_request_unique_peer_count={0}" -f $parentPackRequestUniquePeerCount)
    ("parent_pack_request_failure_count={0}" -f $parentPackRequestFailureCount)
    ("live_orphan_buffered_count={0}" -f $liveOrphanBufferedCount)
    ("out_of_plan_orphan_buffered_count={0}" -f $outOfPlanOrphanBufferedCount)
    ("orphan_buffered_count={0}" -f $orphanBufferedCount)
    ("orphan_buffered_total_count={0}" -f $orphanBufferedTotalCount)
    ("out_of_plan_missing_block_resync_count={0}" -f $outOfPlanMissingBlockResyncCount)
    ("missing_candidate_payload_recovery_count={0}" -f $missingCandidatePayloadRecoveryCount)
    ("opencl_enabled_count={0}" -f $openClEnabledCount)
    ("opencl_template_count={0}" -f $openClTemplateCount)
    ("mined_block_found_count={0}" -f $blockFoundCount)
    ("mined_stored_canonical_count={0}" -f $minedStoredCount)
    ("mined_stored_side_count={0}" -f $minedSideCount)
    ("mined_rejected_count={0}" -f $minedRejectedCount)
    ("miner_public_key={0}" -f $normalizedMinerPublicKey)
    ("opencl_device_selector={0}" -f $OpenClDevice)
    ("bootstrap_peers={0}" -f ($Peers -join ","))
    ("node_stdout_log={0}" -f $nodeOutLog)
    ("node_stderr_log={0}" -f $nodeErrLog)
    ("runner_log={0}" -f $runnerLog)
    ("tip_samples={0}" -f $samplesPath)
)

Set-Content -Path $summaryPath -Value $summary
Write-RunnerLog ("Summary written to {0}" -f $summaryPath)

Get-Content -Path $summaryPath

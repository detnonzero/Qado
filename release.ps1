param(
    [string]$Project = ".\Qado\Qado.csproj",
    [string]$Configuration = "Release",
    [string[]]$Runtimes = @("win-x64"),
    [string]$OutputRoot = ".\release",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' not found in PATH."
    }
}

function To-RelativeUnixPath {
    param(
        [string]$Root,
        [string]$Path
    )

    $relative = $Path.Substring($Root.Length)
    $relative = $relative.TrimStart([char[]]@('\', '/'))
    return $relative.Replace('\', '/')
}

Assert-Command "dotnet"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir
try {
    if ($Clean -and (Test-Path $OutputRoot)) {
        Remove-Item -Path $OutputRoot -Recurse -Force
    }

    New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null

    foreach ($rid in $Runtimes) {
        if ([string]::IsNullOrWhiteSpace($rid)) {
            throw "Runtime identifier list contains an empty value."
        }

        $outDir = Join-Path $OutputRoot $rid
        New-Item -Path $outDir -ItemType Directory -Force | Out-Null

        Write-Host "Publishing for $rid ..."
        $args = @(
            "publish",
            $Project,
            "-c", $Configuration,
            "-r", $rid,
            "--self-contained", "true",
            "/p:PublishSingleFile=true",
            "-o", $outDir
        )

        & dotnet @args
        if ($LASTEXITCODE -ne 0) {
            throw "dotnet publish failed for runtime '$rid' with exit code $LASTEXITCODE."
        }
    }

    $rootPath = (Resolve-Path $OutputRoot).Path
    $sumsPath = Join-Path $rootPath "SHA256SUMS.txt"

    if (Test-Path $sumsPath) {
        Remove-Item -Path $sumsPath -Force
    }

    $lines = Get-ChildItem -Path $rootPath -Recurse -File |
        Where-Object { $_.FullName -ne $sumsPath } |
        Sort-Object FullName |
        ForEach-Object {
            $hash = (Get-FileHash -Path $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            $rel = To-RelativeUnixPath -Root $rootPath -Path $_.FullName
            "{0} *{1}" -f $hash, $rel
        }

    Set-Content -Path $sumsPath -Value $lines -Encoding ascii

    Write-Host ""
    Write-Host "Release build completed."
    Write-Host "Output folder:   $rootPath"
    Write-Host "SHA256 sums:     $sumsPath"
}
finally {
    Pop-Location
}

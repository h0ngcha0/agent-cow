$ErrorActionPreference = "Stop"

$Repo = if ($env:AGENT_COW_REPO) { $env:AGENT_COW_REPO } else { "h0ngcha0/agent-cow" }
$Version = if ($env:AGENT_COW_VERSION) { $env:AGENT_COW_VERSION } else { "latest" }
$InstallDir = if ($env:AGENT_COW_INSTALL_DIR) { $env:AGENT_COW_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Programs\\agent-cow\\bin" }
$BinName = "agent-cow.exe"
$GitHubToken = if ($env:AGENT_COW_GITHUB_TOKEN) { $env:AGENT_COW_GITHUB_TOKEN } elseif ($env:GITHUB_TOKEN) { $env:GITHUB_TOKEN } elseif ($env:GH_TOKEN) { $env:GH_TOKEN } else { $null }

$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
switch ($arch) {
    "X64" { $target = "x86_64-pc-windows-msvc" }
    default { throw "Published Windows binaries currently support x86_64 only." }
}

$archive = "agent-cow-$target.zip"

$tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("agent-cow-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $tmpRoot | Out-Null

try {
    $zipPath = Join-Path $tmpRoot $archive
    if ($GitHubToken) {
        if ($Version -eq "latest") {
            $releaseApi = "https://api.github.com/repos/$Repo/releases/latest"
        } else {
            $releaseApi = "https://api.github.com/repos/$Repo/releases/tags/$Version"
        }
        $release = Invoke-RestMethod -Uri $releaseApi -Headers @{ Authorization = "Bearer $GitHubToken" }
        $asset = $release.assets | Where-Object { $_.name -eq $archive } | Select-Object -First 1
        if (-not $asset) {
            throw "Release asset not found: $archive"
        }
        Invoke-WebRequest -Uri $asset.url -OutFile $zipPath -Headers @{
            Authorization = "Bearer $GitHubToken"
            Accept = "application/octet-stream"
        }
    } else {
        if ($Version -eq "latest") {
            $url = "https://github.com/$Repo/releases/latest/download/$archive"
        } else {
            $url = "https://github.com/$Repo/releases/download/$Version/$archive"
        }
        Invoke-WebRequest -Uri $url -OutFile $zipPath
    }
    Expand-Archive -Path $zipPath -DestinationPath $tmpRoot -Force

    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item -Path (Join-Path $tmpRoot $BinName) -Destination (Join-Path $InstallDir $BinName) -Force

    Write-Host "installed agent-cow to $(Join-Path $InstallDir $BinName)"
    Write-Host "If needed, add this directory to PATH:"
    Write-Host "  $InstallDir"
}
finally {
    Remove-Item -Path $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
}

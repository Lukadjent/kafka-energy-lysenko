param([string]$Python = 'python')
$ErrorActionPreference = 'Stop'
$labsRuntime = Join-Path $PSScriptRoot '.runtime'
New-Item -ItemType Directory -Path $labsRuntime -Force | Out-Null
# services.py uses the portable Java 11 even when JAVA_HOME points to Java 17.
$labsCassandraRuntime = Join-Path (Split-Path $PSScriptRoot -Parent) 'lab2-cassandra-lysenko/.runtime'
New-Item -ItemType Directory -Path $labsCassandraRuntime -Force | Out-Null
if (-not (Test-Path -Path (Join-Path $labsCassandraRuntime 'jdk-11*/bin/java.exe'))) {
    $labsJavaAssets = Invoke-RestMethod 'https://api.adoptium.net/v3/assets/latest/11/hotspot?architecture=x64&image_type=jre&os=windows&vendor=eclipse'
    $labsJavaPackage = $labsJavaAssets[0].binary.package
    $labsJavaArchive = Join-Path $labsCassandraRuntime 'temurin-11-jre.zip'
    curl.exe -fLsS --retry 2 $labsJavaPackage.link -o $labsJavaArchive
    if ($LASTEXITCODE) { throw 'Portable Java 11 download failed' }
    if ((Get-FileHash -LiteralPath $labsJavaArchive -Algorithm SHA256).Hash -ne $labsJavaPackage.checksum) { throw 'Portable Java 11 SHA-256 mismatch' }
    tar.exe -xf $labsJavaArchive -C $labsCassandraRuntime
    if ($LASTEXITCODE) { throw 'Portable Java 11 extraction failed' }
}
function Fetch-File([string]$Url, [string]$Target) {
    if (-not (Test-Path -LiteralPath $Target)) {
        curl.exe -fLsS --retry 2 $Url -o $Target
        if ($LASTEXITCODE) { throw "Download failed: $Url" }
    }
}
function Check-Hash([string]$Target, [string]$Expected, [string]$Algorithm='SHA256') {
    if ((Get-FileHash -LiteralPath $Target -Algorithm $Algorithm).Hash -ne $Expected) {
        throw "Checksum mismatch: $Target"
    }
}
$labsPromName = 'prometheus-3.16.0.windows-amd64.zip'
$labsPromBase = 'https://github.com/prometheus/prometheus/releases/download/v3.16.0/'
Fetch-File ($labsPromBase + $labsPromName) (Join-Path $labsRuntime $labsPromName)
Fetch-File ($labsPromBase + 'sha256sums.txt') (Join-Path $labsRuntime 'prometheus-sha256sums.txt')
$labsPromExpected = ((Get-Content -LiteralPath (Join-Path $labsRuntime 'prometheus-sha256sums.txt') | Where-Object { $_.EndsWith($labsPromName) }) -split '\s+')[0]
Check-Hash (Join-Path $labsRuntime $labsPromName) $labsPromExpected
if (-not (Test-Path -LiteralPath (Join-Path $labsRuntime 'prometheus-3.16.0.windows-amd64/prometheus.exe'))) {
    tar.exe -xf (Join-Path $labsRuntime $labsPromName) -C $labsRuntime
    if ($LASTEXITCODE) { throw 'Prometheus extraction failed' }
}
$labsGrafanaName = 'grafana_13.3.1_42849527790_windows_amd64.tar.gz'
$labsGrafanaUrl = 'https://dl.grafana.com/grafana/release/13.3.1/' + $labsGrafanaName
Fetch-File $labsGrafanaUrl (Join-Path $labsRuntime $labsGrafanaName)
Check-Hash (Join-Path $labsRuntime $labsGrafanaName) 'cb78dceb990948fe7f1a189a0737fa7f6cf212076fc139a57d48cbc45dc0393f'
if (-not (Get-ChildItem -LiteralPath $labsRuntime -Directory | Where-Object Name -Like 'grafana-13.3.1*')) {
    tar.exe -xf (Join-Path $labsRuntime $labsGrafanaName) -C $labsRuntime
    if ($LASTEXITCODE) { throw 'Grafana extraction failed' }
}
$labsJmxName = 'jmx_prometheus_javaagent-1.7.0.jar'
$labsJmxBase = 'https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/'
Fetch-File ($labsJmxBase + $labsJmxName) (Join-Path $labsRuntime $labsJmxName)
Fetch-File ($labsJmxBase + $labsJmxName + '.sha256') (Join-Path $labsRuntime ($labsJmxName + '.sha256'))
$labsJmxExpected = [regex]::Match((Get-Content -Raw -LiteralPath (Join-Path $labsRuntime ($labsJmxName + '.sha256'))), '[0-9a-fA-F]{64}').Value
Check-Hash (Join-Path $labsRuntime $labsJmxName) $labsJmxExpected
& $Python -m pip --disable-pip-version-check install --only-binary=:all: --target (Join-Path $labsRuntime 'site-packages') -r (Join-Path $PSScriptRoot 'requirements.txt')
if ($LASTEXITCODE) { throw 'Python dependencies failed' }
$labsHashes = @{
    prometheus=@{version='3.16.0';url=($labsPromBase+$labsPromName);sha256=$labsPromExpected}
    grafana=@{version='13.3.1';url=$labsGrafanaUrl;sha256='cb78dceb990948fe7f1a189a0737fa7f6cf212076fc139a57d48cbc45dc0393f'}
    jmx_exporter=@{version='1.7.0';url=($labsJmxBase+$labsJmxName);sha256=$labsJmxExpected}
}
$labsHashes | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $labsRuntime 'verified-downloads.json') -Encoding utf8
Write-Output 'SETUP PASS: verified official portable binaries and Python packages'

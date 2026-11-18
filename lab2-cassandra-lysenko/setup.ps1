param([string]$Python = 'python')

$ErrorActionPreference = 'Stop'
$runtime = New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot '.runtime')
$dist = Join-Path $runtime.FullName 'apache-cassandra-4.0.21'
if (-not (Test-Path -LiteralPath (Join-Path $dist 'bin/cqlsh.py'))) {
    $name = 'apache-cassandra-4.0.21-bin.tar.gz'
    $archive = Join-Path $runtime.FullName $name
    $url = "https://dlcdn.apache.org/cassandra/4.0.21/$name"
    curl.exe -fLsS --retry 1 $url -o $archive
    if ($LASTEXITCODE) {
        $url = "https://archive.apache.org/dist/cassandra/4.0.21/$name"
        curl.exe -fLsS --retry 1 $url -o $archive
        if ($LASTEXITCODE) { throw 'Cassandra download failed.' }
    }
    $checksum = $archive + '.sha512'
    curl.exe -fLsS ($url + '.sha512') -o $checksum
    if ($LASTEXITCODE) { throw 'Cassandra checksum download failed.' }
    $expected = [regex]::Match((Get-Content -Raw -LiteralPath $checksum), '\b[0-9a-fA-F]{128}\b').Value
    if ((Get-FileHash -LiteralPath $archive -Algorithm SHA512).Hash -ne $expected) {
        throw 'Cassandra archive SHA-512 mismatch.'
    }
    tar.exe -xf $archive -C $runtime.FullName
    if ($LASTEXITCODE) { throw 'Cannot extract Cassandra.' }
}

$localJava = Get-ChildItem -Path (Join-Path $runtime.FullName 'jdk-11*/bin/java.exe')
if (-not $localJava -and -not $env:JAVA_HOME) {
    $releases = Invoke-RestMethod -Uri 'https://api.adoptium.net/v3/assets/latest/11/hotspot?architecture=x64&image_type=jre&os=windows&vendor=eclipse'
    $package = $releases[0].binary.package
    $archive = Join-Path $runtime.FullName 'temurin-11-jre.zip'
    curl.exe -fLsS --retry 1 $package.link -o $archive
    if ($LASTEXITCODE) { throw 'Temurin download failed.' }
    if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash -ne $package.checksum) {
        throw 'Temurin archive SHA-256 mismatch.'
    }
    tar.exe -xf $archive -C $runtime.FullName
    if ($LASTEXITCODE) { throw 'Cannot extract Temurin.' }
}

$packages = Join-Path $runtime.FullName 'site-packages'
$requirements = Join-Path $PSScriptRoot 'requirements.txt'
& $Python -m pip --disable-pip-version-check install --upgrade --target $packages -r $requirements
if ($LASTEXITCODE) { throw 'Python package setup failed.' }

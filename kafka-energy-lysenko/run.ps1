param(
    [Parameter(Position=0)]
    [ValidateSet('setup','start','stop','status','create-topics','producer','consumer','verify','test')]
    [string]$Action = 'status',
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$LabArgs
)
$ErrorActionPreference = 'Stop'
$pythonPrefix = @()
$bundledPython = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$pythonCommand = Get-Command python -ErrorAction SilentlyContinue
$pyCommand = Get-Command py -ErrorAction SilentlyContinue
if ($pythonCommand -and $pythonCommand.Source -notmatch 'WindowsApps') {
    $pythonPath = $pythonCommand.Source
} elseif ($pyCommand) {
    $pythonPath = $pyCommand.Source
    $pythonPrefix = @('-3')
} elseif (Test-Path -LiteralPath $bundledPython) {
    $pythonPath = $bundledPython
} else {
    throw 'Install Python 3.12 or newer from python.org, then run this command again.'
}
Push-Location $PSScriptRoot
try {
    switch ($Action) {
        'setup' { & $pythonPath @pythonPrefix -X utf8 scripts/download_dependencies.py }
        'producer' { & $pythonPath @pythonPrefix -X utf8 scripts/simple_producer_lysenko.py @LabArgs }
        'consumer' { & $pythonPath @pythonPrefix -X utf8 scripts/simple_consumer_lysenko.py @LabArgs }
        'verify' { & $pythonPath @pythonPrefix -X utf8 scripts/verify_lab.py @LabArgs }
        'test' { & $pythonPath @pythonPrefix -X utf8 -m unittest discover -s tests -v }
        default { & $pythonPath @pythonPrefix -X utf8 lab.py $Action @LabArgs }
    }
    if ($LASTEXITCODE -ne 0) { throw "Lab command exited with code $LASTEXITCODE" }
} finally {
    Pop-Location
}


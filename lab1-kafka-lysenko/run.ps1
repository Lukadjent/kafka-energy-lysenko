param(
    [Parameter(Position=0)]
    [ValidateSet('setup','start','stop','status','create-topics','data','experiments','load','serialization','warehouse','natural','analyze','validate','report','finalize')]
    [string]$Action = 'status'
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
    throw 'Install Python 3.12 from python.org.'
}
Push-Location $PSScriptRoot
try {
    switch ($Action) {
        'setup' {
            & $pythonPath @pythonPrefix -X utf8 scripts/download_dependencies.py
            if ($LASTEXITCODE -ne 0) { throw 'Portable runtime download failed' }
            & $pythonPath @pythonPrefix -X utf8 -m pip install --only-binary=:all: --target .runtime/python-packages -r requirements.txt
            if ($LASTEXITCODE -ne 0) { throw 'Python dependencies failed' }
            & $pythonPath @pythonPrefix -X utf8 scripts/compile_benchmark.py
        }
        'data' { & $pythonPath @pythonPrefix -X utf8 scripts/generate_data.py }
        'experiments' { & $pythonPath @pythonPrefix -X utf8 scripts/run_experiments.py }
        'load' { & $pythonPath @pythonPrefix -X utf8 scripts/run_load_scenarios.py }
        'serialization' { & $pythonPath @pythonPrefix -X utf8 scripts/capture_step.py serialization }
        'warehouse' { & $pythonPath @pythonPrefix -X utf8 scripts/capture_step.py warehouse }
        'natural' { & $pythonPath @pythonPrefix -X utf8 scripts/natural_rate_study.py }
        'analyze' { & $pythonPath @pythonPrefix -X utf8 scripts/analyze_results.py }
        'validate' { & $pythonPath @pythonPrefix -X utf8 scripts/validate_evidence.py }
        'report' { & $pythonPath @pythonPrefix -X utf8 scripts/build_report.py }
        'finalize' { & $pythonPath @pythonPrefix -X utf8 scripts/finalize_services.py }
        default { & $pythonPath @pythonPrefix -X utf8 lab.py $Action }
    }
    if ($LASTEXITCODE -ne 0) { throw "Lab command exited $LASTEXITCODE" }
} finally { Pop-Location }


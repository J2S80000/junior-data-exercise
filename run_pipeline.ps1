param(
    [switch]$UseSparkSubmit
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot
$venvPython = Join-Path $projectRoot "venv\Scripts\python.exe"
$pythonCmd = "python"

if (Test-Path $venvPython) {
    $pythonCmd = $venvPython
}

# Verifie la dependance cle avant de lancer le job
$previousPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $pythonCmd -m pip show pyspark 1>$null 2>$null
$hasPyspark = ($LASTEXITCODE -eq 0)
$ErrorActionPreference = $previousPreference

if (-not $hasPyspark) {
    throw "pyspark est manquant. Installez les dependances avec: $pythonCmd -m pip install -r requirements.txt"
}

if ($UseSparkSubmit) {
    $sparkSubmit = Get-Command spark-submit -ErrorAction SilentlyContinue
    if (-not $sparkSubmit) {
        throw "spark-submit est introuvable. Lancez sans -UseSparkSubmit ou installez Spark."
    }

    & spark-submit .\src\jobs\build_patient_identity.py
    exit $LASTEXITCODE
}

# Mode simple: utilise l'environnement Python actif (venv recommandé)
& $pythonCmd .\src\jobs\build_patient_identity.py
exit $LASTEXITCODE

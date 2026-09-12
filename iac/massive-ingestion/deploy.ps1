[CmdletBinding()]
param(
    [string]$StackName = "alphaengine-ingestion",
    [string]$Region = "us-east-1",
    [string]$BucketName = "alphaengine-543466170164-us-east-1-an",
    [string]$ServiceRoleName = "AlphaEngineServiceRole",
    [string]$SecretName = "alphaengine/massive"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$envFile = Join-Path $repoRoot ".env"
$template = Join-Path $PSScriptRoot "template.yaml"

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Missing $envFile. Put MASSIVE_API_KEY in .env before deployment."
}

function Invoke-Aws {
    param([Parameter(Mandatory)][string[]]$Arguments)
    & aws @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI command failed: aws $($Arguments -join ' ')"
    }
}

$keyLines = @(Get-Content -LiteralPath $envFile | Where-Object { $_ -match '^MASSIVE_API_KEY=(.+)$' })
if ($keyLines.Count -ne 1) {
    throw ".env must contain exactly one non-empty MASSIVE_API_KEY entry."
}
$massiveKey = [regex]::Match($keyLines[0], '^MASSIVE_API_KEY=(.+)$').Groups[1].Value.Trim()
if ([string]::IsNullOrWhiteSpace($massiveKey)) {
    throw "MASSIVE_API_KEY is empty."
}

Invoke-Aws @("s3api", "head-bucket", "--bucket", $BucketName, "--region", $Region)
Invoke-Aws @("iam", "get-role", "--role-name", $ServiceRoleName)

$secretFile = Join-Path ([IO.Path]::GetTempPath()) ("alphaengine-massive-" + [guid]::NewGuid().ToString("N") + ".json")
$artifactDir = Join-Path $repoRoot "build\ingestion-lambda"
$stamp = Get-Date -Format "yyyyMMddHHmmss"
$controlZip = Join-Path $artifactDir "control.zip"
$workerZip = Join-Path $artifactDir "worker.zip"
$controlKey = "_deploy/ingestion/$stamp/control.zip"
$workerKey = "_deploy/ingestion/$stamp/worker.zip"

try {
    [IO.File]::WriteAllText($secretFile, (@{ MASSIVE_API_KEY = $massiveKey } | ConvertTo-Json -Compress))

    & aws secretsmanager describe-secret --secret-id $SecretName --region $Region *> $null
    $secretExists = $LASTEXITCODE -eq 0
    if ($secretExists) {
        Invoke-Aws @("secretsmanager", "put-secret-value", "--secret-id", $SecretName, "--secret-string", "file://$secretFile", "--region", $Region)
    } else {
        Invoke-Aws @("secretsmanager", "create-secret", "--name", $SecretName, "--description", "AlphaEngine Massive API key", "--secret-string", "file://$secretFile", "--region", $Region)
    }
    $secretArn = (& aws secretsmanager describe-secret --secret-id $SecretName --region $Region --query ARN --output text)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($secretArn)) {
        throw "Could not resolve the Massive secret ARN."
    }

    New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $artifactDir "control") | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $artifactDir "worker") | Out-Null
    $oldGoOS = $env:GOOS
    $oldGoArch = $env:GOARCH
    $oldCGO = $env:CGO_ENABLED
    try {
        $env:GOOS = "linux"
        $env:GOARCH = "arm64"
        $env:CGO_ENABLED = "0"
        go build -trimpath -ldflags "-s -w" -o (Join-Path $artifactDir "control\bootstrap") (Join-Path $repoRoot "cmd\ingestion-control")
        go build -trimpath -ldflags "-s -w" -o (Join-Path $artifactDir "worker\bootstrap") (Join-Path $repoRoot "cmd\ingestion-worker")
    } finally {
        $env:GOOS = $oldGoOS
        $env:GOARCH = $oldGoArch
        $env:CGO_ENABLED = $oldCGO
    }
    Compress-Archive -Path (Join-Path $artifactDir "control\bootstrap") -DestinationPath $controlZip -Force
    Compress-Archive -Path (Join-Path $artifactDir "worker\bootstrap") -DestinationPath $workerZip -Force
    Invoke-Aws @("s3", "cp", $controlZip, "s3://$BucketName/$controlKey", "--region", $Region)
    Invoke-Aws @("s3", "cp", $workerZip, "s3://$BucketName/$workerKey", "--region", $Region)

    Invoke-Aws @(
        "cloudformation", "deploy", "--template-file", $template,
        "--stack-name", $StackName, "--region", $Region,
        "--parameter-overrides", "BucketName=$BucketName", "ServiceRoleName=$ServiceRoleName",
        "MassiveSecretArn=$secretArn", "ControlCodeKey=$controlKey", "WorkerCodeKey=$workerKey",
        "--capabilities", "CAPABILITY_NAMED_IAM"
    )
    Invoke-Aws @("cloudformation", "describe-stacks", "--stack-name", $StackName, "--region", $Region, "--query", "Stacks[0].Outputs", "--output", "table")
} finally {
    if (Test-Path -LiteralPath $secretFile) {
        Remove-Item -LiteralPath $secretFile -Force
    }
}

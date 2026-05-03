param(
  [string]$ApiKey = $env:RENDER_API_KEY,
  [string]$OwnerId = $env:RENDER_OWNER_ID,
  [string]$ServiceName = "pocketfm-scraper-tool",
  [string]$DatabaseName = "pocketfm-scraper-db",
  [string]$Repo = "https://github.com/Navya123445/PocketFM_scraper_tool",
  [string]$Branch = "main",
  [string]$Region = "singapore",
  [switch]$SkipDatabase
)

$ErrorActionPreference = "Stop"

if (-not $ApiKey) {
  throw "Set RENDER_API_KEY first."
}

$headers = @{
  Authorization = "Bearer $ApiKey"
  Accept = "application/json"
}
$jsonHeaders = $headers.Clone()
$jsonHeaders["Content-Type"] = "application/json"

function Invoke-RenderJson {
  param(
    [string]$Method,
    [string]$Path,
    [object]$Body = $null
  )

  $uri = "https://api.render.com/v1$Path"
  if ($null -eq $Body) {
    return Invoke-RestMethod -Method $Method -Uri $uri -Headers $headers -TimeoutSec 120
  }
  return Invoke-RestMethod -Method $Method -Uri $uri -Headers $jsonHeaders -Body ($Body | ConvertTo-Json -Depth 30) -TimeoutSec 120
}

if (-not $OwnerId) {
  $owners = Invoke-RenderJson -Method GET -Path "/owners"
  if (-not $owners -or $owners.Count -lt 1) {
    throw "No Render workspaces were returned for this API key."
  }
  $OwnerId = $owners[0].owner.id
  Write-Host "Using Render owner/workspace: $OwnerId ($($owners[0].owner.name))"
}

$databaseUrl = $null
if (-not $SkipDatabase) {
  $existingDatabases = Invoke-RenderJson -Method GET -Path "/postgres?ownerId=$OwnerId&name=$DatabaseName"
  $db = $null
  if ($existingDatabases -and $existingDatabases.Count -gt 0) {
    $db = $existingDatabases[0].postgres
    Write-Host "Using existing Postgres database: $($db.id)"
  } else {
    Write-Host "Creating Render Postgres database: $DatabaseName"
    $db = Invoke-RenderJson -Method POST -Path "/postgres" -Body @{
      name = $DatabaseName
      ownerId = $OwnerId
      plan = "free"
      region = $Region
      databaseName = "pocketfm"
      databaseUser = "pocketfm"
      version = "16"
    }
  }

  for ($i = 1; $i -le 60; $i++) {
    $db = Invoke-RenderJson -Method GET -Path "/postgres/$($db.id)"
    Write-Host "Database status: $($db.status)"
    if ($db.status -eq "available") { break }
    Start-Sleep -Seconds 10
  }

  $connection = Invoke-RenderJson -Method GET -Path "/postgres/$($db.id)/connection-info"
  $databaseUrl = $connection.internalConnectionString
}

$envVars = @(
  @{ key = "COMMISSIONING_ALLOWED_ORIGINS"; value = "*" },
  @{ key = "COMMISSIONING_JOB_WORKERS"; value = "1" },
  @{ key = "AMAZON_DETAIL_WORKERS"; value = "1" },
  @{ key = "AMAZON_DETAIL_RETRY_ROUNDS"; value = "1" },
  @{ key = "GOODREADS_LOOKUP_WORKERS"; value = "1" },
  @{ key = "GOODREADS_REQUEST_DELAY_SECONDS"; value = "1.5" },
  @{ key = "PYTHONUNBUFFERED"; value = "1" },
  @{ key = "PLAYWRIGHT_BROWSERS_PATH"; value = "/ms-playwright" }
)
if ($databaseUrl) {
  $envVars += @{ key = "COMMISSIONING_DATABASE_URL"; value = $databaseUrl }
}

$existingServices = Invoke-RenderJson -Method GET -Path "/services?ownerId=$OwnerId&name=$ServiceName"
$service = $null
if ($existingServices -and $existingServices.Count -gt 0) {
  $service = $existingServices[0].service
  Write-Host "Service already exists: $($service.id)"
} else {
  Write-Host "Creating Render web service: $ServiceName"
  $service = Invoke-RenderJson -Method POST -Path "/services" -Body @{
    type = "web_service"
    name = $ServiceName
    ownerId = $OwnerId
    repo = $Repo
    branch = $Branch
    autoDeploy = "yes"
    envVars = $envVars
    serviceDetails = @{
      runtime = "docker"
      plan = "free"
      region = $Region
      healthCheckPath = "/api/health"
      numInstances = 1
      envSpecificDetails = @{
        dockerContext = "."
        dockerfilePath = "./Dockerfile"
      }
    }
  }
}

$url = $service.serviceDetails.url
if (-not $url) {
  $url = "https://$($service.slug).onrender.com"
}

Write-Host "Render service dashboard: $($service.dashboardUrl)"
Write-Host "Render public URL: $url"
Write-Host "Polling health endpoint. First Docker deploy can take several minutes..."

for ($i = 1; $i -le 90; $i++) {
  try {
    $health = Invoke-WebRequest -Uri "$url/api/health" -UseBasicParsing -TimeoutSec 30 -SkipHttpErrorCheck
    Write-Host "Attempt $i health status: $($health.StatusCode) $($health.Content)"
    if ($health.StatusCode -eq 200 -and $health.Content -match '"status"\s*:\s*"ok"') {
      Write-Host "READY $url"
      exit 0
    }
  } catch {
    Write-Host "Attempt $i health error: $($_.Exception.Message)"
  }
  Start-Sleep -Seconds 20
}

throw "Timed out waiting for $url/api/health"

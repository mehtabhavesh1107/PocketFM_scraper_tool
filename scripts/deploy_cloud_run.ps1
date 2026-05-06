param(
    [Parameter(Mandatory = $true)]
    [string] $ProjectId,

    [Parameter(Mandatory = $true)]
    [string] $DbPassword,

    [string] $Region = "us-central1",
    [string] $Repository = "pocketfm",
    [string] $Service = "pocketfm-commissioning",
    [string] $WorkerService = "pocketfm-commissioning-worker",
    [string] $SqlInstance = "pocketfm-postgres",
    [string] $Database = "pocketfm",
    [string] $DbUser = "pocketfm",
    [string] $DbPasswordSecret = "pocketfm-db-password",
    [string] $BucketSuffix = "pocketfm-exports"
)

$ErrorActionPreference = "Stop"

$gcloud = (Get-Command gcloud -ErrorAction SilentlyContinue).Source
if (-not $gcloud) {
    $candidate = "C:\Users\Admin\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
    if (Test-Path $candidate) {
        $gcloud = $candidate
    }
}
if (-not $gcloud) {
    throw "gcloud was not found. Install Google Cloud SDK first."
}

& $gcloud config set project $ProjectId
$projectNumber = (& $gcloud projects describe $ProjectId --format="value(projectNumber)").Trim()
$cloudBuildSa = "$projectNumber@cloudbuild.gserviceaccount.com"
$runSa = "$projectNumber-compute@developer.gserviceaccount.com"
$bucketName = "$ProjectId-$BucketSuffix"

& $gcloud services enable cloudbuild.googleapis.com run.googleapis.com artifactregistry.googleapis.com sqladmin.googleapis.com secretmanager.googleapis.com storage.googleapis.com

foreach ($role in @("roles/run.admin", "roles/iam.serviceAccountUser", "roles/artifactregistry.writer", "roles/cloudsql.client", "roles/secretmanager.secretAccessor", "roles/storage.admin")) {
    & $gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$cloudBuildSa" --role $role --quiet | Out-Null
}
foreach ($role in @("roles/cloudsql.client", "roles/secretmanager.secretAccessor", "roles/storage.objectAdmin")) {
    & $gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$runSa" --role $role --quiet | Out-Null
}

& $gcloud artifacts repositories describe $Repository --location $Region 2>$null
if ($LASTEXITCODE -ne 0) {
    & $gcloud artifacts repositories create $Repository --repository-format=docker --location=$Region
}

& $gcloud sql instances describe $SqlInstance 2>$null
if ($LASTEXITCODE -ne 0) {
    & $gcloud sql instances create $SqlInstance --database-version=POSTGRES_16 --region=$Region --tier=db-custom-1-3840 --storage-size=20GB
}

& $gcloud sql databases describe $Database --instance=$SqlInstance 2>$null
if ($LASTEXITCODE -ne 0) {
    & $gcloud sql databases create $Database --instance=$SqlInstance
}

& $gcloud sql users describe $DbUser --instance=$SqlInstance 2>$null
if ($LASTEXITCODE -ne 0) {
    & $gcloud sql users create $DbUser --instance=$SqlInstance --password=$DbPassword
} else {
    & $gcloud sql users set-password $DbUser --instance=$SqlInstance --password=$DbPassword
}

$tempSecret = New-TemporaryFile
try {
    Set-Content -LiteralPath $tempSecret -Value $DbPassword -NoNewline
    & $gcloud secrets describe $DbPasswordSecret 2>$null
    if ($LASTEXITCODE -ne 0) {
        & $gcloud secrets create $DbPasswordSecret --data-file=$tempSecret
    } else {
        & $gcloud secrets versions add $DbPasswordSecret --data-file=$tempSecret
    }
} finally {
    Remove-Item -LiteralPath $tempSecret -Force
}

& $gcloud storage buckets describe "gs://$bucketName" 2>$null
if ($LASTEXITCODE -ne 0) {
    & $gcloud storage buckets create "gs://$bucketName" --location=$Region
}

& $gcloud builds submit --config cloudbuild.yaml --substitutions="_REGION=$Region,_REPOSITORY=$Repository,_SERVICE=$Service,_WORKER_SERVICE=$WorkerService,_CLOUD_SQL_INSTANCE=$SqlInstance,_CLOUD_SQL_DATABASE=$Database,_CLOUD_SQL_USER=$DbUser,_DB_PASSWORD_SECRET=$DbPasswordSecret,_GCS_EXPORT_BUCKET_SUFFIX=$BucketSuffix"

& $gcloud run services describe $Service --region=$Region --format="value(status.url)"

#!/usr/bin/env pwsh

Write-Host "Starting Real-Time Flight Tracker..." -ForegroundColor Green

# Check if Docker is running
try {
    docker version > $null 2>&1
} catch {
    Write-Host "Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Check for .env file
if (-not (Test-Path ".env")) {
    Write-Host "No .env file found. Creating template..." -ForegroundColor Yellow
    @"
# OpenSky API (optional - for higher rate limits)
OPENSKY_USERNAME=
OPENSKY_PASSWORD=

# Databricks Configuration
DATABRICKS_HOST=your-workspace-url.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id

# Kafka
KAFKA_BROKER=kafka:29092
"@ | Out-File -FilePath ".env" -Encoding UTF8
    Write-Host "Created .env template. Please update with your credentials." -ForegroundColor Green
}

# Create necessary directories
$directories = @("airflow/dags", "airflow/scripts", "airflow/plugins", "airflow/logs", "database")
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force > $null
        Write-Host "Created directory: $dir" -ForegroundColor Green
    }
}

# Start core services
Write-Host "`nStarting core services (Zookeeper, Kafka, PostgreSQL)..." -ForegroundColor Cyan
docker-compose up -d zookeeper kafka postgres

Write-Host "Waiting for services to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Check if core services are healthy
Write-Host "`nChecking core services health..." -ForegroundColor Cyan
$healthy = $true

try {
    $postgresHealth = docker-compose ps postgres --format json | ConvertFrom-Json
    if ($postgresHealth.Health -ne "healthy") {
        Write-Host "PostgreSQL is not healthy" -ForegroundColor Red
        $healthy = $false
    }
} catch {
    Write-Host "Failed to check PostgreSQL health" -ForegroundColor Red
    $healthy = $false
}

if (-not $healthy) {
    Write-Host "`nCore services are not ready. Please check logs with: docker-compose logs" -ForegroundColor Yellow
    Write-Host "Trying to continue anyway..." -ForegroundColor Yellow
}

# Start flight tracker services
Write-Host "`nStarting flight tracker services..." -ForegroundColor Cyan
docker-compose up -d airflow flight-backend flight-frontend flight-producer

Write-Host "Waiting for application services to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

Write-Host "`nAll services started successfully!" -ForegroundColor Green

# Display service information
Write-Host "`nService Endpoints:" -ForegroundColor Magenta
Write-Host "   Airflow:     http://localhost:8080 (username: admin, password: admin)" -ForegroundColor Yellow
Write-Host "   Frontend:    http://localhost:3000" -ForegroundColor White
Write-Host "   Backend API: http://localhost:8000" -ForegroundColor White
Write-Host "   Flask App:   http://localhost:5000" -ForegroundColor White

Write-Host "`nMonitoring Commands:" -ForegroundColor Magenta
Write-Host "   View all logs:    docker-compose logs -f" -ForegroundColor Gray
Write-Host "   View specific:    docker-compose logs -f [service-name]" -ForegroundColor Gray
Write-Host "   List containers:  docker-compose ps" -ForegroundColor Gray
Write-Host "   Stop services:    docker-compose down" -ForegroundColor Gray

# Check service status after a brief delay
Write-Host "`nChecking service status..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Display running containers
Write-Host "`nRunning Containers:" -ForegroundColor Cyan
docker-compose ps

# Test backend connection
Write-Host "`nTesting backend connection..." -ForegroundColor Cyan
try {
    $response = Invoke-RestMethod -Uri "http://localhost:8000/health" -TimeoutSec 5
    Write-Host "Backend API is responding: $($response.status)" -ForegroundColor Green
    if ($response.databricks -eq "connected") {
        Write-Host "Databricks integration: CONNECTED" -ForegroundColor Green
    } else {
        Write-Host "Databricks integration: MOCK MODE (check .env file)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "Backend API is not responding yet. It may need more time to start." -ForegroundColor Red
}

Write-Host "`nReady to track flights! Open http://localhost:3000 in your browser." -ForegroundColor Green
Write-Host "   First load might take a few seconds as services initialize." -ForegroundColor Yellow
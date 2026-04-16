# ============================================================================
# Start Beautiful Kafka Dashboard Setup (PowerShell)
# ============================================================================
# This script starts all monitoring services and displays the URLs

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  KAFKA STREAMING DASHBOARD STARTUP" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Push-Location $ScriptDir

Write-Host "Starting Prometheus, Grafana, and Kafka Exporter..." -ForegroundColor Yellow
Write-Host ""

# Start services
docker-compose up -d kafka-exporter prometheus grafana

Write-Host ""
Write-Host "Waiting for services to start (waiting for health checks)..." -ForegroundColor Yellow

# Wait for services
Start-Sleep -Seconds 30

# Check status
Write-Host ""
docker-compose ps

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "  DASHBOARD URLS - READY TO ACCESS!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""

$dashboards = @(
    @{
        Name = "GRAFANA"
        URL = "http://localhost:3000"
        Username = "admin"
        Password = "admin"
        Notes = "Dashboard auto-loads from monitoring/grafana-dashboards/"
    },
    @{
        Name = "PROMETHEUS"
        URL = "http://localhost:9090"
        Notes = "View metrics and query explorer"
    },
    @{
        Name = "KAFKA UI"
        URL = "http://localhost:8080"
        Notes = "View topics, brokers, consumer groups"
    },
    @{
        Name = "KAFKA EXPORTER"
        URL = "http://localhost:9308/metrics"
        Notes = "Raw Kafka metrics in Prometheus format"
    },
    @{
        Name = "YOUR APP METRICS"
        URL = "http://localhost:8001/metrics"
        Notes = "Your producer/consumer metrics"
    }
)

foreach ($dash in $dashboards) {
    Write-Host "[" -NoNewline
    Write-Host $dash.Name -ForegroundColor Cyan -NoNewline
    Write-Host "]" -NoNewline
    Write-Host "  $($dash.URL)" -ForegroundColor Green
    if ($dash.Username) {
        Write-Host "   Username: $($dash.Username)"
        Write-Host "   Password: $($dash.Password)"
    }
    if ($dash.Notes) {
        Write-Host "   ($($dash.Notes))"
    }
    Write-Host ""
}

Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Start your producer:  " -NoNewline; Write-Host "python -m ad_stream_producer.run_producer" -ForegroundColor Magenta
Write-Host "  2. Start your consumer:  " -NoNewline; Write-Host "python -m consumer.run_consumer" -ForegroundColor Magenta
Write-Host "  3. Open Grafana at:      " -NoNewline; Write-Host "http://localhost:3000" -ForegroundColor Magenta
Write-Host "  4. Watch metrics flow in real-time!" -ForegroundColor Yellow
Write-Host ""
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""

Pop-Location

Write-Host "Press any key to continue..." -ForegroundColor Gray
$null = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")


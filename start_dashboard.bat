@echo off
REM ============================================================================
REM Start Beautiful Kafka Dashboard Setup
REM ============================================================================
REM This script starts all monitoring services and displays the URLs

echo.
echo ================================================================================
echo  KAFKA STREAMING DASHBOARD STARTUP
echo ================================================================================
echo.
echo Starting Prometheus, Grafana, and Kafka Exporter...
echo.

cd /d "%~dp0"

REM Start services
docker-compose up -d kafka-exporter prometheus grafana

echo.
echo Waiting for services to start (30 seconds)...
timeout /t 30 /nobreak

REM Check status
docker-compose ps

echo.
echo ================================================================================
echo  DASHBOARD URLS - READY TO ACCESS!
echo ================================================================================
echo.
echo [GRAFANA]          http://localhost:3000
echo   Username: admin
echo   Password: admin
echo   (Dashboard auto-loads from monitoring/grafana-dashboards/)
echo.
echo [PROMETHEUS]       http://localhost:9090
echo   (View metrics and query explorer)
echo.
echo [KAFKA UI]         http://localhost:8080
echo   (View topics, brokers, consumer groups)
echo.
echo [KAFKA EXPORTER]   http://localhost:9308/metrics
echo   (Raw Kafka metrics in Prometheus format)
echo.
echo [YOUR APP METRICS] http://localhost:8001/metrics
echo   (Your producer/consumer metrics)
echo.
echo ================================================================================
echo.
echo Next steps:
echo   1. Start your producer:  python -m ad_stream_producer.run_producer
echo   2. Start your consumer:  python -m consumer.run_consumer
echo   3. Open Grafana dashboard at http://localhost:3000
echo   4. Watch metrics flow in real-time!
echo.
echo ================================================================================
echo.

pause


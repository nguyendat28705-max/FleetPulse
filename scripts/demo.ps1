# FleetPulse - Demo Orchestration Script
param(
    [int]$Devices = 20,
    [float]$Speed = 60.0,
    [int]$Seed = 42
)

$ErrorActionPreference = "Continue"

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$env:PYTHONUNBUFFERED = "1"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "       FleetPulse - Real-time IoT Operations Demo        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Kiem tra va khoi dong Docker ha tang (Mosquitto & Docker Mongo neu dung local)
Write-Host "[1/5] Kiem tra Mosquitto MQTT & MongoDB (Docker)..." -ForegroundColor Yellow
docker compose up -d mosquitto mongo

$mongoHealth = docker compose exec mongo mongosh --quiet --eval "rs.status().ok" 2>&1
if ($mongoHealth -notmatch "1") {
    Write-Host "[!] Khoi tao Replica Set rs0 cho Docker MongoDB..." -ForegroundColor Yellow
    docker compose exec mongo mongosh --eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})" 2>&1 | Out-Null
    Start-Sleep 3
}
Write-Host "[OK] Ha tang MQTT Mosquitto & Docker MongoDB san sang." -ForegroundColor Green

# 2. Don dep tien trinh cu va chuan bi thu muc log
Write-Host "[2/5] Don tien trinh cu & khoi tao logs/..." -ForegroundColor Yellow
& "$PSScriptRoot\down.ps1"

$logDir = Join-Path $PSScriptRoot "..\logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

# 3. Xac dinh python binary
$pythonExe = "$PSScriptRoot\..\.venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

# 4. Khoi dong cac dich vu nen (ghi log vao thu muc logs/)
Write-Host "[3/5] Khoi dong Ingestion, Gateway, Streaming Engine, Scheduler..." -ForegroundColor Yellow

Start-Process -FilePath $pythonExe -ArgumentList "-u -m ingestion.run" -WorkingDirectory "$PSScriptRoot\.." -RedirectStandardOutput "$logDir\ingestion.log" -RedirectStandardError "$logDir\ingestion_err.log" -WindowStyle Minimized
Write-Host "  -> Ingestion Service (MQTT -> MongoDB) da chay." -ForegroundColor DarkCyan

Start-Process -FilePath $pythonExe -ArgumentList "-u -m gateway.run --gateway-id all" -WorkingDirectory "$PSScriptRoot\.." -RedirectStandardOutput "$logDir\gateway.log" -RedirectStandardError "$logDir\gateway_err.log" -WindowStyle Minimized
Write-Host "  -> Edge Gateway (Window 10s -> Features) da chay." -ForegroundColor DarkCyan

Start-Process -FilePath $pythonExe -ArgumentList "-u -m engine.run --shard all" -WorkingDirectory "$PSScriptRoot\.." -RedirectStandardOutput "$logDir\engine.log" -RedirectStandardError "$logDir\engine_err.log" -WindowStyle Minimized
Write-Host "  -> Streaming Engine (Change Streams -> HI/RUL) da chay." -ForegroundColor DarkCyan

Start-Process -FilePath $pythonExe -ArgumentList "-u -m uvicorn scheduler.app:app --host 0.0.0.0 --port 8002" -WorkingDirectory "$PSScriptRoot\.." -RedirectStandardOutput "$logDir\scheduler.log" -RedirectStandardError "$logDir\scheduler_err.log" -WindowStyle Minimized
Write-Host "  -> Scheduler & Action Service (port 8002) da chay." -ForegroundColor DarkCyan

Start-Sleep 2

# 5. Khoi dong Simulator
Write-Host "[4/5] Khoi dong Fleet Simulator ($Devices thiet bi, toc do ${Speed}x)..." -ForegroundColor Yellow
Start-Process -FilePath $pythonExe -ArgumentList "-u -m simulator.run --devices $Devices --speed $Speed --seed $Seed" -WorkingDirectory "$PSScriptRoot\.." -RedirectStandardOutput "$logDir\simulator.log" -RedirectStandardError "$logDir\simulator_err.log" -WindowStyle Minimized
Write-Host "  -> Simulator da bat dau phat du lieu telemetry." -ForegroundColor DarkCyan

Write-Host "[5/5] Cho du lieu dong bo (5 giay)..." -ForegroundColor Yellow
Start-Sleep 5

# Kiem tra trang thai thuc te
& $pythonExe "$PSScriptRoot\check_status.py"

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "[OK] Toan bo he thong FleetPulse dang hoat dong!" -ForegroundColor Green
Write-Host "Cac lenh thao tac huu ich:" -ForegroundColor White
Write-Host "  - Kiem tra du lieu:     python scripts/check_status.py" -ForegroundColor Cyan
Write-Host "  - Xem log chi tiet:     Get-Content logs\engine.log -Tail 20" -ForegroundColor Gray
Write-Host "  - Tiem loi ro khi:      python -m simulator.inject --device M-017 --type air_leak --severity 0.7" -ForegroundColor Gray
Write-Host "  - Xem lenh bao tri:     Invoke-RestMethod http://localhost:8002/work_orders" -ForegroundColor Gray
Write-Host "  - Dung tat ca:          .\scripts\down.ps1" -ForegroundColor Gray
Write-Host "==========================================================" -ForegroundColor Green

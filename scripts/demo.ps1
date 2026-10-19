# FleetPulse - Demo Orchestration Script
param(
    [int]$Devices = 20,
    [float]$Speed = 60.0,
    [int]$Seed = 42
)

$ErrorActionPreference = "Continue"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "       FleetPulse - Real-time IoT Operations Demo        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Kiểm tra hạ tầng Docker
Write-Host "[1/5] Kiểm tra MongoDB & Mosquitto (Docker)..." -ForegroundColor Yellow
docker compose up -d --wait

$mongoHealth = docker compose exec mongo mongosh --quiet --eval "rs.status().ok" 2>&1
if ($mongoHealth -notmatch "1") {
    Write-Host "[!] Đang khởi tạo Replica Set rs0..." -ForegroundColor Yellow
    docker compose exec mongo mongosh --eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
    Start-Sleep 5
}
Write-Host "[OK] Hạ tầng MongoDB Replica Set & Mosquitto sẵn sàng." -ForegroundColor Green

# 2. Khởi tạo cấu trúc Database
Write-Host "[2/5] Đảm bảo cấu trúc collection & index..." -ForegroundColor Yellow
docker compose exec mongo mongosh --quiet "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" /scripts/setup.js

# 3. Dọn tiến trình cũ nếu còn sót
& "$PSScriptRoot\down.ps1"

# 4. Khởi động các dịch vụ nền
Write-Host "[3/5] Khởi động Ingestion, Gateway, Engine, Scheduler..." -ForegroundColor Yellow

$pythonExe = "$PSScriptRoot\..\.venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

# Ingestion
Start-Process -FilePath $pythonExe -ArgumentList "-m ingestion.run" -WorkingDirectory "$PSScriptRoot\.." -WindowStyle Minimized
Write-Host "  -> Ingestion Service đã chạy." -ForegroundColor DarkCyan

# Gateway
Start-Process -FilePath $pythonExe -ArgumentList "-m gateway.run --gateway-id all" -WorkingDirectory "$PSScriptRoot\.." -WindowStyle Minimized
Write-Host "  -> Edge Gateway đã chạy." -ForegroundColor DarkCyan

# Engine
Start-Process -FilePath $pythonExe -ArgumentList "-m engine.run --shard 0 --shards 4" -WorkingDirectory "$PSScriptRoot\.." -WindowStyle Minimized
Write-Host "  -> Streaming Engine (Shard 0) đã chạy." -ForegroundColor DarkCyan

# Scheduler (FastAPI)
Start-Process -FilePath $pythonExe -ArgumentList "-m uvicorn scheduler.app:app --port 8002" -WorkingDirectory "$PSScriptRoot\.." -WindowStyle Minimized
Write-Host "  -> Scheduler & Action Service (port 8002) đã chạy." -ForegroundColor DarkCyan

Start-Sleep 3

# 5. Khởi động Simulator
Write-Host "[4/5] Khởi động Fleet Simulator ($Devices thiết bị, tốc độ ${Speed}x)..." -ForegroundColor Yellow
Start-Process -FilePath $pythonExe -ArgumentList "-m simulator.run --devices $Devices --speed $Speed --seed $Seed" -WorkingDirectory "$PSScriptRoot\.." -WindowStyle Minimized
Write-Host "  -> Simulator đã phát dữ liệu lên MQTT." -ForegroundColor DarkCyan

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "[OK] Toàn bộ hệ thống FleetPulse đã được khởi động!" -ForegroundColor Green
Write-Host "Để theo dõi trạng thái:" -ForegroundColor White
Write-Host "  - Trạng thái thiết bị:  mongosh 'mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' --eval 'use fleetpulse_dev; db.device_state.find().pretty()'" -ForegroundColor Gray
Write-Host "  - Tiêm lỗi rò khí:      python -m simulator.inject --device M-017 --type air_leak --severity 0.7" -ForegroundColor Gray
Write-Host "  - Xem lệnh bảo trì:     Invoke-RestMethod http://localhost:8002/work_orders" -ForegroundColor Gray
Write-Host "  - Dừng tất cả:          .\scripts\down.ps1" -ForegroundColor Gray
Write-Host "==========================================================" -ForegroundColor Green

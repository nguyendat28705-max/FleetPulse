# Dừng tất cả tiến trình Python của FleetPulse
Write-Host "[*] Đang dừng các tiến trình FleetPulse..." -ForegroundColor Yellow

$services = @("simulator.run", "gateway.run", "ingestion.run", "engine.run", "scheduler.app")

Get-CimInstance Win32_Process | Where-Object { 
    $cmd = $_.CommandLine
    if ($cmd) {
        foreach ($s in $services) {
            if ($cmd -match $s) { return $true }
        }
    }
    return $false
} | ForEach-Object {
    Write-Host " -> Dừng tiến trình $($_.ProcessId): $($_.CommandLine)" -ForegroundColor DarkGray
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}

Write-Host "[OK] Đã dừng toàn bộ các tiến trình Python FleetPulse." -ForegroundColor Green

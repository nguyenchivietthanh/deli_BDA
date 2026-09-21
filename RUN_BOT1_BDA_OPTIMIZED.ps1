$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
$env:PYTHONUNBUFFERED = "1"
# Vietnamese names/labels in logs -> avoid UnicodeEncodeError on redirected stdout.
$env:PYTHONUTF8 = "1"
# Deferred-expand: rã LT KHÔNG tạo candidate đơn con ngay; BOT 2 rã TO detail +
# tạo candidate khi TO gần tới precheck (arrived+20h). Giảm lt_pending_candidate
# ~5-10x. PHẢI đặt giống nhau ở CẢ 3 bot. Bỏ dòng này + restart để quay lại cũ.
$env:BOT_DELI_TO_TASK_MODE = "1"
# Full Chrome restart (relaunch + FMS login wait) every 50 requests is very
# expensive for BOT 1's hundreds of TO-detail calls. 120 is a middle ground:
# raise further if Chrome RAM stays healthy, drop back to 50 if "Aw, Snap"
# / Out of Memory returns.
$env:BOT_DELI_BROWSER_RESTART_EVERY_REQUESTS = "120"

$from = (Get-Date).Date.ToString("yyyy-MM-dd HH:mm:ss")
$to = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

Write-Host "BOT 1 fresh window: $from -> $to"
python .\BOT-Deli_BDA_run_bot1_bda.py `
  --once `
  --stop-on-error `
  --lt-from $from `
  --lt-to $to

if ($LASTEXITCODE -ne 0) {
  throw "BOT 1 fresh cycle failed with exit code $LASTEXITCODE."
}

Write-Host "BOT 1 switches to continuous mode."
python .\BOT-Deli_BDA_run_bot1_bda.py `
  --interval-minutes 15 `
  --error-retry-seconds 90

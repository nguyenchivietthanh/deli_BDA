$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
$env:PYTHONUNBUFFERED = "1"
# Staff names and FMS status labels are Vietnamese; without this a print() of a
# name crashes the whole cycle with UnicodeEncodeError when stdout is redirected.
$env:PYTHONUTF8 = "1"
# Deferred-expand mode - PHẢI đặt giống nhau ở CẢ 3 bot (xem RUN_BOT1).
$env:BOT_DELI_TO_TASK_MODE = "1"

# BOT 3 owns final BatchSearch/Tracking Info, Sheets and assignment.
# Raised 200 -> 400 on 2026-09-07 to cut the ~10 Chrome recycles per cycle
# (each a relaunch + FMS login wait, roughly 15-20% of cycle time) while BOT 3
# was the pipeline bottleneck: three back-to-back cycles all hit the 12000
# --batch-limit cap, process_due_pending_slice ran 1225-1323s, whole cycles
# took 1441-1491s against the 900s interval and ended "sleep 0s". Real capacity
# is ~12000 x 2.4 cycles/hour = ~29k/hour, NOT the ~41.5k/hour estimated on
# 06/09 from a light-load cycle.
# REVERTED to 200 the same evening: BOT 3 started hitting Chrome memory errors
# and stalling, which is exactly the rollback trigger the original note called
# out. BatchSearch sends 500-order batches and pulls large JSON back through
# the renderer, so doubling the requests between recycles doubles what the
# renderer accumulates. Recycling more often is cheaper than a stall. Do not
# raise this again without a way to cap renderer memory per batch.
# The other big drag was Google Sheets HTTP 429 in sync_pending_work, but on
# 07/09 that step ran in 17.8s with updated=11103 (vs 101-239s / 24509), so it
# is not currently the constraint - re-measure before acting on it.
$env:BOT_DELI_BROWSER_RESTART_EVERY_REQUESTS = "200"
$env:BOT_DELI_CHROME_USER_DATA_DIR_BOT3 = Join-Path $env:LOCALAPPDATA "BOT_DELI_FMS_BROWSER_BOT3_ADMIN\User Data"

# 5000/cycle was only using ~40% of the 15-min interval (query cost doesn't
# grow with this limit - the ORDER BY already scans/sorts every due candidate
# regardless of LIMIT, so raising it is close to free query-wise). 12000 is
# sized to land near ~800-850s of real work, leaving a small safety margin
# before the 900s interval; push toward the 25000 hard cap later if cycles
# keep finishing with idle time to spare.
#
# COGS: 2026-09-15 ops chuyen han sang extension "OneBI Fetch Bridge" (Data Suite),
# chay tay, khong bi tran 200301004 cua tai khoan Admin tren FMS.
#
# --skip-cogs phai di CUNG LUC voi TOOL_OWNED_HEADERS = {"COGS"} trong
# BOT-Deli_BDA_run_bot3_work.py. Tat mot ben thoi la hong:
#   - tat FMS, quen cot  -> moi vong BOT 3 ghi rong de len COGS extension vua dien
#   - bat cot, quen FMS  -> ton request FMS cho gia tri khong bao gio len sheet
#
# Muon quay lai nguon FMS: bo --skip-cogs, them lai --cogs-limit 80, va dat
# TOOL_OWNED_HEADERS = set(). Ba thu, mot luot.
#
# Lan thu 2026-09-11 that bai vi payload nhet danh sach don vao o loc
# MULTI_SELECT/UPLOAD - widget phia client, khong phai bo loc phia may chu, nen
# may chu tra ve truy van tren MOCK_TABLE. Khong phai loi cua Data Suite.
python .\BOT-Deli_BDA_run_bot3_work.py `
  --interval-minutes 15 `
  --batch-limit 12000 `
  --skip-cogs `
  --assign `
  --retention-days 5

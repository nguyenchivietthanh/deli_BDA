$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
$env:PYTHONUNBUFFERED = "1"
# Ten tram tieng Viet trong log -> tranh UnicodeEncodeError khi ghi ra file.
$env:PYTHONUTF8 = "1"

# Con nay KHONG chay theo vong nhu ba con bot kia. Chay mot lan roi thoat, khi
# nao can thi mo lai. Ly do: COGS khong doi sau khi don da outbound, nen hoi lai
# lien tuc chi ton request ma khong duoc gi.
#
# Nguon la dashboard "SOC - Download Board" tren Data Suite, KHONG phai API COGS
# cua FMS. API FMS moi don mot request va co han muc ngay cua tai khoan Admin;
# Data Suite loc shipment_id IN (...) nen hoi ca lo mot lan.
#
# Lan dau tien tren mot may moi phai chay buoc nay truoc de dang nhap SSO va bat
# lay mau request cua trang:
#     python .\datasuite_cogs.py capture
#
# GIOI HAN PHAI BIET: bang ten la "L5D", tuc chi giu du lieu 5 ngay gan nhat.
# Don da outbound qua 5 ngay se khong tra ve dong nao, va o COGS cua no se trong
# vinh vien. Vi vay nen chay it nhat vai ngay mot lan, dung de don troi qua han.
#
# BOT 3 khong con ghi cot COGS nua (TOOL_OWNED_HEADERS trong
# BOT-Deli_BDA_run_bot3_work.py), nen hai ben khong dam nhau.
python .\datasuite_cogs.py sheet

Write-Host ""
Write-Host "Xong. Bam phim bat ky de dong." -ForegroundColor Green
[void][System.Console]::ReadKey($true)

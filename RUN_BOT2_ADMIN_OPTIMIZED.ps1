$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
$env:PYTHONUNBUFFERED = "1"
# Vietnamese names/labels in logs -> avoid UnicodeEncodeError on redirected stdout.
$env:PYTHONUTF8 = "1"
# Deferred-expand mode - PHẢI đặt giống nhau ở CẢ 3 bot (xem RUN_BOT1).
$env:BOT_DELI_TO_TASK_MODE = "1"

# Recycling the automated Chrome driver prevents the long-running Admin
# session from growing until Chrome reports Aw, Snap / Out of Memory.
# 120 matches the value already running stable on BOT1 for the same kind of
# heavy TO-detail fetching; raise further only after watching RAM for a few
# days, drop back to 50 if "Aw, Snap" / Out of Memory returns.
#
# 120 -> 40 (2026-09-08). Measured mid-cycle: this bot's Chrome reached
# 9,222 MB of COMMIT while its working set showed only 1,892 MB - i.e. 7.3 GB
# had already been pushed into the pagefile. Machine-wide that put commit at
# 25,283 MB against 16,163 MB of RAM, and the 9 GB shortfall is what makes the
# disk sit at 100%: 25,014 hard faults in 12 seconds, %DiskTime 534, queue 9,
# 146 MB/s of reads that are almost entirely pagefile.
#
# Measure COMMIT (Win32_Process.PageFileUsage), not WorkingSet. Working set
# looks small precisely because Windows has paged the rest out - that metric
# hid this problem all morning.
#
# Only killing the Chrome process returns that commit, so the recycle budget is
# the only lever. BOT 2 is by far the worst of the three because its cycle is
# the longest and most request-heavy ([1/3] handover TO details + [2/3] 200
# sequences + [3/3] 517 TOs). ~33 restarts per cycle instead of ~11 costs
# roughly 4 minutes against a ~60 minute cycle. BOT1 and BOT3 stay as they are
# until measured mid-cycle - do not change all three at once.
$env:BOT_DELI_BROWSER_RESTART_EVERY_REQUESTS = "40"

# --handover-limit is the real throttle on this worker (2026-09-05). Step 3/3
# can only collect the TOs that step 1/3 just expanded in the same cycle, so
# with handover-limit 2 the TO slice never saw more than ~72 TOs and the whole
# cycle finished in 8-267s, then slept 600-900s. Measured cost is ~130s per
# handover LT end to end (~68s expand in 1/3 + ~62s collect in 3/3), so 5 LTs
# lands near 700-750s of the 900s interval with margin left. Queue at the time
# of the change: 317 handover LT-sequences waiting, draining at 8/hour with
# limit 2 (~40h) vs ~20/hour with limit 5 (~16h). If a cycle does overrun 900s
# it is harmless - the runner sleeps max(0, interval - elapsed), so cycles just
# run back to back. Raise further only after watching a few cycles.
#
# --to-limit 750 (was 250) is the headroom that makes the above safe: 5 LTs
# yield roughly 180 TOs per cycle, well under the cap. Request pacing between
# FMS calls is unchanged by either knob, so this does not increase burst rate.
#
# --bulky-limit 10 -> 50 (2026-09-08). This is the Pending Inbound slice, the
# only source of bulky candidates. It had been doing nothing at all: the query
# took the 10 oldest sequences with no "already asked" filter, so the window
# stayed full of finished 01-04/09 trips and bulky produced zero candidates
# from 06/09 04:21 onward. With that fixed in the pipeline, the limit itself
# turns out to be too small anyway - new sequences arrive at ~55/hour
# (ENDED 28 + HANDOVER 27) while 10/cycle x ~4 cycles/hour is only 40/hour, so
# it could never even keep pace, let alone clear the 5,497-sequence backlog.
# 50/cycle = ~200/hour, draining ~145/hour net, so the backlog clears in about
# 38 hours and then stays clear. BOT 2 has the room: its cycles were running
# 78s of work against an 822s sleep. Each sequence costs roughly one FMS
# request, so expect the cycle to grow by ~40-60s. Lower this if BOT 2 starts
# ending cycles with "sleep 0s".
#
# --bulky-limit 50 -> 200 (2026-09-08). The "~4 cycles/hour" above was wrong.
# Timed from the 10:44:55 cycle: [1/3] handover 289s, [2/3] Pending Inbound
# 140s, [3/3] TO detail ~33 min, plus the 15 min sleep = ~55 min per cycle, so
# ~1.1 cycles/hour and 50/cycle is only ~54 sequences/hour. Arrivals are ~55/
# hour, i.e. exactly break-even - the backlog sat at 2,998 then rose to 3,174
# instead of draining. [2/3] is the cheapest step in the cycle at 2.8s per
# sequence (140s / 50), so buying throughput here is cheap: 200/cycle costs
# ~7 more minutes and yields ~194/hour, clearing the 3,174 backlog in ~23h.
#
# MEASURED, do not trust the "~7 more minutes" estimate above - it extrapolated
# linearly from a 50-sequence slice and was wrong. Real [2/3] cost at 200:
# 1,982s while the machine was thrashing, then 1,520s on a clean run = 7.6s per
# sequence, not 2.8s. The head of the queue is cheap (most sequences return
# total: 0); it gets dearer as the cheap ones drain. Full cycle is now ~85 min
# ([1/3] 530s + [2/3] 1,520s + [3/3] 750 TOs), so BOT 2 ends cycles at
# "sleep 0s" and runs back to back. That is acceptable here - its work is
# bounded per cycle, so saturation just means no idle gap - but it leaves no
# margin. Throughput ~138 sequences/hour against ~55/hour of arrivals drains
# the backlog in ~35h. The TO branch barely notices: 750 TOs per 85 min is
# ~529/hour versus ~564/hour at the old 55-minute cycle.
# [3/3] is the real bottleneck at 33 min - shrink --to-limit before touching
# this knob again. Watch for "sleep 0s" and for the 925-candidate LTs: one
# sequence can expand to hundreds of candidates, so cycle cost is lumpy.
#
# --to-limit 750 -> 1500 (2026-09-10). The paragraph above was written while the
# Pending Inbound backlog was 3,174 sequences and [2/3] cost 1,520s; that
# backlog has since drained to ~111 in-window sequences, so [2/3] is cheap again
# and BOT 2 is no longer saturated. Measured over the last 24h from the
# ADMIN_HANDOVER_TO_DETAIL checkpoints: it hits the 750 cap almost every cycle
# (750, 750, 728, 707) and then sits idle 35-124 minutes between cycles, so the
# cap - not capacity - is what limits it.
#
# [3/3] costs ~2.1s per TO (26-27 min for 750), so 1500 lands near 53 min.
# Throughput goes from ~409 TO/hour to roughly 1,100, which clears the 7,423-TO
# queue in about 7 hours instead of 18 and pulls its 28-hour age down with it.
# Expect cycles to run back to back at "sleep 0s" again - acceptable, the work
# per cycle is bounded. RAM checked first: BOT 2 Chrome at 2,648 MB commit,
# 2,722 MB free, pagefile 3,611 MB, recycle still 40 so a longer cycle does not
# let Chrome grow further. Drop back to 750 if [3/3] passes ~60 min or if
# commit climbs.
# --handover-limit 5 -> 20 (2026-09-18). Do duoc: hang doi ra chang dung yen o
# 220 chang / 126 chuyen, chang cu nhat 44,7h. Dem moc HANDOVER_SEQUENCE_COMPLETE
# theo gio ra DUNG 5 o phan lon cac gio -> tran nay la thu chan, khoang 1 vong/gio.
# BOT 1 chi nhan chuyen CO 1 CHANG da toi (doc thang tu FMS), nen chuyen nhieu tram
# hoan toan phu thuoc lan nay: LT0Q9F4XM6QZ2 (5 tram) va LT0Q9G4XMIRJ1 (9 tram) ra
# cham 43-45h, TO cua chung vao chu trinh khi da qua ca moc 24h lan 32h.
# 20/vong ~ 20/gio -> 220 chang ton rã het trong ~11h thay vi ~44h.
# Gia: moi chang = 1 lan goi danh sach do hang + 1 TO detail cho moi TO cua BDA
# (chang 3 cua LT0Q9F4XM6QZ2 co 65 TO). Vong cua BOT 2 von da chay sat nhau, nen
# neu [1/3] phinh qua ~15 phut thi ha --to-limit 1500 -> 1000 de bu lai.
python .\BOT-Deli_BDA_run_bot2_admin.py `
  --interval-minutes 15 `
  --error-retry-seconds 90 `
  --handover-limit 20 `
  --bulky-limit 200 `
  --to-limit 1500

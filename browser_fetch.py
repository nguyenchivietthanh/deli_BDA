"""Browser-backed FMS API client with pacing, cooldown and fail-closed pagination support.

All FMS API calls are executed by ``fetch()`` inside a real Chrome session that is
already logged into https://spx.shopee.vn.  The Chrome driver is kept alive and
reused across BDA/Admin role switches.

Environment knobs (optional):
  BOT_DELI_CHROME_USER_DATA_DIR
  BOT_DELI_CHROME_PROFILE=Default
  BOT_DELI_CHROME_USER_DATA_DIR_BDA
  BOT_DELI_CHROME_USER_DATA_DIR_ADMIN
  BOT_DELI_BROWSER_GLOBAL_INTERVAL=0.25
  BOT_DELI_BROWSER_ENDPOINT_INTERVAL=0.8
  BOT_DELI_BROWSER_COOLDOWNS=5,15,30,60
  BOT_DELI_BROWSER_MAX_RETRIES=5
  BOT_DELI_BROWSER_RESTART_EVERY_REQUESTS=50

The module deliberately raises on blocked/incomplete API responses.  Callers must
not interpret an error as an empty page, otherwise FMS throttling can silently
produce missing data.
"""

import atexit
import json
import os
import random
import re
import signal
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit


BASE = Path(os.getenv("LOCALAPPDATA", "")) / "Google" / "Chrome"
SPX_HOME = "https://spx.shopee.vn/"


class BrowserFetchError(RuntimeError):
    pass


class BrowserThrottleError(BrowserFetchError):
    pass


class BrowserNoRecordError(BrowserFetchError):
    pass


class BrowserLoginError(BrowserFetchError):
    pass


class BrowserSessionUnavailableError(BrowserLoginError):
    """Raised instantly (no Chrome/Selenium I/O) while the login circuit is open.

    Without this, a dead FMS session (e.g. Chrome stuck behind a Google
    "verify it's you" challenge left unattended overnight) makes every single
    queued item retry the full ~5x1.5s role-switch dance before giving up.
    Across a large backlog that turns one bad login into many hours of BOT
    time. Once a role fails LOGIN_FAILURE_FAST_FAIL_THRESHOLD times in a row,
    every further call for that role fails immediately for a cooldown window
    instead of touching Chrome at all.
    """
    pass


FETCH_JS = r"""
var url = arguments[0];
var method = arguments[1] || 'GET';
var payload = arguments[2];
var callback = arguments[arguments.length - 1];
var options = {
    method: method,
    credentials: 'include',
    headers: {
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json;charset=UTF-8'
    }
};
if (method !== 'GET' && method !== 'HEAD' && payload !== null && payload !== undefined) {
    options.body = JSON.stringify(payload);
}
fetch(url, options)
  .then(async function (r) {
      var text = '';
      try { text = await r.text(); } catch (e) { text = ''; }
      callback({transport_ok: true, status: r.status, http_ok: r.ok, text: text});
  })
  .catch(function (err) {
      callback({transport_ok: false, status: 0, http_ok: false, error: String(err), text: ''});
  });
"""

_drivers = {}              # profile_key -> webdriver
_driver_roles = {}         # profile_key -> last confirmed role
_driver_request_counts = {}  # profile_key -> completed FMS requests
_lock = threading.RLock()
_last_global_at = 0.0
_last_endpoint_at = {}
_blocked_until = {}

# Login circuit breaker (see BrowserSessionUnavailableError above).
_login_failure_counts = {}       # profile_key -> consecutive role-switch failures
_login_circuit_open_until = {}   # profile_key -> monotonic() deadline
LOGIN_FAILURE_FAST_FAIL_THRESHOLD = 2


def _float_env(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)


def _int_env(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _cooldowns():
    raw = os.getenv("BOT_DELI_BROWSER_COOLDOWNS", "5,15,30,60")
    result = []
    for value in raw.split(","):
        try:
            result.append(max(0.0, float(value.strip())))
        except ValueError:
            pass
    return result or [5.0, 15.0, 30.0, 60.0]


def _role_env_suffix(role):
    if str(role).strip().lower() == "admin":
        return "ADMIN"
    return "BDA"


def profile_config(role):
    suffix = _role_env_suffix(role)
    generic_dir = os.getenv("BOT_DELI_CHROME_USER_DATA_DIR")
    role_dir = os.getenv(f"BOT_DELI_CHROME_USER_DATA_DIR_{suffix}")
    default_bot_dir = Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER" / "User Data"
    user_data_dir = role_dir or generic_dir or str(default_bot_dir)
    profile = os.getenv(f"BOT_DELI_CHROME_PROFILE_{suffix}") or os.getenv("BOT_DELI_CHROME_PROFILE", "Default")
    return str(Path(user_data_dir)), profile


def _profile_key(role):
    user_data_dir, profile = profile_config(role)
    return f"{user_data_dir}|{profile}"


def _is_alive(driver):
    if driver is None:
        return False
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def _drop_driver(role, reason=""):
    """Forget one role driver so its next request starts a fresh Chrome."""
    key = _profile_key(role)
    with _lock:
        broken = _drivers.pop(key, None)
        _driver_roles.pop(key, None)
        _driver_request_counts.pop(key, None)
    try:
        if broken is not None:
            broken.quit()
    except Exception:
        pass
    if reason:
        print(f"[browser_fetch] Reset Chrome driver for {role}: {reason}")


def _is_fatal_browser_error(value):
    """Return True only for failures where reusing the current tab is unsafe."""
    text = str(value or "").casefold()
    markers = (
        "out of memory",
        "aw, snap",
        "tab crashed",
        "chrome not reachable",
        "invalid session id",
        "session deleted",
        "disconnected",
        "renderer",
        "devtoolsactiveport",
        # A tab stuck auto-reloading (observed in production: Chrome F5-loops
        # and only a manual close/reopen clears it) blows the 45s
        # set_script_timeout every time. driver.quit() alone does not
        # reliably stop a wedged renderer, so treat this like a crash: force
        # kill chrome.exe for this profile too, not just drop the Python handle.
        "timed out",
        "timeout",
        "unresponsive",
    )
    return any(marker in text for marker in markers)


def _recover_crashed_profile(role, reason):
    """Close only the dedicated BOT profile after an actual Chrome crash."""
    _drop_driver(role, reason)
    if _is_fatal_browser_error(reason):
        user_data_dir, profile_directory = profile_config(role)
        _close_profile_chrome(user_data_dir)
        _clear_profile_locks(user_data_dir)
        _clear_profile_session(user_data_dir, profile_directory)


def _clear_profile_session(user_data_dir, profile_directory=None):
    """Drop the crashed session so the next launch does not reopen the page.

    After a crash Chrome restores the previous tabs, which for an Out of Memory
    crash means immediately reloading the very page that exhausted the
    renderer: the worker then loops relaunch -> OOM -> relaunch and never makes
    progress (observed on BOT 3, 2026-09-07). --disable-session-crashed-bubble
    is already set but only hides the prompt - the session files still drive the
    restore, so they have to go.

    The FMS login is NOT affected: cookies live in the profile's Network/Cookies
    database, which this does not touch.
    """
    profile_dir = Path(user_data_dir) / (profile_directory or "Default")
    cleared = []
    for name in ("Current Session", "Current Tabs", "Last Session", "Last Tabs"):
        path = profile_dir / name
        try:
            if path.exists():
                path.unlink()
                cleared.append(name)
        except OSError:
            # Chrome still holds the file; the next recovery pass retries.
            pass

    # Chrome decides to offer a restore from these two flags, not from the
    # session files alone, so reset them as well.
    preferences_path = profile_dir / "Preferences"
    try:
        preferences = json.loads(preferences_path.read_text(encoding="utf-8"))
        profile_node = preferences.setdefault("profile", {})
        if (
            profile_node.get("exit_type") != "Normal"
            or profile_node.get("exited_cleanly") is False
        ):
            profile_node["exit_type"] = "Normal"
            profile_node["exited_cleanly"] = True
            preferences_path.write_text(json.dumps(preferences), encoding="utf-8")
            cleared.append("Preferences exit_type")
    except (OSError, ValueError):
        pass

    if cleared:
        print(
            "[browser_fetch] Cleared crashed session state so Chrome reopens "
            f"blank: {', '.join(cleared)}"
        )


def _close_profile_chrome(user_data_dir):
    """Close only Chrome processes owned by one dedicated BOT profile."""
    if os.name != "nt":
        return 0

    # Do not kill the user's normal Chrome.  Every BOT profile has its own
    # --user-data-dir, so command-line matching keeps recovery role-specific.
    profile = str(Path(user_data_dir)).replace("'", "''")
    command = (
        "$profile = '" + profile + "'; "
        "Get-CimInstance Win32_Process -Filter \"Name = 'chrome.exe'\" | "
        "Where-Object { $_.CommandLine -and $_.CommandLine -like ('*' + $profile + '*') } | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; $_.ProcessId }"
    )
    try:
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        closed = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
        if closed:
            print(f"[browser_fetch] Closed stale Chrome for BOT profile: {', '.join(closed)}")
        return len(closed)
    except Exception as exc:
        print(f"[browser_fetch] Cannot close stale BOT Chrome safely: {exc}")
        return 0


def _clear_profile_locks(user_data_dir):
    """Remove only Chromium's transient locks after its BOT process is gone."""
    removed = []
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        path = Path(user_data_dir) / name
        try:
            path.unlink(missing_ok=True)
            removed.append(name)
        except OSError:
            # A live Chrome still owns the lock.  The next launch retry will
            # either succeed after it exits or leave the profile untouched.
            pass
    if removed:
        print(f"[browser_fetch] Cleared stale Chrome lock(s): {', '.join(removed)}")


def _trim_browser_tabs(driver):
    """Keep the dedicated BOT profile to one tab to prevent session-restore RAM growth."""
    try:
        handles = list(driver.window_handles)
        if not handles:
            return
        keep = driver.current_window_handle if driver.current_window_handle in handles else handles[0]
        for handle in handles:
            if handle == keep:
                continue
            try:
                driver.switch_to.window(handle)
                driver.close()
            except Exception:
                pass
        driver.switch_to.window(keep)
        if len(handles) > 1:
            print(f"[browser_fetch] Closed {len(handles) - 1} restored tab(s) from BOT profile")
    except Exception:
        pass


def _restart_if_request_budget_reached(role):
    """Recycle Chrome before long API runs exhaust the renderer memory."""
    # FMS detail endpoints can return large payloads.  A short-lived Chrome is
    # considerably more reliable than keeping one renderer alive for hundreds
    # of TO-detail calls on this workstation.
    request_budget = _int_env("BOT_DELI_BROWSER_RESTART_EVERY_REQUESTS", 50)
    if request_budget <= 0:
        return
    key = _profile_key(role)
    with _lock:
        completed = _driver_request_counts.get(key, 0)
    if completed < request_budget:
        return
    _drop_driver(role, f"periodic memory recycle after {completed} FMS request(s)")


def _mark_successful_request(role):
    key = _profile_key(role)
    with _lock:
        _driver_request_counts[key] = _driver_request_counts.get(key, 0) + 1
    _clear_login_failure(role)


def _login_cooldown_seconds():
    return _float_env("BOT_DELI_BROWSER_LOGIN_COOLDOWN_SECONDS", 300)


def _login_fail_threshold():
    return max(1, _int_env("BOT_DELI_BROWSER_LOGIN_FAIL_THRESHOLD", LOGIN_FAILURE_FAST_FAIL_THRESHOLD))


def _check_login_circuit(role):
    """Fail instantly, with no Chrome I/O, while a role's session is known-dead."""
    key = _profile_key(role)
    with _lock:
        until = _login_circuit_open_until.get(key)
    if until and time.monotonic() < until:
        raise BrowserSessionUnavailableError(
            f"FMS {role}: chua dang nhap lai duoc (khong thay nut chon role/station sau nhieu lan). "
            f"Tam dung goi API them ~{until - time.monotonic():.0f}s de tranh treo hang gio - "
            "hay mo Chrome profile nay va dang nhap / xac minh tai khoan Google thu cong."
        )


def _record_login_failure(role):
    key = _profile_key(role)
    threshold = _login_fail_threshold()
    just_opened = False
    with _lock:
        count = _login_failure_counts.get(key, 0) + 1
        _login_failure_counts[key] = count
        if count >= threshold and key not in _login_circuit_open_until:
            _login_circuit_open_until[key] = time.monotonic() + _login_cooldown_seconds()
            just_opened = True
    if just_opened:
        print(
            f"[browser_fetch] !!! FMS {role}: chua dang nhap sau {count} lan lien tiep. "
            f"TAM DUNG goi API {_login_cooldown_seconds():.0f}s de khong treo hang gio - "
            "CAN MO CHROME PROFILE NAY VA DANG NHAP / XAC MINH TAI KHOAN GOOGLE THU CONG."
        )


def _clear_login_failure(role):
    key = _profile_key(role)
    with _lock:
        _login_failure_counts.pop(key, None)
        _login_circuit_open_until.pop(key, None)


def _open_driver(role):
    try:
        from selenium import webdriver
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError as e:
        raise BrowserFetchError("Thieu selenium. Cai bang: python -m pip install selenium") from e

    user_data_dir, profile_directory = profile_config(role)
    Path(user_data_dir).mkdir(parents=True, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--user-data-dir=" + user_data_dir)
    options.add_argument(f"--profile-directory={profile_directory}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-session-crashed-bubble")
    options.add_argument("--noerrdialogs")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-domain-reliability")
    options.add_argument("--disable-features=BackForwardCache,MediaRouter,OptimizationHints,Translate")
    # V8 sizes its heap from the machine's free RAM, so on a 16 GB workstation a
    # renderer will grow past 4 GB before it bothers collecting.  Every FMS call
    # returns the whole response body as a string through the DevTools bridge,
    # so a TO-detail slice piles up hundreds of MB of dead strings that nothing
    # asks V8 to reclaim.  Measured 2026-09-08: BOT 2 hit 4,278 MB after 2.9
    # minutes while BOT 1 sat at 200 MB; three bots peaking together exhausted
    # the machine and produced BOT 3's "Aw, Snap! Out of Memory" on 2026-09-07.
    # Capping old-space forces a collection at the cap instead of at the RAM
    # ceiling.  A response that genuinely needs more kills the renderer, which
    # _recover_crashed_profile already handles as a retry - no silent data loss.
    # NOTE: do NOT try to cap renderer memory with Chrome flags here.  Both were
    # tried on 2026-09-08 and both were reverted within the hour.
    #
    # --js-flags=--max-old-space-size=768 does NOT bound what actually grows.
    # Measured with the flag confirmed active on the command line: BOT 2's
    # renderer was at 1,971 MB and BOT 3's at 1,767 MB, both 36 seconds old.
    # max-old-space-size caps V8's old space only; the FMS response bodies
    # arrive as external strings/ArrayBuffers that live outside it, so the
    # growth is untouched.  What the cap DID do is make V8 throw a fatal OOM on
    # the portion that does count, killing the renderer - "Aw, Snap! / Out of
    # Memory" on spx.shopee.vn/#/index, plus BrowserLoginError ("Chua thay nut
    # chon role/station") on the next cycle because Chrome reopens on the crash
    # page instead of FMS.  Worst of both: no bound, but new fatal crashes.
    #
    # My "6.7 GB -> 3.0 GB" reading that seemed to justify the cap was taken
    # minutes after a restart, when every renderer was still young.  Measure
    # renderer memory mid-cycle, never right after a restart.
    #
    # --renderer-process-limit=1 was worse: BOT 3 failed on its very first
    # "Batch tracking search 500" with "target frame detached: received
    # Inspector.detached event".  One renderer for every frame means Chrome
    # must swap a frame out of its process when a cross-site frame needs one,
    # detaching the frame ChromeDriver is executing in.
    #
    # The only lever that has ever worked here is
    # BOT_DELI_BROWSER_RESTART_EVERY_REQUESTS (see the RUN_BOT*.ps1 files).  Tried 2026-09-08 and
    # reverted the same hour: BOT 3 failed on its very first "Batch tracking
    # search 500" with "target frame detached: received Inspector.detached
    # event" and could not get past the retry loop.  With a hard cap of one
    # renderer, Chrome has to swap an existing frame out of its process when a
    # cross-site frame needs one, which detaches the frame ChromeDriver is
    # executing in.  It failed immediately on a freshly started bot, so it was
    # not memory pressure.  The flag contributed nothing measurable anyway -
    # the heap cap above is what took Chrome from 6.7 GB to 3.0 GB.

    driver = None
    last_error = None
    for attempt in range(1, 4):
        try:
            driver = webdriver.Chrome(options=options)
            break
        except Exception as exc:
            last_error = exc
            if attempt == 3:
                break
            print(
                f"[browser_fetch] Chrome start failed {attempt}/3 for {user_data_dir}; "
                "cleaning this BOT profile and retrying..."
            )
            _close_profile_chrome(user_data_dir)
            _clear_profile_locks(user_data_dir)
            # A profile left mid-crash can also refuse to start; clear the
            # session so the retry does not reopen whatever killed it.
            _clear_profile_session(user_data_dir, profile_directory)
            time.sleep(3 * attempt)
    if driver is None:
        raise BrowserFetchError(
            "Khong mo duoc Chrome profile cho BOT sau 3 lan thu. "
            f"Profile dang dung: {user_data_dir} / {profile_directory}. "
            "BOT da tu dong dong Chrome loi va xoa lock cua dung profile nay. "
            f"Loi goc: {last_error}"
        ) from last_error
    driver.set_script_timeout(45)
    _trim_browser_tabs(driver)
    driver.get(SPX_HOME)

    # Wait for the FMS shell/station selector. A slow login page should not be
    # mistaken for a ready API session.
    try:
        wait_seconds = _int_env("BOT_DELI_BROWSER_LOGIN_WAIT_SECONDS", 180)
        print(f"[browser_fetch] Waiting FMS login/session up to {wait_seconds}s: {user_data_dir} / {profile_directory}")
        WebDriverWait(driver, 30).until(
            lambda d: "spx.shopee.vn" in (d.current_url or "") and bool(_station_text(d))
        )
    except Exception:
        try:
            WebDriverWait(driver, max(1, wait_seconds - 30)).until(
                lambda d: "spx.shopee.vn" in (d.current_url or "") and bool(_station_text(d))
            )
        except Exception:
            print("[browser_fetch] Warning: chua thay station selector; neu la profile moi, hay login SPX trong cua so Chrome BOT.")

    print(f"[browser_fetch] Chrome opened: {user_data_dir} / {profile_directory}")
    return driver


def _station_text(driver):
    from selenium.webdriver.common.by import By
    candidates = [
        (By.XPATH, '//*[@id="fms-container"]/div[1]/div[1]/span[1]/span[1]/div/span/span'),
        (By.CLASS_NAME, "station-label"),
    ]
    for by, value in candidates:
        try:
            text = (driver.find_element(by, value).text or "").strip()
            if text:
                return text
        except Exception:
            pass
    return ""


def _ensure_role(driver, role):
    """Switch the FMS station/role in the live browser when necessary."""
    from selenium.webdriver.common.by import By

    role = str(role or "").strip()
    if not role:
        return

    # Dismiss the station-changed modal when present.
    try:
        modal_text = driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div[2]/div[1]/div").text
        if "Login station is changed by someone else" in modal_text:
            driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div[3]/div[2]/button").click()
            time.sleep(1)
    except Exception:
        pass

    current = _station_text(driver)
    if current == role:
        return

    station_xpath = '//*[@id="fms-container"]/div[1]/div[1]/span[1]/span[1]/div/span/span'

    def visible_elements(by, selector):
        try:
            return [item for item in driver.find_elements(by, selector) if item.is_displayed()]
        except Exception:
            return []

    def click_safely(element):
        # FMS frequently keeps an animation overlay for a short time after a
        # role/API transition. Native click can then report "not interactable".
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        try:
            element.click()
        except Exception:
            driver.execute_script("arguments[0].click();", element)

    def station_trigger():
        candidates = []
        candidates.extend(visible_elements(By.XPATH, station_xpath))
        candidates.extend(visible_elements(By.CLASS_NAME, "station-label"))
        for item in candidates:
            if (item.text or "").strip():
                return item
        return None

    last_error = None
    for attempt in range(5):
        try:
            trigger = station_trigger()
            if trigger is None:
                raise BrowserLoginError("Chua thay nut chon role/station tren trang FMS")
            click_safely(trigger)

            selected = None
            for _wait in range(20):
                options = visible_elements(By.XPATH, "//li")
                selected = next((item for item in options if (item.text or "").strip() == role), None)
                if selected is not None:
                    break
                time.sleep(0.25)
            if selected is None:
                items = [line.strip() for item in visible_elements(By.XPATH, "//li") for line in (item.text or "").split("\n") if line.strip()]
                raise BrowserLoginError(f"Khong tim thay role/station '{role}' trong FMS. Current={current!r}")

            click_safely(selected)
            for _wait in range(20):
                time.sleep(0.25)
                if _station_text(driver) == role:
                    return
            current = _station_text(driver)
            raise BrowserLoginError(f"Da click '{role}' nhung station hien tai van la {current!r}")
        except Exception as e:
            last_error = e
            # A stale FMS view can leave the station dropdown covered. Reload
            # once before the final retries; browser cookies/session are kept.
            if attempt == 1:
                try:
                    driver.get(SPX_HOME)
                except Exception:
                    pass
            time.sleep(1.5)
    raise BrowserLoginError(str(last_error or f"Khong switch duoc role {role}"))


def get_driver(role):
    _check_login_circuit(role)
    _restart_if_request_budget_reached(role)
    key = _profile_key(role)
    with _lock:
        driver = _drivers.get(key)
        if not _is_alive(driver):
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
            driver = _open_driver(role)
            _drivers[key] = driver
            _driver_roles.pop(key, None)
        if _driver_roles.get(key) != role or _station_text(driver) != role:
            try:
                _ensure_role(driver, role)
            except BrowserLoginError:
                _record_login_failure(role)
                raise
            _clear_login_failure(role)
            _driver_roles[key] = role
        return driver


def ensure_role(role):
    return get_driver(role)


def shutdown_all():
    with _lock:
        for key, driver in list(_drivers.items()):
            try:
                driver.quit()
            except Exception:
                pass
            _drivers.pop(key, None)
            _driver_roles.pop(key, None)


atexit.register(shutdown_all)


def _handle_signal(signum, frame):
    shutdown_all()
    raise SystemExit(0)


for _sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
    _sig = getattr(signal, _sig_name, None)
    if _sig is not None:
        try:
            signal.signal(_sig, _handle_signal)
        except Exception:
            pass


def endpoint_key(url):
    parts = urlsplit(url)
    return parts.path or url


def _endpoint_interval(endpoint):
    # High-volume FMS endpoints get a slightly larger default spacing.
    # Trimmed ~15-20% from the original values below (2026-09-05): no
    # "[browser_fetch] cooldown" throttle event had ever been observed in
    # logs at the old, more conservative numbers. tracking_list/search is
    # deliberately left untouched here since its BATCH_SIZE just went 250->500
    # - stacking two changes on the same endpoint at once would make any
    # regression hard to attribute. If cooldown lines start appearing after
    # this change, raise the affected endpoint's value back up.
    overrides = {
        "/api/admin/transportation/trip/loading/list": 0.8,
        "/api/admin/transportation/trip/history/loading/list": 0.8,
        "/api/in-station/general_to/detail/search": 0.75,
        "/api/fleet_order/order/tracking_list/search": 1.2,
        # tracking_info la endpoint nang nhat cua BOT 3: do 2026-09-19 co 27.601
        # luot trong 24h, trung vi 1,00s moi luot - trong do ~0,5s la nhip nghi
        # mac dinh o duoi, tuc bot tu cho mat ~3,8 gio/ngay. Ha xuong 0,3 de lay
        # lai ~1,5 gio/ngay. Chua tung thay dong "[browser_fetch] cooldown" cho
        # endpoint nay, nen con cho. Neu cooldown bat dau xuat hien thi dua lai 0,5.
        "/api/fleet_order/order/detail/tracking_info": 0.3,
    }
    configured = os.getenv("BOT_DELI_BROWSER_ENDPOINT_INTERVAL")
    if configured is not None:
        try:
            return max(0.0, float(configured))
        except ValueError:
            pass
    return overrides.get(endpoint, 0.5)


def _pace(endpoint):
    global _last_global_at
    global_interval = _float_env("BOT_DELI_BROWSER_GLOBAL_INTERVAL", 0.2)
    endpoint_interval = _endpoint_interval(endpoint)

    with _lock:
        now = time.monotonic()
        target = max(
            _last_global_at + global_interval,
            _last_endpoint_at.get(endpoint, 0.0) + endpoint_interval,
            _blocked_until.get(endpoint, 0.0),
        )
    if target > now:
        time.sleep(target - now)
    with _lock:
        stamp = time.monotonic()
        _last_global_at = stamp
        _last_endpoint_at[endpoint] = stamp


def _set_cooldown(endpoint, seconds, reason=""):
    seconds = max(0.0, float(seconds)) + random.uniform(0.15, 0.85)
    until = time.monotonic() + seconds
    with _lock:
        _blocked_until[endpoint] = max(_blocked_until.get(endpoint, 0.0), until)
    print(f"[browser_fetch] cooldown {endpoint}: {seconds:.1f}s | {reason}")


def _api_error_info(data):
    if not isinstance(data, dict):
        return None, ""
    for key in ("code", "retcode", "error_code"):
        code = data.get(key)
        if code not in (None, 0, "0"):
            return code, str(data.get("msg") or data.get("message") or data.get("error") or "")
    return None, ""


def _is_throttle(status, code, message, text):
    if int(status or 0) in {403, 408, 409, 418, 425, 429, 502, 503, 504}:
        return True
    haystack = f"{code} {message} {text}".lower()
    tokens = [
        "too many", "too frequent", "request frequent", "frequent request",
        "rate limit", "rate-limit", "thrott", "blocked", "try again later",
        "please retry later", "system busy", "request limit", "频繁", "稍后再试",
    ]
    return any(token in haystack for token in tokens)


def _is_login_error(status, code, message, text):
    if int(status or 0) == 401:
        return True
    haystack = f"{code} {message} {text}".lower()
    return "login credentials expired" in haystack or "not login" in haystack or "unauthorized" in haystack


def request_json(role, method, url, payload=None, label="", max_retries=None):
    """Execute one FMS request through Chrome fetch and return the full JSON dict.

    Non-zero API codes are errors.  Known no-record responses fail immediately without
    pointless retries.  Suspected throttling uses progressively longer cooldowns.
    """
    method = str(method or "GET").upper()
    endpoint = endpoint_key(url)
    max_retries = max_retries or _int_env("BOT_DELI_BROWSER_MAX_RETRIES", 5)
    cooldowns = _cooldowns()
    last_error = None

    for attempt in range(1, max_retries + 1):
        _pace(endpoint)
        driver = get_driver(role)
        try:
            result = driver.execute_async_script(FETCH_JS, url, method, payload)
        except Exception as e:
            # Driver may have crashed. Drop it so the next attempt opens a new one.
            _recover_crashed_profile(role, str(e))
            last_error = BrowserFetchError(f"execute_async_script: {e}")
            if attempt >= max_retries:
                break
            delay = min(10.0, 1.5 * attempt) + random.uniform(0.1, 0.6)
            print(f"[{label}] browser loi {attempt}/{max_retries}: {e}. Retry sau {delay:.1f}s")
            time.sleep(delay)
            continue

        if not isinstance(result, dict) or not result.get("transport_ok"):
            error = (result or {}).get("error") if isinstance(result, dict) else result
            if _is_fatal_browser_error(error):
                _recover_crashed_profile(role, f"fetch transport: {error}")
            last_error = BrowserFetchError(f"fetch transport error: {error}")
            if attempt >= max_retries:
                break
            delay = min(10.0, 1.5 * attempt) + random.uniform(0.1, 0.6)
            print(f"[{label}] fetch transport loi {attempt}/{max_retries}: {error}. Retry sau {delay:.1f}s")
            time.sleep(delay)
            continue

        status = int(result.get("status") or 0)
        text = result.get("text") or ""
        data = None
        if text:
            try:
                data = json.loads(text)
            except Exception:
                data = None

        code, message = _api_error_info(data)

        if str(code) == "131101002" or "no record found in the database" in f"{message} {text}".lower():
            raise BrowserNoRecordError(f"API {code}: {message or 'No record found in the database'}")

        if _is_login_error(status, code, message, text):
            # Re-ensure role/session once before retrying.
            last_error = BrowserLoginError(f"HTTP {status} API {code}: {message or text[:200]}")
            key = _profile_key(role)
            _driver_roles.pop(key, None)
            if attempt >= max_retries:
                break
            print(f"[{label}] login/session loi, refresh role trong Chrome roi retry...")
            time.sleep(1.0)
            continue

        # A successful FMS JSON response can contain words such as "blocked" or
        # "throttle" inside order data. Do not run throttle keyword detection on
        # valid retcode/code=0 payloads, otherwise BatchSearch can be retried even
        # though the page was returned correctly.
        if result.get("http_ok") and 200 <= status < 300 and data is not None and code in (None, 0, "0"):
            _mark_successful_request(role)
            return data

        if _is_throttle(status, code, message, text):
            last_error = BrowserThrottleError(f"HTTP {status} API {code}: {message or text[:250]}")
            if attempt >= max_retries:
                break
            cooldown = cooldowns[min(attempt - 1, len(cooldowns) - 1)]
            _set_cooldown(endpoint, cooldown, reason=f"{label}: {last_error}")
            continue

        if not result.get("http_ok") or status < 200 or status >= 300:
            last_error = BrowserFetchError(f"HTTP {status}: {text[:300]}")
            if attempt >= max_retries:
                break
            delay = min(15.0, 2.0 * attempt) + random.uniform(0.1, 0.8)
            print(f"[{label}] HTTP loi {attempt}/{max_retries}: {last_error}. Retry sau {delay:.1f}s")
            time.sleep(delay)
            continue

        if data is None:
            last_error = BrowserFetchError(f"Response khong phai JSON: {text[:300]}")
            if attempt >= max_retries:
                break
            time.sleep(1.0 + attempt)
            continue

        if code not in (None, 0, "0"):
            last_error = BrowserFetchError(f"API code={code}: {message}")
            if attempt >= max_retries:
                break
            delay = min(15.0, 2.0 * attempt) + random.uniform(0.1, 0.8)
            print(f"[{label}] API loi {attempt}/{max_retries}: {last_error}. Retry sau {delay:.1f}s")
            time.sleep(delay)
            continue

        _mark_successful_request(role)
        return data

    raise last_error or BrowserFetchError(f"Request failed: {method} {url}")

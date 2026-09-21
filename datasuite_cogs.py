"""COGS tu Data Suite (OneBI) thay vi API COGS cua FMS.

Nguon FMS (`fleet_order/order/detail/show_sensitive_data`) moi don mot request va
co han muc ngay cua tai khoan Admin.  Dashboard "SOC - Download Board" tren
datasuite.shopee.io tra ve cung so `cogs`, loc theo `shipment_id IN (...)`, nen
goi duoc theo LO.

Hai buoc, chay bang tay truoc khi noi vao BOT 3:

    python datasuite_cogs.py capture
    python datasuite_cogs.py get SPXVN062416873319

`capture` mo Chrome that de ban dang nhap, roi TU BAT lay payload that ma trang
gui di, thay vi cheo tay lai mot khoi JSON 45 KB (chep tay la sai, va moi lan BI
sua dashboard la hong).  Payload duoc luu vao datasuite_payload_template.json.

`get` nap lai payload do, chi thay danh sach shipment trong bo loc "Shipment ID",
roi goi fetch() ngay trong tab da dang nhap - cung cach browser_fetch.py dang lam
voi FMS, nen cookie phien dang nhap khong bao gio phai roi khoi trinh duyet.

Bien moi truong:
  BOT_DELI_DATASUITE_USER_DATA_DIR   mac dinh %LOCALAPPDATA%\\BOT_DELI_DATASUITE_BROWSER\\User Data
  BOT_DELI_DATASUITE_PROFILE         mac dinh Default
  BOT_DELI_DATASUITE_DASHBOARD_URL   mac dinh dashboard SOC - Download Board
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_PATH = BASE_DIR / "datasuite_payload_template.json"

# Trinh duyet tu dat may header nay va khong cho script ghi de. Gui lai chung
# chi to rac, nen bo han khi luu.
_HEADER_KHONG_GUI_LAI = {
    "cookie", "content-length", "host", "connection", "accept-encoding",
    "origin", "referer", "user-agent", "content-type", "accept",
}


def _usable_headers(headers):
    return {
        str(k): str(v)
        for k, v in (headers or {}).items()
        if str(k).strip()
        and str(k).strip().lower() not in _HEADER_KHONG_GUI_LAI
        and not str(k).startswith(":")
        and not str(k).lower().startswith("sec-")
    }

EXECUTE_URL = "https://datasuite.shopee.io/dashboard/api/v1/report/execute"
DASHBOARD_URL = os.getenv(
    "BOT_DELI_DATASUITE_DASHBOARD_URL",
    "https://datasuite.shopee.io/dashboard/dashboard/"
    "9375433f-8a51-4135-bb48-47b4bf18f0f3/normal?page=0",
)
SHIPMENT_FILTER_NAME = "Shipment ID"

# Mot don di qua nhieu chang, moi chang mot dong, va `cogs` LAP LAI nguyen gia
# tri tren tung dong - khong phai chia nho ra.  Doc duoc tu chinh cau SQL ma
# API tra ve: SUM(cogs) GROUP BY <tat ca cot chieu>, trong do co outbound_trip_id
# va outbound_to_num.  SPXVN062416873319 ra ba dong deu la 415.000, tong cua
# chung la 1.245.000 va hoan toan sai.  Vi vay lay gia tri duy nhat, khong cong.
DEFAULT_BATCH_SIZE = 200

_driver = None


# ---------------------------------------------------------------- trinh duyet

def _profile():
    user_data_dir = os.getenv("BOT_DELI_DATASUITE_USER_DATA_DIR") or str(
        Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_DATASUITE_BROWSER" / "User Data"
    )
    return user_data_dir, os.getenv("BOT_DELI_DATASUITE_PROFILE", "Default")


def get_driver():
    """Chrome rieng cho Data Suite.

    Co tinh KHONG dung chung profile voi browser_fetch.py: ba con bot FMS deu
    chay lien tuc, mot user-data-dir chung se bi khoa cheo, va dang nhap o day la
    SSO cua Data Suite chu khong phai phien FMS.
    """
    global _driver
    if _driver is not None:
        try:
            _driver.current_url
            return _driver
        except Exception:
            _driver = None

    from selenium import webdriver

    user_data_dir, profile_directory = _profile()
    Path(user_data_dir).mkdir(parents=True, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--user-data-dir=" + user_data_dir)
    options.add_argument(f"--profile-directory={profile_directory}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-background-networking")
    _driver = webdriver.Chrome(options=options)
    _driver.set_script_timeout(180)
    return _driver


def _on_dashboard(driver):
    if "datasuite.shopee.io" not in (driver.current_url or ""):
        driver.get(DASHBOARD_URL)
        time.sleep(3)


# ------------------------------------------------------------- bat payload

# Cai truoc khi script cua trang chay, nen bat duoc ca request dau tien.
#
# Bat ca HEADER chu khong chi body: goi lai chi voi accept + content-type thi
# Data Suite tra ve HTTP 403.  Trang gui kem header rieng cua no, va doan xem la
# header nao thi vo ich - cu chep lai y nguyen cai trang that su gui.
_INTERCEPT_JS = r"""
(function () {
  if (window.__botDeliCapture) { return; }
  window.__botDeliCapture = [];
  function docHeaders(h) {
    var out = {};
    if (!h) { return out; }
    try {
      if (typeof Headers !== 'undefined' && h instanceof Headers) {
        h.forEach(function (v, k) { out[k] = v; });
      } else if (Array.isArray(h)) {
        h.forEach(function (p) { if (p && p.length >= 2) { out[p[0]] = p[1]; } });
      } else {
        Object.keys(h).forEach(function (k) { out[k] = h[k]; });
      }
    } catch (e) {}
    return out;
  }
  var goc = window.fetch;
  window.fetch = function (input, init) {
    var ket = goc.apply(this, arguments);
    try {
      var url = (typeof input === 'string') ? input : (input && input.url) || '';
      if (url.indexOf('/report/execute') !== -1) {
        var body = (init && init.body) || (input && input.body) || null;
        if (typeof body === 'string') {
          var h = docHeaders(init && init.headers);
          if (!Object.keys(h).length && input && input.headers) {
            h = docHeaders(input.headers);
          }
          var rec = {body: body, headers: h, resp: null};
          window.__botDeliCapture.push(rec);
          // Doc ca PHAN HOI ma chinh trang nhan duoc. Khong co cai nay thi chi
          // suy doan duoc "trang co ra dong khong"; co roi thi biet chac.
          // clone() vi than Response chi doc duoc mot lan, va ung dung con can.
          ket.then(function (r) {
            try {
              r.clone().text().then(function (t) {
                try {
                  var j = JSON.parse(t);
                  var d = j.data || {};
                  rec.resp = {
                    code: j.code,
                    rows: (d.rows || []).length,
                    param: d.parameter || null
                  };
                } catch (e) { rec.resp = {code: null, rows: -1, param: null}; }
              });
            } catch (e) {}
          });
        }
      }
    } catch (e) {}
    return ket;
  };
})();
"""


_session = {}


def _has_shipment_value(payload):
    """Payload nay co mang theo mot ma don that khong."""
    return any(f.get("valueList") for f in _shipment_filters(payload))


def ensure_session(timeout=600, need_template=False, need_filter=False,
                   report_name=None, match_resource=None, verbose=True):
    """Mo dashboard, de chinh trang do goi /report/execute, roi muon header cua no.

    Header KHONG con duoc ghi ra dia.  Ban dau toi luu chung vao
    datasuite_request_headers.json roi doc lai o lan chay sau, va Data Suite tra
    ve HTTP 403: X-CSRF-TOKEN cung SESSION-CONTEXT gan voi phien dang nhap luc
    do, sang tien trinh sau la het han.  Muon cua phien DANG song thi luon tuoi,
    va token cung khong con nam tren dia de lo nua.

    need_template=True thi phai cho dung bang co cot cogs, vi con phai luu mau.
    need_template=False thi bat ky request nao cung du, vi header giong nhau o
    moi bang - do la duong di cua `get`, khoi phai cho ca trang ve xong.
    """
    driver = get_driver()
    if _session.get("driver_id") == id(driver) and _session.get("headers") is not None:
        if not need_template or _session.get("raw_body"):
            return _session

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument", {"source": _INTERCEPT_JS}
        )
    except Exception as exc:
        if verbose:
            print(f"Khong cai duoc bo bat qua CDP ({exc}); se cai sau khi trang tai xong.")

    driver.get(DASHBOARD_URL)
    if verbose:
        print("Da mo dashboard.")
        print("  - Neu hien man hinh dang nhap, dang nhap binh thuong.")
        print("  - Khi bang du lieu hien ra la xong. Neu mai khong thay, bam Apply mot lan.")
        print(f"  - Cho toi da {timeout}s.")

    # Mot trang dashboard co NHIEU bang, moi bang tu goi /report/execute cua no.
    # Lan dau toi chon theo do dai payload va vo phai bang khac: no dai hon that,
    # nhung "measures" rong tuc khong co cot cogs.  Nen chon theo NOI DUNG - bang
    # nao co do cogs va co bo loc Shipment ID - chu khong theo kich thuoc.
    seen = {}
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            driver.execute_script(_INTERCEPT_JS)
            captured = driver.execute_script("return window.__botDeliCapture || [];")
        except Exception:
            captured = []

        for goi in captured:
            try:
                raw_body = goi["body"]
                payload = json.loads(raw_body)
                headers = _usable_headers(goi.get("headers") or {})
            except (ValueError, KeyError, TypeError):
                continue

            # Header KHONG giong nhau o moi bang - it nhat PG-I thi khong - nen
            # luu rieng theo tung bao cao, va khi goi bang nao thi dung header
            # cua chinh bang do.
            ma_bc = str((payload.get("report") or {}).get("resourceIdCode") or "")
            if headers:
                _session.setdefault("headers_by_resource", {})[ma_bc] = headers
            if not _session.get("headers"):
                _session["headers"] = headers
                _session["driver_id"] = id(driver)
                if verbose:
                    # Chi in TEN header. Gia tri co token phien dang nhap.
                    print(f"  muon duoc header: {sorted(headers)}")

            if raw_body in seen:
                # Phan hoi ve sau request vai tram ms, nen moi vong cap nhat lai.
                cu = seen[raw_body]
                moi = goi.get("resp")
                if moi and not cu.get("resp"):
                    cu["resp"] = moi
                    if verbose and not cu["loi"]:
                        print(f"  ung vien {cu['ten']!r}: trang nhan "
                              f"{moi.get('rows')} dong (code={moi.get('code')})")
                continue

            loi = _template_problems(payload)
            bc = payload.get("report") or {}
            # Ten bao cao KHONG phan biet duoc cac widget: trang nay co hai bang
            # cung mang ten 'SOC OUTBOUND ORDER L5D' (tieu de hien thi cua mot
            # trong hai da duoc doi thanh L30D nhung ten ben trong giu nguyen).
            # resourceIdCode moi la thu dinh danh duy nhat.
            ten = "{} [{} / ds {}]".format(
                bc.get("name") or "(khong ten)",
                bc.get("resourceIdCode") or "?",
                bc.get("datasetId") or "?",
            )
            # Mau bat luc TAI TRANG khac mau bat luc BAM APPLY: cai dau khong mang
            # ma don nao.
            if not loi and need_filter and not _has_shipment_value(payload):
                loi = ["chua co ma don trong bo loc (mau luc tai trang)"]
            seen[raw_body] = {
                "payload": payload, "ten": ten, "loi": loi, "resp": goi.get("resp"),
                "thay_luc": time.time(), "headers": headers,
            }
            if verbose:
                if loi:
                    print(f"  bo qua bang {ten!r}: {'; '.join(loi)}")
                else:
                    # In ca bang DAT hinh dang, kem so dong trang nhan duoc, de
                    # thay ro bang nao co du lieu va ten that cua no la gi.
                    print(f"  ung vien {ten!r}: dat hinh dang, dang doi phan hoi")

        # Chon bang nao THAT SU tra ve dong, chu khong phai bang dau tien co du
        # hinh dang.  Trang co HAI bang outbound - L5D va L30D - va ca hai deu co
        # do cogs lan bo loc Shipment ID, nen kiem theo hinh dang khong tach duoc.
        # Do chinh la cai bay ngay 2026-09-11: toi bat trung bang L5D trong khi
        # nguoi dung dang xem L30D, va mat nua ngay tuong la loi goi API.
        du_dieu_kien = [(b, c) for b, c in seen.items() if not c["loi"]]
        co_dong = [(b, c) for b, c in du_dieu_kien
                   if (c.get("resp") or {}).get("rows", 0) > 0]

        # --report chi la uu tien, khong phai dieu kien cung.  Ten hien tren man
        # hinh khong chac trung voi report.name ben trong payload - tieu de widget
        # co the da duoc doi ma ten bao cao thi giu nguyen ten cu.  Mot bang that
        # su co du lieu van hon mot bang trung ten ma rong.
        if report_name:
            khop = [(b, c) for b, c in co_dong
                    if report_name.strip().lower() in c["ten"].lower()]
            if khop:
                co_dong = khop
            elif co_dong and verbose:
                print(f"  (khong bang nao ten chua {report_name!r} ma co du lieu; "
                      f"lay bang co du lieu: {co_dong[0][1]['ten']!r})")

        chon = None
        if co_dong:
            chon = co_dong[0]
        elif need_filter:
            # Loi thoat. Ban dau toi chi bat truong hop CHUA doc duoc phan hoi,
            # nen khi phan hoi ve ma bao 0 dong thi khong nhanh nao nhan va vong
            # lap chay mai - dung cai da xay ra 2026-09-11. Gio het 45 giay la
            # lay, du phan hoi noi gi, mien la ung vien do mang ma don.
            qua_han = [
                (b, c) for b, c in du_dieu_kien
                if time.time() - c["thay_luc"] > 45
            ]
            if report_name:
                khop = [(b, c) for b, c in qua_han
                        if report_name.strip().lower() in c["ten"].lower()]
                qua_han = khop or qua_han
            if qua_han:
                chon = qua_han[-1]
                so_dong = (chon[1].get("resp") or {}).get("rows")
                if verbose:
                    print(f"  (het 45s; tam lay {chon[1]['ten']!r}, trang bao "
                          f"{'chua ro' if so_dong is None else so_dong} dong. "
                          f"Mau van dung hinh dang; chay `get` de biet co du lieu khong.)")
        if chon is None and du_dieu_kien and not need_filter:
            # Khong doi du lieu that thi lay cai dau tien, nhu truoc.
            chon = du_dieu_kien[0]

        if chon:
            raw_body, c = chon
            _session["raw_body"] = raw_body
            _session["payload"] = c["payload"]
            _session["report_name"] = c["ten"]
            _session["page_response"] = c.get("resp")
            # Dung header cua CHINH request nay, khong phai cua request dau tien
            # bat duoc.  Truoc do toi giu header cua widget dau tien roi dem dung
            # cho moi lan goi; may chu tra ve truy van tren "MOCK_TABLE" - no nhan
            # request nhung khong phan giai ra bang that.  PG-I va SESSION-CONTEXT
            # nhieu kha nang gan voi tung widget, nen lay lech la hong.
            if c.get("headers"):
                _session["headers"] = c["headers"]
            if verbose:
                print(f"  nhan bang {c['ten']!r}")
                _in_phan_hoi_trang(c.get("resp"))
            return _session

        if not need_template:
            if match_resource:
                rieng = (_session.get("headers_by_resource") or {}).get(match_resource)
                if rieng:
                    _session["headers"] = rieng
                    return _session
            elif _session.get("headers"):
                return _session
        time.sleep(2)

    if _session.get("headers") and not need_template:
        return _session
    if not seen:
        raise RuntimeError(
            "Het gio ma khong bat duoc request /report/execute nao. "
            "Mo DevTools > Network xem trang co goi execute khong, roi chay lai."
        )
    ten_da_thay = sorted({c["ten"] for c in seen.values()})
    raise RuntimeError(
        f"Bat duoc {len(seen)} request /report/execute nhung khong cai nao vua la "
        f"bang cogs vua tra ve du lieu. Cac bang da thay: {ten_da_thay}. "
        f"Go ma don vao dung bang dang hien du lieu roi bam Apply, "
        f"hoac chi dinh --report <ten bang>."
    )


def capture_template(timeout=600, need_filter=False, report_name=None):
    """Bat payload that cua bang cogs va luu ra file lam mau."""
    if need_filter:
        print("=> Go MOT ma don vao o Shipment ID roi bam Apply. Doi den khi bang")
        print("   hien du lieu. Toi chi nhan bang NAO that su tra ve dong.")
    phien = ensure_session(timeout=timeout, need_template=True,
                           need_filter=need_filter, report_name=report_name)
    TEMPLATE_PATH.write_text(
        json.dumps(phien["payload"], ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(f"Da luu {TEMPLATE_PATH.name} ({TEMPLATE_PATH.stat().st_size:,} byte).")
    return phien["payload"]


def _tim_lai(driver, raw_body):
    """Doc lai ban ghi da bat, de lay phan hoi vua ve."""
    try:
        for goi in driver.execute_script("return window.__botDeliCapture || [];"):
            if goi.get("body") == raw_body:
                return goi
    except Exception:
        pass
    return None


def _in_phan_hoi_trang(resp):
    """CHINH TRANG nhan duoc bao nhieu dong. Day la con so quyet dinh: neu trang
    cung khong ra dong nao thi cach goi cua ta khong sai, du lieu moi la van de."""
    if resp is None:
        print("     (chua doc duoc phan hoi cua trang)")
        return
    rows = resp.get("rows")
    print(f"     CHINH TRANG nhan duoc: {rows} dong | code={resp.get('code')} "
          f"| parameter={resp.get('param')!r}")
    if rows == 0:
        print("     => Trinh duyet cung KHONG ra dong nao. Vay cach goi khong sai,")
        print("        van de nam o du lieu hoac o ma don dang tim.")


def _template_problems(payload):
    """Liet ke ly do mot payload khong phai bang can tim. Rong = dung bang.

    Tim cogs o CA measures LAN dimensions.  Ban dau toi chi tim trong measures va
    bo sot dung bang can tim: bang L30D dung cogs nhu mot cot chieu, nen no bi
    loai voi ly do "khong co thuoc do cogs (dang co: [])" trong khi no chinh la
    bang dang hien du lieu tren man hinh.  Noi rong ra khong nguy hiem, vi khau
    chon cuoi cung con doi bang do phai THAT SU tra ve dong.
    """
    report = (payload or {}).get("report") or {}
    ten_do = [str(m.get("name") or "") for m in (report.get("measures") or [])]
    ten_chieu = [str(d.get("name") or "") for d in (report.get("dimensions") or [])]

    loi = []
    if not ten_chieu:
        loi.append("thieu dimensions")
    if "cogs" not in ten_do and "cogs" not in ten_chieu:
        loi.append(f"khong co cot cogs (do: {ten_do}, chieu: {len(ten_chieu)} cot)")
    if not _shipment_filters(payload):
        loi.append(f"khong co bo loc '{SHIPMENT_FILTER_NAME}'")
    return loi


def _check_template(payload):
    """Chan mot template hong ngay luc luu, thay vi de no hong luc BOT 3 chay."""
    loi = _template_problems(payload)
    if loi:
        raise RuntimeError("Payload bat duoc khong dung bang can tim: " + "; ".join(loi))


# ------------------------------------------------------------------ goi API

def _shipment_filters(payload):
    filters = ((payload or {}).get("report") or {}).get("dashFilters") or []
    return [
        f for f in filters
        if str(f.get("name") or "").strip().lower() == SHIPMENT_FILTER_NAME.lower()
    ]


def _widget(dash_filter):
    return str((dash_filter.get("parameters") or {}).get("widgetName") or "").upper()


def build_payload(shipment_ids, template=None):
    """Nhan ban template va chi thay danh sach don trong bo loc Shipment ID."""
    import copy

    payload = copy.deepcopy(template if template is not None else load_template())
    # dict.fromkeys: bo trung nhung giu nguyen thu tu, de doc log de doi chieu.
    shipment_ids = list(
        dict.fromkeys(str(s).strip() for s in shipment_ids if str(s).strip())
    )

    filters = _shipment_filters(payload)
    o_nhap = next((f for f in filters if _widget(f) == "INPUT"), None)
    o_tai_len = next((f for f in filters if _widget(f) == "UPLOAD"), None)

    # Xoa het truoc, vi template co the duoc bat luc tren trang dang loc san mot
    # don - de sot lai la moi lan goi deu tra ve dung mot don do.
    for f in filters:
        f["valueList"] = []

    if len(shipment_ids) == 1 and o_nhap is not None:
        # O INPUT la duong da duoc chung minh: chinh no sinh ra
        # shipment_id IN ('SPXVN...') trong cau SQL ma API tra ve.
        o_nhap["valueList"] = list(shipment_ids)
    elif o_tai_len is not None:
        # O "Click to Upload" khai bao dashFilterType = MULTI_SELECT, tuc la
        # nhan danh sach.  Day la duong di theo LO.
        o_tai_len["valueList"] = list(shipment_ids)
    elif o_nhap is not None:
        o_nhap["valueList"] = list(shipment_ids)
    else:
        raise RuntimeError("Template khong co bo loc Shipment ID nao dung duoc")

    payload["executionId"] = _execution_id(payload.get("executionId"))
    return payload


_exec_counter = [0]


def _execution_id(mau):
    """Giu dung dang executionId cua trang: <ma nhan vien>_<epoch ms>_<dem>.

    Ban dau toi dat "botdeli_<ms>" cho de doc. Neu may chu co doc gi trong chuoi
    nay thi mot dang la khac se lam no bo qua ma khong bao loi - de loai kha nang
    do ra khoi cuoc dieu tra, cu bat chuoc dung dang goc.
    """
    _exec_counter[0] += 1
    phan = str(mau or "").split("_")
    tien_to = phan[0] if phan and phan[0] else "SPXVN"
    return f"{tien_to}_{int(time.time() * 1000)}_{_exec_counter[0]}"


_FETCH_JS = r"""
var url = arguments[0];
var payload = arguments[1];
var extra = arguments[2] || {};
var callback = arguments[arguments.length - 1];
var headers = {
    'accept': 'application/json, text/plain, */*',
    'content-type': 'application/json;charset=UTF-8'
};
Object.keys(extra).forEach(function (k) { headers[k] = extra[k]; });
fetch(url, {
    method: 'POST',
    credentials: 'include',
    headers: headers,
    // Chuoi thi gui nguyen xi. JSON.stringify mot chuoi se boc them dau nhay va
    // thanh mot chuoi JSON chu khong phai doi tuong - do la nguyen nhan
    // code=400 'Wrong input data' cua lenh probe ngay 2026-09-11.
    body: (typeof payload === 'string') ? payload : JSON.stringify(payload)
}).then(async function (r) {
    var text = '';
    try { text = await r.text(); } catch (e) { text = ''; }
    callback({ok: r.ok, status: r.status, text: text});
}).catch(function (err) {
    callback({ok: false, status: 0, text: '', error: String(err)});
});
"""


def load_template():
    if not TEMPLATE_PATH.exists():
        raise RuntimeError(
            f"Chua co {TEMPLATE_PATH.name}. Chay truoc: python {Path(__file__).name} capture"
        )
    return json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))


def _post(driver, body, match_resource=None):
    """Gui mot request /report/execute voi header cua phien dang song.

    body la chuoi thi gui nguyen xi, la dict thi JSON.stringify - nho vay lenh
    `probe` goi lai duoc dung cai ma trang gui, khong sua mot ky tu nao.
    """
    phien = ensure_session(need_template=False, match_resource=match_resource,
                           verbose=False)
    result = driver.execute_async_script(
        _FETCH_JS, EXECUTE_URL, body, phien.get("headers") or {}
    )
    if not result.get("ok"):
        raise RuntimeError(
            f"HTTP {result.get('status')} tu Data Suite "
            f"(header da gui: {sorted((phien.get('headers') or {}))}): "
            f"{(result.get('error') or result.get('text') or '')[:300]}"
        )
    parsed = json.loads(result.get("text") or "{}")
    if parsed.get("code") != 200 or not parsed.get("success"):
        raise RuntimeError(
            f"Data Suite tra ve code={parsed.get('code')} message={parsed.get('message')!r}"
        )
    return parsed


def probe(need_filter=False, report_name=None):
    """Goi lai Y NGUYEN request cua trang, khong sua gi.

    Neu cai nay chay duoc ma `get` van 403 thi loi nam o cho toi sua payload.
    Neu ca cai nay cung 403 thi loi nam o header hoac phien dang nhap. Khong co
    phep thu nay thi chi ngoi doan giua hai kha nang do.
    """
    driver = get_driver()
    if need_filter:
        print("=> Go MOT ma don vao o Shipment ID roi bam Apply, doi bang hien du lieu.")
    phien = ensure_session(need_template=True, need_filter=need_filter,
                           report_name=report_name)
    body = phien["raw_body"]
    print(f"\nGoi lai nguyen xi request cua bang {phien['report_name']!r} "
          f"({len(body):,} ky tu)...")
    for lan in range(1, 5):
        parsed = _post(driver, body)
        data = parsed.get("data") or {}
        rows = data.get("rows") or []
        dem = (data.get("parameter") or {}).get("cacheCreateTime")
        print(f"  lan {lan}: {len(rows)} dong | parameter={data.get('parameter')!r}"
              f" | sql={'co' if data.get('sql') else 'khong'}")
        if rows:
            print(f"OK. Cot cogs dong dau: {rows[0].get('cogs')}")
            _print_sql(parsed)
            return rows
        if dem:
            print("  (da co bo nho dem ma van rong -> khong phai chuyen cho tinh toan)")
        time.sleep(3)
    print("Ca 4 lan deu rong.")
    print("\nSo sanh voi chinh trang, cung payload do:")
    _in_phan_hoi_trang(_session.get("page_response"))
    _print_sql(parsed)
    return []


def _print_sql(parsed):
    """In cau SQL may chu sinh ra. Day la bang chung truc tiep nhat: nhin menh de
    WHERE la biet bo loc shipment co di duoc vao trong hay khong, khoi phai doan
    giua 'gui sai' va 'gui dung nhung khong co du lieu'."""
    data = parsed.get("data") or {}
    sql = data.get("sql")
    if not sql:
        print("Khong co cau SQL trong ket qua -> may chu chua chay truy van nao.")
        return
    where = sql.find("WHERE")
    print("\n--- SQL may chu chay ---")
    print(sql[where:where + 700] if where != -1 else sql[:700])
    print(f"--- (tong {len(sql):,} ky tu) ---")


def fetch_rows(shipment_ids, template=None, tries=4, wait_seconds=3, verbose=True):
    """Goi va cho ket qua, tra ve danh sach dong tho.

    Goi LAI cung mot request neu lan dau rong. Bang chung cho viec nay: `probe`
    goi lai request luc tai trang - request KHONG co bo loc shipment nao, tuc
    quet ca bang 5 ngay - va van ra 0 dong. Mot truy van khong loc gi ma rong thi
    khong the la "het du lieu". Phan hoi chay duoc cua ops lai co
    parameter.cacheCreateTime, tuc no lay tu BO NHO DEM. Nen gia thuyet la
    /report/execute lan dau chi khoi dong truy van roi tra ve rong, lan sau moi
    nhan duoc ket qua da tinh xong.
    """
    driver = get_driver()
    _on_dashboard(driver)
    # Payload dung CHUNG cho moi lan thu, ke ca executionId. Neu may chu dat
    # khoa bo nho dem theo executionId thi doi no moi lan la khong bao gio cham
    # duoc vao ket qua da tinh.
    payload = build_payload(shipment_ids, template=template)

    ma_bc = str(((template or load_template()).get("report") or {})
                .get("resourceIdCode") or "") or None

    parsed = None
    for lan in range(1, max(1, int(tries)) + 1):
        parsed = _post(driver, payload, match_resource=ma_bc)
        data = parsed.get("data") or {}
        rows = data.get("rows") or []
        dem = ((data.get("parameter") or {}).get("cacheCreateTime")
               or (parsed.get("parameter") or {}).get("cacheCreateTime"))
        if verbose:
            print(f"    lan {lan}: {len(rows)} dong"
                  f"{f', dem luc {dem}' if dem else ', chua co bo nho dem'}")
        if rows:
            return rows, parsed
        if lan < tries:
            time.sleep(wait_seconds)
    return [], (parsed or {})


def cogs_by_shipment(shipment_ids, batch_size=DEFAULT_BATCH_SIZE, verbose=True):
    """{shipment_id: cogs}.  Lay gia tri duy nhat cua don, KHONG cong cac dong."""
    template = load_template()

    # Moi bang co tran rieng: SOC OUTBOUND ORDER L5D dat execLimit 50000, con
    # SOC ORDER DETAIL chi 2000. Vuot tran thi ket qua bi cat am tham, khong bao
    # loi, nen phai tu ha lo xuong.
    tran = ((template.get("report") or {}).get("execOptions") or {}).get("execLimit")
    if tran and batch_size > tran:
        print(f"  (bang nay chi cho {tran:,} dong mot lan; ha lo tu {batch_size} xuong {tran})")
        batch_size = int(tran)

    ket = {}
    canh_bao = []
    shipment_ids = list(dict.fromkeys(str(s).strip() for s in shipment_ids if str(s).strip()))

    for start in range(0, len(shipment_ids), batch_size):
        lo = shipment_ids[start:start + batch_size]
        rows, _ = fetch_rows(lo, template=template)
        theo_don = {}
        for row in rows:
            sid = str(row.get("shipment_id") or "").strip()
            if not sid:
                continue
            gia = row.get("cogs")
            if gia is None:
                continue
            theo_don.setdefault(sid, set()).add(float(gia))
        for sid, gia_tri in theo_don.items():
            if len(gia_tri) > 1:
                # Khong tu cong lai: neu cac chang that su khac nhau thi do la
                # chuyen nghiep vu, phai bao ra chu khong doan.
                canh_bao.append((sid, sorted(gia_tri)))
            ket[sid] = max(gia_tri)
        if verbose:
            print(f"  lo {start // batch_size + 1}: hoi {len(lo)} don, "
                  f"{len(rows)} dong, ra {len(theo_don)} don co cogs")

    if canh_bao and verbose:
        print(f"\nCANH BAO: {len(canh_bao)} don co nhieu gia tri cogs khac nhau, "
              f"dang lay gia tri lon nhat:")
        for sid, gia_tri in canh_bao[:10]:
            print(f"  {sid}: {gia_tri}")

    thieu = [s for s in shipment_ids if s not in ket]
    if thieu and verbose:
        print(f"\n{len(thieu)} don khong co dong nao tra ve (vi du: {thieu[:5]})")
    return ket


# ------------------------------------------------------- cap nhat len sheet

WORKSHEET_TITLE = "pending_work"


def _bot3():
    """Nap module BOT 3 de dung chung ket noi sheet va bang dem cogs cua no."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "bot3_for_cogs", BASE_DIR / "BOT-Deli_BDA_run_bot3_work.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def update_sheet(limit=None, refetch=False, batch_size=DEFAULT_BATCH_SIZE):
    """Dien COGS con trong tren pending_work, tu Data Suite.

    Ghi VAO CA HAI cho, va thu tu do la co y:

    1. bang dem bot3_cogs_cache - vi BOT 3 ghi cot COGS tren sheet tu bang dem
       nay (work_row_from_pending).  Chi ghi len sheet thoi thi vong sau BOT 3
       lay gia tri rong trong bang dem ra ghi de lai, coi nhu cong toi.
    2. o tren sheet - de cac ban thay ngay, khong phai doi vong BOT 3 ke tiep.

    Hai cho nhan cung mot so nen BOT 3 co ghi de cung khong doi gi.
    """
    import gspread

    B3 = _bot3()
    service = B3.sqlite_store.create_service()
    B3.ensure_cogs_table(service)
    cache = B3.load_cogs_cache(service)

    worksheet = B3.open_spreadsheet().worksheet(WORKSHEET_TITLE)
    values = worksheet.get_all_values()
    headers = [str(h).strip() for h in (values[0] if values else [])]
    for ten in ("Shipment ID", "COGS"):
        if ten not in headers:
            raise RuntimeError(f"pending_work khong co cot {ten!r}")
    cot_don = headers.index("Shipment ID")
    cot_cogs = headers.index("COGS")

    can_dien = []  # (so dong, shipment_id)
    for so_dong, hang in enumerate(
        values[B3.WORK_DATA_START_ROW - 1:], start=B3.WORK_DATA_START_ROW
    ):
        don = str(hang[cot_don]).strip() if cot_don < len(hang) else ""
        cogs = str(hang[cot_cogs]).strip() if cot_cogs < len(hang) else ""
        if don and (refetch or not cogs):
            can_dien.append((so_dong, don))

    print(f"pending_work: {len(values) - 2} dong, {len(can_dien)} dong can COGS")
    if not can_dien:
        return 0

    # Co san trong bang dem thi khoi hoi Data Suite lam gi.
    tu_dem = {}
    can_hoi = []
    for _, don in can_dien:
        gia = str((cache.get(don) or {}).get("cogs") or "").strip()
        if gia and not refetch:
            tu_dem[don] = gia
        elif don not in can_hoi:
            can_hoi.append(don)
    if limit is not None:
        can_hoi = can_hoi[:max(0, int(limit))]
    print(f"  {len(tu_dem)} don lay tu bang dem, {len(can_hoi)} don phai hoi Data Suite")

    moi = cogs_by_shipment(can_hoi, batch_size=batch_size) if can_hoi else {}

    # Lo dau tien khong ra gi thi DUNG, dung ghi gi ca.  Ngay 2026-09-11 lenh nay
    # chay 40 lo lien tiep deu rong roi ghi 8.436 dong "that bai" vao bang dem,
    # kem mot ly do tu doan ma chua he kiem chung.  Ca lo rong la dau hieu duong
    # lay du lieu hong, khong phai dau hieu tung don khong co du lieu.
    if can_hoi and not moi:
        raise RuntimeError(
            f"Hoi {len(can_hoi)} don ma khong don nao co du lieu. Duong lay dang "
            f"hong chu khong phai thieu du lieu, nen khong ghi gi vao bang dem. "
            f"Chay `get <ma don> --sql` de xem may chu dang chay truy van gi."
        )

    luc_nay = time.strftime("%Y-%m-%d %H:%M:%S")
    de_luu = []
    for don in can_hoi:
        gia = moi.get(don)
        de_luu.append({
            "shipment_id": don,
            "cogs": "" if gia is None else gia,
            "fetched_at": luc_nay,
            # Chi noi dung cai da quan sat duoc: bang khong tra ve dong nao cho
            # don nay.  KHONG doan ly do.  Ban dau toi ghi "khong co trong bang
            # L5D", mot suy doan chua kiem chung, va no bien thanh du lieu sai
            # nam trong co so du lieu.
            "last_error": "" if gia is not None else "Data Suite khong tra ve dong nao",
        })
    if de_luu:
        service.store.insert_rows(
            B3.COGS_TABLE_ID,
            de_luu,
            fields=[
                {"name": "shipment_id", "type": "STRING"},
                {"name": "cogs", "type": "NUMERIC"},
                {"name": "fetched_at", "type": "DATETIME"},
                {"name": "last_error", "type": "STRING"},
            ],
        )
        print(f"  da ghi {len(de_luu)} dong vao bang dem {B3.COGS_TABLE_ID}")

    tat_ca = dict(tu_dem)
    tat_ca.update({k: v for k, v in moi.items() if v is not None})
    updates = []
    for so_dong, don in can_dien:
        gia = tat_ca.get(don)
        if gia is None or str(gia).strip() == "":
            continue
        updates.append({
            "range": gspread.utils.rowcol_to_a1(so_dong, cot_cogs + 1),
            "values": [[gia]],
        })
    for start in range(0, len(updates), 300):
        B3.sheets_batch_update_with_retry(worksheet, updates[start:start + 300])
    print(f"  da ghi {len(updates)} o COGS len sheet")

    thieu = len(can_dien) - len(updates)
    if thieu:
        print(f"  con {thieu} dong chua co COGS (het han 5 ngay, hoac chua toi luot)")
    return len(updates)


# --------------------------------------------------------------------- CLI

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="lenh", required=True)

    p_cap = sub.add_parser("capture", help="Mo Chrome, dang nhap, bat payload that")
    p_cap.add_argument("--timeout", type=int, default=600)
    p_cap.add_argument("--with-filter", action="store_true",
                       help="Chi nhan payload bat duoc luc bam Apply co ma don")
    p_cap.add_argument("--report", default=None,
                       help="Chi nhan bang co ten chua chuoi nay, vi du L30D")

    p_probe = sub.add_parser("probe", help="Goi lai y nguyen request cua trang, de tach loi")
    p_probe.add_argument("--with-filter", action="store_true",
                         help="Goi lai payload luc bam Apply thay vi luc tai trang")
    p_probe.add_argument("--report", default=None,
                         help="Chi nhan bang co ten chua chuoi nay, vi du L30D")

    p_sheet = sub.add_parser("sheet", help="Dien COGS con trong tren pending_work")
    p_sheet.add_argument("--limit", type=int, default=None,
                         help="So don toi da hoi Data Suite trong lan chay nay")
    p_sheet.add_argument("--refetch", action="store_true",
                         help="Hoi lai ca nhung don da co COGS")
    p_sheet.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)

    p_get = sub.add_parser("get", help="Lay cogs cho mot hay nhieu shipment")
    p_get.add_argument("shipment_ids", nargs="+")
    p_get.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_get.add_argument("--raw", action="store_true", help="In ca dong tho")
    p_get.add_argument("--sql", action="store_true", help="In cau SQL may chu chay")

    args = parser.parse_args()

    if args.lenh == "capture":
        capture_template(timeout=args.timeout, need_filter=args.with_filter,
                         report_name=args.report)
        print("\nThu ngay:")
        print(f"  python {Path(__file__).name} get SPXVN062416873319")
        return 0

    if args.lenh == "probe":
        probe(need_filter=args.with_filter, report_name=args.report)
        return 0

    if args.lenh == "sheet":
        update_sheet(
            limit=args.limit,
            refetch=args.refetch,
            batch_size=args.batch_size,
        )
        return 0

    if args.raw or args.sql:
        rows, parsed = fetch_rows(args.shipment_ids)
        if args.raw:
            print(json.dumps(rows, ensure_ascii=False, indent=1)[:8000])
        print(f"\n{len(rows)} dong")
        _print_sql(parsed)
        return 0

    ket = cogs_by_shipment(args.shipment_ids, batch_size=args.batch_size)
    print()
    for sid in args.shipment_ids:
        gia = ket.get(sid)
        print(f"  {sid:<24}{'' if gia is None else format(gia, ',.0f')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

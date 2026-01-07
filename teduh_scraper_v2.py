import os
import re
import time
import csv
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
    InvalidSessionIdException,
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager


# =========================================================
# CONFIG (EDIT THESE VALUES ONLY)
# =========================================================
CONFIG = {
    "HEADLESS": False,   # True = hidden browser, False = visible
    "JENIS_CARIAN": "Pemaju",
    "NEGERI": "Melaka",
    "BASE_URL": "https://teduh.kpkt.gov.my/semakan-status-kemajuan",

    # Timing
    "DELAY_CLICK": 1.5,
    "DELAY_PAGE_LOAD": 3.5,
    "MAX_WAIT_SECONDS": 30,

    # Input list
    "PEMAJU_LIST_TXT": "pemaju_list.txt",

    # Output Root
    "ROOT_DIR": "KPKT_SCRAPED_DATA",
}

NOW = datetime.now()
SCRAPE_DATE = NOW.strftime("%Y-%m-%d")
SCRAPE_TIMESTAMP = NOW.strftime("%Y-%m-%d %H:%M:%S")
DATE_SUFFIX = NOW.strftime("%Y%m%d")
TIME_SUFFIX = NOW.strftime("%Y%m%d_%H%M%S")


# =========================================================
# OUTPUT SCHEMAS (LOCKED ORDER)
# =========================================================
# ✅ Master CSV DOES NOT include the house-type table columns
PROJECT_MASTER_HEADERS = [
    "Bil",
    "Kod Projek & Nama Projek",
    "Kod Pemaju & Nama Pemaju",
    "No. Permit",
    "Status Projek Keseluruhan",
    "Maklumat Pembangunan",
    "Lokasi Projek",
    "Daerah Projek",
    "Negeri Projek",
    "Tarikh Sah Laku Permit Terkini",
    "Scraped_Date",
    "Scraped_Timestamp",
]

# ✅ House type CSV: follow the schema you wanted
HOUSE_TYPE_HEADERS = [
    "Kod Projek",
    "Nama Projek",
    "Jenis Rumah",
    "Bil Tingkat",
    "Bil Bilik",
    "Bil Tandas",
    "Keluasan Binaan (Mps)",
    "Bil.Unit",
    "Harga Minimum (RM)",
    "Harga Maksimum (RM)",
    "Peratus Sebenar %",
    "Status Komponen",
    "Tarikh CCC/CFO",
    "Tarikh VP",
    "Scraped_Date",
    "Scraped_Timestamp",
]

UNIT_DETAILS_HEADERS = [
    "Bil",
    "Kod Projek & Nama Projek",
    "Kod Pemaju & Nama Pemaju",
    "No. Permit",

    "No PT/Lot/Plot",
    "No Unit",
    "Harga Jualan (RM)",
    "Harga SPJB (RM)",
    "Status Jualan",
    "Kuota Bumi",

    "Scraped_Date",
    "Scraped_Timestamp",
]


# =========================================================
# SMALL HELPERS
# =========================================================
def ok(msg): logging.info(f"✅ {msg}")
def fail(msg): logging.error(f"❌ {msg}")
def info(msg): logging.info(f"ℹ️ {msg}")

def sanitize_filename(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[<>:\"/\\|?*]+", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:80] if s else "UNKNOWN"

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def safe_click(driver, element):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(CONFIG["DELAY_CLICK"] / 2)
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)
    time.sleep(CONFIG["DELAY_CLICK"])

def wait_clickable(driver, locator, timeout=None):
    timeout = timeout or CONFIG["MAX_WAIT_SECONDS"]
    return WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))

def wait_visible(driver, locator, timeout=None):
    timeout = timeout or CONFIG["MAX_WAIT_SECONDS"]
    return WebDriverWait(driver, timeout).until(EC.visibility_of_element_located(locator))


# =========================================================
# DRIVER
# =========================================================
def init_driver():
    chrome_options = Options()

    if CONFIG.get("HEADLESS", False):
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")

    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(60)

    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
    )

    ok(f"Chrome started (headless={CONFIG.get('HEADLESS')})")
    return driver


# =========================================================
# LOGGING PER PEMAJU
# =========================================================
def setup_logging_for_pemaju(log_dir: str, pemaju_key: str):
    ensure_dir(log_dir)
    log_file = os.path.join(log_dir, f"KPKT_SCRAPE_{pemaju_key}_{TIME_SUFFIX}.log")

    for h in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(h)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )
    ok(f"Log file: {log_file}")
    return log_file


# =========================================================
# FORM ACTIONS (UPDATED ROBUST VERSION)
# =========================================================
def perform_search(driver, keyword: str):
    driver.get(CONFIG["BASE_URL"])
    time.sleep(CONFIG["DELAY_PAGE_LOAD"])
    ok(f"Opened {CONFIG['BASE_URL']}")

    # Jenis Carian
    jenis_dropdown = wait_clickable(driver, (By.XPATH, "//label[contains(normalize-space(.),'Jenis Carian')]/following::select[1]"))
    safe_click(driver, jenis_dropdown)
    jenis_opt = wait_clickable(driver, (By.XPATH, f"//label[contains(normalize-space(.),'Jenis Carian')]/following::select[1]/option[normalize-space(.)='{CONFIG['JENIS_CARIAN']}']"))
    safe_click(driver, jenis_opt)
    ok(f"Jenis Carian = {CONFIG['JENIS_CARIAN']}")

    # Negeri
    negeri_dropdown = wait_clickable(driver, (By.XPATH, "//label[contains(normalize-space(.),'Negeri')]/following::select[1]"))
    safe_click(driver, negeri_dropdown)
    negeri_opt = wait_clickable(driver, (By.XPATH, f"//label[contains(normalize-space(.),'Negeri')]/following::select[1]/option[normalize-space(.)='{CONFIG['NEGERI']}']"))
    safe_click(driver, negeri_opt)
    ok(f"Negeri = {CONFIG['NEGERI']}")

    # Kata Kunci
    inp = wait_clickable(driver, (By.XPATH, "//input[@placeholder='Kata Kunci' or @type='text']"))
    inp.clear()
    inp.send_keys(keyword)
    time.sleep(CONFIG["DELAY_CLICK"])
    ok(f"Kata Kunci = {keyword}")

    # Cari
    btn = wait_clickable(driver, (By.XPATH, "//button[contains(@class,'btn-search') or contains(.,'Cari') or contains(.,'CARI')]"))
    safe_click(driver, btn)
    ok("Clicked CARI - Waiting for Robust Verification...")

    # --- START ROBUST WAIT LOGIC ---
    # We loop up to 10 times to check if the first row actually contains our keyword.
    max_retries = 10
    found_match = False

    for attempt in range(max_retries):
        time.sleep(2.0) # Wait for page refresh

        try:
            # 1. Grab the table
            table = driver.find_element(By.XPATH, "//table[.//tbody//tr]")
            
            # 2. Grab the first row
            first_row = table.find_element(By.XPATH, ".//tbody//tr[1]")
            
            # 3. Check text match (Case Insensitive)
            row_text = first_row.text.upper().strip()
            target_text = keyword.upper().strip()

            if target_text in row_text:
                ok(f"✅ VERIFIED: Search result matches '{keyword}'")
                found_match = True
                break
            else:
                # If we see a row but it's not our developer, it might be the OLD result
                info(f"⏳ Attempt {attempt+1}: Scraper saw '{row_text[:30]}...' (Waiting for '{keyword}')")
        
        except Exception:
            # Table might be loading, detached, or empty
            info(f"⏳ Attempt {attempt+1}: Waiting for results table...")

    if not found_match:
        fail(f"⚠️ WARNING: Time out waiting for '{keyword}'. The scraper will try to process whatever is there.")
    
    # Ensure table is visible before returning
    wait_visible(driver, (By.XPATH, "//table[.//tbody//tr]"))


# =========================================================
# LISTING TABLE + PAGINATION
# =========================================================
def get_listing_rows(driver):
    table = wait_visible(driver, (By.XPATH, "//table[.//tbody//tr]"))
    return table.find_elements(By.XPATH, ".//tbody//tr")

def get_next_page_button(driver):
    xps = [
        "//button[contains(@class,'page-btn')][.//i[contains(@class,'pi-chevron-right')]]",
        "//button[.//i[contains(@class,'pi-chevron-right')]]",
    ]
    for xp in xps:
        try:
            return driver.find_element(By.XPATH, xp)
        except Exception:
            continue
    return None

def has_next_page(driver):
    btn = get_next_page_button(driver)
    if not btn:
        return False
    disabled_attr = (btn.get_attribute("disabled") or "").strip()
    class_attr = (btn.get_attribute("class") or "").lower()
    return not (disabled_attr or ("disabled" in class_attr))

def click_next_page(driver):
    btn = get_next_page_button(driver)
    if not btn:
        raise Exception("Next page button not found")
    safe_click(driver, btn)
    time.sleep(CONFIG["DELAY_PAGE_LOAD"])
    ok("Next page clicked")


# =========================================================
# DETAIL OPEN / CLOSE
# =========================================================
def open_project_detail_from_row(driver, row):
    eye = row.find_element(By.XPATH, ".//i[contains(@class,'pi-eye') or contains(@class,'tindakan-eye')]")
    safe_click(driver, eye)
    time.sleep(CONFIG["DELAY_PAGE_LOAD"])
    ok("Opened project detail")

def close_project_detail(driver):
    try:
        driver.execute_script("document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'}));")
        time.sleep(CONFIG["DELAY_CLICK"])
    except Exception:
        pass

    wait_visible(driver, (By.XPATH, "//table[.//tbody//tr]"))
    ok("Returned to listing")


# =========================================================
# FIELD EXTRACTION
# =========================================================
def click_side_tab(driver, tab_text_lower: str):
    xp = f"//span[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{tab_text_lower}')]/ancestor::*[self::button or self::a][1]"
    tab = wait_clickable(driver, (By.XPATH, xp), timeout=12)
    safe_click(driver, tab)
    time.sleep(CONFIG["DELAY_CLICK"])
    ok(f"Tab opened: {tab_text_lower}")

def scrape_info_text_value(driver, label: str, timeout=15) -> str:
    xp = f"//h4[normalize-space(.)='{label}']/parent::div/following-sibling::div[1]"
    def _non_empty(d):
        els = d.find_elements(By.XPATH, xp)
        for el in els:
            val = (el.get_attribute("textContent") or "").strip()
            if val:
                return val
        return False
    return WebDriverWait(driver, timeout).until(_non_empty)

def extract_google_map_link(driver) -> str:
    try:
        iframe = driver.find_element(By.XPATH, "//iframe[contains(@src,'google.com/maps') or contains(@src,'maps.google.com/maps')]")
        src = iframe.get_attribute("src") or ""
        if not src:
            return ""
        parsed = urlparse(src)
        qs = parse_qs(parsed.query)
        q = (qs.get("q", [""])[0] or "").strip()
        if q and "," in q:
            return f"https://maps.google.com/maps?q={q}"
        return src
    except Exception:
        return ""

def extract_status_header_fields(driver):
    """
    Correct way for D. Status Terkini Projek:
    Read the Status Terkini section text, then regex extract:
      Maklumat Pembangunan : Berfasa
      Status Keseluruhan : Lancar
    """
    out = {"Maklumat Pembangunan": "", "Status Projek Keseluruhan": ""}

    # 1) Locate the "Status Terkini Projek" section/container
    scope = None
    xps = [
        # card/container that contains the title
        "//*[contains(normalize-space(.),'Status Terkini Projek')]/ancestor::div[contains(@class,'card')][1]",
        # fallback: nearest big container
        "//*[contains(normalize-space(.),'Status Terkini Projek')]/ancestor::div[1]",
    ]
    for xp in xps:
        try:
            scope = driver.find_element(By.XPATH, xp)
            if scope:
                break
        except Exception:
            continue

    if not scope:
        info("Status Terkini container not found")
        return out

    raw = normalize_space(scope.get_attribute("textContent") or "")

    # 2) Regex extract ONLY the value after ":"
    # Stop capture before the next known label (or end)
    import re as _re

    m1 = _re.search(r"Maklumat\s*Pembangunan\s*:\s*(.+?)(?:Status\s*Keseluruhan\s*:|$)", raw, _re.IGNORECASE)
    if m1:
        val = normalize_space(m1.group(1))
        # safety: cut off if it still contains table headers
        val = val.split("Jenis Rumah")[0].strip()
        out["Maklumat Pembangunan"] = val
        ok(f"Maklumat Pembangunan = {val}")
    else:
        info("Maklumat Pembangunan not found (regex)")

    m2 = _re.search(r"Status\s*Keseluruhan\s*:\s*(.+?)(?:Status\s*komponen|Jenis\s*Rumah|$)", raw, _re.IGNORECASE)
    if m2:
        val = normalize_space(m2.group(1))
        val = val.split("Jenis Rumah")[0].strip()
        out["Status Projek Keseluruhan"] = val
        ok(f"Status Keseluruhan = {val}")
    else:
        info("Status Keseluruhan not found (regex)")

    return out



def extract_status_table_rows(driver):
    rows_out = []
    try:
        table = wait_visible(driver, (By.XPATH, "//table[contains(@class,'table-status')]"), timeout=15)
    except Exception:
        table = wait_visible(driver, (By.XPATH, "//div[contains(@class,'status-table-wrap')]//table"), timeout=15)

    trs = table.find_elements(By.XPATH, ".//tbody//tr")
    for tr in trs:
        tds = tr.find_elements(By.XPATH, ".//td")
        if len(tds) < 12:
            continue

        rows_out.append({
            "Jenis Rumah": normalize_space(tds[0].text),
            "Bil Tingkat": normalize_space(tds[1].text),
            "Bil Bilik": normalize_space(tds[2].text),
            "Bil Tandas": normalize_space(tds[3].text),
            "Keluasan Binaan (Mps)": normalize_space(tds[4].text),
            "Bil.Unit": normalize_space(tds[5].text),
            "Harga Minimum (RM)": normalize_space(tds[6].text),
            "Harga Maksimum (RM)": normalize_space(tds[7].text),
            "Peratus Sebenar %": normalize_space(tds[8].text),
            "Status Komponen": normalize_space(tds[9].text),
            "Tarikh CCC/CFO": normalize_space(tds[10].text),
            "Tarikh VP": normalize_space(tds[11].text),
        })

    return rows_out


# =========================================================
# UNIT MODAL
# =========================================================
def open_unit_modal(driver):
    btn = wait_clickable(driver, (By.XPATH, "//button[contains(.,'Lihat Terperinci Unit') or contains(.,'LIHAT TERPERINCI UNIT')]"), timeout=12)
    safe_click(driver, btn)
    time.sleep(CONFIG["DELAY_PAGE_LOAD"])
    ok("Unit modal opened")

def ensure_paparan_senarai(driver):
    try:
        active = driver.find_elements(By.XPATH, "//button[contains(@class,'view-btn') and contains(@class,'active') and @title='Paparan Senarai']")
        if active:
            ok("Paparan Senarai already active")
            return True

        btn = wait_clickable(driver, (By.XPATH, "//button[contains(@class,'view-btn') and @title='Paparan Senarai']"), timeout=10)
        safe_click(driver, btn)
        time.sleep(CONFIG["DELAY_CLICK"])
        ok("Clicked Paparan Senarai")
        return True
    except Exception as e:
        fail(f"Paparan Senarai click failed: {e}")
        return False

def scrape_unit_table(driver):
    rows_out = []
    table = wait_visible(driver, (By.XPATH, "//table[contains(@class,'unit-list-table')]"), timeout=15)

    trs = table.find_elements(By.XPATH, ".//tbody//tr")
    for tr in trs:
        tds = tr.find_elements(By.XPATH, ".//td")
        if len(tds) < 7:
            continue
        rows_out.append({
            "Bil": normalize_space(tds[0].get_attribute("textContent") or ""),
            "No PT/Lot/Plot": normalize_space(tds[1].get_attribute("textContent") or ""),
            "No Unit": normalize_space(tds[2].get_attribute("textContent") or ""),
            "Harga Jualan (RM)": normalize_space(tds[3].get_attribute("textContent") or ""),
            "Harga SPJB (RM)": normalize_space(tds[4].get_attribute("textContent") or ""),
            "Status Jualan": normalize_space(tds[5].get_attribute("textContent") or ""),
            "Kuota Bumi": normalize_space(tds[6].get_attribute("textContent") or ""),
        })
    ok(f"Unit rows scraped = {len(rows_out)}")
    return rows_out

def close_unit_modal(driver):
    try:
        btn = driver.find_element(By.XPATH, "//button[contains(.,'TUTUP') or contains(.,'Tutup')]")
        safe_click(driver, btn)
        ok("Unit modal closed (TUTUP)")
    except Exception:
        info("Unit modal close not found")


# =========================================================
# CSV WRITERS
# =========================================================
def write_csv(path, headers, rows):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})
    ok(f"Saved CSV: {path}")


# =========================================================
# MAIN SCRAPE PER PEMAJU
# =========================================================
def split_kod_nama(kod_proj_nama: str):
    s = normalize_space(kod_proj_nama)
    if not s:
        return "", ""
    parts = s.split()
    kod = parts[0]
    nama = " ".join(parts[1:]) if len(parts) > 1 else ""
    return kod, nama

def scrape_one_pemaju(pemaju_name: str):
    pemaju_key = sanitize_filename(pemaju_name)
    root = CONFIG["ROOT_DIR"]
    data_dir = os.path.join(root, "data", "pemaju", pemaju_key)
    log_dir = os.path.join(root, "logs")

    log_file = setup_logging_for_pemaju(log_dir, pemaju_key)
    ok(f"Start Pemaju: {pemaju_name}")
    ok(f"Output folder: {data_dir}")

    project_master_csv = os.path.join(data_dir, f"{pemaju_key}_MELAKA_ALL_PROJECTS_{DATE_SUFFIX}.csv")
    house_type_csv = os.path.join(data_dir, f"{pemaju_key}_MELAKA_HOUSE_TYPE_{DATE_SUFFIX}.csv")
    unit_details_csv = os.path.join(data_dir, f"{pemaju_key}_MELAKA_UNIT_DETAILS_{DATE_SUFFIX}.csv")

    driver = None
    project_master_rows = []
    house_type_rows = []
    unit_detail_rows = []

    try:
        driver = init_driver()
        perform_search(driver, pemaju_name)

        bil_project = 1
        bil_unit_global = 1

        page_num = 1
        while True:
            ok(f"Processing listing page {page_num}")
            rows = get_listing_rows(driver)
            if not rows:
                info("No listing rows found. Stop.")
                break

            for idx in range(len(rows)):
                rows = get_listing_rows(driver)  # refresh avoid stale
                if idx >= len(rows):
                    continue

                row = rows[idx]
                cells = row.find_elements(By.XPATH, ".//td")

                kod_proj_nama = normalize_space(cells[1].get_attribute("textContent") or "") if len(cells) > 1 else ""
                kod_pemaju_nama = normalize_space(cells[2].get_attribute("textContent") or "") if len(cells) > 2 else ""
                no_permit = normalize_space(cells[3].get_attribute("textContent") or "") if len(cells) > 3 else ""
                status_list = normalize_space(cells[4].get_attribute("textContent") or "") if len(cells) > 4 else ""

                ok(f"[{bil_project}] Open: {kod_proj_nama}")

                # open detail
                try:
                    open_project_detail_from_row(driver, row)
                except Exception as e:
                    fail(f"Open detail failed: {e}")
                    continue

                # --- B. Maklumat Projek ---
                daerah = ""
                negeri = CONFIG["NEGERI"]
                permit_valid = ""
                lokasi_link = ""

                try:
                    click_side_tab(driver, "maklumat projek")
                    try:
                        daerah = scrape_info_text_value(driver, "Daerah Projek")
                        ok(f"Daerah Projek = {repr(daerah)}")
                    except Exception:
                        fail("Daerah Projek extract failed")

                    try:
                        negeri = scrape_info_text_value(driver, "Negeri Projek") or negeri
                    except Exception:
                        pass

                    try:
                        permit_valid = scrape_info_text_value(driver, "Tarikh Sah Laku Permit Terkini")
                    except Exception:
                        permit_valid = ""

                    lokasi_link = extract_google_map_link(driver) or ""
                    ok("Maklumat Projek scraped")
                except Exception as e:
                    fail(f"Maklumat Projek step failed: {e}")

                # --- D. Status Terkini Projek ---
                maklumat_pembangunan = ""
                status_overall = status_list
                status_rows = []
                try:
                    click_side_tab(driver, "status terkini projek")
                    hdr = extract_status_header_fields(driver)
                    maklumat_pembangunan = hdr.get("Maklumat Pembangunan", "") or ""
                    status_overall = hdr.get("Status Projek Keseluruhan", "") or status_overall
                    status_rows = extract_status_table_rows(driver)
                    ok(f"Status table rows = {len(status_rows)}")
                except Exception as e:
                    fail(f"Status Terkini extract failed: {e}")

                # PROJECT_MASTER row (no table columns inside)
                pm = {h: "" for h in PROJECT_MASTER_HEADERS}
                pm.update({
                    "Bil": str(bil_project),
                    "Kod Projek & Nama Projek": kod_proj_nama,
                    "Kod Pemaju & Nama Pemaju": kod_pemaju_nama,
                    "No. Permit": no_permit,
                    "Status Projek Keseluruhan": status_overall,
                    "Maklumat Pembangunan": maklumat_pembangunan,
                    "Lokasi Projek": lokasi_link,
                    "Daerah Projek": daerah,
                    "Negeri Projek": negeri,
                    "Tarikh Sah Laku Permit Terkini": permit_valid,
                    "Scraped_Date": SCRAPE_DATE,
                    "Scraped_Timestamp": SCRAPE_TIMESTAMP,
                })
                project_master_rows.append(pm)

                # HOUSE TYPE rows (all status rows)
                kod_projek, nama_projek = split_kod_nama(kod_proj_nama)
                for srow in status_rows:
                    ht = {h: "" for h in HOUSE_TYPE_HEADERS}
                    ht.update({
                        "Kod Projek": kod_projek,
                        "Nama Projek": nama_projek,
                        "Jenis Rumah": srow.get("Jenis Rumah", ""),
                        "Bil Tingkat": srow.get("Bil Tingkat", ""),
                        "Bil Bilik": srow.get("Bil Bilik", ""),
                        "Bil Tandas": srow.get("Bil Tandas", ""),
                        "Keluasan Binaan (Mps)": srow.get("Keluasan Binaan (Mps)", ""),
                        "Bil.Unit": srow.get("Bil.Unit", ""),
                        "Harga Minimum (RM)": srow.get("Harga Minimum (RM)", ""),
                        "Harga Maksimum (RM)": srow.get("Harga Maksimum (RM)", ""),
                        "Peratus Sebenar %": srow.get("Peratus Sebenar %", ""),
                        "Status Komponen": srow.get("Status Komponen", ""),
                        "Tarikh CCC/CFO": srow.get("Tarikh CCC/CFO", ""),
                        "Tarikh VP": srow.get("Tarikh VP", ""),
                        "Scraped_Date": SCRAPE_DATE,
                        "Scraped_Timestamp": SCRAPE_TIMESTAMP,
                    })
                    house_type_rows.append(ht)

                # --- UNIT DETAILS ---
                try:
                    click_side_tab(driver, "maklumat projek")
                    open_unit_modal(driver)
                    ensure_paparan_senarai(driver)
                    urows = scrape_unit_table(driver)

                    for ur in urows:
                        out = {h: "" for h in UNIT_DETAILS_HEADERS}
                        out.update({
                            "Bil": str(bil_unit_global),
                            "Kod Projek & Nama Projek": kod_proj_nama,
                            "Kod Pemaju & Nama Pemaju": kod_pemaju_nama,
                            "No. Permit": no_permit,

                            "No PT/Lot/Plot": ur.get("No PT/Lot/Plot", ""),
                            "No Unit": ur.get("No Unit", ""),
                            "Harga Jualan (RM)": ur.get("Harga Jualan (RM)", ""),
                            "Harga SPJB (RM)": ur.get("Harga SPJB (RM)", ""),
                            "Status Jualan": ur.get("Status Jualan", ""),
                            "Kuota Bumi": ur.get("Kuota Bumi", ""),

                            "Scraped_Date": SCRAPE_DATE,
                            "Scraped_Timestamp": SCRAPE_TIMESTAMP,
                        })
                        unit_detail_rows.append(out)
                        bil_unit_global += 1

                    close_unit_modal(driver)
                    ok("Unit details scraped")
                except Exception as e:
                    fail(f"Unit detail step failed: {e}")
                    try:
                        close_unit_modal(driver)
                    except Exception:
                        pass

                close_project_detail(driver)
                bil_project += 1

            # pagination
            if has_next_page(driver):
                click_next_page(driver)
                page_num += 1
            else:
                ok("No next page. Pagination done.")
                break

        # WRITE OUTPUTS
        write_csv(project_master_csv, PROJECT_MASTER_HEADERS, project_master_rows)
        write_csv(house_type_csv, HOUSE_TYPE_HEADERS, house_type_rows)
        write_csv(unit_details_csv, UNIT_DETAILS_HEADERS, unit_detail_rows)

        ok(f"SUMMARY: Projects={len(project_master_rows)}, HouseTypes={len(house_type_rows)}, UnitRows={len(unit_detail_rows)}")
        ok("DONE pemaju scrape")

    except Exception as e:
        fail(f"Fatal pemaju scrape error: {e}")
        logging.exception(e)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            ok("Chrome driver closed")

    return {
        "pemaju": pemaju_name,
        "project_master_csv": project_master_csv,
        "house_type_csv": house_type_csv,
        "unit_details_csv": unit_details_csv,
        "log_file": log_file,
    }


# =========================================================
# READ PEMAJU LIST & RUN
# =========================================================
def read_pemaju_list(txt_path: str):
    if not os.path.exists(txt_path):
        raise FileNotFoundError(f"Pemaju list file not found: {txt_path}")
    names = []
    with open(txt_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            names.append(s)
    return names

def main():
    pemaju_list = read_pemaju_list(CONFIG["PEMAJU_LIST_TXT"])
    print(f"Pemaju to scrape: {len(pemaju_list)}")
    results = []

    ensure_dir(os.path.join(CONFIG["ROOT_DIR"], "data"))
    ensure_dir(os.path.join(CONFIG["ROOT_DIR"], "logs"))

    for i, pemaju in enumerate(pemaju_list, 1):
        print(f"\n=== ({i}/{len(pemaju_list)}) SCRAPING: {pemaju} ===")
        res = scrape_one_pemaju(pemaju)
        results.append(res)

    print("\nALL DONE.")
    for r in results:
        print(f"- {r['pemaju']}")
        print(f"  Master: {r['project_master_csv']}")
        print(f"  House : {r['house_type_csv']}")
        print(f"  Units : {r['unit_details_csv']}")
        print(f"  Log   : {r['log_file']}")
        print()

if __name__ == "__main__":
    main()

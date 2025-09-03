import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import time
import datetime
import re
import urllib.parse
import os
import json
import random
from flask import Flask

app = Flask(__name__)

# -------------------- Safe Helpers --------------------
def fetch_url(url, headers, retries=3, timeout=10):
    """Robust fetch with retries + backoff and immediate logs."""
    for attempt in range(retries):
        try:
            print(f"🌐 Fetching: {url} (attempt {attempt+1})", flush=True)
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            print(f"   ✅ Success on attempt {attempt+1}", flush=True)
            return response
        except Exception as e:
            print(f"   ⚠️ Error fetching {url}: {e} (attempt {attempt+1}/{retries})", flush=True)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # backoff: 1s, 2s, ...
            else:
                print(f"   ❌ Giving up on {url}", flush=True)
                return None

def safe_update(ws, values, rng):
    """Retry wrapper for Google Sheet update with logs."""
    for attempt in range(3):
        try:
            ws.update(values=values, range_name=rng, raw=False)
            return True
        except Exception as e:
            print(f"⚠️ Sheet update failed (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2)
    print(f"❌ Failed to update range {rng} after retries", flush=True)
    return False


@app.route("/run-script")
def run_script():
    # -------------------- Google Sheet Setup --------------------
    SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_CREDS_JSON')
    if not SERVICE_ACCOUNT_INFO:
        raise ValueError("GOOGLE_CREDS_JSON environment variable not found!")

    service_account_info = json.loads(SERVICE_ACCOUNT_INFO)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.authorize(credentials)

    SPREADSHEET_NAME = 'ABCDEFGD'
    sh = gc.open(SPREADSHEET_NAME)
    try:
        master_sheet = sh.worksheet("Master")
    except gspread.exceptions.WorksheetNotFound:
        master_sheet = sh.add_worksheet(title="Master", rows="2000", cols="10")
        safe_update(master_sheet, [['Date', 'Player', 'Age', 'From', 'To', 'Fee']], 'A1')

    worksheet = sh.sheet1
    start_date_raw = worksheet.acell('H1').value
    end_date_raw = worksheet.acell('I1').value
    try:
        start_date_obj = datetime.datetime.strptime(start_date_raw, '%m/%d/%Y')
        end_date_obj = datetime.datetime.strptime(end_date_raw, '%m/%d/%Y')
    except ValueError:
        raise ValueError("❌ Invalid date format in H1 or I1. Use MM/DD/YYYY.")

    if start_date_obj > end_date_obj:
        start_date_obj, end_date_obj = end_date_obj, start_date_obj

    # -------------------- Transfermarkt Setup --------------------
    SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
    if not SCRAPERAPI_KEY:
        raise ValueError("SCRAPERAPI_KEY environment variable not found!")

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/139.0.0.0 Safari/537.36"
    }

    # -------------------- Step 1: Fetch transfer dates --------------------
    print("Fetching transfer dates...", flush=True)
    base_url = (
        f"https://www.transfermarkt.com/statistik/transfertage?"
        f"land_id_zu=0&land_id_ab=0&"
        f"datum_von={start_date_obj.strftime('%Y-%m-%d')}&"
        f"datum_bis={end_date_obj.strftime('%Y-%m-%d')}&leihe="
    )
    proxy_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(base_url)}"
    response = fetch_url(proxy_url, HEADERS)
    if not response:
        return "❌ Failed to fetch transfer dates", 500
    soup = BeautifulSoup(response.text, 'html.parser')

    dates_list = []
    rows = soup.select("table.items tbody tr")
    for row in rows:
        first_td = row.find("td")
        if first_td:
            link = first_td.find("a")
            if link:
                date_text = link.text.strip()
                if re.match(r'\d{2}\.\d{2}\.\d{4}$', date_text):
                    day, month, year = [x.strip() for x in date_text.split(".")]
                    date_obj = datetime.date(int(year), int(month), int(day))
                    if start_date_obj.date() <= date_obj <= end_date_obj.date():
                        date_url = (
                            "https://www.transfermarkt.com/transfers/"
                            f"transfertagedetail/statistik/top/land_id_zu/0/"
                            f"land_id_ab/0/leihe//datum/{year}-{month}-{day}"
                        )
                        dates_list.append([date_text, date_url])

    if not dates_list:
        raise ValueError("❌ No transfers available for the provided date range.")
    print(f"Found {len(dates_list)} valid transfer dates.", flush=True)

    # -------------------- Step 2: Scrape transfers with full pagination --------------------
    PAGE_SIZE_HINT = 25  # used only as a last-resort hint
    all_rows = []

    def extract_page_num(url: str) -> int:
        m = re.search(r'/(?:page|seite)/(\d+)', url)
        return int(m.group(1)) if m else 1

    def build_next_from_current(url: str, next_num: int) -> str:
        # Replace existing /page|seite/N or append /page/N just before query/fragment
        if re.search(r'/(?:page|seite)/\d+', url):
            return re.sub(r'/(?:page|seite)/\d+', f'/page/{next_num}', url)
        parts = urllib.parse.urlsplit(url)
        path = parts.path.rstrip('/') + f'/page/{next_num}'
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))

    for date_text, date_url in dates_list:
        print(f"\n📅 Scraping transfers for {date_text}...", flush=True)

        visited = set()
        current_url = date_url
        page_num = 0

        while current_url and current_url not in visited:
            visited.add(current_url)
            page_num += 1

            proxy_page_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(current_url)}"
            response = fetch_url(proxy_page_url, HEADERS)
            if not response:
                print(f"❌ Skipping page due to fetch failure: {current_url}", flush=True)
                break
            soup = BeautifulSoup(response.text, 'html.parser')

            # safer selector (ignore odd/even class dependency)
            transfer_rows = soup.select("table.items tbody tr.odd, table.items tbody tr.even")
            if not transfer_rows:
                print(f"⚠️ No transfers found on page {page_num}, stopping pagination for this date.", flush=True)
                break

            print(f" ✅ Page {page_num} scraped ({len(transfer_rows)} transfers)", flush=True)

            for row in transfer_rows:
                cols = row.find_all("td")
                keep_indices = [0, 1, 5, 8, 12, 14]  # keep your existing mapping
                data = []
                for idx, col in enumerate(cols, start=1):
                    try:
                        if idx in keep_indices:
                            text_value = col.get_text(strip=True)
                            a_tag = col.select_one("a")
                            if a_tag and a_tag.get("href"):
                                full_url = "https://www.transfermarkt.com" + a_tag["href"]
                                text_value = f'=HYPERLINK("{full_url}", "{a_tag.text.strip()}")'
                            data.append(text_value)
                    except Exception as e:
                        print(f"⚠️ Error parsing cell idx {idx} on page {page_num}: {e}", flush=True)
                if data:
                    data.insert(0, date_text)
                    all_rows.append(data)

            # ---- Determine the next page URL (several strategies) ----
            next_url = None

            # 1) rel="next" (anchor or <link>)
            rel_next = soup.select_one('a[rel="next"], link[rel="next"]')
            if rel_next and rel_next.get('href'):
                next_url = urllib.parse.urljoin('https://www.transfermarkt.com', rel_next['href'])

            # 2) right-arrow/icon classes
            if not next_url:
                right = soup.select_one('ul.tm-pagination a[class*="icon-right"], ul.tm-pagination a[class*="right"], ul.tm-pagination a.tm-pagination__link--icon-right')
                if right and right.get('href'):
                    next_url = urllib.parse.urljoin('https://www.transfermarkt.com', right['href'])

            # 3) numeric links: pick the (current + 1)
            if not next_url:
                current_n = extract_page_num(current_url)
                nums = []
                for a in soup.select('ul.tm-pagination a[href]'):
                    href = a.get('href', '')
                    m = re.search(r'/(?:page|seite)/(\d+)', href)
                    if m:
                        nums.append((int(m.group(1)), href))
                if nums:
                    # exact next candidate
                    for n, href in nums:
                        if n == current_n + 1:
                            next_url = urllib.parse.urljoin('https://www.transfermarkt.com', href)
                            break
                    # if we saw a higher page exists but not the exact next link, synthesize it
                    if not next_url:
                        max_n = max(n for n, _ in nums)
                        if max_n > current_n:
                            next_url = build_next_from_current(current_url, current_n + 1)

            # 4) last resort: if page looks "full", assume there's a next page and synthesize it
            if not next_url and len(transfer_rows) >= PAGE_SIZE_HINT:
                current_n = extract_page_num(current_url)
                next_url = build_next_from_current(current_url, current_n + 1)

            # Stop if no next or loop detected
            if next_url and next_url not in visited:
                print(f"   → Next page: {next_url}", flush=True)
                current_url = next_url
            else:
                current_url = None

            # polite randomized delay
            time.sleep(random.uniform(1, 3))

    # -------------------- Step 3: Append to Master sheet --------------------
    if all_rows:
        master_existing = master_sheet.get_all_values()
        start_row = len(master_existing) + 1
        safe_update(master_sheet, all_rows, f'A{start_row}')
        print(f"✅ Appended {len(all_rows)} rows to Master sheet.", flush=True)
    else:
        print("⚠️ No new transfers to append to Master sheet.", flush=True)

    # -------------------- Step 4: Create timestamped new tab --------------------
    now = datetime.datetime.now()
    new_tab_name = now.strftime("Transfers_%Y-%m-%d_%H-%M")
    new_worksheet = sh.add_worksheet(title=new_tab_name, rows="2000", cols="10")
    safe_update(new_worksheet, [['Date', 'Player', 'Age', 'From', 'To', 'Fee']], 'A1')
    if all_rows:
        safe_update(new_worksheet, all_rows, 'A2')
        print(f"✅ Data successfully written to new tab: {new_tab_name}", flush=True)
    else:
        safe_update(new_worksheet, [['No transfers found in this range']], 'A2')
        print(f"⚠️ No transfers found for the selected range. Created tab: {new_tab_name}", flush=True)

    return "Scraping completed!", 200


if __name__ == "__main__":
    # default port (Railway/Heroku supply PORT env var)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

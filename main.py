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
from flask import Flask

app = Flask(__name__)

# -------------------- Google Sheets Setup --------------------
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

SPREADSHEET_NAME = "Transfer_Data_Master"
spreadsheet = client.open(SPREADSHEET_NAME)

# -------------------- Headers --------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/139.0.0.0 Safari/537.36"
}

# -------------------- Step 1: Collect Dates --------------------
def get_transfer_dates():
    url = "https://www.transfermarkt.com/transfers/transfertagedetail/statistik/top"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    date_links = []
    for a in soup.select("a.tm-pagination__link"):
        date_text = a.text.strip()
        date_url = "https://www.transfermarkt.com" + a.get("href")
        date_links.append((date_text, date_url))

    return date_links

# -------------------- Step 2: Scrape transfers with full pagination + retry --------------------
def scrape_transfers(dates_list):
    all_rows = []
    SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
    if not SCRAPERAPI_KEY:
        raise ValueError("SCRAPERAPI_KEY environment variable not found!")

    for date_text, date_url in dates_list:
        print(f"\n📅 Scraping transfers for {date_text}...", flush=True)
        page_num = 1
        visited = set()
        current_url = date_url

        while current_url and current_url not in visited:
            visited.add(current_url)
            page_num += 1

            # --- Retry loop for robustness ---
            retries = 3
            for attempt in range(retries):
                try:
                    proxy_page_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(current_url)}"
                    response = requests.get(proxy_page_url, headers=HEADERS, timeout=30)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    break  # success
                except Exception as e:
                    print(f"⚠️ Error fetching {current_url} (attempt {attempt+1}/{retries}): {e}", flush=True)
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)  # backoff: 1s, 2s
                    else:
                        print(f"❌ Failed after {retries} attempts, skipping {current_url}", flush=True)
                        soup = None
            if not soup:
                break

            table = soup.select_one("table.items")
            if not table:
                print(f"⚠️ No transfers found on {current_url}", flush=True)
                break

            rows = table.select("tbody > tr")
            transfers = []
            for row in rows:
                cols = [col.get_text(strip=True) for col in row.select("td")]
                if cols:
                    transfers.append(cols)

            print(f"✅ Page {page_num-1} scraped ({len(transfers)} transfers)", flush=True)
            all_rows.extend(transfers)

            # find next page
            next_link = soup.select_one("a.tm-pagination__link--icon-right")
            if next_link and "href" in next_link.attrs:
                current_url = "https://www.transfermarkt.com" + next_link["href"]
                print(f"   → Next page: {current_url}", flush=True)
                time.sleep(1)  # politeness delay
            else:
                current_url = None

    return all_rows

# -------------------- Step 3: Write to Google Sheets --------------------
def write_to_sheets(rows):
    if not rows:
        print("⚠️ No data to write.", flush=True)
        return

    master_sheet = spreadsheet.worksheet("Master")
    master_sheet.append_rows(rows)
    print(f"✅ Appended {len(rows)} rows to Master sheet.", flush=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    new_tab_name = f"Transfers_{timestamp}"
    spreadsheet.add_worksheet(title=new_tab_name, rows="1000", cols="20")
    new_tab = spreadsheet.worksheet(new_tab_name)
    new_tab.append_rows(rows)
    print(f"✅ Data successfully written to new tab: {new_tab_name}", flush=True)

# -------------------- Flask Route --------------------
@app.route("/run-script")
def run_script():
    try:
        dates_list = get_transfer_dates()
        rows = scrape_transfers(dates_list)
        write_to_sheets(rows)
        return "✅ Script executed successfully!"
    except Exception as e:
        return f"❌ Error: {str(e)}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

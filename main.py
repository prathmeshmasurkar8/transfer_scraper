import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import time
import datetime
import re
import urllib.parse
import os
import random
from flask import Flask

app = Flask(__name__)

# -------------------- Google Sheets Setup --------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "service_account.json"

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
sh = client.open_by_key(SPREADSHEET_ID)

# Master sheet
master_sheet = sh.worksheet("Master")

# -------------------- Headers --------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/139.0.0.0 Safari/537.36"
}

SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
if not SCRAPERAPI_KEY:
    raise ValueError("SCRAPERAPI_KEY environment variable not found!")


# -------------------- Step 1: Get transfer dates --------------------
def get_transfer_dates():
    url = "https://www.transfermarkt.com/transfers/transfertagedetail/statistik/top"
    proxy_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(url)}"

    response = requests.get(proxy_url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    date_links = soup.select("div.box td.zentriert a")
    dates_list = []
    for link in date_links:
        date_text = link.text.strip()
        href = link.get("href")
        if href:
            full_url = urllib.parse.urljoin("https://www.transfermarkt.com", href)
            dates_list.append((date_text, full_url))

    return dates_list


# -------------------- Step 2: Scrape transfers with pagination --------------------
def scrape_transfers(dates_list):
    all_rows = []

    for date_text, date_url in dates_list:
        print(f"\n📅 Scraping transfers for {date_text}...", flush=True)

        current_url = date_url
        page_num = 0

        while current_url:
            page_num += 1
            attempt = 0
            success = False

            while attempt < 3 and not success:
                try:
                    proxy_page_url = (
                        f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(current_url)}"
                    )
                    response = requests.get(proxy_page_url, headers=HEADERS, timeout=30)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")
                    success = True
                except Exception as e:
                    attempt += 1
                    wait_time = 2 ** attempt
                    print(f"⚠️ Error on page {page_num}, attempt {attempt}: {e}. Retrying in {wait_time}s...", flush=True)
                    time.sleep(wait_time)

            if not success:
                print(f"❌ Failed to scrape page {page_num} for {date_text} after 3 attempts. Skipping...", flush=True)
                break

            # Extract transfers
            transfer_rows = soup.select("table.items tbody tr.odd, table.items tbody tr.even")
            print(f" ✅ Page {page_num} scraped ({len(transfer_rows)} transfers)", flush=True)

            for row in transfer_rows:
                cols = row.find_all("td")
                keep_indices = [0, 1, 5, 8, 12, 14]
                data = []
                for idx, col in enumerate(cols, start=1):
                    if idx in keep_indices:
                        text_value = col.get_text(strip=True)
                        a_tag = col.select_one("a")
                        if a_tag and a_tag.get("href"):
                            full_url = "https://www.transfermarkt.com" + a_tag["href"]
                            text_value = f'=HYPERLINK("{full_url}", "{a_tag.text.strip()}")'
                        data.append(text_value)
                if data:
                    data.insert(0, date_text)
                    all_rows.append(data)

            # Pagination
            next_anchor = (
                soup.select_one("ul.tm-pagination a.tm-pagination__link--icon-right")
                or soup.select_one("a.tm-pagination__link[title='Go to next page']")
                or soup.select_one("a[rel='next']")
            )
            if next_anchor and next_anchor.get("href"):
                next_url = urllib.parse.urljoin("https://www.transfermarkt.com", next_anchor["href"])
                if next_url != current_url:
                    print(f"   → Next page: {next_url}", flush=True)
                    current_url = next_url
                else:
                    current_url = None
            else:
                current_url = None

            time.sleep(random.uniform(1, 3))  # polite delay

    return all_rows


# -------------------- Step 3 & 4: Write results --------------------
def write_results(all_rows):
    # Append to Master
    if all_rows:
        master_existing = master_sheet.get_all_values()
        start_row = len(master_existing) + 1
        master_sheet.update(values=all_rows, range_name=f"A{start_row}", raw=False)
        print(f"✅ Appended {len(all_rows)} rows to Master sheet.", flush=True)
    else:
        print("⚠️ No new transfers to append to Master sheet.", flush=True)

    # New tab with timestamp
    now = datetime.datetime.now()
    new_tab_name = now.strftime("Transfers_%Y-%m-%d_%H-%M")
    new_worksheet = sh.add_worksheet(title=new_tab_name, rows="2000", cols="10")
    new_worksheet.update(values=[["Date", "Player", "Age", "From", "To", "Fee"]], range_name="A1")

    if all_rows:
        new_worksheet.update(values=all_rows, range_name="A2", raw=False)
        print(f"✅ Data successfully written to new tab: {new_tab_name}", flush=True)
    else:
        new_worksheet.update(values=[["No transfers found in this range"]], range_name="A2", raw=False)
        print(f"⚠️ No transfers found for the selected range. Created tab: {new_tab_name}", flush=True)


# -------------------- Flask route --------------------
@app.route("/run-script")
def run_script():
    dates_list = get_transfer_dates()
    all_rows = scrape_transfers(dates_list)
    write_results(all_rows)
    return "Scraping completed!", 200


# -------------------- Main --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

import time
import datetime
import os
import json
import urllib.parse
from flask import Flask
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import requests

app = Flask(__name__)

# -------------------- Step 1: Generate transfer URLs --------------------
def generate_transfer_urls(start_date_obj, end_date_obj):
    urls = []
    delta = (end_date_obj - start_date_obj).days
    for i in range(delta + 1):
        date = start_date_obj + datetime.timedelta(days=i)
        # Correct URL for Transfermarkt
        url = f"https://www.transfermarkt.com/transfers/transfertagedetail/statistik/top/land_id_zu/0/land_id_ab/0/leihe/datum/{date.strftime('%Y-%m-%d')}"
        urls.append([date.strftime("%d.%m.%Y"), url])
    return urls

# -------------------- Step 2: Scrape transfers via ScraperAPI with pagination --------------------
def scrape_transfers(dates_list):
    all_rows = []
    SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
    if not SCRAPERAPI_KEY:
        raise ValueError("SCRAPERAPI_KEY environment variable not found!")

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }

    for date_text, base_url in dates_list:
        print(f"\n📅 Scraping transfers for {date_text}...", flush=True)
        page_num = 1

        while True:
            # Construct page URL
            if page_num == 1:
                page_url = base_url
            else:
                page_url = base_url.replace("/datum/", f"/seite/{page_num}/datum/")

            success = False
            for attempt in range(3):
                try:
                    proxy_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(page_url)}"
                    response = requests.get(proxy_url, headers=HEADERS, timeout=20)
                    if response.status_code != 200:
                        raise Exception(f"HTTP {response.status_code}")

                    soup = BeautifulSoup(response.text, 'html.parser')
                    transfer_rows = soup.select("table.items tbody tr.odd, table.items tbody tr.even")

                    # STOP CONDITION: last page reached
                    if not transfer_rows or soup.find("div", class_="no-records"):
                        print(f" 🛑 Last page reached at page {page_num}")
                        success = True
                        break

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

                    success = True
                    break
                except Exception as e:
                    print(f"⚠️ Attempt {attempt + 1} failed for {page_url}: {e}", flush=True)
                    time.sleep(2)

            if not success:
                print(f"⚠️ Failed to fetch {page_url} after 3 attempts", flush=True)
                break

            # If last page reached, stop
            if not transfer_rows or soup.find("div", class_="no-records"):
                break

            page_num += 1  # move to next page

    return all_rows
# -------------------- Flask Route --------------------
@app.route("/run-script")
def run_script():
    # Google Sheets Setup
    SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_CREDS_JSON')
    if not SERVICE_ACCOUNT_INFO:
        return "GOOGLE_CREDS_JSON environment variable not found!", 500
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
        master_sheet.update(values=[['Date', 'Player', 'Age', 'From', 'To', 'Fee']], range_name='A1')

    worksheet = sh.sheet1
    start_date_raw = worksheet.acell('H1').value
    end_date_raw = worksheet.acell('I1').value

    try:
        start_date_obj = datetime.datetime.strptime(start_date_raw, '%m/%d/%Y')
        end_date_obj = datetime.datetime.strptime(end_date_raw, '%m/%d/%Y')
    except ValueError:
        return "Invalid date format in H1 or I1. Use MM/DD/YYYY.", 500

    if start_date_obj > end_date_obj:
        start_date_obj, end_date_obj = end_date_obj, start_date_obj

    # Generate URLs and scrape
    print("Generating transfer URLs...", flush=True)
    dates_list = generate_transfer_urls(start_date_obj, end_date_obj)
    all_rows = scrape_transfers(dates_list)

    # Append to Master sheet
    if all_rows:
        master_existing = master_sheet.get_all_values()
        start_row = len(master_existing) + 1
        master_sheet.update(values=all_rows, range_name=f'A{start_row}', raw=False)
        print(f"✅ Appended {len(all_rows)} rows to Master sheet.", flush=True)
    else:
        print("⚠️ No new transfers to append to Master sheet.", flush=True)

    # Create timestamped new tab
    now = datetime.datetime.now()
    new_tab_name = now.strftime("Transfers_%Y-%m-%d_%H-%M")
    new_worksheet = sh.add_worksheet(title=new_tab_name, rows="2000", cols="10")
    new_worksheet.update(values=[['Date', 'Player', 'Age', 'From', 'To', 'Fee']], range_name='A1')

    if all_rows:
        new_worksheet.update(values=all_rows, range_name='A2', raw=False)
        print(f"✅ Data successfully written to new tab: {new_tab_name}", flush=True)
    else:
        new_worksheet.update(values=[['No transfers found in this range']], range_name='A2', raw=False)
        print(f"⚠️ No transfers found for the selected range. Created tab: {new_tab_name}", flush=True)

    return "Scraping completed!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

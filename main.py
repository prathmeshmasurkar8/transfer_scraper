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

@app.route("/run-script")
def run_script():
    # -------------------- Google Sheet Setup --------------------
    SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_CREDS_JSON')
    if not SERVICE_ACCOUNT_INFO:
        raise ValueError("GOOGLE_CREDS_JSON environment variable not found!")

    service_account_info = json.loads(SERVICE_ACCOUNT_INFO)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
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
    response = requests.get(proxy_url, headers=HEADERS)
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
                    proxy_page_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(current_url)}"
                    response = requests.get(proxy_page_url, headers=HEADERS, timeout=30)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    success = True
                except Exception as e:
                    attempt += 1
                    wait_time = 2 ** attempt
                    print(f"⚠️ Error on page {page_num}, attempt {attempt}: {e}. Retrying in {wait_time}s...", flush=True)
                    time.sleep(wait_time)

            if not success:
                print(f"❌ Failed to scrape page {page_num} for {date_text} after 3 attempts. Skipping...", flush=True)
                break

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

            # pagination (with fallback selectors)
            next_anchor = (
                soup.select_one("ul.tm-pagination a.tm-pagination__link--icon-right") or
                soup.select_one("a.tm-pagination__link[title='Go to next page']") or
                soup.select_one("a[rel='next']")
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

    # -------------------- Step 3: Append to Master sheet --------------------
    if all_rows:
        master_existing = master_sheet.get_all_values()
        start_row = len(master_existing) + 1
        master_sheet.update(values=all_rows, range_name=f'A{start_row}', raw=False)
        print(f"✅ Appended {len(all_rows)} rows to Master sheet.", flush=True)
    else:
        print("⚠️ No new transfers to append to Master sheet.", flush=True)

    # -------------------- Step 4: Create timestamped new tab --------------------
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

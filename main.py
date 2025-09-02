import time
import datetime
import re
import os
import json
import urllib.parse
from flask import Flask
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import requests

app = Flask(__name__)

# -------------------- Step 1: Fetch transfer dates using requests --------------------
def fetch_transfer_dates_requests(start_date_obj, end_date_obj):
    BASE_URL = f"https://www.transfermarkt.com/statistik/transfertage?land_id_zu=0&land_id_ab=0&datum_von={start_date_obj.strftime('%Y-%m-%d')}&datum_bis={end_date_obj.strftime('%Y-%m-%d')}&leihe="
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }

    response = requests.get(BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')

    dates_list = []
    tds = soup.select("table.items tbody tr td.links a")
    for td in tds:
        date_text = td.text.strip()
        href = td.get("href")
        if re.match(r'\d{1,2}\.\d{1,2}\.\d{4}$', date_text):
            day, month, year = [x.strip().zfill(2) for x in date_text.split(".")]
            date_obj = datetime.date(int(year), int(month), int(day))
            if start_date_obj.date() <= date_obj <= end_date_obj.date():
                full_url = urllib.parse.urljoin("https://www.transfermarkt.com", href)
                dates_list.append([date_text, full_url])

    if not dates_list:
        print("⚠️ No transfer dates found for this range.")
        return []  # Instead of raising ValueError

    print(f"Found {len(dates_list)} valid transfer dates.")
    return dates_list


@app.route("/run-script")
def run_script():
    # -------------------- Google Sheet Setup --------------------
    SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_CREDS_JSON')
    if not SERVICE_ACCOUNT_INFO:
        return "❌ GOOGLE_CREDS_JSON environment variable not found!", 500

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
        return "❌ Invalid date format in H1 or I1. Use MM/DD/YYYY.", 500

    if start_date_obj > end_date_obj:
        start_date_obj, end_date_obj = end_date_obj, start_date_obj

    # -------------------- Step 1: Fetch transfer dates --------------------
    print("Fetching transfer dates...", flush=True)
    dates_list = fetch_transfer_dates_requests(start_date_obj, end_date_obj)

    if not dates_list:
        return "⚠️ No transfer dates available for the selected range.", 200

    # -------------------- Step 2: Scrape transfers with full pagination --------------------
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }

    all_rows = []

    for date_text, date_url in dates_list:
        print(f"\n📅 Scraping transfers for {date_text}...", flush=True)
        response = requests.get(date_url, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')

        pagination_links = [date_url]
        page_anchors = soup.select("ul.tm-pagination a[href]")
        for a in page_anchors:
            href = a.get("href", "")
            if "page" in href or "seite" in href:
                full_link = urllib.parse.urljoin("https://www.transfermarkt.com", href)
                if full_link not in pagination_links:
                    pagination_links.append(full_link)

        def extract_page_num(url):
            match = re.search(r"(page|seite)/(\d+)", url)
            return int(match.group(2)) if match else 1

        pagination_links = sorted(pagination_links, key=extract_page_num)
        print(f" 🔎 Found {len(pagination_links)} pages for this date", flush=True)

        for page_num, page_url in enumerate(pagination_links, 1):
            response = requests.get(page_url, headers=HEADERS)
            soup = BeautifulSoup(response.text, 'html.parser')
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
            time.sleep(1)

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
    try:
        new_worksheet = sh.add_worksheet(title=new_tab_name, rows="2000", cols="10")
    except Exception as e:
        return f"❌ Error creating new sheet/tab: {str(e)}", 500

    new_worksheet.update(values=[['Date', 'Player', 'Age', 'From', 'To', 'Fee']], range_name='A1')
    if all_rows:
        new_worksheet.update(values=all_rows, range_name='A2', raw=False)
        print(f"✅ Data successfully written to new tab: {new_tab_name}", flush=True)
    else:
        print("⚠️ No transfers to write in new tab.", flush=True)

    return "Scraping completed!", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

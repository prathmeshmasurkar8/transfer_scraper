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

# -------------------- Google Sheet Setup --------------------
# Get JSON from Railway environment variable
SERVICE_ACCOUNT_INFO = os.environ.get('GOOGLE_CREDS_JSON')
if not SERVICE_ACCOUNT_INFO:
    raise ValueError("GOOGLE_CREDS_JSON environment variable not found!")

service_account_info = json.loads(SERVICE_ACCOUNT_INFO)
scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
gc = gspread.authorize(credentials)

SPREADSHEET_NAME = 'ABCDEFGD'
sh = gc.open(SPREADSHEET_NAME)

# Ensure Master sheet exists
try:
    master_sheet = sh.worksheet("Master")
except gspread.exceptions.WorksheetNotFound:
    master_sheet = sh.add_worksheet(title="Master", rows="2000", cols="10")
    master_sheet.update(values=[['Date', 'Player', 'Age', 'From', 'To', 'Fee']], range_name='A1')

# Read date range from Sheet (MM/DD/YYYY)
worksheet = sh.sheet1
start_date_raw = worksheet.acell('H1').value
end_date_raw = worksheet.acell('I1').value

# Convert to datetime
try:
    start_date_obj = datetime.datetime.strptime(start_date_raw, '%m/%d/%Y')
    end_date_obj = datetime.datetime.strptime(end_date_raw, '%m/%d/%Y')
except ValueError:
    raise ValueError("❌ Invalid date format in H1 or I1. Use MM/DD/YYYY.")

if start_date_obj > end_date_obj:
    start_date_obj, end_date_obj = end_date_obj, start_date_obj

# -------------------- Transfermarkt Setup --------------------
BASE_URL = f"https://www.transfermarkt.com/statistik/transfertage?land_id_zu=0&land_id_ab=0&datum_von={start_date_obj.strftime('%Y-%m-%d')}&datum_bis={end_date_obj.strftime('%Y-%m-%d')}&leihe="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
}

# -------------------- Step 1: Fetch transfer dates --------------------
print("Fetching transfer dates...", flush=True)
response = requests.get(BASE_URL, headers=HEADERS)
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
                    date_url = f"https://www.transfermarkt.com/transfers/transfertagedetail/statistik/top/land_id_zu/0/land_id_ab/0/leihe//datum/{year}-{month}-{day}"
                    dates_list.append([date_text, date_url])

if not dates_list:
    raise ValueError("❌ No transfers available for the provided date range.")

print(f"Found {len(dates_list)} valid transfer dates.", flush=True)

# -------------------- Step 2: Scrape transfers with full pagination --------------------
all_rows = []

for date_text, date_url in dates_list:
    print(f"\n📅 Scraping transfers for {date_text}...", flush=True)

    # Step A: Load the first page of that date
    response = requests.get(date_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Step B: Collect pagination links
    pagination_links = [date_url]  # always include first page
    page_anchors = soup.select("ul.tm-pagination a[href]")

    for a in page_anchors:
        href = a.get("href", "")
        if "page" in href or "seite" in href:  # handle both English/German
            full_link = urllib.parse.urljoin("https://www.transfermarkt.com", href)
            if full_link not in pagination_links:
                pagination_links.append(full_link)

    # sort pagination by page number
    def extract_page_num(url):
        match = re.search(r"(page|seite)/(\d+)", url)
        return int(match.group(2)) if match else 1

    pagination_links = sorted(pagination_links, key=extract_page_num)

    print(f"   🔎 Found {len(pagination_links)} pages for this date", flush=True)

    # Step C: Loop through each page
    for page_num, page_url in enumerate(pagination_links, 1):
        response = requests.get(page_url, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')

        transfer_rows = soup.select("table.items tbody tr.odd, table.items tbody tr.even")
        print(f"      ✅ Page {page_num} scraped ({len(transfer_rows)} transfers)", flush=True)

        for row in transfer_rows:
            cols = row.find_all("td")
            keep_indices = [0, 1, 5, 8, 12, 14]  # adjust as needed
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
    raise Exception("Error creating new sheet/tab: " + str(e))

# Write headers and scraped data
new_worksheet.update(values=[['Date', 'Player', 'Age', 'From', 'To', 'Fee']], range_name='A1')
if all_rows:
    new_worksheet.update(values=all_rows, range_name='A2', raw=False)
    print(f"✅ Data successfully written to new tab: {new_tab_name}", flush=True)
else:
    print("⚠️ No transfers to write in new tab.", flush=True)
# -------------------- Run Flask --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

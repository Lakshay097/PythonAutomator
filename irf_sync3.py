import os
import json
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound
from requests.exceptions import ConnectionError as RequestsConnectionError


# ---------------- CONFIG (from environment variables) ----------------
API_KEY          = os.environ['JOTFORM_API_KEY']
FORM_ID          = os.environ['JOTFORM_FORM_ID']
BASE_URL         = os.environ.get('JOTFORM_BASE_URL', 'https://pw.jotform.com/API')
SPREADSHEET_NAME = os.environ.get('GOOGLE_SHEET_NAME_3', 'test')
WORKSHEET_NAME   = os.environ.get('GOOGLE_WORKSHEET_NAME_3', 'testing')
START_DATE       = os.environ.get('START_DATE', '2025-10-01 00:00:00')
CREDENTIALS      = os.environ.get('GOOGLE_CREDENTIALS_JSON', 'credentials.json')

PAGE_SIZE           = 100
SLEEP_BETWEEN_CALLS = 1
MAX_PAGES           = 500
WRITE_BATCH_SIZE    = 500   # rows per Google Sheets API write call


# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS, scope)
client = gspread.authorize(creds)

spreadsheet = client.open(SPREADSHEET_NAME)

try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
except WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=10)

headers = ['Approval Status', 'Unique ID', 'Last Update Date']
sheet.clear()
sheet.update('A1', [headers])


# ---------------- HELPERS ----------------
def fetch_submissions(offset=0, limit=100):
    url = f"{BASE_URL}/form/{FORM_ID}/submissions"
    params = {
        'apiKey': API_KEY,
        'limit': limit,
        'offset': offset,
        'orderby[created_at]': 'asc',
        'addWorkflowStatus': 1,
        'filter': json.dumps({
            'created_at:gt': START_DATE
        })
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if data.get('responseCode') != 200:
        raise Exception(f"Jotform API error: {data}")

    return data.get('content', [])


def extract_unique_id(answers):
    for _, meta in answers.items():
        if meta.get('name') == 'uniqueId' or meta.get('text') == 'Unique ID':
            return meta.get('answer', '')
    return ''


def append_with_retry(sheet, batch, retries=3):
    """Write a batch of rows to Google Sheets with retry on connection errors."""
    for attempt in range(retries):
        try:
            sheet.append_rows(batch, value_input_option='RAW')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


# ---------------- FETCH & WRITE (streaming batches) ----------------
rows_buffer   = []
total_written = 0
offset        = 0
page          = 0

print("🚀 Fetching submissions...")

while page < MAX_PAGES:
    submissions = fetch_submissions(offset=offset, limit=PAGE_SIZE)

    if not submissions:
        break

    for sub in submissions:
        answers          = sub.get('answers', {})
        approval_status  = sub.get('workflowStatus', '')
        unique_id        = extract_unique_id(answers)
        last_update_date = sub.get('updated_at', '')

        rows_buffer.append([
            approval_status,
            unique_id,
            last_update_date
        ])

    # Flush buffer to Sheets whenever it reaches WRITE_BATCH_SIZE
    if len(rows_buffer) >= WRITE_BATCH_SIZE:
        append_with_retry(sheet, rows_buffer)
        total_written += len(rows_buffer)
        print(f"📝 Written {total_written} rows so far...")
        rows_buffer = []
        time.sleep(2)   # brief pause after each write

    offset += PAGE_SIZE
    page   += 1
    print(f"✔ Pulled {total_written + len(rows_buffer)} rows so far...")
    time.sleep(SLEEP_BETWEEN_CALLS)

# ---------------- FLUSH REMAINING ROWS ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — Wrote {total_written} rows to '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")
import os
import json
import time
import random
import requests
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
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

PAGE_SIZE             = 100
SLEEP_BETWEEN_BATCHES = 5     # wait between batches of parallel fetches
MAX_PAGES             = 500
WRITE_BATCH_SIZE      = 500   # rows per Google Sheets API write call
MAX_WORKERS           = 2     # fewer concurrent requests to go easy on Jotform

# Fetch retry settings
FETCH_RETRIES       = 6
FETCH_BACKOFF_BASE  = 3       # bigger base -> longer waits: 3,9,27,81,243s...
FETCH_MAX_WAIT       = 60     # cap any single wait at 60s


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

# Only write headers if the sheet is empty — never clear existing data
existing_values = sheet.get_all_values()
if not existing_values:
    sheet.update('A1', [headers])


# ---------------- HELPERS ----------------
def get_approval_status(sub):
    """
    Return the human-readable approval status, matching what Jotform's own
    UI shows.

    Jotform's raw `workflowStatus` field only holds a resolved value
    (e.g. "Invalid Request", "Approved", "Denied") once the workflow has
    reached that outcome. While a submission is still moving through the
    approval chain, `workflowStatus` is just the generic engine state
    "ACTIVE" and there's no `workflowStatusDetails` object at all - the
    UI is the one that translates that generic "ACTIVE" into "In Progress".

    Priority:
      1. workflowStatusDetails.text - present for resolved outcomes
         (Invalid Request, Approved, Denied, etc.) and is the exact label
         Jotform's UI displays.
      2. workflowStatus == "ACTIVE" -> "In Progress", to match the UI
         when no resolved outcome exists yet.
      3. Any other raw workflowStatus value, as-is.
      4. '' if neither field is present.
    """
    details = sub.get('workflowStatusDetails') or {}
    if details.get('text'):
        return details['text']

    raw_status = sub.get('workflowStatus', '')
    if raw_status == 'ACTIVE':
        return 'In Progress'

    return raw_status


def fetch_submissions(offset=0, limit=100):
    """Fetch a page of submissions from the Jotform API with retry on transient errors.

    Raises on persistent failure or on 4xx client errors (which are not retried).
    """
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

    response = None
    for attempt in range(FETCH_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=60)
            # If the server returns 5xx, treat as transient and retry
            if response.status_code >= 500:
                raise requests.exceptions.HTTPError(f"{response.status_code} Server Error", response=response)

            response.raise_for_status()
            data = response.json()

            if data.get('responseCode') != 200:
                # API returned an error payload despite HTTP 200
                raise Exception(f"Jotform API error: {data}")

            return data.get('content', [])

        except (RequestsConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            # If it's a client error (4xx) we should not retry
            if isinstance(e, requests.exceptions.HTTPError) and response is not None:
                status = getattr(response, 'status_code', None)
                if status and 400 <= status < 500:
                    raise

            if attempt < FETCH_RETRIES - 1:
                wait = min((FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 2), FETCH_MAX_WAIT)
                print(f"⚠️  Fetch failed (attempt {attempt + 1}/{FETCH_RETRIES}) for offset {offset}, retrying in {wait:.1f}s... [{e}]")
                time.sleep(wait)
                continue
            else:
                # Exhausted retries
                print(f"❌ Failed to fetch submissions for offset {offset} after {FETCH_RETRIES} attempts: {e}")
                raise


def fetch_page(page_num):
    """Wrapper so we can submit (page_num -> submissions) to the thread pool."""
    time.sleep(random.uniform(0, 1.5))  # stagger thread start times
    offset = page_num * PAGE_SIZE
    submissions = fetch_submissions(offset=offset, limit=PAGE_SIZE)
    return page_num, submissions


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


# ---------------- FETCH (parallel, throttled) & WRITE (sequential, ordered) ----------------
rows_buffer   = []
total_written = 0
page          = 0
stop_fetching = False

print(f"🚀 Fetching submissions with {MAX_WORKERS} parallel workers (throttled)...")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    while not stop_fetching and page < MAX_PAGES:
        batch_page_nums = list(range(page, min(page + MAX_WORKERS, MAX_PAGES)))

        futures = {executor.submit(fetch_page, p): p for p in batch_page_nums}

        results = {}
        fetch_error = None
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                page_num, submissions = fut.result()
                results[page_num] = submissions
            except Exception as e:
                fetch_error = e
                print(f"❌ Error fetching page {p} (offset {p * PAGE_SIZE}): {e}")

        if fetch_error is not None:
            print("Aborting sync. You can re-run the job to resume from the last written offset.")
            stop_fetching = True
            break

        # Process results IN ORDER (page_num ascending) to keep row order deterministic
        for p in batch_page_nums:
            submissions = results.get(p, [])

            if not submissions:
                stop_fetching = True
                break

            for sub in submissions:
                answers          = sub.get('answers', {})
                approval_status  = get_approval_status(sub)
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
            time.sleep(2)

        page += len(batch_page_nums)
        print(f"✔ Pulled {total_written + len(rows_buffer)} rows so far (through page {page})...")

        if not stop_fetching:
            print(f"⏳ Waiting {SLEEP_BETWEEN_BATCHES}s before next batch...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

# ---------------- FLUSH REMAINING ROWS ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — Wrote {total_written} rows to '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")
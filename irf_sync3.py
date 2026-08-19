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

HEADERS = ['Approval Status', 'Unique ID', 'Last Update Date']


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


# ---------------- HELPERS ----------------
def get_approval_status(sub):
    """
    Return the human-readable approval status, matching what Jotform's own
    UI shows.

    IMPORTANT: `workflowStatusDetails` is NOT limited to resolved outcomes.
    It is also present while a submission is still ACTIVE and mid-chain -
    in that case `workflowStatusDetails.text` (e.g. "Approve") together
    with `buttonColor` is the label/color of the ACTION BUTTON shown to
    whichever approver needs to act next. It is a call-to-action, not a
    record of what already happened, so it must never be surfaced as the
    submission's status.

    Priority:
      1. workflowStatus == "ACTIVE" -> "In Progress", REGARDLESS of
         whether workflowStatusDetails is present - a pending action
         button does not mean the workflow resolved.
      2. workflowStatusDetails.text - only trusted once workflowStatus is
         NOT "ACTIVE", where it holds the true resolved outcome label
         (e.g. "Invalid Request", "Approved", "Denied").
      3. Any other raw workflowStatus value, as-is.
      4. '' if neither field is present.
    """
    raw_status = sub.get('workflowStatus', '')
    if raw_status == 'ACTIVE':
        return 'In Progress'

    details = sub.get('workflowStatusDetails') or {}
    if details.get('text'):
        return details['text']

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


def write_batch_with_retry(sheet, batch, start_row, retries=3):
    """Write a batch of rows starting at a specific row (update, not append),
    with retry on connection errors."""
    end_row = start_row + len(batch) - 1
    range_str = f"A{start_row}:C{end_row}"
    for attempt in range(retries):
        try:
            sheet.update(range_str, batch, value_input_option='RAW')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


# ---------------- FETCH EVERYTHING FIRST (parallel, throttled) ----------------
# We deliberately fetch the FULL dataset into memory before touching the sheet.
# START_DATE never advances between runs, so every run re-pulls the entire
# range from Jotform - this is a full re-sync, not an incremental one. That
# means the sheet should be fully rewritten each run, not appended to.
#
# We only clear/rewrite the sheet AFTER a successful full fetch, so a failed
# or partial fetch never leaves the sheet wiped with incomplete data.

all_rows      = []
page          = 0
stop_fetching = False
fetch_failed  = False

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
            print("❌ Aborting sync due to fetch error. Sheet was NOT touched — re-run the job when ready.")
            stop_fetching = True
            fetch_failed = True
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

                all_rows.append([
                    approval_status,
                    unique_id,
                    last_update_date
                ])

        page += len(batch_page_nums)
        print(f"✔ Fetched {len(all_rows)} rows so far (through page {page})...")

        if not stop_fetching:
            print(f"⏳ Waiting {SLEEP_BETWEEN_BATCHES}s before next batch...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

if fetch_failed:
    raise SystemExit(1)


# ---------------- CLEAR + REWRITE SHEET (only after full successful fetch) ----------------
print(f"🧹 Fetch complete ({len(all_rows)} rows). Clearing sheet and rewriting...")
sheet.clear()
sheet.update('A1', [HEADERS])

total_written = 0
for i in range(0, len(all_rows), WRITE_BATCH_SIZE):
    chunk = all_rows[i:i + WRITE_BATCH_SIZE]
    start_row = i + 2  # +2 because row 1 is the header and rows are 1-indexed
    write_batch_with_retry(sheet, chunk, start_row)
    total_written += len(chunk)
    print(f"📝 Written {total_written}/{len(all_rows)} rows so far...")
    time.sleep(2)

print(f"✅ DONE — Wrote {total_written} rows to '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")
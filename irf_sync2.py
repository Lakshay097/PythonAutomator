import os
import json
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from http.client import IncompleteRead
from requests.exceptions import ConnectionError as RequestsConnectionError


def col_letter(n):
    """Convert column number to Excel-style letter (1 -> A, 27 -> AA)"""
    result = ''
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


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


def get_form_submissions_raw(form_id, api_key, limit=100, offset=0, retries=3, debug=False):
    """
    Fetch from Jotform's public /API/form/{id}/submissions endpoint.

    This endpoint (unlike the internal /API/sheets/.../rows endpoint) returns
    `workflowStatusDetails` alongside `workflowStatus` when a submission's
    approval workflow has reached a resolved outcome (e.g. "Invalid Request",
    "Approved", "Denied") - that's the field get_approval_status() needs to
    show the same label Jotform's own UI shows.
    """
    url = f"https://pw.jotform.com/API/form/{form_id}/submissions"
    filter_param = json.dumps({"status:ne": ["ARCHIVED", "DELETED"]})
    params = {
        'apiKey': api_key,
        'filter': filter_param,
        'orderby': 'created_at,desc',
        'limit': limit,
        'offset': offset,
        'addWorkflowStatus': 1,
    }
    headers = {
        'User-Agent': 'JOTFORM_PYTHON_WRAPPER'
    }
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if debug:
                print("DEBUG - status code:", resp.status_code)
                print("DEBUG - response preview:", resp.text[:500])
            resp.raise_for_status()
            data = resp.json()
            return data.get('content', [])
        except (RequestsConnectionError, requests.exceptions.RequestException) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Fetch failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


def append_with_retry(sheet, batch, retries=3):
    """Write a batch of rows to Google Sheets with retry on connection errors."""
    for attempt in range(retries):
        try:
            sheet.append_rows(batch, value_input_option='USER_ENTERED')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


# ---------------- CONFIG (from environment variables) ----------------
API_KEY        = os.environ['JOTFORM_API_KEY']
FORM_ID        = os.environ.get('JOTFORM_FORM_ID', '231751320990049')  # <-- defaults to the form ID from your URL
SHEET_NAME     = os.environ.get('GOOGLE_SHEET_NAME_2', 'IRF_2.0_AdminSheet- 7 January 2026 onwards')
WORKSHEET_NAME = os.environ.get('GOOGLE_WORKSHEET_NAME_2', 'IRF 2.0 Updated')
CREDENTIALS    = os.environ.get('GOOGLE_CREDENTIALS_JSON', 'credentials.json')

TOTAL_LIMIT         = 8000
PAGE_SIZE           = 100   # matches the `limit=100` in your URL
SLEEP_BETWEEN_CALLS = 1
WRITE_BATCH_SIZE    = 500   # rows per Google Sheets API write call

# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS, scope)
client = gspread.authorize(creds)
sheet  = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

# ---------------- PRESERVE HEADERS ----------------
existing_headers = sheet.row_values(1)
if not existing_headers:
    raise Exception("Header row missing in destination sheet")

# ---- SAFE CLEAR: values only, no row deletion ----
row_count = sheet.row_count
col_count = sheet.col_count

if row_count > 1:
    last_col = col_letter(col_count)
    sheet.batch_clear([f"A2:{last_col}{row_count}"])

print("🧹 Old data cleared (values only), header preserved")

# ---------------- DISCOVER JOTFORM FIELDS ----------------
first_batch = get_form_submissions_raw(FORM_ID, API_KEY, limit=1, offset=0, debug=True)
if not first_batch:
    raise Exception("No submissions found")

first_sub    = first_batch[0]
answers_meta = first_sub.get('answers', {})

# TEMP DEBUG: confirm workflowStatus survives the raw fetch
print("DEBUG - keys in first submission:", list(first_sub.keys()))
print("DEBUG - workflowStatus value:", first_sub.get('workflowStatus', '<<MISSING>>'))

header_to_qid = {}
new_headers   = []

for qid, meta in answers_meta.items():
    col_name = meta.get('text', f'Q_{qid}')
    if col_name in existing_headers:
        header_to_qid[col_name] = qid
    else:
        new_headers.append(col_name)
        header_to_qid[col_name] = qid

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update('A1', [updated_headers])
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")

# ---------------- FETCH & WRITE (streaming batches) ----------------
offset        = 0
fetched       = 0
rows_buffer   = []
total_written = 0

print("🚀 Fetching latest submissions...")

while fetched < TOTAL_LIMIT:
    try:
        submissions = get_form_submissions_raw(
            FORM_ID,
            API_KEY,
            limit=PAGE_SIZE,
            offset=offset
        )

        if not submissions:
            break

        for sub in submissions:
            row_data = {
                'Submission ID':    sub.get('id'),
                'Submission Date':  sub.get('created_at', ''),
                'Last Update Date': sub.get('updated_at', ''),
                'Approval Status':  get_approval_status(sub)
            }

            answers = sub.get('answers', {})
            for header, qid in header_to_qid.items():
                if qid in answers and 'answer' in answers[qid]:
                    ans = answers[qid]['answer']
                    row_data[header] = (
                        '\n'.join(map(str, ans))
                        if isinstance(ans, list)
                        else str(ans)
                    )
                else:
                    row_data[header] = ''

            rows_buffer.append([row_data.get(h, '') for h in existing_headers])
            fetched += 1
            if fetched >= TOTAL_LIMIT:
                break

        # Flush buffer to Sheets whenever it reaches WRITE_BATCH_SIZE
        if len(rows_buffer) >= WRITE_BATCH_SIZE:
            append_with_retry(sheet, rows_buffer)
            total_written += len(rows_buffer)
            print(f"📝 Written {total_written} rows so far...")
            rows_buffer = []
            time.sleep(2)   # brief pause after each write

        offset += PAGE_SIZE
        print(f"✔ Pulled {fetched} submissions")
        time.sleep(SLEEP_BETWEEN_CALLS)

    except IncompleteRead:
        print("⚠️  IncompleteRead detected, retrying...")
        time.sleep(5)
        continue

# ---------------- FLUSH REMAINING ROWS ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — {total_written} rows written successfully")
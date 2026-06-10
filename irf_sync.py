import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from jotform import JotformAPIClient
from http.client import IncompleteRead
from requests.exceptions import ConnectionError as RequestsConnectionError


def col_letter(n):
    """Convert column number to Excel-style letter (1 -> A, 27 -> AA)"""
    result = ''
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


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
FORM_ID        = os.environ['JOTFORM_FORM_ID']
SHEET_NAME     = os.environ.get('GOOGLE_SHEET_NAME', 'IRF Data sheet-version 2.0')
WORKSHEET_NAME = os.environ.get('GOOGLE_WORKSHEET_NAME', 'IRF 2.0 Updated')
CREDENTIALS    = os.environ.get('GOOGLE_CREDENTIALS_JSON', 'credentials.json')

TOTAL_LIMIT         = 8000
PAGE_SIZE           = 200
SLEEP_BETWEEN_CALLS = 1
WRITE_BATCH_SIZE    = 500   # rows per Google Sheets API write call

# ---------------- JOTFORM (custom enterprise server) ----------------
jotform = JotformAPIClient(API_KEY)
jotform.set_baseurl('https://pw.jotform.com/API/')

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
first_batch = jotform.get_form_submissions(FORM_ID, limit=1, offset=0)
if not first_batch:
    raise Exception("No submissions found")

first_sub    = first_batch[0]
answers_meta = first_sub.get('answers', {})

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
offset       = 0
fetched      = 0
rows_buffer  = []
total_written = 0

print("🚀 Fetching latest submissions...")

while fetched < TOTAL_LIMIT:
    try:
        submissions = jotform.get_form_submissions(
            FORM_ID,
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
                'Approval Status':  (
                    sub.get('workflowStatus')
                    or sub.get('workflow_status')
                    or sub.get('status')
                    or ''
                )
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
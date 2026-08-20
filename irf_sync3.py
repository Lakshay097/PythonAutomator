import os
import sys
import json
import time
import random
import argparse
import requests
import gspread
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, wait, as_completed, FIRST_COMPLETED
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound
from requests.exceptions import ConnectionError as RequestsConnectionError

DATE_FMT = '%Y-%m-%d %H:%M:%S'


API_KEY          = os.environ['JOTFORM_API_KEY']
FORM_ID          = os.environ['JOTFORM_FORM_ID']
BASE_URL         = os.environ.get('JOTFORM_BASE_URL', 'https://pw.jotform.com/API')
SPREADSHEET_NAME = os.environ.get('GOOGLE_SHEET_NAME_3', 'IRF Data sheet-version 2.0')
WORKSHEET_NAME   = os.environ.get('GOOGLE_WORKSHEET_NAME_3', 'Bring last 4 month data')
START_DATE       = os.environ.get('START_DATE', '2026-01-01 00:00:00')
CREDENTIALS      = os.environ.get('GOOGLE_CREDENTIALS_JSON', 'credentials.json')

# All of these are tunable via env vars so you can dial speed up/down
# without editing code, depending on how Jotform's API responds.
PAGE_SIZE         = int(os.environ.get('JOTFORM_PAGE_SIZE', 200))     # rows per fetch call
MAX_PAGES_PER_WINDOW = int(os.environ.get('JOTFORM_MAX_PAGES_PER_WINDOW', 100))
WRITE_BATCH_SIZE  = int(os.environ.get('SHEETS_WRITE_BATCH_SIZE', 2000))  # rows per Sheets write call
MAX_WORKERS       = int(os.environ.get('JOTFORM_MAX_WORKERS', 6))     # concurrent in-flight fetches
WRITE_SLEEP       = float(os.environ.get('SHEETS_WRITE_SLEEP', 0.3))  # pause between Sheets write calls

WINDOW_DAYS = int(os.environ.get('JOTFORM_WINDOW_DAYS', 7))

# Fetch retry settings.
# NOTE: previously tuned for the flaky /form/{id}/submissions endpoint, which
# failed unpredictably (even at shallow offsets, not just past a deep-offset
# ceiling) and needed long backoffs to have any chance of succeeding.
# fetch_submissions() now uses the Sheets-view /rows endpoint instead - the
# same undocumented internal endpoint the recovery path already relied on -
# which has tested reliable (see test_sheets_endpoint.py). Backoffs are
# shortened accordingly; if you start seeing frequent skips again, this is
# the first place to loosen back up.
FETCH_RETRIES      = 4
FETCH_BACKOFF_BASE = 2       # 2,4,8,16s...
FETCH_MAX_WAIT      = 20     # cap any single wait at 20s

HEADERS = ['Approval Status', 'Unique ID', 'Last Update Date']

# Sheets-view lookup endpoint - this is now the PRIMARY fetch path (see
# fetch_submissions below), not just used for one-off single-ID recovery.
SHEET_ID = os.environ.get('JOTFORM_SHEET_ID', FORM_ID)
VIEW_ID  = os.environ.get('JOTFORM_VIEW_ID', FORM_ID)

UNRECOVERED_FILE  = os.environ.get('UNRECOVERED_FILE', 'jotform_sync_unrecovered.json')
RECOVERY_WORKERS  = int(os.environ.get('JOTFORM_RECOVERY_WORKERS', 4))

CHECKPOINT_FILE = os.environ.get('CHECKPOINT_FILE', 'jotform_sync_checkpoint.json')


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


def fetch_submissions(offset=0, limit=100, gt=None, lt=None):
    """Fetch a page of submissions via the Sheets-view /rows endpoint.

    Switched from /form/{id}/submissions, which 500'd unpredictably - not
    only past a deep-offset ceiling but at offset 0 in some windows too.
    This is the same internal endpoint the single-ID recovery path already
    used successfully (see fetch_submission_by_unique_id) - verified via
    test_sheets_endpoint.py that offset paging and created_at:gt/lt
    filtering both work correctly here.

    `gt`/`lt` bound the created_at filter for this call (window pagination) -
    defaults to the global START_DATE with no upper bound if not given.

    Raises on persistent failure or on 4xx client errors (which are not retried).
    """
    url = f"{BASE_URL}/sheets/{SHEET_ID}/sheet/{SHEET_ID}/view/{VIEW_ID}/rows"
    filt = {
        'status:ne': ['ARCHIVED', 'DELETED'],
        'created_at:gt': gt if gt is not None else START_DATE,
    }
    if lt is not None:
        filt['created_at:lt'] = lt

    params = {
        'apiKey': API_KEY,
        'filter': json.dumps(filt),
        'orderby': 'created_at,asc',
        'limit': limit,
        'offset': offset,
        'addAutomationRunHistory': 1,
        'addWorkflowStatus': 1,
        'skipWorkflowTaskExtraInfo': 1,
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
                backoff = min((FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 2), FETCH_MAX_WAIT)
                print(f"⚠️  Fetch failed (attempt {attempt + 1}/{FETCH_RETRIES}) for offset {offset} "
                      f"[{gt} .. {lt or 'now'}], retrying in {backoff:.1f}s... [{e}]")
                time.sleep(backoff)
                continue
            else:
                # Exhausted retries
                print(f"❌ Failed to fetch submissions for offset {offset} [{gt} .. {lt or 'now'}] "
                      f"after {FETCH_RETRIES} attempts: {e}")
                raise


def fetch_page(page_num, gt, lt):
    """Wrapper so we can submit (page_num -> submissions) to the thread pool."""
    time.sleep(random.uniform(0, 0.3))  # tiny jitter so threads don't all hit the API in lockstep
    offset = page_num * PAGE_SIZE
    submissions = fetch_submissions(offset=offset, limit=PAGE_SIZE, gt=gt, lt=lt)
    return page_num, submissions


def extract_unique_id(answers):
    for _, meta in answers.items():
        if meta.get('name') == 'uniqueId' or meta.get('text') == 'Unique ID':
            return meta.get('answer', '')
    return ''


def submission_to_row(sub):
    """Turn one raw submission dict into a [Approval Status, Unique ID, Last Update Date] row."""
    answers = sub.get('answers', {})
    return [
        get_approval_status(sub),
        extract_unique_id(answers),
        sub.get('updated_at', ''),
    ]


def is_client_error(exc):
    """True for a genuine 4xx (bad request/auth/etc) - these mean something is
    actually wrong with the request itself, so we should abort the whole run
    rather than skip-and-continue. 5xx / connection / timeout errors are NOT
    client errors and are treated as skippable after retries are exhausted."""
    return (isinstance(exc, requests.exceptions.HTTPError)
            and exc.response is not None
            and 400 <= exc.response.status_code < 500)


def fetch_submission_by_unique_id(unique_id, retries=3):
    """Look up a single submission by its Unique ID via Jotform's Sheets-view API.
    Kept as a manual one-off recovery tool (--recover-id) for the rare case
    where even the primary fetch path can't find/return a specific row -
    this uses a broad fullText match instead of an offset-based page.
    """
    url = f"{BASE_URL}/sheets/{SHEET_ID}/sheet/{SHEET_ID}/view/{VIEW_ID}/rows"
    params = {
        'apiKey': API_KEY,
        'filter': json.dumps({
            'fullText': unique_id,
            'status:ne': ['ARCHIVED', 'DELETED'],
        }),
        'orderby': 'created_at,desc',
        'limit': 20,
        'addAutomationRunHistory': 1,
        'next5': 1,
        'addWorkflowStatus': 1,
        'skipWorkflowTaskExtraInfo': 1,
    }

    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if data.get('responseCode') != 200:
                raise Exception(f"Jotform API error: {data}")

            content = data.get('content', [])
            for sub in content:
                if extract_unique_id(sub.get('answers', {})) == unique_id:
                    return sub
            return content[0] if content else None

        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))

    raise last_err


def build_windows():
    """Split [START_DATE, now] into fixed-size date windows so pagination
    within each window starts at offset 0. Kept even though the new endpoint
    tested reliable at higher offsets too - it's still a reasonable safety
    margin and keeps each window's row count (and thus recovery cost if a
    page ever does fail) small."""
    start_dt = datetime.strptime(START_DATE, DATE_FMT)
    now_dt = datetime.utcnow()

    windows = []
    cur = start_dt
    while cur < now_dt:
        nxt = cur + timedelta(days=WINDOW_DAYS)
        if nxt >= now_dt:
            windows.append({'gt': cur.strftime(DATE_FMT), 'lt': None})
            break
        windows.append({'gt': cur.strftime(DATE_FMT), 'lt': nxt.strftime(DATE_FMT)})
        cur = nxt

    if not windows:
        windows.append({'gt': start_dt.strftime(DATE_FMT), 'lt': None})

    return windows


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return [], 0
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
        if data.get('start_date') != START_DATE or data.get('window_days') != WINDOW_DAYS:
            print("ℹ️  Checkpoint is for different START_DATE/WINDOW_DAYS settings, ignoring it and starting fresh.")
            return [], 0
        print(f"ℹ️  Resuming from checkpoint: {len(data['rows'])} rows already fetched, "
              f"{data['completed_windows']} window(s) already completed.")
        return data['rows'], data['completed_windows']
    except Exception as e:
        print(f"⚠️  Could not read checkpoint ({e}), starting fresh.")
        return [], 0


def save_checkpoint(rows, completed_windows):
    tmp_path = CHECKPOINT_FILE + '.tmp'
    with open(tmp_path, 'w') as f:
        json.dump({
            'start_date': START_DATE,
            'window_days': WINDOW_DAYS,
            'completed_windows': completed_windows,
            'rows': rows,
        }, f)
    os.replace(tmp_path, CHECKPOINT_FILE)


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


def fetch_window(gt, lt, window_label):
    rows = []
    skipped_offsets = []
    client_error = None

    next_submit = [0]
    next_process = 0
    stop_signal = False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        in_flight = {}
        completed = {}

        def _fill_window():
            while (not stop_signal and client_error is None
                   and next_submit[0] < MAX_PAGES_PER_WINDOW and len(in_flight) < MAX_WORKERS):
                fut = executor.submit(fetch_page, next_submit[0], gt, lt)
                in_flight[fut] = next_submit[0]
                next_submit[0] += 1

        _fill_window()

        while in_flight:
            done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)

            for fut in done:
                p = in_flight.pop(fut)
                try:
                    page_num, submissions = fut.result()
                    completed[page_num] = submissions
                except Exception as e:
                    if is_client_error(e):
                        client_error = e
                        print(f"❌ Client error fetching window [{window_label}] page {p} "
                              f"(offset {p * PAGE_SIZE}), aborting: {e}")
                    else:
                        print(f"⏭️  Giving up on window [{window_label}] page {p} "
                              f"(offset {p * PAGE_SIZE}) after retries — will attempt row-level recovery. [{e}]")
                        skipped_offsets.append({'offset': p * PAGE_SIZE, 'gt': gt, 'lt': lt})
                        completed[p] = 'SKIPPED'

            if client_error is not None:
                break

            while next_process in completed:
                submissions = completed.pop(next_process)

                if submissions == 'SKIPPED':
                    next_process += 1
                    continue

                if not submissions:
                    stop_signal = True
                    break

                for sub in submissions:
                    rows.append(submission_to_row(sub))

                next_process += 1

            _fill_window()

    return rows, skipped_offsets, client_error


def recover_skipped_offsets(skipped_offsets):
    tasks = []
    for sp in skipped_offsets:
        for i in range(PAGE_SIZE):
            tasks.append((sp['offset'] + i, sp['gt'], sp['lt']))

    def _try_one(task):
        offset, gt, lt = task
        try:
            subs = fetch_submissions(offset=offset, limit=1, gt=gt, lt=lt)
            return task, subs, None
        except Exception as e:
            return task, None, e

    recovered_rows = []
    unrecovered = []

    with ThreadPoolExecutor(max_workers=RECOVERY_WORKERS) as executor:
        futures = [executor.submit(_try_one, t) for t in tasks]
        done_count = 0
        for fut in as_completed(futures):
            (offset, gt, lt), subs, err = fut.result()
            done_count += 1
            if err is not None:
                unrecovered.append({'offset': offset, 'gt': gt, 'lt': lt, 'error': str(err)})
            elif subs:
                recovered_rows.append(submission_to_row(subs[0]))

            if done_count % 20 == 0 or done_count == len(tasks):
                print(f"🔎 Row-level recovery: {done_count}/{len(tasks)} offsets checked "
                      f"({len(recovered_rows)} recovered, {len(unrecovered)} still failing)...")

    unrecovered.sort(key=lambda x: (x['gt'], x['offset']))
    return recovered_rows, unrecovered


def write_batch_with_retry(sheet, batch, start_row, retries=3):
    end_row = start_row + len(batch) - 1
    range_str = f"A{start_row}:C{end_row}"
    for attempt in range(retries):
        try:
            sheet.update(range_name=range_str, values=batch, value_input_option='RAW')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait_s = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait_s}s... [{e}]")
                time.sleep(wait_s)
            else:
                raise


# ---------------- CLI: one-off recovery of a single submission by Unique ID ----------------
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--recover-id', help="Fetch one submission by Unique ID via the Sheets "
                                          "lookup endpoint and append it to the sheet, then exit.")
cli_args, _ = parser.parse_known_args()

if cli_args.recover_id:
    print(f"🔎 Recovering single submission '{cli_args.recover_id}' via Sheets lookup endpoint...")
    sub = fetch_submission_by_unique_id(cli_args.recover_id)
    if sub is None:
        print(f"❌ No submission found for Unique ID '{cli_args.recover_id}'.")
        raise SystemExit(1)
    row = submission_to_row(sub)
    sheet.append_rows([row], value_input_option='RAW')
    print(f"✅ Appended recovered row for '{cli_args.recover_id}': {row}")
    raise SystemExit(0)


# ---------------- FETCH EVERYTHING FIRST (parallel, windowed) ----------------
all_rows, completed_windows = load_checkpoint()
windows = build_windows()

print(f"🚀 Fetching submissions across {len(windows)} window(s) of {WINDOW_DAYS} day(s) each, "
      f"up to {MAX_WORKERS} concurrent requests per window ({PAGE_SIZE} rows/page)...")

all_skipped = []
aborted = False

for idx, win in enumerate(windows):
    if idx < completed_windows:
        continue

    label = f"{win['gt']} .. {win['lt'] or 'now'}"
    print(f"📅 Window {idx + 1}/{len(windows)}: {label}")

    rows, skipped, client_error = fetch_window(win['gt'], win['lt'], label)

    if client_error is not None:
        save_checkpoint(all_rows, idx)
        print(f"❌ Aborting sync due to a client error in window [{label}]. Sheet was NOT touched. "
              f"Progress saved to '{CHECKPOINT_FILE}' ({len(all_rows)} rows, {idx} window(s) completed) — "
              f"re-run the job and it will resume from here.")
        aborted = True
        break

    all_rows.extend(rows)
    all_skipped.extend(skipped)
    save_checkpoint(all_rows, idx + 1)

    print(f"✔ Window {idx + 1}/{len(windows)} done: {len(rows)} rows"
          f"{f', {len(skipped)} page(s) skipped for recovery' if skipped else ''}. "
          f"Total so far: {len(all_rows)} rows.")

if aborted:
    raise SystemExit(1)


# ---------------- ROW-LEVEL RECOVERY FOR SKIPPED PAGES ----------------
if all_skipped:
    total_offsets = len(all_skipped) * PAGE_SIZE
    print(f"🔎 {len(all_skipped)} page(s) were skipped across all windows ({total_offsets} offsets). "
          f"Retrying each row individually...")

    recovered_rows, unrecovered = recover_skipped_offsets(all_skipped)
    all_rows.extend(recovered_rows)
    save_checkpoint(all_rows, len(windows))

    print(f"✔ Row-level recovery done: {len(recovered_rows)} rows recovered, "
          f"{len(unrecovered)} still unrecoverable.")

    if unrecovered:
        with open(UNRECOVERED_FILE, 'w') as f:
            json.dump(unrecovered, f, indent=2)
        print(f"⚠️  {len(unrecovered)} offset(s) could not be recovered even individually — "
              f"logged to '{UNRECOVERED_FILE}'.")
        print(f"   1. Open Jotform's UI and find the submission near that offset's position/date window.")
        print(f"   2. Once you have its Unique ID, run:")
        print(f"        python {os.path.basename(sys.argv[0])} --recover-id <UNIQUE_ID>")


# ---------------- CLEAR + REWRITE SHEET (only after full successful fetch) ----------------
print(f"🧹 Fetch complete ({len(all_rows)} rows). Clearing sheet and rewriting...")

# Grow the grid first if needed - clear() does not resize, and an existing
# sheet may have fewer rows in its grid than we're about to write (that's
# what caused the "exceeds grid limits" error: grid had 2001 rows, we tried
# to write up to row 4001+). Only grows, never shrinks, so it's safe to run
# every time regardless of dataset size.
needed_rows = len(all_rows) + 10  # +10 headroom above the header row
if sheet.row_count < needed_rows:
    print(f"📐 Resizing sheet grid from {sheet.row_count} to {needed_rows} rows...")
    sheet.resize(rows=needed_rows)

sheet.batch_clear(['A:C'])
sheet.update(range_name='A1', values=[HEADERS])

total_written = 0
for i in range(0, len(all_rows), WRITE_BATCH_SIZE):
    chunk = all_rows[i:i + WRITE_BATCH_SIZE]
    start_row = i + 2
    write_batch_with_retry(sheet, chunk, start_row)
    total_written += len(chunk)
    print(f"📝 Written {total_written}/{len(all_rows)} rows so far...")
    time.sleep(WRITE_SLEEP)

clear_checkpoint()
print(f"✅ DONE — Wrote {total_written} rows to '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")
import os
import json
import time
import random
import requests
import gspread

from typing import List, Any
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError, RequestException
from urllib3.util.retry import Retry
from http.client import IncompleteRead
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound


API_KEY = os.environ["JOTFORM_API_KEY"]
FORM_ID = os.environ["JOTFORM_FORM_ID"]
BASE_URL = os.environ.get("JOTFORM_BASE_URL", "https://pw.jotform.com/API")

SPREADSHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME_3", "test")
WORKSHEET_NAME = os.environ.get("GOOGLE_WORKSHEET_NAME_3", "testing")
START_DATE = os.environ.get("START_DATE", "2025-10-01 00:00:00")
CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "credentials.json")

PAGE_SIZE = int(os.environ.get("PAGE_SIZE_3", "100"))
WRITE_BATCH_SIZE = int(os.environ.get("WRITE_BATCH_SIZE_3", "100"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "500"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
MAX_FETCH_RETRIES = int(os.environ.get("MAX_FETCH_RETRIES", "5"))
MAX_WRITE_RETRIES = int(os.environ.get("MAX_WRITE_RETRIES", "6"))
SLEEP_BETWEEN_FETCH_CALLS = float(os.environ.get("SLEEP_BETWEEN_FETCH_CALLS", "1"))


def backoff_sleep(attempt: int, cap: int = 60) -> None:
    time.sleep(min(cap, (2 ** attempt) + random.uniform(0.5, 1.5)))


def chunked(items: List[List[Any]], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_FETCH_RETRIES,
        connect=MAX_FETCH_RETRIES,
        read=MAX_FETCH_RETRIES,
        backoff_factor=1,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# FIX 3: Replaced deprecated gspread.authorize() with gspread.Client(auth=creds)
def get_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    return gspread.Client(auth=creds)


def fetch_submissions(session: requests.Session, offset: int, limit: int):
    url = f"{BASE_URL.rstrip('/')}/form/{FORM_ID}/submissions"
    params = {
        "apiKey": API_KEY,
        "limit": limit,
        "offset": offset,
        "orderby[created_at]": "asc",
        "addWorkflowStatus": 1,
        "filter": json.dumps({
            "created_at:gt": START_DATE
        }),
    }

    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if data.get("responseCode") != 200:
                raise Exception(f"Jotform API error: {data}")

            return data.get("content", [])

        except (IncompleteRead, ConnectionError, Timeout, ChunkedEncodingError, RequestException) as e:
            if attempt == MAX_FETCH_RETRIES:
                raise
            print(f"⚠️ Fetch failed at offset {offset} ({type(e).__name__}), retry {attempt}/{MAX_FETCH_RETRIES}")
            backoff_sleep(attempt)

    return []


def append_rows_with_retry(sheet: gspread.Worksheet, batch: List[List[Any]]) -> None:
    for attempt in range(1, MAX_WRITE_RETRIES + 1):
        try:
            sheet.append_rows(
                batch,
                value_input_option="RAW",
                insert_data_option="INSERT_ROWS",
                table_range="A1",
            )
            return
        except (IncompleteRead, ConnectionError, Timeout, ChunkedEncodingError, RequestException) as e:
            if attempt == MAX_WRITE_RETRIES:
                raise
            print(f"⚠️ Write failed ({type(e).__name__}), retry {attempt}/{MAX_WRITE_RETRIES}")
            backoff_sleep(attempt)


def extract_unique_id(answers) -> str:
    for _, meta in (answers or {}).items():
        if meta.get("name") == "uniqueId" or meta.get("text") == "Unique ID":
            value = meta.get("answer", "")
            if isinstance(value, list):
                return "\n".join(map(str, value))
            if isinstance(value, dict):
                return json.dumps(value, ensure_ascii=False)
            return str(value)

    # FIX 6: Warn explicitly when Unique ID field is not found rather than
    #         silently returning "", which makes debugging mismatches very hard.
    print("⚠️ Warning: 'Unique ID' field not found in submission answers. "
          "Check that the field name/text matches 'uniqueId' or 'Unique ID'.")
    return ""


def main():
    session = create_session()
    client = get_client()

    spreadsheet = client.open(SPREADSHEET_NAME)

    try:
        sheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=10)

    headers = ["Approval Status", "Unique ID", "Last Update Date"]
    sheet.clear()
    sheet.update("A1", [headers])

    print("🚀 Fetching and writing submissions...")

    rows_written = 0
    offset = 0
    page = 0

    while page < MAX_PAGES:
        submissions = fetch_submissions(session, offset=offset, limit=PAGE_SIZE)
        if not submissions:
            break

        page_rows = []

        for sub in submissions:
            answers = sub.get("answers", {}) or {}
            approval_status = str(sub.get("workflowStatus", "") or "")
            unique_id = extract_unique_id(answers)
            last_update_date = str(sub.get("updated_at", "") or "")

            page_rows.append([
                approval_status,
                unique_id,
                last_update_date,
            ])

        for batch in chunked(page_rows, WRITE_BATCH_SIZE):
            append_rows_with_retry(sheet, batch)
            rows_written += len(batch)

        offset += PAGE_SIZE
        page += 1

        print(f"✔ Pulled page {page} | ✔ Wrote {rows_written} rows")

        # FIX 7: Warn when MAX_PAGES limit is hit so truncation is never silent.
        if page >= MAX_PAGES and len(submissions) == PAGE_SIZE:
            print(
                f"⚠️ Warning: MAX_PAGES limit ({MAX_PAGES}) reached but the last page "
                f"was full ({PAGE_SIZE} rows). There may be more submissions not written. "
                f"Increase MAX_PAGES to fetch all data."
            )

        # Only sleep if there are more pages to fetch
        if len(submissions) == PAGE_SIZE:
            time.sleep(SLEEP_BETWEEN_FETCH_CALLS)

    print(f"✅ DONE — Wrote {rows_written} rows to '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")


if __name__ == "__main__":
    main()
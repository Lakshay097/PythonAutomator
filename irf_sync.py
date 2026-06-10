import os
import json
import time
import random
import requests
import gspread

from typing import Dict, List, Any
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError, RequestException
from urllib3.util.retry import Retry
from http.client import IncompleteRead
from google.oauth2.service_account import Credentials


API_KEY = os.environ["JOTFORM_API_KEY"]
FORM_ID = os.environ["JOTFORM_FORM_ID"]
BASE_URL = os.environ.get("JOTFORM_BASE_URL", "https://pw.jotform.com/API")

SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "IRF Data sheet-version 2.0")
WORKSHEET_NAME = os.environ.get("GOOGLE_WORKSHEET_NAME", "IRF 2.0 Updated")
CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "credentials.json")

TOTAL_LIMIT = int(os.environ.get("TOTAL_LIMIT", "8000"))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "200"))
WRITE_BATCH_SIZE = int(os.environ.get("WRITE_BATCH_SIZE", "100"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
MAX_FETCH_RETRIES = int(os.environ.get("MAX_FETCH_RETRIES", "5"))
MAX_WRITE_RETRIES = int(os.environ.get("MAX_WRITE_RETRIES", "6"))
SLEEP_BETWEEN_FETCH_CALLS = float(os.environ.get("SLEEP_BETWEEN_FETCH_CALLS", "1"))


def col_letter(n: int) -> str:
    result = ""
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def backoff_sleep(attempt: int, cap: int = 60) -> None:
    time.sleep(min(cap, (2 ** attempt) + random.uniform(0.5, 1.5)))


def chunked(items: List[List[Any]], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def normalize_answer(ans: Any) -> str:
    if ans is None:
        return ""
    if isinstance(ans, list):
        return "\n".join(map(str, ans))
    if isinstance(ans, dict):
        return json.dumps(ans, ensure_ascii=False)
    return str(ans)


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


# FIX 1: Fetch all questions from the form's questions endpoint instead of
#         inferring columns from only the first submission. This ensures questions
#         added at any point in the form's lifetime are all captured.
def fetch_all_questions(session: requests.Session) -> Dict[str, str]:
    """Returns {col_name: qid} for every question on the form."""
    url = f"{BASE_URL.rstrip('/')}/form/{FORM_ID}/questions"
    params = {"apiKey": API_KEY}

    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if data.get("responseCode") != 200:
                raise Exception(f"Jotform questions API error: {data}")

            questions = data.get("content", {}) or {}
            header_to_qid: Dict[str, str] = {}
            seen_names: Dict[str, int] = {}

            for qid, meta in questions.items():
                col_name = (meta or {}).get("text") or f"Q_{qid}"

                # FIX 5: Deduplicate column names that share the same label text
                # by appending a counter suffix so no column is silently overwritten.
                if col_name in seen_names:
                    seen_names[col_name] += 1
                    col_name = f"{col_name}_{seen_names[col_name]}"
                else:
                    seen_names[col_name] = 1

                header_to_qid[col_name] = qid

            return header_to_qid

        except (IncompleteRead, ConnectionError, Timeout, ChunkedEncodingError, RequestException) as e:
            if attempt == MAX_FETCH_RETRIES:
                raise
            print(f"⚠️ Questions fetch failed ({type(e).__name__}), retry {attempt}/{MAX_FETCH_RETRIES}")
            backoff_sleep(attempt)

    return {}


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


def main():
    session = create_session()
    client = get_client()
    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

    existing_headers = sheet.row_values(1)
    if not existing_headers:
        raise Exception("Header row missing in destination sheet")

    base_headers = ["Submission ID", "Submission Date", "Last Update Date", "Approval Status"]
    for header in base_headers:
        if header not in existing_headers:
            existing_headers.append(header)

    # FIX 1: Build column map from the form's questions endpoint (not just submission #1)
    print("📋 Fetching form questions...")
    header_to_qid = fetch_all_questions(session)

    new_headers: List[str] = []
    for col_name in header_to_qid:
        if col_name not in existing_headers:
            existing_headers.append(col_name)
            new_headers.append(col_name)

    sheet.update("A1", [existing_headers])

    if new_headers:
        print(f"➕ Added new columns: {new_headers}")

    # FIX 4: Use sheet.clear() + re-write header rather than batch_clear with
    #         row_count, which may be smaller than the actual number of data rows.
    sheet.clear()
    sheet.update("A1", [existing_headers])

    print("🧹 Old data cleared (values only), header preserved")
    print("🚀 Fetching and writing submissions...")

    offset = 0
    fetched = 0
    written = 0

    while fetched < TOTAL_LIMIT:
        submissions = fetch_submissions(session, offset=offset, limit=PAGE_SIZE)
        if not submissions:
            break

        page_rows = []

        for sub in submissions:
            row_data = {
                "Submission ID": str(sub.get("id", "") or ""),
                "Submission Date": str(sub.get("created_at", "") or ""),
                "Last Update Date": str(sub.get("updated_at", "") or ""),
                "Approval Status": str(
                    sub.get("workflowStatus")
                    or sub.get("workflow_status")
                    or sub.get("status")
                    or ""
                ),
            }

            answers = sub.get("answers", {}) or {}
            for header, qid in header_to_qid.items():
                meta = answers.get(qid, {}) or {}
                row_data[header] = normalize_answer(meta.get("answer", ""))

            page_rows.append([row_data.get(h, "") for h in existing_headers])
            fetched += 1

            if fetched >= TOTAL_LIMIT:
                break

        for batch in chunked(page_rows, WRITE_BATCH_SIZE):
            append_rows_with_retry(sheet, batch)
            written += len(batch)

        print(f"✔ Pulled {fetched} submissions | ✔ Wrote {written} rows")

        # Only sleep if there are more pages to fetch
        if fetched < TOTAL_LIMIT and len(submissions) == PAGE_SIZE:
            time.sleep(SLEEP_BETWEEN_FETCH_CALLS)

        offset += PAGE_SIZE

    print(f"✅ DONE — {written} rows written successfully")


if __name__ == "__main__":
    main()
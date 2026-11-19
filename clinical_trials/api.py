"""ClinicalTrials.gov API v2 client and bronze landing."""
import gzip
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://clinicaltrials.gov/api/v2"
RETRYABLE = {429, 500, 502, 503, 504}
WAREHOUSE_FIELDS = (
    "NCTId", "BriefTitle", "StudyType", "OverallStatus", "LeadSponsorName",
    "StartDate", "StartDateType", "CompletionDate", "CompletionDateType",
    "LastUpdatePostDate", "Phase", "Condition", "InterventionType",
    "InterventionName", "LocationFacility", "LocationCity", "LocationState",
    "LocationZip", "LocationCountry",
)


class Client:
    def __init__(self, delay=1.0, attempts=5):
        self.delay = delay
        self.attempts = attempts
        self.next_request = 0.0

    def get(self, endpoint, params=None):
        query = "?" + urlencode(params) if params else ""
        request = Request(
            f"{BASE_URL}/{endpoint}{query}",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip",
                     "User-Agent": "LocalClinicalTrialsPipeline/1.0"},
        )
        for attempt in range(self.attempts):
            time.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + self.delay
            retry_after = None
            try:
                with urlopen(request, timeout=45) as response:
                    if getattr(response, "headers", {}).get("Content-Encoding") == "gzip":
                        with gzip.GzipFile(fileobj=response) as compressed:
                            return json.load(compressed)
                    return json.load(response)
            except HTTPError as error:
                if error.code not in RETRYABLE or attempt == self.attempts - 1:
                    raise
                retry_after = error.headers.get("Retry-After")
            except (URLError, TimeoutError):
                if attempt == self.attempts - 1:
                    raise
            backoff = 2 ** attempt
            if retry_after:
                try:
                    wait = int(retry_after)
                except ValueError:
                    try:
                        retry_at = parsedate_to_datetime(retry_after)
                        wait = (retry_at - datetime.now(timezone.utc)).total_seconds()
                    except (TypeError, ValueError):
                        wait = 0
                backoff = max(backoff, wait)
            self.next_request = max(self.next_request, time.monotonic() + backoff)

    def snapshot(self):
        return self.get("version")["dataTimestamp"]

    def registry_count(self):
        return self.get("studies", {"pageSize": 1, "fields": "NCTId", "countTotal": "true"})["totalCount"]

    def extract(self, condition, lower, upper, page_size, directory):
        params = {
            "format": "json",
            "pageSize": page_size,
        }
        if condition is not None:
            params["query.cond"] = condition
        else:
            params["fields"] = ",".join(WAREHOUSE_FIELDS)
        if condition is not None or lower != "MIN":
            params["filter.advanced"] = f"AREA[LastUpdatePostDate]RANGE[{lower},{upper}]"
        pages = 0
        seen_tokens = set()
        while True:
            pages += 1
            page = directory / f"page-{pages:06}.json"
            cached = page.is_file()
            payload = json.loads(page.read_text(encoding="utf-8")) if cached else self.get("studies", params)
            if not isinstance(payload.get("studies"), list):
                raise ValueError("Expected a studies array")
            token = payload.get("nextPageToken")
            if token in seen_tokens:
                raise ValueError("API returned a repeated pagination token")
            if not cached:
                temporary = page.with_suffix(".tmp")
                temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
                temporary.replace(page)
            if not token:
                return pages
            seen_tokens.add(token)
            params["pageToken"] = token

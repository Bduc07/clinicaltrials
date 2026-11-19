import json
from datetime import date
from io import BytesIO
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import duckdb
import pytest

from clinical_trials import api
from clinical_trials.pipeline import REPORTS, benchmark, cohort_dir, load_cached, report, sync

CONDITION = "Type 1 Diabetes"


def study(nct_id, status, updated, conditions=("Type 1 Diabetes",)):
    return {
        "protocolSection": {
            "identificationModule": {"nctId": nct_id, "briefTitle": "Synthetic trial"},
            "statusModule": {
                "overallStatus": status,
                "lastUpdatePostDateStruct": {"date": updated},
                "startDateStruct": {"date": "2025-01-01", "type": "ACTUAL"},
                "completionDateStruct": {"date": "2025-02-01", "type": "ACTUAL"},
            },
            "designModule": {"studyType": "INTERVENTIONAL", "phases": ["PHASE1", "PHASE2"]},
            "conditionsModule": {"conditions": list(conditions)},
            "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Example Sponsor"}},
            "armsInterventionsModule": {"interventions": [{"name": "Example", "type": "OTHER"}]},
            "contactsLocationsModule": {"locations": [{"city": "Boston", "country": "United States"}]},
        }
    }


def fake_api(monkeypatch, timestamp, pages):
    requests = []

    def open_request(request, timeout):
        url = urlsplit(request.full_url)
        if url.path.endswith("/version"):
            payload = {"dataTimestamp": timestamp}
        else:
            params = parse_qs(url.query)
            requests.append(params)
            payload = pages[int(params.get("pageToken", ["0"])[0])]
        return BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr(api, "urlopen", open_request)
    return requests


def test_pipeline_keeps_history_and_replaces_bridges(tmp_path, monkeypatch):
    first = study("NCT00000001", "RECRUITING", "2025-01-01", ("Type 1 Diabetes", "Old label"))
    second = study("NCT00000002", "RECRUITING", "2025-01-01")
    second["protocolSection"]["statusModule"]["startDateStruct"]["date"] = "2025-01"
    requests = fake_api(monkeypatch, "2025-01-02T10:00:00", [
        {"studies": [first], "nextPageToken": "1"},
        {"studies": [second]},
    ])
    client = api.Client(delay=0)
    initial = sync(client, tmp_path, CONDITION, page_size=1, since=date(2025, 1, 1))
    assert initial["pages"] == initial["studies"] == 2
    assert requests[1]["pageToken"] == ["1"]
    assert requests[0]["filter.advanced"] == ["AREA[LastUpdatePostDate]RANGE[2025-01-01,2025-01-02]"]
    assert sync(client, tmp_path, CONDITION)["skipped"]
    assert len(requests) == 2

    changed = study("NCT00000001", "COMPLETED", "2025-02-03")
    requests = fake_api(monkeypatch, "2025-02-03T10:00:00", [{"studies": [changed]}])
    next_run = sync(client, tmp_path, CONDITION)
    assert requests[0]["filter.advanced"] == ["AREA[LastUpdatePostDate]RANGE[2025-01-02,2025-02-03]"]
    cohort = cohort_dir(tmp_path, CONDITION)
    assert (cohort / "bronze" / initial["run_id"] / "manifest.json").is_file()
    parquet = cohort / "silver" / next_run["run_id"] / "trial.parquet"
    with duckdb.connect(str(cohort / "trials.duckdb"), read_only=True) as con:
        assert con.read_parquet(str(parquet)).count("*").fetchone() == (1,)
        assert con.execute(
            "SELECT status, valid_to IS NULL FROM dim_trial_status WHERE nct_id = ? ORDER BY valid_from",
            ["NCT00000001"],
        ).fetchall() == [("RECRUITING", False), ("COMPLETED", True)]

    outputs = {name: report(tmp_path, CONDITION, name)["rows"] for name in REPORTS}
    assert all(outputs.values())
    assert {row["condition_name"] for row in outputs["trials_by_phase_condition_year"]} == {CONDITION}
    assert {row["trials"] for row in outputs["trials_by_phase_condition_year"]} == {2}
    assert outputs["average_duration_by_phase"][0]["average_days"] == 31
    assert outputs["top_sponsors_by_condition"][0]["sponsor_rank"] == 1
    assert outputs["monthly_status_changes"][0]["trials_changed"] == 1
    assert outputs["recruiting_by_condition"] == [{"condition_name": CONDITION, "recruiting_trials": 1}]

    fake_api(monkeypatch, "2025-02-04T10:00:00", [{"studies": [changed]}])
    sync(client, tmp_path, CONDITION)
    fake_api(monkeypatch, "2025-02-05T10:00:00", [{"studies": []}])
    assert sync(client, tmp_path, CONDITION)["studies"] == 0
    assert report(tmp_path, CONDITION, "monthly_status_changes")["rows"] == outputs["monthly_status_changes"]


@pytest.mark.parametrize("error", ["duplicate", "phase", "status"])
def test_bad_snapshot_leaves_previous_watermark(tmp_path, monkeypatch, error):
    good = study("NCT00000001", "RECRUITING", "2025-01-01")
    fake_api(monkeypatch, "2025-01-02T10:00:00", [{"studies": [good]}])
    sync(api.Client(delay=0), tmp_path, CONDITION)

    invalid = study("NCT00000001", "COMPLETED", "2025-01-03")
    records = [invalid]
    if error == "duplicate":
        records.append(invalid)
    elif error == "phase":
        invalid["protocolSection"]["designModule"]["phases"] = ["INVALID"]
    else:
        invalid["protocolSection"]["statusModule"]["overallStatus"] = "INVALID"
    fake_api(monkeypatch, "2025-01-03T10:00:00", [{"studies": records}])
    with pytest.raises((ValueError, duckdb.ConstraintException)):
        sync(api.Client(delay=0), tmp_path, CONDITION)
    result = report(tmp_path, CONDITION, "recruiting_by_condition")
    assert result["data_timestamp"] == "2025-01-02T10:00:00"
    assert result["rows"][0]["recruiting_trials"] == 1


def test_source_refresh_during_download_does_not_publish(tmp_path, monkeypatch):
    fake_api(monkeypatch, "2025-01-02T10:00:00", [{"studies": []}])
    client = api.Client(delay=0)
    snapshots = iter(["2025-01-02T10:00:00", "2025-01-03T10:00:00"])
    monkeypatch.setattr(client, "snapshot", lambda: next(snapshots))
    with pytest.raises(RuntimeError, match="refreshed during extraction"):
        sync(client, tmp_path, CONDITION)
    with pytest.raises(ValueError, match="successful sync"):
        report(tmp_path, CONDITION, "recruiting_by_condition")


def test_full_registry_cache_and_silver_parity(tmp_path, monkeypatch):
    pytest.importorskip("pyspark")
    record = study("NCT00000001", "RECRUITING", "2025-01-01", (CONDITION, CONDITION, "Other"))
    requests = fake_api(monkeypatch, "2025-01-02T10:00:00", [{"studies": [record]}])
    client = api.Client(delay=0)
    monkeypatch.setattr(client, "registry_count", lambda: 1)
    result = sync(client, tmp_path, None, bronze_only=True)
    assert "query.cond" not in requests[0] and "filter.advanced" not in requests[0]
    assert "NCTId" in requests[0]["fields"][0]
    assert sync(client, tmp_path, None, bronze_only=True) == result
    assert len(requests) == 1

    bronze = cohort_dir(tmp_path, None) / "bronze" / result["run_id"]
    stats = benchmark(bronze)
    assert stats["identical_rows"] == {"trial": 1, "phase": 2, "condition": 2, "intervention": 1, "location": 1}
    loaded = load_cached(tmp_path, None, bronze, "spark")
    assert loaded["studies"] == 1
    assert report(tmp_path, None, "recruiting_by_condition")["rows"][0]["recruiting_trials"] == 1


def test_capture_resumes_from_last_saved_page(tmp_path, monkeypatch):
    client = api.Client(delay=0)
    first = {"studies": [study("NCT00000001", "RECRUITING", "2025-01-01")], "nextPageToken": "1"}

    def interrupted_request(endpoint, params):
        if params.get("pageToken"):
            raise TimeoutError("second page timed out")
        return first

    with monkeypatch.context() as patch:
        patch.setattr(client, "get", interrupted_request)
        with pytest.raises(TimeoutError):
            client.extract(None, "MIN", "2025-01-02", 1000, tmp_path)

    requests = fake_api(monkeypatch, "2025-01-02T10:00:00", [first, {"studies": []}])
    assert client.extract(None, "MIN", "2025-01-02", 1000, tmp_path) == 2
    assert [request["pageToken"] for request in requests] == [["1"]]


def test_client_honors_retry_after(monkeypatch):
    delays = []
    attempts = iter([HTTPError("https://clinicaltrials.gov", 429, "Throttled", {"Retry-After": "7"}, None),
                     BytesIO(b'{"dataTimestamp":"2025-01-02T10:00:00"}')])

    def open_request(request, timeout):
        response = next(attempts)
        if isinstance(response, HTTPError):
            raise response
        return response

    monkeypatch.setattr(api, "urlopen", open_request)
    monkeypatch.setattr(api.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(api.time, "sleep", delays.append)
    assert api.Client().snapshot() == "2025-01-02T10:00:00"
    assert delays == [0, 7]

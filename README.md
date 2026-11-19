# ClinicalTrials.gov data pipeline

A local analytics warehouse for public study records. Python downloads API pages, PySpark flattens nested JSON into Parquet, and DuckDB loads a dimensional model with status history and serves analytical SQL. The project runs on a laptop with free software and free source data.

## Run it

On Windows, use Docker Compose to run Spark in Linux. The Compose setup follows the claims pipeline's pattern of installing Java in the image and mounting the data directory from the host. Each command below runs from the project directory.

```powershell
docker compose build trials
docker compose run --rm trials --all sync --bronze-only
docker compose run --rm trials --all sync
docker compose run --rm trials --all report average_duration_by_phase
```

On macOS or Linux with Python 3.13 and Java 17, install dependencies with `python -m pip install -r requirements-spark.txt -r requirements-dev.txt`. Replace `docker compose run --rm trials` with `python -m clinical_trials` and use `python -m pytest -q` for tests. Use `--engine duckdb` on `sync` or `load` to run only the DuckDB runtime. Reports require DuckDB alone.

The `--all` cohort covers every study and requests the nested fields used by the warehouse. Bronze therefore contains an API projection for every record, with the API's nesting preserved. The first capture takes several minutes because requests are paced and serial. Restarting the same capture with the same page size reuses saved pages while the source snapshot is stable. A completed capture prints its directory. To process it without an API call, use `--all load <bronze-directory>`. Docker paths are relative to `/app`, so a host batch under `data/registry/bronze/` has the same relative path inside the container.

For small development runs, the default condition search is `Type 1 Diabetes` and uses DuckDB silver. Add `--engine spark` to exercise Spark on a condition search. `--since` limits the first condition bootstrap by last-update date. The registry can return related studies. Reports group by the condition labels actually listed on each study. Set `--condition` and `--data-dir` before the subcommand, for example:

```powershell
docker compose run --rm trials --condition "Asthma" --data-dir data/asthma sync --since 2026-11-19
```

## Data layers

- **Bronze** stores API response pages as JSON and a manifest with the source refresh timestamp, query scope, date bounds, and page count.
- **Silver** uses an explicit Spark schema and `explode` to turn nested arrays into five Snappy Parquet datasets. The trial dataset preserves the original start and completion date text alongside parsed values. Conditions, phases, interventions, and sites are deduplicated within each trial. The lead sponsor is a single nested object and is extracted directly.
- **Gold** loads those Parquet files into `trials.duckdb`. The fact has one row per NCT ID. Lead sponsors and dates are dimensions, and bridges connect trials to multiple phases, conditions, interventions, and locations. Composite dimension keys reflect published names and site fields. Blank site key components represent unspecified fields. The status dimension records observed versions with half-open intervals, `[valid_from, valid_to)`. A null end means the version is current.

Condition searches get a directory under `data/` named by a hash of the expression. The full registry lives in `data/registry/`. Successful `pipeline_runs` rows identify committed batches; a failed batch can leave diagnostic files on disk. A registry bootstrap verifies its row count against the API's total before committing. The warehouse and watermark advance together after validation. The next refresh re-reads the prior boundary date so updates on that date are included. Pagination is serial with a one-second minimum request interval, exponential backoff on transient failures, and support for `Retry-After`.

## Silver benchmark

```powershell
$bronze = "data/registry/bronze/YOUR_RUN_ID"
docker compose run --rm trials benchmark $bronze
```

The command times the same cached JSON-to-Snappy-Parquet transformation in DuckDB and PySpark. It reports bronze bytes and verifies exact row parity, including duplicate multiplicity, across all five output tables. Timings include Spark startup and Parquet writes. Download, parity verification, and gold loading are outside the timed interval.

One local run used the ClinicalTrials.gov snapshot processed at `2026-11-19T10:00:07`. It contained **612,751 studies** in **613 pages** of projected JSON (**831.57 MiB**). Both engines used four local workers on a Windows laptop with an Intel Core i7-13650HX. DuckDB ran first. Spark used Java 17 and a local Hadoop filesystem adapter for Windows.

| Silver engine | Elapsed |
| --- | ---: |
| DuckDB 1.5.6 | 25.41 s |
| PySpark 4.0.3 | 107.38 s |

All five outputs matched exactly, including 3,564,238 trial-location rows. Spark's JVM startup, separate write jobs, and cache spill to disk contributed overhead. DuckDB was about 4.2× faster in this run and is sufficient for this workload on one machine. Spark still handles a substantive silver workload here by parsing the pages, expanding nested arrays, and deduplicating the resulting rows.

## SQL reports

Run `python -m clinical_trials report <name>` using the same cohort options as the sync. Each result includes the source name, cohort, and source refresh timestamp.

| Report name | Definition |
| --- | --- |
| `trials_by_phase_condition_year` | Counts distinct trial membership in each recorded phase, condition, and start year. A trial can count in multiple groups. |
| `average_duration_by_phase` | Mean days from actual start to actual completion among completed trials with both dates known to the day. |
| `top_sponsors_by_condition` | Top five lead-sponsor ranks per condition using `RANK()`, with ties included. |
| `monthly_status_changes` | Counts unique trials with an observed transition and total observed transitions by source refresh month. Initial observations are excluded. |
| `recruiting_by_condition` | Counts currently recruiting trials by their recorded condition labels. |

Status history starts with the first successful observation. The latest-record endpoint can omit intermediate changes between runs. Trials that disappear from the source or stop matching the search remain at their last observed version. A full rebuild in a fresh data directory gives a new current cohort baseline.

The offline suite uses synthetic study records to exercise pagination, caching, inclusive boundaries, empty batches, all reports, status changes, `Retry-After`, and rollback on duplicate IDs or invalid phases and statuses. Its Spark integration test checks exact silver parity and loads Spark Parquet into DuckDB gold. GitHub Actions installs Java 17 and PySpark and runs this suite on pushes and pull requests.

## Source and terms

Data are provided by ClinicalTrials.gov, a service of the U.S. National Library of Medicine. Review the official API documentation and terms before publishing derived data. The terms call for attribution, keeping distributed data current, displaying the source processing date, and describing modifications. These transformations select protocol metadata, flatten repeated fields, trim labels, deduplicate trial links, parse dates, and derive history and aggregates. Generated data stays local and is ignored by Git. [ClinicalTrials.gov API v2](https://clinicaltrials.gov/data-api/api) · [Study data fields](https://clinicaltrials.gov/data-api/about-api/study-data-structure) · [Terms and Conditions](https://clinicaltrials.gov/terms-conditions)

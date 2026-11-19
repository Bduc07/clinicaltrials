"""Bronze capture, Parquet staging, and transactional gold loading."""
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
BRIDGES = ("phase", "condition", "intervention", "location")
TABLES = ("trial", *BRIDGES)
REPORTS = (
    "trials_by_phase_condition_year",
    "average_duration_by_phase",
    "top_sponsors_by_condition",
    "monthly_status_changes",
    "recruiting_by_condition",
)


def read_sql(name):
    return (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


def cohort_dir(root, condition):
    if condition is None:
        return root / "registry"
    slug = hashlib.sha256(condition.encode("utf-8")).hexdigest()[:20]
    return root / slug


def write_duckdb_silver(pages, silver):
    silver.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(config={"threads": 4}) as con:
        con.execute(
            """
            CREATE TEMP TABLE raw_study AS
            SELECT (unnest(studies)).protocolSection AS protocol
            FROM read_json(?, columns = {studies: 'STRUCT(protocolSection STRUCT(
                identificationModule STRUCT(nctId VARCHAR, briefTitle VARCHAR),
                designModule STRUCT(studyType VARCHAR, phases VARCHAR[]),
                statusModule STRUCT(
                    overallStatus VARCHAR,
                    startDateStruct STRUCT("date" VARCHAR, "type" VARCHAR),
                    completionDateStruct STRUCT("date" VARCHAR, "type" VARCHAR),
                    lastUpdatePostDateStruct STRUCT("date" VARCHAR)),
                sponsorCollaboratorsModule STRUCT(leadSponsor STRUCT(name VARCHAR)),
                conditionsModule STRUCT(conditions VARCHAR[]),
                armsInterventionsModule STRUCT(interventions STRUCT("type" VARCHAR, name VARCHAR)[]),
                contactsLocationsModule STRUCT(locations STRUCT(
                    facility VARCHAR, city VARCHAR, state VARCHAR, zip VARCHAR, country VARCHAR)[])
            ))[]'}, maximum_object_size = 268435456)
            """,
            [[str(page) for page in pages]],
        )
        con.execute(read_sql("silver"))
        for name in TABLES:
            con.table(f"stg_{name}").write_parquet(str(silver / f"{name}.parquet"), compression="snappy")


def transform(engine, bronze, manifest, silver):
    pages = [bronze / f"page-{index:06}.json" for index in range(1, manifest["pages"] + 1)]
    if engine == "spark":
        from .spark_silver import write_silver

        write_silver(pages, silver)
    else:
        write_duckdb_silver(pages, silver)


def parquet_path(engine, silver, name):
    return str(silver / name / "*.parquet" if engine == "spark" else silver / f"{name}.parquet")


def publish(con, engine, silver, manifest):
    snapshot = datetime.fromisoformat(manifest["data_timestamp"]).replace(tzinfo=None)
    con.begin()
    try:
        for name in TABLES:
            con.execute(
                f"CREATE TEMP TABLE stg_{name} AS SELECT * FROM read_parquet(?)",
                [parquet_path(engine, silver, name)],
            )
        count, distinct_ids = con.execute(
            "SELECT count(*), count(DISTINCT nct_id) FROM stg_trial"
        ).fetchone()
        if count != distinct_ids:
            raise ValueError("Bronze snapshot contains a missing or duplicate NCT ID")
        if count != manifest.get("expected_studies", count):
            raise ValueError(f"Registry reported {manifest['expected_studies']} studies, captured {count}")

        con.execute("CREATE TEMP TABLE run_context (source_timestamp TIMESTAMP)")
        con.execute("INSERT INTO run_context VALUES (?)", [snapshot])
        con.execute(read_sql("load"))
        for name in BRIDGES:
            con.execute(f"DELETE FROM bridge_trial_{name} WHERE nct_id IN (SELECT nct_id FROM stg_trial)")
            con.execute(f"INSERT INTO bridge_trial_{name} SELECT * FROM stg_{name}")

        issues = con.execute(read_sql("quality")).fetchall()
        if issues:
            raise ValueError(f"Gold quality checks failed: {issues}")

        con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?)",
            [manifest["run_id"], snapshot, manifest["pages"], count],
        )
        con.commit()
        return count
    except BaseException:
        con.rollback()
        raise


def load_batch(con, cohort, bronze, manifest, engine):
    silver = cohort / "silver" / manifest["run_id"]
    transform(engine, bronze, manifest, silver)
    count = publish(con, engine, silver, manifest)
    return {**manifest, "studies": count, "silver_engine": engine}


def sync(client, root, condition, page_size=100, since=None, bronze_only=False, engine="duckdb"):
    if condition is None and since is not None:
        raise ValueError("The full registry must start from an unrestricted snapshot")
    cohort = cohort_dir(root, condition)
    cohort.mkdir(parents=True, exist_ok=True)
    # The writable DuckDB connection prevents a second process from writing this cohort.
    with duckdb.connect(str(cohort / "trials.duckdb")) as con:
        con.execute(read_sql("schema"))
        watermark = con.execute("SELECT max(source_timestamp) FROM pipeline_runs").fetchone()[0]
        source_timestamp = client.snapshot()
        snapshot = datetime.fromisoformat(source_timestamp).replace(tzinfo=None)
        manifest = {"source": "ClinicalTrials.gov", "condition": condition, "data_timestamp": source_timestamp}
        if watermark is not None and snapshot <= watermark:
            return {**manifest, "skipped": True, "reason": "snapshot already processed or older"}
        if watermark is None and since is not None and since > snapshot.date():
            raise ValueError("Bootstrap date is after the source snapshot date")

        lower = watermark.date().isoformat() if watermark else (since.isoformat() if since else "MIN")
        upper = snapshot.date().isoformat()
        run_id = hashlib.sha256(f"{source_timestamp}|{lower}|{page_size}".encode()).hexdigest()[:24]
        bronze = cohort / "bronze" / run_id
        bronze.mkdir(parents=True, exist_ok=True)
        manifest_path = bronze / "manifest.json"
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        else:
            if condition is None and lower == "MIN":
                manifest["expected_studies"] = client.registry_count()
            pages = client.extract(condition, lower, upper, page_size, bronze)
            if client.snapshot() != source_timestamp:
                raise RuntimeError("ClinicalTrials.gov refreshed during extraction. Retry against the new snapshot.")
            manifest.update(run_id=run_id, lower_date=lower, upper_date=upper, pages=pages)
            pending = manifest_path.with_suffix(".tmp")
            pending.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            pending.replace(manifest_path)
        if bronze_only:
            return {**manifest, "bronze": str(bronze)}
        return load_batch(con, cohort, bronze, manifest, engine)


def load_cached(root, condition, bronze, engine):
    manifest = json.loads((bronze / "manifest.json").read_text(encoding="utf-8"))
    if manifest["condition"] != condition:
        raise ValueError("Cached batch belongs to a different condition or registry scope")
    cohort = cohort_dir(root, condition)
    cohort.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(cohort / "trials.duckdb")) as con:
        con.execute(read_sql("schema"))
        watermark = con.execute("SELECT max(source_timestamp) FROM pipeline_runs").fetchone()[0]
        snapshot = datetime.fromisoformat(manifest["data_timestamp"]).replace(tzinfo=None)
        if watermark is not None and snapshot <= watermark:
            return {**manifest, "skipped": True, "reason": "snapshot already processed or older"}
        lower = manifest["lower_date"]
        if watermark is not None and lower != "MIN" and lower > watermark.date().isoformat():
            raise ValueError("Cached batch would leave a gap in the warehouse")
        if watermark is None and condition is None and lower != "MIN":
            raise ValueError("The full registry must start from an unrestricted snapshot")
        return load_batch(con, cohort, bronze, manifest, engine)


def benchmark(bronze):
    manifest = json.loads((bronze / "manifest.json").read_text(encoding="utf-8"))
    timings = {}
    row_counts = {}
    with TemporaryDirectory(prefix="benchmark-", dir=bronze.parent.parent) as temporary:
        workspace = Path(temporary)
        for engine in ("duckdb", "spark"):
            start = time.perf_counter()
            transform(engine, bronze, manifest, workspace / engine)
            timings[engine] = round(time.perf_counter() - start, 2)
        with duckdb.connect(config={"threads": 4}) as con:
            for name in TABLES:
                paths = [parquet_path(engine, workspace / engine, name) for engine in ("duckdb", "spark")]
                row_counts[name] = con.execute("SELECT count(*) FROM read_parquet(?)", [paths[0]]).fetchone()[0]
                different = con.execute(
                    """
                    SELECT EXISTS (
                        SELECT * EXCLUDE (row_delta)
                        FROM (
                            SELECT *, 1 AS row_delta FROM read_parquet(?)
                            UNION ALL
                            SELECT *, -1 AS row_delta FROM read_parquet(?)
                        ) AS combined
                        GROUP BY ALL
                        HAVING sum(row_delta) <> 0
                    )
                    """,
                    paths,
                ).fetchone()[0]
                if different:
                    raise ValueError(f"Benchmark output differs for the {name} table")
    size = sum((bronze / f"page-{i:06}.json").stat().st_size for i in range(1, manifest["pages"] + 1))
    return {**manifest, "bronze_bytes": size, "seconds": timings, "identical_rows": row_counts}


def report(root, condition, name):
    database = cohort_dir(root, condition) / "trials.duckdb"
    with duckdb.connect(str(database), read_only=True) as con:
        snapshot = con.execute("SELECT max(source_timestamp) FROM pipeline_runs").fetchone()[0]
        if snapshot is None:
            raise ValueError("Run a successful sync before generating reports")
        result = con.execute(read_sql(name))
        columns = [column[0] for column in result.description]
        return {
            "source": "ClinicalTrials.gov",
            "condition": condition,
            "data_timestamp": snapshot.isoformat(),
            "report": name,
            "rows": [dict(zip(columns, row)) for row in result.fetchall()],
        }

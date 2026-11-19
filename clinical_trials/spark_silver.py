"""Transform ClinicalTrials.gov API pages with Spark SQL."""
from pyspark.sql import SparkSession, functions as F

PAGE_SCHEMA = """
studies ARRAY<STRUCT<protocolSection: STRUCT<
    identificationModule: STRUCT<nctId: STRING, briefTitle: STRING>,
    designModule: STRUCT<studyType: STRING, phases: ARRAY<STRING>>,
    statusModule: STRUCT<
        overallStatus: STRING,
        startDateStruct: STRUCT<date: STRING, type: STRING>,
        completionDateStruct: STRUCT<date: STRING, type: STRING>,
        lastUpdatePostDateStruct: STRUCT<date: STRING>>,
    sponsorCollaboratorsModule: STRUCT<leadSponsor: STRUCT<name: STRING>>,
    conditionsModule: STRUCT<conditions: ARRAY<STRING>>,
    armsInterventionsModule: STRUCT<interventions: ARRAY<STRUCT<type: STRING, name: STRING>>>,
    contactsLocationsModule: STRUCT<locations: ARRAY<STRUCT<
        facility: STRING, city: STRING, state: STRING, zip: STRING, country: STRING>>>
>>>
"""


def create_session():
    return (
        SparkSession.builder.appName("ClinicalTrials Silver")
        .master("local[4]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def write_silver(pages, destination):
    spark = create_session()
    protocols = None
    try:
        protocols = (
            spark.read.schema(PAGE_SCHEMA).option("multiLine", "true").option("mode", "FAILFAST")
            .json([path.resolve().as_uri() for path in pages])
            .select(F.explode("studies").alias("study"))
            .select("study.protocolSection.*")
            .withColumn("nct_id", F.col("identificationModule.nctId"))
            .cache()
        )

        start = F.col("statusModule.startDateStruct.date")
        end = F.col("statusModule.completionDateStruct.date")
        lead = F.trim(F.col("sponsorCollaboratorsModule.leadSponsor.name"))
        trial = protocols.select(
            F.trim("nct_id").alias("nct_id"),
            F.col("identificationModule.briefTitle").alias("title"),
            F.col("designModule.studyType").alias("study_type"),
            F.col("statusModule.overallStatus").alias("status"),
            F.when(lead != "", lead).alias("sponsor_name"),
            start.alias("start_text"),
            end.alias("completion_text"),
            F.expr("try_cast(left(statusModule.startDateStruct.date, 4) AS INT)").alias("start_year"),
            F.when(F.length(start) == 10, F.to_date(start)).alias("start_date"),
            F.col("statusModule.startDateStruct.type").alias("start_date_type"),
            F.when(F.length(end) == 10, F.to_date(end)).alias("completion_date"),
            F.col("statusModule.completionDateStruct.type").alias("completion_date_type"),
            F.to_date("statusModule.lastUpdatePostDateStruct.date").alias("updated_on"),
        )
        phase = protocols.select("nct_id", F.explode("designModule.phases").alias("phase"))
        condition = protocols.select(
            "nct_id", F.explode("conditionsModule.conditions").alias("condition_name")
        ).withColumn("condition_name", F.trim("condition_name")).where(F.col("condition_name") != "")
        intervention = (
            protocols.select("nct_id", F.explode("armsInterventionsModule.interventions").alias("item"))
            .select(
                "nct_id",
                F.coalesce("item.type", F.lit("")).alias("intervention_type"),
                F.trim("item.name").alias("intervention_name"),
            )
            .where(F.col("intervention_name") != "")
        )
        location_fields = {
            "facility": "facility", "city": "city", "state": "state", "zip": "postal_code", "country": "country"
        }
        location = protocols.select(
            "nct_id", F.explode("contactsLocationsModule.locations").alias("item")
        ).select("nct_id", *[
            F.coalesce(F.trim(F.col(f"item.{source}")), F.lit("")).alias(target)
            for source, target in location_fields.items()
        ])
        targets = {
            "trial": trial, "phase": phase.distinct(), "condition": condition.distinct(),
            "intervention": intervention.distinct(), "location": location.distinct(),
        }
        for name, dataframe in targets.items():
            dataframe.write.mode("overwrite").option("compression", "snappy").parquet(
                (destination / name).resolve().as_uri()
            )
    finally:
        if protocols is not None:
            protocols.unpersist()
        spark.stop()

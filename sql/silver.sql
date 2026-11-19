CREATE TEMP TABLE stg_trial AS
WITH fields AS (
    SELECT
        trim(protocol.identificationModule.nctId) AS nct_id,
        protocol.identificationModule.briefTitle AS title,
        protocol.designModule.studyType AS study_type,
        protocol.statusModule.overallStatus AS status,
        nullif(trim(protocol.sponsorCollaboratorsModule.leadSponsor.name), '') AS sponsor_name,
        protocol.statusModule.startDateStruct."date" AS start_text,
        protocol.statusModule.startDateStruct."type" AS start_date_type,
        protocol.statusModule.completionDateStruct."date" AS completion_text,
        protocol.statusModule.completionDateStruct."type" AS completion_date_type,
        CAST(protocol.statusModule.lastUpdatePostDateStruct."date" AS DATE) AS updated_on
    FROM raw_study
)
SELECT nct_id, title, study_type, status, sponsor_name, start_text, completion_text,
    try_cast(left(start_text, 4) AS INTEGER) AS start_year,
    CASE WHEN length(start_text) = 10 THEN CAST(start_text AS DATE) END AS start_date,
    start_date_type,
    CASE WHEN length(completion_text) = 10 THEN CAST(completion_text AS DATE) END AS completion_date,
    completion_date_type, updated_on
FROM fields;

CREATE TEMP TABLE stg_phase AS
SELECT DISTINCT protocol.identificationModule.nctId AS nct_id,
    unnest(protocol.designModule.phases) AS phase
FROM raw_study;

CREATE TEMP TABLE stg_condition AS
WITH expanded AS (
    SELECT protocol.identificationModule.nctId AS nct_id,
        trim(unnest(protocol.conditionsModule.conditions)) AS condition_name
    FROM raw_study
)
SELECT DISTINCT nct_id, condition_name
FROM expanded
WHERE condition_name <> '';

CREATE TEMP TABLE stg_intervention AS
WITH expanded AS (
    SELECT protocol.identificationModule.nctId AS nct_id,
        unnest(protocol.armsInterventionsModule.interventions) AS item
    FROM raw_study
)
SELECT DISTINCT nct_id, coalesce(item."type", '') AS intervention_type,
    trim(item.name) AS intervention_name
FROM expanded
WHERE trim(item.name) <> '';

CREATE TEMP TABLE stg_location AS
WITH expanded AS (
    SELECT protocol.identificationModule.nctId AS nct_id,
        unnest(protocol.contactsLocationsModule.locations) AS item
    FROM raw_study
)
SELECT DISTINCT nct_id, coalesce(trim(item.facility), '') AS facility,
    coalesce(trim(item.city), '') AS city,
    coalesce(trim(item.state), '') AS state,
    coalesce(trim(item.zip), '') AS postal_code,
    coalesce(trim(item.country), '') AS country
FROM expanded;

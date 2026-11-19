INSERT OR IGNORE INTO dim_trial
SELECT nct_id FROM stg_trial;

INSERT OR IGNORE INTO dim_date
SELECT DISTINCT value, year(value), month(value), day(value)
FROM stg_trial
CROSS JOIN LATERAL (VALUES (start_date), (completion_date), (updated_on)) AS dates(value)
WHERE value IS NOT NULL;

INSERT OR IGNORE INTO dim_sponsor
SELECT DISTINCT sponsor_name FROM stg_trial WHERE sponsor_name IS NOT NULL;

INSERT OR IGNORE INTO dim_condition SELECT DISTINCT condition_name FROM stg_condition;
INSERT OR IGNORE INTO dim_intervention SELECT DISTINCT intervention_type, intervention_name FROM stg_intervention;
INSERT OR IGNORE INTO dim_location SELECT DISTINCT facility, city, state, postal_code, country FROM stg_location;

INSERT INTO fact_trial BY NAME
SELECT nct_id, title, study_type, status, sponsor_name, start_year, start_date, start_date_type,
       completion_date, completion_date_type, updated_on
FROM stg_trial
ON CONFLICT (nct_id) DO UPDATE SET
    title = excluded.title,
    study_type = excluded.study_type,
    status = excluded.status,
    sponsor_name = excluded.sponsor_name,
    start_year = excluded.start_year,
    start_date = excluded.start_date,
    start_date_type = excluded.start_date_type,
    completion_date = excluded.completion_date,
    completion_date_type = excluded.completion_date_type,
    updated_on = excluded.updated_on;

UPDATE dim_trial_status AS history
SET valid_to = run.source_timestamp
FROM stg_trial AS incoming CROSS JOIN run_context AS run
WHERE history.nct_id = incoming.nct_id AND history.valid_to IS NULL
    AND history.status <> incoming.status;

INSERT INTO dim_trial_status
SELECT incoming.nct_id, incoming.status, run.source_timestamp, NULL, incoming.updated_on
FROM stg_trial AS incoming CROSS JOIN run_context AS run
WHERE NOT EXISTS (
    SELECT 1 FROM dim_trial_status AS history
    WHERE history.nct_id = incoming.nct_id AND history.valid_to IS NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    source_timestamp TIMESTAMP NOT NULL UNIQUE,
    bronze_pages INTEGER NOT NULL,
    study_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sponsor (
    sponsor_name VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_condition (
    condition_name VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_intervention (
    intervention_type VARCHAR,
    intervention_name VARCHAR,
    PRIMARY KEY (intervention_type, intervention_name)
);

CREATE TABLE IF NOT EXISTS dim_location (
    facility VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    PRIMARY KEY (facility, city, state, postal_code, country)
);

CREATE TABLE IF NOT EXISTS dim_trial (
    nct_id VARCHAR PRIMARY KEY CHECK (regexp_full_match(nct_id, 'NCT[0-9]{8}'))
);

CREATE TABLE IF NOT EXISTS fact_trial (
    nct_id VARCHAR PRIMARY KEY REFERENCES dim_trial(nct_id),
    title VARCHAR,
    study_type VARCHAR,
    status VARCHAR NOT NULL CHECK (status IN (
        'ACTIVE_NOT_RECRUITING', 'COMPLETED', 'ENROLLING_BY_INVITATION',
        'NOT_YET_RECRUITING', 'RECRUITING', 'SUSPENDED', 'TERMINATED',
        'WITHDRAWN', 'AVAILABLE', 'NO_LONGER_AVAILABLE', 'TEMPORARILY_NOT_AVAILABLE',
        'APPROVED_FOR_MARKETING', 'WITHHELD', 'UNKNOWN'
    )),
    sponsor_name VARCHAR REFERENCES dim_sponsor(sponsor_name),
    start_year INTEGER,
    start_date DATE REFERENCES dim_date(date_key),
    start_date_type VARCHAR,
    completion_date DATE REFERENCES dim_date(date_key),
    completion_date_type VARCHAR,
    updated_on DATE NOT NULL REFERENCES dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS bridge_trial_phase (
    nct_id VARCHAR REFERENCES dim_trial(nct_id),
    phase VARCHAR CHECK (phase IN ('NA', 'EARLY_PHASE1', 'PHASE1', 'PHASE2', 'PHASE3', 'PHASE4')),
    PRIMARY KEY (nct_id, phase)
);

CREATE TABLE IF NOT EXISTS bridge_trial_condition (
    nct_id VARCHAR REFERENCES dim_trial(nct_id),
    condition_name VARCHAR REFERENCES dim_condition(condition_name),
    PRIMARY KEY (nct_id, condition_name)
);

CREATE TABLE IF NOT EXISTS bridge_trial_intervention (
    nct_id VARCHAR REFERENCES dim_trial(nct_id),
    intervention_type VARCHAR,
    intervention_name VARCHAR,
    FOREIGN KEY (intervention_type, intervention_name)
        REFERENCES dim_intervention(intervention_type, intervention_name),
    PRIMARY KEY (nct_id, intervention_type, intervention_name)
);

CREATE TABLE IF NOT EXISTS bridge_trial_location (
    nct_id VARCHAR REFERENCES dim_trial(nct_id),
    facility VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    FOREIGN KEY (facility, city, state, postal_code, country)
        REFERENCES dim_location(facility, city, state, postal_code, country),
    PRIMARY KEY (nct_id, facility, city, state, postal_code, country)
);

CREATE TABLE IF NOT EXISTS dim_trial_status (
    nct_id VARCHAR REFERENCES dim_trial(nct_id),
    status VARCHAR NOT NULL,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    source_update_date DATE NOT NULL REFERENCES dim_date(date_key),
    PRIMARY KEY (nct_id, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

WITH violations AS (
    SELECT 'current status missing, duplicated, or inconsistent' AS problem
    FROM fact_trial AS trial
    LEFT JOIN dim_trial_status AS history
        ON history.nct_id = trial.nct_id AND history.valid_to IS NULL
    GROUP BY trial.nct_id, trial.status
    HAVING count(history.nct_id) <> 1 OR max(history.status) <> trial.status
    UNION ALL
    SELECT 'history intervals are discontinuous or repeat the same status'
    FROM (
        SELECT status, valid_to,
               lead(valid_from) OVER versions AS next_start,
               lead(status) OVER versions AS next_status
        FROM dim_trial_status
        WINDOW versions AS (PARTITION BY nct_id ORDER BY valid_from)
    ) AS versions
    WHERE valid_to IS DISTINCT FROM next_start OR status = next_status
)
SELECT problem, count(*) AS affected FROM violations GROUP BY problem;

WITH versions AS (
    SELECT nct_id, valid_from,
           lag(status) OVER (PARTITION BY nct_id ORDER BY valid_from) AS previous_status
    FROM dim_trial_status
)
SELECT date_trunc('month', valid_from)::DATE AS observed_month,
       count(DISTINCT nct_id) AS trials_changed, count(*) AS transitions
FROM versions
WHERE previous_status IS NOT NULL
GROUP BY 1
ORDER BY 1;

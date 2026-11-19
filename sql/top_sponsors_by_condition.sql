WITH counts AS (
    SELECT condition.condition_name, trial.sponsor_name, count(*) AS trials
    FROM fact_trial AS trial
    JOIN bridge_trial_condition AS condition USING (nct_id)
    WHERE trial.sponsor_name IS NOT NULL
    GROUP BY ALL
), ranked AS (
    SELECT *, rank() OVER (PARTITION BY condition_name ORDER BY trials DESC) AS sponsor_rank
    FROM counts
)
SELECT * FROM ranked WHERE sponsor_rank <= 5
ORDER BY condition_name, sponsor_rank, sponsor_name;

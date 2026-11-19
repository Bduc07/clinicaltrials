SELECT coalesce(phase.phase, 'UNSPECIFIED') AS phase,
       count(*) AS trials,
       round(avg(date_diff('day', trial.start_date, trial.completion_date)), 1) AS average_days
FROM fact_trial AS trial
LEFT JOIN bridge_trial_phase AS phase USING (nct_id)
WHERE trial.status = 'COMPLETED'
    AND trial.start_date_type = 'ACTUAL' AND trial.completion_date_type = 'ACTUAL'
    AND trial.start_date <= trial.completion_date
GROUP BY 1
ORDER BY 1;

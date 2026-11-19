SELECT condition.condition_name, count(*) AS recruiting_trials
FROM fact_trial AS trial
JOIN bridge_trial_condition AS condition USING (nct_id)
WHERE trial.status = 'RECRUITING'
GROUP BY 1
ORDER BY recruiting_trials DESC, condition_name;

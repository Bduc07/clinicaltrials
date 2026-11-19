SELECT trial.start_year,
       coalesce(phase.phase, 'UNSPECIFIED') AS phase,
       condition.condition_name, count(*) AS trials
FROM fact_trial AS trial
JOIN bridge_trial_condition AS condition USING (nct_id)
LEFT JOIN bridge_trial_phase AS phase USING (nct_id)
WHERE trial.start_year IS NOT NULL
GROUP BY ALL
ORDER BY start_year, phase, trials DESC, condition_name;

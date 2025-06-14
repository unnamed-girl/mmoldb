DROP TABLE "custom.era";
CREATE TEMP TABLE "custom.era" AS 
select RUNS_ALLOWED.pitcher_name, allowed_runs, home_runs, innings_pitched, CASE WHEN innings_pitched < 0.1 then NULL ELSE ((allowed_runs + home_runs)/innings_pitched)*9 END AS era FROM
(
    (select pitcher_name, COUNT(*) as allowed_runs from data.events E INNER JOIN data.event_baserunners B ON E.id = B.event_id WHERE base_after = 1 GROUP BY pitcher_name) RUNS_ALLOWED
    INNER JOIN
    (select pitcher_name, SUM(outs_after - outs_before)/3.0 as innings_pitched from data.events GROUP BY pitcher_name) INNINGS_PITCHED
    ON RUNS_ALLOWED.pitcher_name = INNINGS_PITCHED.pitcher_name
)
INNER JOIN
(select pitcher_name, COUNT(*) as home_runs from data.events INNER JOIN taxa.event_type ON taxa.event_type.id = data.events.event_type WHERE name = 'HomeRun' GROUP BY pitcher_name) HOME_RUNS
ON RUNS_ALLOWED.pitcher_name = HOME_RUNS.pitcher_name
ORDER BY RUNS_ALLOWED.pitcher_name ASC;
\copy "custom.era" TO 'era.csv' DELIMITER ',' CSV HEADER;
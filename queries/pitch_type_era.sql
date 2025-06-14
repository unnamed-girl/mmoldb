DROP TABLE "temp_table";
CREATE TEMP TABLE "temp_table" AS 
select pitch_types, COUNT(PITCH_TYPES.pitcher_name) as count_pitchers, SUM(count_pitches) as count_pitches, AVG(era) FROM
(select pitcher_name, COUNT(DISTINCT name) AS pitch_types, COUNT(*) as count_pitches from data.events E INNER JOIN taxa.pitch_type P ON E.pitch_type = P.id GROUP BY pitcher_name) PITCH_TYPES
INNER JOIN
(SELECT * from "custom.era") ERA
ON PITCH_TYPES.pitcher_name = ERA.pitcher_name
WHERE innings_pitched > 20
GROUP BY pitch_types ORDER BY pitch_types DESC;
\copy "temp_table" TO 'pitch_types_era.csv' DELIMITER ',' CSV HEADER;
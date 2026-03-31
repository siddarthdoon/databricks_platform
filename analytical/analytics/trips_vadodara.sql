CREATE OR REPLACE VIEW poc_project.analytical.fact_trips_vadodara
AS (
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'GJ02'
);
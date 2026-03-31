CREATE VIEW poc_project.analytical.fact_trips_jaipur
AS
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'RJ01';
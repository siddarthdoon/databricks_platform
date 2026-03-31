CREATE VIEW poc_project.analytical.fact_trips_surat
AS
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'GJ01';
CREATE VIEW poc_project.analytical.fact_trips_kochi
AS
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'KL01';
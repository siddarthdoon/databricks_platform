CREATE VIEW poc_project.analytical.fact_trips_indore
AS
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'MP01';
CREATE VIEW poc_project.analytical.fact_trips_lucknow
AS
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'UP01';
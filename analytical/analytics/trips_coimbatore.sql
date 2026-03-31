CREATE VIEW poc_project.analytical.fact_trips_coimbatore
AS
SELECT *
FROM poc_project.analytical.fact_trips
WHERE city_id = 'TN01';
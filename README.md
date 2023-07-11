# DeMiniProject

SET UP AIRFLOW, POSTGRES and MONGODB local

run docker compose up

To acheive 1a, run ingest_uri_publish_jdbc DAG

To acheive 1b, run ingest_uri_publish_mongodb

SQL query for 2:
1. Top 10 from_station, to_station has the most bike rentals:

SELECT from_station_name, to_station_name

FROM trips

GROUP BY 1, 2

ORDER BY count(1) desc

LIMIT 10;

3. How many new bike rentals on 2019-05-16 and the running totals until that day of each
from_station?
**New bike rentals on 2019-05-16:**
SELECT count(1)
FROM trips
WHERE  start_time = '2019-05-16'

**--the running totals until 2019-05-16 of each from_station?**
SELECT
    from_station_name,
    COUNT(1) AS new_rentals,
    SUM(COUNT(1)) OVER (ORDER BY start_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM
    trips
WHERE
    DATE_TRUNC('day', start_time) = DATE '2019-05-16'
GROUP BY
    from_station_name, start_time::date
ORDER BY
    from_station_name

3. Calculate Day-over-Day (DoD), Month-over-Month (MoM) of bike rentals

**-- day over day**
SELECT
    DATE_TRUNC('day', start_time)::date AS rental_date,
    COUNT(1) AS rentals_total,
    LAG(COUNT(1)) OVER (ORDER BY DATE_TRUNC('day', start_time)) AS rentals_previous_day
FROM
    trips
GROUP BY
    DATE_TRUNC('day', start_time)
ORDER BY
    rental_date
    
**-- month over month**
SELECT
    DATE_TRUNC('month', start_time)::date AS rental_month,
    COUNT(1) AS rentals_total,
    LAG(COUNT(1)) OVER (ORDER BY DATE_TRUNC('month', start_time)) AS rentals_previous_month
FROM
    trips
GROUP BY
    DATE_TRUNC('month', start_time)
ORDER BY
    rental_month

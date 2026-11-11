WITH dates AS (
    SELECT CAST('2024-01-01' AS DATE) + INTERVAL (i) DAY AS date_day
    FROM generate_series(0, 730) AS t(i)
)
SELECT 
    date_day,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day
FROM dates
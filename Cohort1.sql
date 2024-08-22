WITH prep AS (
  SELECT
    user_id,
    EXTRACT(ISOWEEK FROM event_timestamp) AS week_number,
    DATE_TRUNC(event_timestamp, WEEK(MONDAY)) AS week_start,
    DATE_TRUNC(event_timestamp, WEEK(MONDAY)) + INTERVAL 6 DAY AS week_end,
    MIN(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS first_event_timestamp,
    event_name
  FROM
    `table1`
  WHERE
    event_name IN ('initial_purchase', 'purchase')
),
session_data AS (
  SELECT
    user_id,
    first_event_timestamp,
    DATE_TRUNC(first_event_timestamp, WEEK(MONDAY)) AS first_week_start,
    DATE_TRUNC(first_event_timestamp, WEEK(MONDAY)) + INTERVAL 6 DAY AS first_week_end,
    EXTRACT(ISOWEEK FROM first_event_timestamp) AS first_week_number,
    EXTRACT(YEAR FROM first_event_timestamp) AS first_year
  FROM
    prep
  GROUP BY
    user_id,
    first_event_timestamp
),
cohorts AS (
  SELECT
    CONCAT(
      CAST(session_data.first_year AS STRING), '-', 
      LPAD(CAST(session_data.first_week_number AS STRING), 2, '0')
    ) AS year_week,
    session_data.first_week_start,
    session_data.first_week_end,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number THEN prep.user_id END) AS week_0,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 1 THEN prep.user_id END) AS week_1,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 2 THEN prep.user_id END) AS week_2,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 3 THEN prep.user_id END) AS week_3,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 4 THEN prep.user_id END) AS week_4,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 5 THEN prep.user_id END) AS week_5,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 6 THEN prep.user_id END) AS week_6,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 7 THEN prep.user_id END) AS week_7,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 8 THEN prep.user_id END) AS week_8,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 9 THEN prep.user_id END) AS week_9,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 10 THEN prep.user_id END) AS week_10,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 11 THEN prep.user_id END) AS week_11,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 12 THEN prep.user_id END) AS week_12,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 13 THEN prep.user_id END) AS week_13,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 14 THEN prep.user_id END) AS week_14,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 15 THEN prep.user_id END) AS week_15,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 16 THEN prep.user_id END) AS week_16,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 17 THEN prep.user_id END) AS week_17,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 18 THEN prep.user_id END) AS week_18,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 19 THEN prep.user_id END) AS week_19,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 20 THEN prep.user_id END) AS week_20,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 21 THEN prep.user_id END) AS week_21,
    COUNT(DISTINCT CASE WHEN prep.week_number = session_data.first_week_number + 22 THEN prep.user_id END) AS week_22
  FROM
    prep
  JOIN
    session_data
  ON
    prep.user_id = session_data.user_id
  GROUP BY
    year_week,
    session_data.first_week_start,
    session_data.first_week_end
  ORDER BY
    year_week
)

SELECT 
  year_week,
  first_week_start AS week_start,
  first_week_end AS week_end,
  week_0,
  week_1,
  week_2,
  week_3,
  week_4,
  week_5,
  week_6,
  week_7,
  week_8,
  week_9,
  week_10,
  week_11,
  week_12,
  week_13,
  week_14,
  week_15,
  week_16,
  week_17,
  week_18,
  week_19,
  week_20,
  week_21,
  week_22
FROM
  cohorts
ORDER BY
  week_start;

WITH first_events AS (
    SELECT
        user_id,
        event_timestamp,
        install_date,
        row_number() OVER (PARTITION BY user_id ORDER BY event_timestamp) AS event_order
    FROM `table1`
),
installs AS (
    SELECT
        user_id,
        DATE(install_date) as install_date
    FROM first_events
),
cohort AS (
    SELECT
        i.install_date,
        DATE_DIFF(DATE(event_timestamp), i.install_date, DAY) AS cohort_age,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM `table1`
        INNER JOIN installs i USING (user_id)
    GROUP BY 1,2
)
select
    install_date,
    sum(case when cohort_age = 0 then cohort_size else null end) as day_0,
    sum(case when cohort_age = 1 then cohort_size else null end) as day_1,
    sum(case when cohort_age = 7 then cohort_size else null end) as day_7,
    round(sum(case when cohort_age = 1 then cohort_size else null end) / sum(case when cohort_age = 0 then cohort_size else null end), 3) as day_1_retention,
    round(sum(case when cohort_age = 7 then cohort_size else null end) / sum(case when cohort_age = 0 then cohort_size else null end), 3) as day_7_retention
    
from cohort
group by 1

WITH
raw AS (
  SELECT DISTINCT 
    -- DATE_TRUNC(DATE(creation_date), WEEK) date_, 
    CASE
      WHEN <Parameters.Date Part> = "year" THEN DATE_TRUNC(DATE(creation_date), YEAR)
      WHEN <Parameters.Date Part> = "quarter" THEN DATE_TRUNC(DATE(creation_date), QUARTER)
      WHEN <Parameters.Date Part> = "month" THEN DATE_TRUNC(DATE(creation_date), MONTH)
      WHEN <Parameters.Date Part> = "week" THEN DATE_TRUNC(DATE(creation_date), WEEK)
      WHEN <Parameters.Date Part> = "day" THEN DATE_TRUNC(DATE(creation_date), DAY)    
    END date_, 
    user_id, 
  FROM `bigquery-public-data.stackoverflow.comments` 
  WHERE DATE(creation_date) >= "2017-01-01" -- untuk menandai cohort paling awal, ask about this later.
)
, 
users_by_time AS (
  SELECT date_, COUNT(1) cnt_users
  FROM raw
  GROUP BY 1
)
, 
time_first_landing AS (
  SELECT user_id, MIN(date_) landing_cohort, 
  FROM raw
  GROUP BY 1
  ORDER BY 2 ASC 
)
, 
time_next_landing AS (
  SELECT r.user_id, 
    -- DATE_DIFF(date_, landing_cohort, WEEK) landing_interval,
    CASE
      WHEN <Parameters.Date Part> = "year" THEN DATE_DIFF(date_, landing_cohort, YEAR)
      WHEN <Parameters.Date Part> = "quarter" THEN DATE_DIFF(date_, landing_cohort, QUARTER)
      WHEN <Parameters.Date Part> = "month" THEN DATE_DIFF(date_, landing_cohort, MONTH)
      WHEN <Parameters.Date Part> = "week" THEN DATE_DIFF(date_, landing_cohort, WEEK)
      WHEN <Parameters.Date Part> = "day" THEN DATE_DIFF(date_, landing_cohort, DAY)
    END landing_interval
  FROM raw r
  JOIN time_first_landing f
    ON r.user_id = f.user_id
)
, 
time_initial_users AS (
  SELECT landing_cohort, COUNT(1) cnt_initial_users
  FROM time_first_landing
  GROUP BY 1
)
, 
time_retained_users AS (
  SELECT landing_cohort, landing_interval, COUNT(DISTINCT n.user_id) cnt_retained_users
  FROM time_first_landing f
  LEFT JOIN time_next_landing n
    ON n.user_id = f.user_id
  -- WHERE landing_interval IS NOT NULL
  GROUP BY 1, 2 
)

SELECT 
  r.landing_cohort date_, cnt_users, cnt_initial_users, 
    r.landing_interval, cnt_retained_users, 
      cnt_retained_users/cnt_initial_users retention
FROM time_retained_users r
LEFT JOIN time_initial_users i
  ON i.landing_cohort = r.landing_cohort
LEFT JOIN users_by_time u
  ON i.landing_cohort = date_
ORDER BY 1,4

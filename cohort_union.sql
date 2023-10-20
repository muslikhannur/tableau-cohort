WITH
raw AS (
  SELECT *
  FROM `bigquery-public-data.stackoverflow.comments` 
  WHERE DATE(creation_date) >= "2017-01-01" 
)

SELECT *
FROM (
  WITH
  raw_date_part AS (
    SELECT DISTINCT 
      DATE_TRUNC(DATE(creation_date), DAY) date_, 
      user_id, 
    FROM raw
  )
  , 
  users_by_time AS (
    SELECT date_, COUNT(1) cnt_users
    FROM raw_date_part
    GROUP BY 1
  )
  , 
  time_first_landing AS (
    SELECT user_id, MIN(date_) landing_cohort, 
    FROM raw_date_part
    GROUP BY 1
    ORDER BY 2 ASC 
  )
  , 
  time_next_landing AS (
    SELECT r.user_id, 
      DATE_DIFF(date_, landing_cohort, DAY) landing_interval,
    FROM raw_date_part r
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
    
    GROUP BY 1, 2 
  )

  SELECT "day" date_part,  
    r.landing_cohort date_, cnt_users, cnt_initial_users, 
      r.landing_interval, cnt_retained_users, 
        cnt_retained_users/cnt_initial_users retention
  FROM time_retained_users r
  LEFT JOIN time_initial_users i
    ON i.landing_cohort = r.landing_cohort
  LEFT JOIN users_by_time u
    ON i.landing_cohort = date_
)
UNION ALL
SELECT *
FROM (
  WITH
  raw_date_part AS (
    SELECT DISTINCT 
      DATE_TRUNC(DATE(creation_date), WEEK) date_, 
      user_id, 
    FROM raw
  )
  , 
  users_by_time AS (
    SELECT date_, COUNT(1) cnt_users
    FROM raw_date_part
    GROUP BY 1
  )
  , 
  time_first_landing AS (
    SELECT user_id, MIN(date_) landing_cohort, 
    FROM raw_date_part
    GROUP BY 1
    ORDER BY 2 ASC 
  )
  , 
  time_next_landing AS (
    SELECT r.user_id, 
      DATE_DIFF(date_, landing_cohort, WEEK) landing_interval,
    FROM raw_date_part r
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
    
    GROUP BY 1, 2 
  )

  SELECT "week" date_part,  
    r.landing_cohort date_, cnt_users, cnt_initial_users, 
      r.landing_interval, cnt_retained_users, 
        cnt_retained_users/cnt_initial_users retention
  FROM time_retained_users r
  LEFT JOIN time_initial_users i
    ON i.landing_cohort = r.landing_cohort
  LEFT JOIN users_by_time u
    ON i.landing_cohort = date_
)
UNION ALL
SELECT *
FROM (
  WITH
  raw_date_part AS (
    SELECT DISTINCT 
      DATE_TRUNC(DATE(creation_date), MONTH) date_, 
      user_id, 
    FROM raw
  )
  , 
  users_by_time AS (
    SELECT date_, COUNT(1) cnt_users
    FROM raw_date_part
    GROUP BY 1
  )
  , 
  time_first_landing AS (
    SELECT user_id, MIN(date_) landing_cohort, 
    FROM raw_date_part
    GROUP BY 1
    ORDER BY 2 ASC 
  )
  , 
  time_next_landing AS (
    SELECT r.user_id, 
      DATE_DIFF(date_, landing_cohort, MONTH) landing_interval,
    FROM raw_date_part r
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
    
    GROUP BY 1, 2 
  )

  SELECT "month" date_part,  
    r.landing_cohort date_, cnt_users, cnt_initial_users, 
      r.landing_interval, cnt_retained_users, 
        cnt_retained_users/cnt_initial_users retention
  FROM time_retained_users r
  LEFT JOIN time_initial_users i
    ON i.landing_cohort = r.landing_cohort
  LEFT JOIN users_by_time u
    ON i.landing_cohort = date_
)
UNION ALL
SELECT *
FROM (
  WITH
  raw_date_part AS (
    SELECT DISTINCT 
      DATE_TRUNC(DATE(creation_date), QUARTER) date_, 
      user_id, 
    FROM raw
  )
  , 
  users_by_time AS (
    SELECT date_, COUNT(1) cnt_users
    FROM raw_date_part
    GROUP BY 1
  )
  , 
  time_first_landing AS (
    SELECT user_id, MIN(date_) landing_cohort, 
    FROM raw_date_part
    GROUP BY 1
    ORDER BY 2 ASC 
  )
  , 
  time_next_landing AS (
    SELECT r.user_id, 
      DATE_DIFF(date_, landing_cohort, QUARTER) landing_interval,
    FROM raw_date_part r
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
    
    GROUP BY 1, 2 
  )

  SELECT "quarter" date_part,  
    r.landing_cohort date_, cnt_users, cnt_initial_users, 
      r.landing_interval, cnt_retained_users, 
        cnt_retained_users/cnt_initial_users retention
  FROM time_retained_users r
  LEFT JOIN time_initial_users i
    ON i.landing_cohort = r.landing_cohort
  LEFT JOIN users_by_time u
    ON i.landing_cohort = date_
)
UNION ALL
SELECT *
FROM (
  WITH
  raw_date_part AS (
    SELECT DISTINCT 
      DATE_TRUNC(DATE(creation_date), YEAR) date_, 
      user_id, 
    FROM raw
  )
  , 
  users_by_time AS (
    SELECT date_, COUNT(1) cnt_users
    FROM raw_date_part
    GROUP BY 1
  )
  , 
  time_first_landing AS (
    SELECT user_id, MIN(date_) landing_cohort, 
    FROM raw_date_part
    GROUP BY 1
    ORDER BY 2 ASC 
  )
  , 
  time_next_landing AS (
    SELECT r.user_id, 
      DATE_DIFF(date_, landing_cohort, YEAR) landing_interval,
    FROM raw_date_part r
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
    
    GROUP BY 1, 2 
  )

  SELECT "year" date_part,  
    r.landing_cohort date_, cnt_users, cnt_initial_users, 
      r.landing_interval, cnt_retained_users, 
        cnt_retained_users/cnt_initial_users retention
  FROM time_retained_users r
  LEFT JOIN time_initial_users i
    ON i.landing_cohort = r.landing_cohort
  LEFT JOIN users_by_time u
    ON i.landing_cohort = date_
)

-- WHERE landing_interval IS NULL
ORDER BY 1,2,5

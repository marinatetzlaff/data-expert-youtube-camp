--Question 4: Backfill query for actors_history_scd

INSERT INTO actors_scd
WITH with_previous as (
    SELECT actor,
    quality_class,
    current_year,
    is_active,
    lag(quality_class,1) over (partition by actor order by current_year) as previous_quality_class,
    lag(is_active,1) over (partition by actor order by current_year) as previous_is_active
    from actors
    where current_year <= 2020 --airflow parameter
    --regenerates history every day, doesn't think about incremental
)
, with_indicators as (
SELECT *,
    CASE
    WHEN quality_class <> previous_quality_class THEN 1
    WHEN is_active <> previous_is_active THEN 1
    ELSE 0
    END as change_ind
FROM with_previous
)

, with_streaks as (
SELECT *
, SUM(change_ind) OVER (PARTITION BY actor ORDER BY current_year) AS streak_ind
FROM with_indicators
)

SELECT actor,
       quality_class,
       is_active,
       MIN(current_year) as start_date,
        MAX(current_year) as end_date,
        2020 as current_year --airflow parameter
FROM with_streaks
GROUP BY actor, streak_ind, is_active, quality_class
ORDER BY actor, streak_ind
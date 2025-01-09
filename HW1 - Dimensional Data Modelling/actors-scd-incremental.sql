--Question 4: Backfill query for actors_history_scd
    -- assume that is_active and quality_class are never null

CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_date INTEGER,
    end_date INTEGER
                     );

WITH last_year_scd as (
    SELECT *
    FROM actors_scd
    WHERE current_year=2020
    AND end_date=2020
),

historical_scd as (
    SELECT actor,
           quality_class,
           is_active,
           start_date,
           end_date
    FROM actors_scd
     WHERE current_year=2020
     AND end_date<2020
    ),

this_year_data as (
      SELECT * FROM actors
     WHERE current_year=2021
    ),

unchanged_records as (
SELECT ts.actor,
    ts.quality_class,
    ts.is_active,
    ls.start_date,
    ls.current_year as end_date
FROM this_year_data ts
JOIN last_year_scd ls
         on ls.actor = ts.actor
WHERE
    ts.quality_class = ls.quality_class
    and ts.is_active = ls.is_active
)

,changed_records AS(
SELECT ts.actor,
    UNNEST(ARRAY [
        ROW(ls.quality_class,
            ls.is_active,
            ls.start_date,
            ls.end_date
            )::scd_type,
         ROW(ts.quality_class,
            ts.is_active,
            ts.current_year,
            ts.current_year
            )::scd_type -- the new record is the current value
        ]) as records
FROM this_year_data ts
LEFT JOIN last_year_scd ls
         on ls.actor = ts.actor
WHERE (ts.quality_class <> ls.quality_class
    OR ts.is_active <> ls.is_active)
)

,unnested_changed_records as (
    select actor,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date,
       (records::scd_type).end_date
    from changed_records
)

, new_records as (
select ts.actor,
       ts.quality_class,
       ts.is_active,
       ts.current_year as start_date,
       ts.current_year as end_date
from this_year_data ts
left join last_year_scd ls
    on ts.actor = ls.actor
where ls.actor is null
)

select * from historical_scd

union all

select * from unchanged_records

union all

select * from unnested_changed_records

union all

select * from new_records




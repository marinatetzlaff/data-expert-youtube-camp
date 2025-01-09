--Question 2: Cumulative table generation query

INSERT INTO actors
WITH
    yesterday as (
    select * from actors
             where current_year=1969
),
    today as (
        select * from actor_films
                 where year=1970
    )

--coalesce values that are not temporal.
SELECT coalesce(t.actor, y.actor) as actor
, CASE WHEN y.films is NULL
-- if array is null
        THEN ARRAY[ROW(
            t.film,
            t.votes,
            t.rating,
            t.filmid
        )::films]
    -- if today is not null
        WHEN t.film is NOT NULL THEN y.films || ARRAY[ROW(
            t.film,
            t.votes,
            t.rating,
            t.filmid
        )::films]
    --otherwise, carry history forward
        ELSE y.films
    END AS films,
    CASE WHEN t.year IS NOT NULL THEN
        CASE WHEN t.rating > 8 THEN 'star'
            WHEN t.rating > 7 THEN 'good'
            WHEN t.rating > 6 THEN 'average'
            ELSE 'bad'
        END::quality_class
    ELSE y.quality_class -- if retired most recent year pulled in
    END as quality_class,
--     NULL as is_active,
    t.year IS NOT NULL AS is_active,
    COALESCE(t.year,y.current_year+1) as current_year
FROM today t FULL OUTER JOIN yesterday y
    ON t.actor = y.actor

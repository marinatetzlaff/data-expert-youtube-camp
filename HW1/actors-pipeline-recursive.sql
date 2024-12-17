--Question 2: Cumulative table generation query: populates the actors table one year at a time.

INSERT INTO actors
WITH RECURSIVE actor_pipeline AS (
    -- Initialize the pipeline with data from 1969 (yesterday) and the first year (1970)
    SELECT
        a.actor,
        ARRAY[ROW(f.film, f.votes, f.rating, f.filmid)::films] AS films,
        CASE
            WHEN f.rating > 8 THEN 'star'
            WHEN f.rating > 7 THEN 'good'
            WHEN f.rating > 6 THEN 'average'
            ELSE 'bad'
        END::quality_class AS quality_class,
        TRUE AS is_active,
        1970 AS current_year
    FROM
        actors a
    LEFT JOIN
        actor_films f
    ON
        a.actor = f.actor AND f.year = 1970

    UNION ALL

    -- Process each subsequent year without using FULL OUTER JOIN
    SELECT
        COALESCE(f.actor, p.actor) AS actor,
        CASE
            WHEN p.films IS NULL THEN ARRAY[ROW(f.film, f.votes, f.rating, f.filmid)::films]
            WHEN f.film IS NOT NULL THEN p.films || ARRAY[ROW(f.film, f.votes, f.rating, f.filmid)::films]
            ELSE p.films
        END AS films,
        CASE
            WHEN f.rating IS NOT NULL THEN
                CASE
                    WHEN f.rating > 8 THEN 'star'
                    WHEN f.rating > 7 THEN 'good'
                    WHEN f.rating > 6 THEN 'average'
                    ELSE 'bad'
                END::quality_class
            ELSE p.quality_class
        END AS quality_class,
        f.year IS NOT NULL AS is_active,
        p.current_year + 1 AS current_year
    FROM
        actor_pipeline p
    LEFT JOIN
        actor_films f
    ON
        p.actor = f.actor AND f.year = p.current_year + 1
    WHERE
        p.current_year < 2021
)
SELECT *
FROM actor_pipeline
ORDER BY current_year, actor;
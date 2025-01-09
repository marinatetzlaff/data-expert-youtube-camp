-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
WITH users AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-04')
)
   ,series AS (
    SELECT generate_series(DATE('2023-01-01'), DATE('2023-01-04'), interval '1 day')::date AS series_date
)
   , place_holder_values AS (
    SELECT users.*
    , series.series_date
    , CAST(CASE WHEN EXISTS (
                    SELECT 1
                    FROM jsonb_each_text(users.device_activity_datelist) AS kv(key, value)
                    WHERE value = series.series_date::text
                )
                THEN CAST(POW(2, 31 - (users.date - series.series_date)) AS BIGINT)
                ELSE 0
            END as bit(32))
        as placeholder_int_value
    FROM users
    CROSS JOIN series
)
SELECT *
FROM place_holder_values
--WHERE user_id = '5361721815930967000'
ORDER BY series_date DESC;
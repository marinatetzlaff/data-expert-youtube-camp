insert into user_devices_cumulated
with deduped_devices as (
    select distinct device_id
    , browser_type
    from devices
),

yesterday as (
    select *
    from user_devices_cumulated
    where date = date('2023-01-03')
),

today as (
    select
        cast(e.user_id as text) as user_id,
        jsonb_object_agg(
            d.browser_type,
            to_jsonb(date(cast(e.event_time as timestamp)))
        ) as device_activity_datelist,
        max(date(cast(e.event_time as timestamp))) as date_active
    from events e
    left join deduped_devices d
        on e.device_id = d.device_id
    where date(cast(e.event_time as timestamp)) = date('2023-01-04')
    and e.user_id is not null
    and d.browser_type is not null
    group by e.user_id
)

select coalesce(t.user_id,y.user_id) as user_id
, case when t.date_active is null then y.device_activity_datelist
    else coalesce(y.device_activity_datelist,'{}'::jsonb) ||
       coalesce(t.device_activity_datelist, '{}'::jsonb)
    end as device_activity_datelist
, coalesce(t.date_active, y.date + interval '1 day') as date
from today t
full outer join yesterday y
on t.user_id = y.user_id;
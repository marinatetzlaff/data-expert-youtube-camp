--insert into hosts_cumulated
with yesterday as (
    select *
    from hosts_cumulated
    where date = date('2022-12-31')
),

today as (
    select distinct host
        , date(cast(event_time as timestamp)) as date_active
    from events
    where date(cast(event_time as timestamp)) = date('2023-01-01')
    and host is not null
    group by host,  date(cast(event_time as timestamp))
)

select coalesce(t.host,y.host) as host
, null ashost_activity_datelist
, coalesce(t.date_active, y.date + interval '1 day') as date
from today t
full outer join yesterday y
on t.host = y.host;
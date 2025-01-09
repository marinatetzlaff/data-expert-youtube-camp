insert into host_activity_reduced

with daily_aggregate as (
    select host
         , date(event_time) as date
    , count(1) as num_site_hits
    , count(distinct user_id) as num_unique_visitors
    from events
    where date(event_time) = date('2023-01-02')
    and host is not null
    group by host, date(event_time)
)

, yesterday_array as (
    select * from host_activity_reduced
    where month = date('2023-01-01')
)

select coalesce(da.host, ya.host) as user_id
, coalesce(ya.month, date_trunc('month',da.date)) as month
, case when ya.hit_array is not null
        then ya.hit_array::INTEGER[] || array[coalesce(da.num_site_hits,0)::INTEGER]
    when ya.hit_array is null
        then array_fill(0, array[coalesce(date- date(date_trunc('month',da.date)),0)::INTEGER]) || array[coalesce(da.num_unique_visitors,0)::INTEGER]
end as hit_array
, case when ya.unique_visitors_array is not null then
        ya.unique_visitors_array::INTEGER[] || array[coalesce(da.num_unique_visitors,0)::INTEGER]
    when ya.unique_visitors_array is null
        then array_fill(0, array[coalesce(date- date(date_trunc('month',da.date)),0)::INTEGER]) || array[coalesce(da.num_unique_visitors,0)::INTEGER]
end as unique_visitors_array
from daily_aggregate da
full outer join yesterday_array ya
on da.host = ya.host

on conflict(host, month)
do
    update set
    hit_array = excluded.hit_array,
    unique_visitors_array = excluded.unique_visitors_array;
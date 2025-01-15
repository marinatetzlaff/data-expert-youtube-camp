
with m1 as (
select CAST (event_hour AS text) as dimension
, avg(num_hits) as metric
, 'Average Web Events per Session' as metric_name
from processed_events_aggregated_ip
group by 1
)

, m2 as (
select host as dimension
, avg(num_hits) as metric
, 'Average Web Events per Host' as metric_name
from processed_events_aggregated_ip
group by 1
)

select * from m1
union all
select * from m2;

select * from processed_events_aggregated_ip
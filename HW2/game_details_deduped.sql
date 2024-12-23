--A query to deduplicate game_details from Day 1 so there's no duplicates
with deduped as (
    select *
    , row_number() over (partition by game_id,team_id,player_id) as row_num
from game_details
)

select *
from deduped
where row_num = 1;


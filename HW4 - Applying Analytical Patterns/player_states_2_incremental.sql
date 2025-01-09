--Run many times updating lines 7 and 15 correspondingly
--INSERT INTO player_states
WITH yesterday AS (
SELECT * 
FROM player_states 
WHERE season = 2000
)

, today AS (
SELECT player_name,
    season as current_season,
    COUNT(1)
FROM player_seasons 
WHERE season = 2001
    AND player_name is not null
GROUP BY player_name, season
)

SELECT coalesce (t.player_name, y.player_name) as player_name,
coalesce(y.first_active_season,  t.current_season) as first_active_season,
coalesce(t.current_season, y.last_active_season) as last_active_season,
    case    when y.player_name is null and t.player_name is not null then 'New'
            when y.last_active_season = (t.current_season - 1) then 'Continued Playing'
            when y.last_active_season < (t.current_season - 1) then 'Returned from Retirement'
            when t.current_season is null and y.last_active_season = y.season then 'Retired'
            else 'Stayed Retired'
            end as yearly_active_state,
coalesce(t.current_season, (y.season + 1)) as season
from today t 
full outer join yesterday y 
on t.player_name = y.player_name 
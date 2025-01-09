with games_augmented as (
select game_details.game_id,
        game_details.team_abbreviation,
        game_details.player_name,
        games.game_date_est as game_date,
        games.season,
        games.pts_home,
        games.home_team_wins
from game_details
join games 
    on game_details.game_id = games.game_id
    and game_details.team_id = games.home_team_id
where player_id is not null
)

, groupings as (
select coalesce(player_name, '(overall)') as player_name
,  coalesce(team_abbreviation, '(overall)')  as team_abbreviation
,  coalesce(season::text, '(overall)') as season
, count(pts_home) as total_points
, sum(home_team_wins) as total_wins
from games_augmented
group by grouping sets (
    (player_name, team_abbreviation),
    (player_name, season),
    (team_abbreviation)
)
)

, max_player_1 as (
select player_name
, team_abbreviation as dim
, 'max player of a team by total points' as metric
, total_points as metric_value
from groupings 
where season = '(overall)'
and player_name != '(overall)'
order by total_points desc
limit 1
)

, max_player_2 as (
select player_name
, season as dim
, 'max player of a season by total points' as metric
, total_points as metric_value
from groupings
where team_abbreviation = '(overall)'
and player_name != '(overall)'
order by total_points desc
limit 1
)

, max_team as (
select team_abbreviation
, null as dim
, 'max team by total wins' as metric
, total_wins as metric_value
from groupings 
where  season = '(overall)'
       and player_name = '(overall)'
order by total_wins desc
limit 1
)

select * from max_player_1
union all 
select * from max_player_2
union all 
select * from max_team
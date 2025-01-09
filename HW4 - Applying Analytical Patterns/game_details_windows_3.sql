--How many games in a row did LeBron James score over 10 points a game?

with games_augmented as (
select  game_details.game_id,
        game_details.player_name,
        game_details.pts as player_points,
        games.game_date_est as game_date
from game_details
join games 
        on game_details.game_id = games.game_id
        and game_details.team_id = games.home_team_id
where player_name = 'LeBron James'
)

, streak_games as (
SELECT  game_date, 
        player_name, 
        CASE WHEN player_points >=10 and LAG(player_points) OVER (PARTITION BY player_name ORDER BY game_date asc) >=10 THEN 0 ELSE 1 END as runtot
FROM games_augmented 
GROUP BY game_date, 
player_name,
player_points
)

, metric as (
SELECT  player_name,
        game_date,
        SUM(runtot) OVER (PARTITION BY player_name ORDER BY game_date) streak_id
from streak_games
)

, final as (
select  streak_id
        , count(streak_id) as streak_length
from metric 
group by streak_id
)

select max(streak_length) from final
--What is the most games a team has won in a 90 game stretch?

with games_augmented as (
select  game_details.game_id,
        game_details.team_abbreviation as team_name,
		game_details.team_id,
        game_details.player_name,
        games.game_date_est as game_date,
        games.home_team_wins
from game_details
join games 
    on game_details.game_id = games.game_id
    and game_details.team_id = games.home_team_id
where player_id is not null
)

, metrics as (
select team_name
, sum(home_team_wins) over (partition by team_id order by game_date rows between 89 preceding and current row) as total_wins
from games_augmented
group by team_name, 
team_id, 
home_team_wins, 
game_date
)

, final as (
select max(total_wins) as winner_value
from metrics 
)

select distinct final.winner_value
, metrics.team_name as winner_team
from final 
inner join metrics on final.winner_value = metrics.total_wins

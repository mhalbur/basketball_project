insert into nba_games
select
    game_id,
    season_id,
    game_date,
    matchup
from working_nba_games_st
where lower(matchup) like '%vs%';
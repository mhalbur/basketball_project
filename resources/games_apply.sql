insert into nba_games
select
    game_id,
    season_id,
    game_date,
    matchup
from working_nba_games_st a
where 
    lower(matchup) like '%vs%'
    and not exists(
        select 1
        from nba_games b
        where 
            a.game_id = b.game_id
    )
;
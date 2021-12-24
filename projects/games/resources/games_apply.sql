insert into games
select
    game_id,
    season_id,
    game_date,
    matchup
from working_games_st a
where 
    lower(matchup) like '%vs%'
    and not exists(
        select 1
        from games b
        where 
            a.game_id = b.game_id
    )
;
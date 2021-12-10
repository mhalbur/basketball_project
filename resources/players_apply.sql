insert into nba_players 
select * 
from working_nba_players_st a
where 
    not exists(
        select 1
        from nba_players b
        where 
            a.player_id = b.player_id
            and a.team_id = b.team_id
    )
;
insert into players 
select * 
from working_players_st a
where 
    not exists(
        select 1
        from players b
        where 
            a.player_id = b.player_id
            and a.team_id = b.team_id
    )
;
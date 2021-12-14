delete from players as a
where 
    not exists(
        select 1
        from working_players_st b
        where 
            a.player_id = b.player_id
            and a.team_id = b.team_id
    )
;
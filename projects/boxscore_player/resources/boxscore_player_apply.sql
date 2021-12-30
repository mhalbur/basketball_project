insert into boxscore_player
select 
    game_id,
    team_id,
    player_id,
    start_position,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "to",
    pf,
    pts,
    plus_minus
from working_boxscore_player_st a
where
    not exists(
        select 1
        from boxscore_player b
        where 
            a.game_id = b.game_id
            and a.player_id = b.player_id
    )
group by 
    game_id,
    team_id,
    player_id,
    start_position,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "to",
    pf,
    pts,
    plus_minus
;
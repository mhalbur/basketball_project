insert into working_boxscore_player_st
(
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
)
values(
    "{game_id}",
    "{team_id}",
    "{player_id}",
    "{start_position}",
    "{min}",
    "{fgm}",
    "{fga}",
    "{fg_pct}",
    "{fg3m}",
    "{fg3a}",
    "{fg3_pct}",
    "{ftm}",
    "{fta}",
    "{ft_pct}",
    "{oreb}",
    "{dreb}",
    "{reb}",
    "{ast}",
    "{stl}",
    "{blk}",
    "{to}",
    "{pf}",
    "{pts}",
    "{plus_minus}"
)

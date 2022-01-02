insert into boxscore_team
select 
    game_id,
    team_id,
    team_name,
    team_abbreviation,
    team_city,
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
from working_boxscore_team_st a
where
    not exists(
        select 1
        from boxscore_team b
        where 
            a.game_id = b.game_id
            and a.team_id = b.team_id
    );
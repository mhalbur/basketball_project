insert into working_players_load
    (
        player_id,
        team_id,
        player,
        jersey_number,
        position,
        age,
        height,
        weight,
        experience
    )
values
    (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    )
;

insert into working_players_st
select
    player_id,
    team_id,
    substr(player, 1, instr(player,' ')) as first_name,
	substr(player, instr(player,' ')) as last_name,
    jersey_number,
    position,
    age,
    height,
    weight,
    experience
from working_players_load

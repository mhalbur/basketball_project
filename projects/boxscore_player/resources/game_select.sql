select game_id
from games a
where 
    not exists(
        select 1
        from working_boxscore_player_st b
        where a.game_id = b.game_id);
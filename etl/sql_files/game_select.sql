select game_id
from games a
where 
    not exists(
        select 1
        from boxscore_player b
        where a.game_id = b.game_id);
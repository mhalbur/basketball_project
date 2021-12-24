import projects.boxscore_player.custom as bsp
from packages.connectors.sqlite3 import clean_table, execute_sql
from packages.infrastructure import formatter


RESOURCES = 'projects/games/resources'

def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='games_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='games.sql')

def stage_boxscore_player():
    clean_table(table="working_boxscore_player_st")
    data = bsp.get_boxscore(game_id = 22100047, type="PlayerStats")
    format = formatter(data=data, fields=boxscore_players_fields)
    loader = bsp.load_boxscore_players()

    for row in format:
        loader.send(row)
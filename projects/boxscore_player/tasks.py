import projects.boxscore_player.custom as bsp
from packages.connectors.sqlite import clean_table, execute_sql
from packages.infrastructure import formatter

#HATE THIS... NEED TO SET UP A CONFIG
boxscore_players_fields = ["game_id",
                           "team_id",
                           "player_id",
                           "start_position",
                           "min",
                           "fgm",
                           "fga",
                           "fg_pct",
                           "fg3m",
                           "fg3a",
                           "fg3_pct",
                           "ftm",
                           "fta",
                           "ft_pct",
                           "oreb",
                           "dreb",
                           "reb",
                           "ast",
                           "stl",
                           "blk",
                           "to",
                           "pf",
                           "pts",
                           "plus_minus"]


RESOURCES = 'projects/boxscore_player/resources'

def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='boxscore_player_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='boxscore_player.sql')


def stage_boxscore_player():
    clean_table(table="working_boxscore_player_st")
    data = bsp.get_boxscore(game_id = 22100047, type="PlayerStats")
    format = formatter(data=data, fields=boxscore_players_fields)
    loader = bsp.load_boxscore_players()

    for row in format:
        loader.send(row)


def apply_nba_boxscore_player():
    execute_sql(sql_file_path=RESOURCES, sql_file_name='boxscore_player_apply.sql')

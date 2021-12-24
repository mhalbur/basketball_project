import projects.players.custom as players
from packages.connectors.sqlite3 import clean_table, execute_sql

RESOURCES = 'projects/players/resources'

def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='players_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='players.sql')


def stage_nba_players():
    clean_table(table="working_players_st")
    team_data = players.get_nba_teams()
    player_data = players.get_nba_players(data=team_data)
    format_player_data = players.format_nba_players(data=player_data)
    loader = players.load_nba_players()

    for row in format_player_data:
        loader.send(row)


def apply_nba_players():
    execute_sql(sql_file_path=RESOURCES, sql_file_name='players_apply.sql')
    execute_sql(sql_file_path=RESOURCES, sql_file_name='players_apply.sql')

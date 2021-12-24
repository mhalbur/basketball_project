import projects.games.custom as games
from packages.connectors.sqlite3 import clean_table, execute_sql
from packages.infrastructure import formatter


RESOURCES = 'projects/games/resources'


def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='games_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='games.sql')


def stage_nba_games():
    nba_games = ["season_id", "game_id", "game_date", "matchup"]

    clean_table(table="working_games_st")
    game_data = games.get_nba_games()
    format = formatter(data=game_data, fields=nba_games)
    loader = games.load_nba_games()

    for row in format:
        loader.send(row)


def apply_nba_games():
    execute_sql(sql_file_path=RESOURCES, sql_file_name='games_apply.sql')
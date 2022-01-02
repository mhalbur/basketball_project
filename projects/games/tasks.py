from projects.games.config import game_fields
import projects.games.custom as games
from packages.connectors.sqlite import SQLite3
from generic.infrastructure import formatter


RESOURCES = 'projects/games/resources'


def install_script():
    ddl_path = f'{RESOURCES}/ddls'

    with SQLite3() as db:
        db.execute_sql(sql_file_path=ddl_path, sql_file_name='games_st.sql')
        db.execute_sql(sql_file_path=ddl_path, sql_file_name='games.sql')


def stage_nba_games():
    with SQLite3() as db:
        db.clean_table(table="working_games_st")

    game_data = games.get_nba_games()
    format = formatter(data=game_data, fields=game_fields)
    loader = games.load_nba_games()

    for row in format:
        loader.send(row)


def apply_nba_games():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=RESOURCES, sql_file_name='games_apply.sql')

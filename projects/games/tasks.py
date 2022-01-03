import projects.games.custom as games
from etl.common import cleaner, formatter, loader
from etl.connectors.sqlite import SQLite3
from projects.games.config import ddls, game_fields, resources


def install_script():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=ddls, sql_file_name='games_st.sql')
        db.execute_sql(sql_file_path=ddls, sql_file_name='games.sql')


def stage_nba_games():
    cleaner(tables=["working_games_st"])

    game_data = games.get_nba_games()
    format = formatter(data=game_data, fields=game_fields)
    game_loader = loader(sql_file=f'{resources}/games_stage.sql')
    

    for row in format:
        game_loader.send(row)


def apply_nba_games():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=resources, sql_file_name='games_apply.sql')

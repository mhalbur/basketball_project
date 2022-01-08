import logging 
import projects.games.custom as games
from etl.common.database import clean_table, execute_sql
from etl.common.generic import formatter, loader, file_put_rows
from etl.common.transform import generator_dict_to_list
from projects.games.config import ddls, game_fields, resources

log = logging.getLogger(__name__)

def install_script():
    file_path_list = [f'{ddls}/games_st.sql', f'{ddls}/games.sql',]
    execute_sql(file_paths=file_path_list)


def games_api_extract():
    log.info(f"Beginning {__name__} api extract...")
    clean_table(tables=["working_games_st"])

    game_data = games.get_nba_games()
    game_to_list = generator_dict_to_list(data=game_data)
    writer = file_put_rows(file_path='/tmp', file_name='test.txt')
    # format = formatter(data=game_data, fields=game_fields)
    # game_loader = loader(sql_file=f'{resources}/games_stage.sql')

    for row in game_to_list:
        print(row)
        writer.send(row)


def apply_nba_games():
    log.info("Applying recent NBA games to table...")
    file_path_list = [f'{resources}/games_apply.sql']
    execute_sql(file_paths=file_path_list)

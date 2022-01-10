import logging

from etl.functions.database import clean_table, execute_sql
from etl.functions.nba import get_game_ids
from projects.boxscore.config import ddl, resources
from projects.boxscore.custom import Boxscore_Traditional

log = logging.getLogger(__name__)


def install_script():
    file_path_list = [f'{ddl}/player_st.sql', 
                      f'{ddl}/player.sql', 
                      f'{ddl}/team_st.sql',
                      f'{ddl}/team.sql']
    execute_sql(file_path=file_path_list)


def boxscore_api_extract():
    log.info("Starting boxscore api extract...")
    clean_table(tables=['working_boxscore_player_st', 'working_boxscore_team_st'])

    game_ids = get_game_ids()

    for game_id in game_ids:
        with Boxscore_Traditional(game_id=game_id) as b:
            b.stage_boxscore_player()
            b.stage_boxscore_team()


def apply_boxscore():
    file_path_list = [f'{resources}/player_apply.sql', f'{resources}/team_apply.sql']
    execute_sql(file_path=file_path_list)

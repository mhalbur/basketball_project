
import logging
import glob
import projects.players.custom as players
from etl.functions.database import clean_table, execute_sql
from etl.common.transform import loader
from etl.functions.nba import get_nba_teams
from projects.players.config import WORKING_FILE_PATH, DDL_FILE_PATH

log = logging.getLogger(__name__)


def install_script():
    log.info("Beginning to build the tables for Games...")
    file_path_list = [f'{DDL_FILE_PATH}/players_st.sql', f'{DDL_FILE_PATH}/players.sql']
    execute_sql(file_paths=file_path_list)
    

def stage_nba_players():
    clean_table(tables=['working_players_st'])
    team_data = get_nba_teams()
    player_data = players.player_api_extract(data=team_data)
    format_player_data = players.format_nba_players(data=player_data)
    loader = players.load_nba_players()

    for row in format_player_data:
        loader.send(row)


def apply_nba_players():
    file_path_list = [f'{resources}/players_apply.sql']
    execute_sql(file_paths=file_path_list)

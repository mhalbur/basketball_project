
import glob
import logging

from etl.functions.database import clean_table, execute_sql, load_sql
from etl.functions.file import move_files
from projects.players.config import (ARCHIVE_FILE_PATH, DDL_FILE_PATH,
                                     SQL_FILE_PATH, WORKING_FILE_PATH)
from projects.players.custom import players_main

log = logging.getLogger(__name__)


def install_script():
    log.info("Beginning to build the tables for Players...")
    file_path_list = [f'{DDL_FILE_PATH}/players_st.sql', 
                      f'{DDL_FILE_PATH}/players.sql', 
                      f'{DDL_FILE_PATH}/players_load.sql']
    execute_sql(file_paths=file_path_list)
    

def players_api_extract():
    players_main()


def load_players():
    log.info("Loading games to SQLite...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    load_sql(file_paths=files, sql_file_path=f'{SQL_FILE_PATH}/players_load.sql', clean_table=False)


def stage_players():
    clean_table(['working_players_st'])
    file_path_list = [f'{SQL_FILE_PATH}/players_stage.sql']
    execute_sql(file_paths=file_path_list)


def apply_nba_players():
    file_path_list = [f'{SQL_FILE_PATH}/players_delete.sql',
                      f'{SQL_FILE_PATH}/players_apply.sql']
    execute_sql(file_paths=file_path_list)


def archive_players_files():
    log.info(f"Archiving files from {WORKING_FILE_PATH} to {ARCHIVE_FILE_PATH}...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    move_files(files=files, destination=ARCHIVE_FILE_PATH, overwrite=True)

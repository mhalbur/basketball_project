import glob
import logging

from etl.functions.database import clean_table, execute_sql, load_sql
from etl.functions.file import make_directory, move_files
from projects.boxscore.config import (ARCHIVE_FILE_PATH, DDL_FILE_PATH,
                                      SQL_FILE_PATH, WORKING_FILE_PATH)
from projects.boxscore.custom import boxscore_main

log = logging.getLogger(__name__)


def install_script():
    file_path_list = [f'{DDL_FILE_PATH}/player_st.sql',
                      f'{DDL_FILE_PATH}/player.sql',
                      f'{DDL_FILE_PATH}/team_st.sql',
                      f'{DDL_FILE_PATH}/team.sql']
    execute_sql(file_paths=file_path_list)
    make_directory([WORKING_FILE_PATH, ARCHIVE_FILE_PATH])


def boxscore_api_extract():
    log.info("Starting boxscore api extract...")
    boxscore_main()


def load_boxscore_player():
    clean_table(tables=['working_boxscore_player_st'])
    log.info("Loading Player Boxscore to SQLite...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*player*')
    load_sql(file_paths=files, sql_file_path=f'{SQL_FILE_PATH}/player_stage.sql', clean_table=False)


def load_boxscore_team():
    clean_table(tables=['working_boxscore_team_st'])
    log.info("Loading Player Boxscore to SQLite...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*team*')
    load_sql(file_paths=files, sql_file_path=f'{SQL_FILE_PATH}/team_stage.sql', clean_table=False)


def apply_boxscore_player():
    log.info("Applying recent games to final table...")
    file_path_list = [f'{SQL_FILE_PATH}/player_apply.sql']
    execute_sql(file_paths=file_path_list)


def apply_boxscore_team():
    log.info("Applying recent games to final table...")
    file_path_list = [f'{SQL_FILE_PATH}/team_apply.sql']
    execute_sql(file_paths=file_path_list)


def archive_boxscore_files():
    log.info(f"Archiving files from {WORKING_FILE_PATH} to {ARCHIVE_FILE_PATH}...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    move_files(files=files, destination=ARCHIVE_FILE_PATH, overwrite=True)
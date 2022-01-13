import glob
import logging

from etl.functions.database import clean_table, execute_sql, load_sql
from etl.functions.file import clean_files, move_files
from projects.games.config import (ARCHIVE_FILE_PATH, DDL_FILE_PATH,
                                   SQL_FILE_PATH, WORKING_FILE_PATH)
from projects.games.custom import games_api_extract, write_game_files

log = logging.getLogger(__name__)


def install_script():
    log.info("Beginning to build the tables for Games...")
    file_path_list = [f'{DDL_FILE_PATH}/games_st.sql', f'{DDL_FILE_PATH}/games.sql',]
    execute_sql(file_paths=file_path_list)


def get_games():
    log.info(f"Retrieving games from the API and writing them to {WORKING_FILE_PATH}...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    clean_files(files=files)
    games = games_api_extract()
    write_game_files(games=games)


def load_games():
    clean_table(tables=['working_games_st'])
    log.info("Loading games to SQLite...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    load_sql(file_paths=files, sql_file_path=f'{SQL_FILE_PATH}/games_stage.sql', clean_table=False)


def apply_games():
    log.info("Applying recent games to final table...")
    file_path_list = [f'{SQL_FILE_PATH}/games_apply.sql']
    execute_sql(file_paths=file_path_list)


def archive_game_files():
    log.info(f"Archiving files from {WORKING_FILE_PATH} to {ARCHIVE_FILE_PATH}...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    move_files(files=files, destination=ARCHIVE_FILE_PATH, overwrite=True)

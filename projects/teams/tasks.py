import glob
import logging

from etl.functions.database import clean_table, execute_sql, load_sql
from etl.functions.file import make_directory, move_files
from projects.teams.config import (ARCHIVE_FILE_PATH, DDL_FILE_PATH,
                                   SQL_FILE_PATH, WORKING_FILE_PATH)
from projects.teams.custom import teams_main

log = logging.Logger(__name__)


def install_script():
    file_path_list = [f'{DDL_FILE_PATH}/teams_st.sql', f'{DDL_FILE_PATH}/teams.sql']
    execute_sql(file_paths=file_path_list)
    make_directory([WORKING_FILE_PATH, ARCHIVE_FILE_PATH])


def teams_api_extract():
    clean_table(tables=["working_teams_st"])
    teams_main()


def load_teams():
    log.info("Loading teams to SQLite...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    load_sql(file_paths=files, sql_file_path=f'{SQL_FILE_PATH}/teams_stage.sql', clean_table=False)


def apply_teams():
    file_path_list = [f'{SQL_FILE_PATH}/teams_apply.sql']
    execute_sql(file_paths=file_path_list)


def archive_teams_files():
    log.info(f"Archiving files from {WORKING_FILE_PATH} to {ARCHIVE_FILE_PATH}...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    move_files(files=files, destination=ARCHIVE_FILE_PATH, overwrite=True)

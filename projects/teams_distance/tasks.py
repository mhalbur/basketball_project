import glob
import logging

from etl.functions.database import clean_table, execute_sql, load_sql
from etl.functions.file import make_directory, move_files
from projects.teams_distance.config import (ARCHIVE_FILE_PATH, DDL_FILE_PATH,
                                            SQL_FILE_PATH, WORKING_FILE_PATH)
from projects.teams_distance.custom import team_distance_main

log = logging.getLogger(__name__)


def install_script():
    file_path_list = [f'{DDL_FILE_PATH}/teams_distance_st.sql', f'{DDL_FILE_PATH}/teams_distance.sql']
    execute_sql(file_paths=file_path_list)
    make_directory([WORKING_FILE_PATH, ARCHIVE_FILE_PATH])


def calculate_teams_distance():
    team_distance_main()


def load_teams_distance():
    clean_table['working_teams_distance_st']
    log.info("Loading team's distances to SQLite...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    load_sql(file_paths=files, sql_file_path=f'{SQL_FILE_PATH}/teams_distance_st.sql', clean_table=False)


def apply_teams_distance():
    file_path_list = [f'{SQL_FILE_PATH}/teams_distance_apply.sql']
    execute_sql(file_paths=file_path_list)


def archive_teams_distance_files():
    log.info(f"Archiving files from {WORKING_FILE_PATH} to {ARCHIVE_FILE_PATH}...")
    files = glob.glob(pathname=f'{WORKING_FILE_PATH}/*')
    move_files(files=files, destination=ARCHIVE_FILE_PATH, overwrite=True)

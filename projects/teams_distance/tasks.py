import projects.teams_distance.custom as teams
from etl.functions.database import clean_table, execute_sql
from etl.common.transform import loader
from projects.teams_distance.config import ddl, resources


def install_script():
    file_path_list = [f'{ddl}/teams_distance_st.sql', f'{ddl}/teams_distance.sql']
    execute_sql(file_paths=file_path_list)


def stage_nba_teams_distance():
    clean_table(tables=["working_teams_distance_st"])
    team_info = teams.get_team_info()
    load = loader(sql_file=f'{resources}/teams_distance_st.sql')

    for row in team_info:
        load.send(row)


def apply_nba_teams_distance():
    file_path_list = [f'{resources}/teams_distance_apply.sql']
    execute_sql(file_paths=file_path_list)

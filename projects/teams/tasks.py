import projects.teams.custom as teams
from etl.functions.database import clean_table, execute_sql
from etl.common.transform import loader
from etl.functions.nba import get_nba_teams
from projects.teams.config import ddl, resources


def install_script():
    file_path_list = [f'{ddl}/teams_st.sql', f'{ddl}/teams.sql']
    execute_sql(file_paths=file_path_list)


def stage_nba_teams():
    clean_table(tables=["working_teams_st"])

    team_data = get_nba_teams()
    format_team_data = teams.format_nba_teams(data=team_data)
    load = loader(sql_file=f'{resources}/teams_stage.sql')

    for row in format_team_data:
        load.send(row)


def apply_nba_teams():
    file_path_list = [f'{resources}/teams_apply.sql']
    execute_sql(file_paths=file_path_list)

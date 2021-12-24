import projects.teams_distance.custom as teams
from packages.connectors.sqlite3 import clean_table, execute_sql


RESOURCES = 'projects/teams_distance/resources'


def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='teams_distance_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='teams_distance.sql')


def stage_nba_teams_distance():
    clean_table(table="working_teams_distance_st")
    team_info = teams.get_team_info()
    loader = teams.load_nba_team_distances()

    for row in team_info:
        loader.send(row)


def apply_nba_teams_distance():
    execute_sql(sql_file_path=RESOURCES, sql_file_name='teams_distance_apply.sql')
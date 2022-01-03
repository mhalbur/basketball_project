import projects.teams_distance.custom as teams
from etl.connectors.sqlite import SQLite3


RESOURCES = 'projects/teams_distance/resources'


def install_script():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='teams_distance_st.sql')
        db.execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='teams_distance.sql')


def stage_nba_teams_distance():
    with SQLite3() as db:
        db.clean_table(table="working_teams_distance_st")
    team_info = teams.get_team_info()
    loader = teams.load_nba_team_distances()

    for row in team_info:
        loader.send(row)


def apply_nba_teams_distance():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=RESOURCES, sql_file_name='teams_distance_apply.sql')
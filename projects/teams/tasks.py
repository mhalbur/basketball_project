import projects.teams.custom as teams
from packages.connectors.sqlite import clean_table, execute_sql


RESOURCES = 'projects/teams/resources'


def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='teams_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='teams.sql')


def stage_nba_teams():
    clean_table(table="working_teams_st")
    team_data = teams.get_nba_teams()
    format_team_data = teams.format_nba_teams(data=team_data)
    loader = teams.load_nba_team()

    for row in format_team_data:
        loader.send(row)


def apply_nba_teams():
    execute_sql(sql_file_path=RESOURCES, sql_file_name='teams_apply.sql')
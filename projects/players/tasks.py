import projects.players.custom as players
from etl.common.database import clean_table, execute_sql
from etl.common.transform import loader
from etl.common.nba_common import get_nba_teams
from projects.players.config import ddls, resources


def install_script():
    file_path_list = [f'{ddls}/players_st.sql', f'{ddls}/players.sql']
    execute_sql(file_paths=file_path_list)


def stage_nba_players():
    clean_table(tables=['working_players_st'])
    team_data = get_nba_teams()
    player_data = players.get_nba_players(data=team_data)
    format_player_data = players.format_nba_players(data=player_data)
    loader = players.load_nba_players()

    for row in format_player_data:
        loader.send(row)


def apply_nba_players():
    file_path_list = [f'{resources}/players_apply.sql']
    execute_sql(file_paths=file_path_list)

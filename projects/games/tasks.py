import projects.games.custom as games
from packages.connectors.sqlite3 import clean_table, execute_sql
from packages.infrastructure import formatter


RESOURCES = 'projects/boxscore_player/resources'

#HATE THIS... NEED TO SET UP A CONFIG FILE
boxscore_players_fields = ["game_id",
                           "team_id",
                           "player_id",
                           "start_position",
                           "min",
                           "fgm",
                           "fga",
                           "fg_pct",
                           "fg3m",
                           "fg3a",
                           "fg3_pct",
                           "ftm",
                           "fta",
                           "ft_pct",
                           "oreb",
                           "dreb",
                           "reb",
                           "ast",
                           "stl",
                           "blk",
                           "to",
                           "pf",
                           "pts",
                           "plus_minus"]


def install_script():
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='boxscore_player_st.sql')
    execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='boxscore_player.sql')


def stage_nba_boxscore_player():
    nba_games = ["season_id", "game_id", "game_date", "matchup"]

    clean_table(table="working_games_st")
    game_data = games.get_nba_games()
    format = formatter(data=game_data, fields=nba_games)
    loader = games.load_nba_games()

    for row in format:
        loader.send(row)


def apply_nba_boxscore_player():
    execute_sql(sql_file_path=RESOURCES, sql_file_name='boxscore_player_apply.sql')
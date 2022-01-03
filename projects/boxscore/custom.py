from etl.common import formatter, loader
from etl.connectors.sqlite import SQLite3
from nba_api.stats.endpoints import boxscoretraditionalv2
from projects.boxscore.config import players_fields, resources, team_fields


class Boxscore():
    def __init__(self):
        self.boxscore = None
        self.game_ids = []

    def clean_staging_tables(self):
        with SQLite3 as db:
            db.clean_table(table="working_boxscore_player_st")
            db.clean_table(table="working_boxscore_team_st")

    def get_game_ids():
        with SQLite3 as db:
            games = db.select_sql(sql_file_path=resources, sql_file_name="game_select.sql")

        game_ids = []
        for game in games:
            game_ids.append(f"00{game[0]}")

        self.game_ids = game_ids
            
        
            
def stage_boxscore_player(data):
    player_data = extract_boxscore_data(data=data, type='PlayerStats')
    player_format = formatter(data=player_data, fields=players_fields, none_val=0)
    player_loader = loader(sql_file=f'{resources}/player_stage.sql')

    for row in player_format:
        player_loader.send(row)


def stage_boxscore_team(data):
    team_data = extract_boxscore_data(data=data, type='TeamStats')
    team_format = formatter(data=team_data, fields=team_fields, none_val=0)
    team_loader = loader(sql_file=f'{resources}/team_stage.sql')

    for row in team_format:
        team_loader.send(row)


def get_boxscore(game_id):
    boxscore_dataset = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=120)
    boxscore = boxscore_dataset.get_normalized_dict()
    yield boxscore


def extract_boxscore_data(data, type):
    for row in data:
        for boxscore in row[type]:
            yield boxscore

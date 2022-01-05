import logging
import rich
from etl.common.generic import formatter, loader
from etl.connectors.sqlite import SQLite3
from nba_api.stats.endpoints import boxscoretraditionalv2
from projects.boxscore.config import players_fields, resources, team_fields

class Boxscore_Traditional():
    def __init__(self, game_id):
        self.boxscore_data = None
        self.game_id = game_id
        self.log = None

    def __enter__(self):
        self.log = logging.getLogger('projects.boxscore.custom')
        boxscore_dataset = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=self.game_id, timeout=120)
        self.boxscore_data = boxscore_dataset.get_normalized_dict()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.log.info("Execution type:", exc_type)
        self.log.info("Execution value:", exc_value)
        self.log.info("Traceback:", traceback)

    def get_boxscore(self):
        boxscore_dataset = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=self.game_id, timeout=120)
        return boxscore_dataset.get_normalized_dict()

    def extract_boxscore_data(self, data, boxscore_field):
        for row in data[boxscore_field]:
            yield row

    def stage_boxscore_player(self):
        player_data = self.extract_boxscore_data(data=self.boxscore_data, boxscore_field='PlayerStats')
        player_format = formatter(data=player_data, fields=players_fields, none_val=0)
        player_loader = loader(sql_file=f'{resources}/player_stage.sql')

        for row in player_format:
            player_loader.send(row)


    def stage_boxscore_team(self):
        team_data = self.extract_boxscore_data(data=self.boxscore_data, boxscore_field='TeamStats')
        team_format = formatter(data=team_data, fields=team_fields, none_val=0)
        team_loader = loader(sql_file=f'{resources}/team_stage.sql')

        for row in team_format:
            team_loader.send(row)

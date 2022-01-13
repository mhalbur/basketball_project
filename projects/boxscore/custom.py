import arrow
import logging

from etl.functions.file import write_gzipped_csv_dict
from etl.functions.nba import get_game_ids
from nba_api.stats.endpoints import boxscoretraditionalv2
from projects.boxscore.config import (PLAYER_FIELDS, TEAM_FIELDS,
                                      WORKING_FILE_PATH)

log = logging.getLogger(__name__)


def boxscore_main():
    game_ids = get_game_ids()

    # for game_id in game_ids:
    game_id = "0022100007"
    print(game_id)
    boxscore_data = get_boxscore(game_id=game_id)
    print(boxscore_data)
    boxscore_player_writer(game_id=game_id, player_data=boxscore_data['PlayerStats'])
    boxscore_team_writer(game_id=game_id, team_data=boxscore_data['TeamStats'])


def boxscore_player_writer(game_id, player_data):
    file_path = f'{WORKING_FILE_PATH}/{arrow.get().format("YYYYMMDD")}_{game_id}_player.txt.gz'
    write_gzipped_csv_dict(file_path=file_path, data=player_data, fields=PLAYER_FIELDS)


def boxscore_team_writer(game_id, team_data):
    file_path = f'{WORKING_FILE_PATH}/{arrow.get().format("YYYYMMDD")}_{game_id}_team.txt.gz'
    write_gzipped_csv_dict(file_path=file_path, data=team_data, fields=TEAM_FIELDS)


def get_boxscore(game_id):
    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=120)
    return boxscore.get_normalized_dict()

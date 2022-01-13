import arrow
import csv
import gzip
from nba_api.stats.endpoints import commonteamroster

from etl.common.transform import format_dict
from etl.functions.nba import get_team_ids
from projects.players.config import PLAYER_FIELDS, WORKING_FILE_PATH


def get_players(team_id):
    roster = commonteamroster.CommonTeamRoster(team_id=team_id, timeout=120)
    roster_dict = roster.get_normalized_dict()
    return roster_dict['CommonTeamRoster']


def write_players(team_id):
    player_data = get_players(team_id)
    file_path = f'{WORKING_FILE_PATH}/{arrow.now().format("YYYYMMDD")}_{team_id}.txt.gz'
    with gzip.open(file_path, 'wt', compresslevel=6) as f:
        writer = csv.DictWriter(f, fieldnames=PLAYER_FIELDS)
        for player in player_data:
            parsed_player = format_dict(row=player, fields=PLAYER_FIELDS)
            writer.writerow(parsed_player)


def players_main():
    team_ids = get_team_ids()
    for team_id in team_ids:
        write_players(team_id=team_id)

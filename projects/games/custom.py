from typing import Dict

import arrow
import pandas as pd
from etl.functions.nba import get_max_game_date
from etl.common.transform import row_formatter
from nba_api.stats.endpoints import leaguegamelog
from projects.games.config import GAME_FIELDS, WORKING_FILE_PATH


def games_api_extract():
    """Retrieves game data from swar's NBA API

    Returns:
        [List]: Parsed/formatted list of data from the API
    """
    game_dict = []
    game_date = get_max_game_date()

    game_logs = leaguegamelog.LeagueGameLog(date_from_nullable=game_date, timeout=120)
    games = game_logs.get_normalized_dict()

    for game in games['LeagueGameLog']:
        row = row_formatter(row=game, fields=GAME_FIELDS)
        game_dict.append(row)

    return game_dict


def write_game_files(games: Dict):
    """Takes games from the API, inserts it into a dataframe, group it by game_date, and write it to a csv file

    Args:
        games (Dict): Game data retrieved from games_api_extract()
    """
    df = pd.DataFrame(data=games, columns=GAME_FIELDS)
    gb = df.groupby('game_date')
    for x in gb.groups:
        file_name = f"{WORKING_FILE_PATH}/{arrow.get(x).format('YYYYMMDD')}.txt.gz"
        date_group = gb.get_group(x)
        date_group.to_csv(path_or_buf=file_name, header=False, index=False, compression="gzip")

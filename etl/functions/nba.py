import logging

import arrow
from nba_api.stats.static import teams
from etl.functions.database import select_sql

log = logging.getLogger(__name__)


def get_nba_teams():
    return teams.get_teams()


def get_team_ids():
    team_ids = []
    teams = get_nba_teams()
    for team in teams:
        team_ids.append(team['id'])
    return team_ids


def get_game_ids():
    games = select_sql(file_paths=['etl/sql_files/game_select.sql'])

    game_ids = []
    for game in games:
        game_ids.append(f"00{game[0]}")

    return game_ids


def get_max_game_date():
    try:
        database_rtn = select_sql(file_paths=['etl/sql_files/game_max_date.sql'])
        for date in database_rtn:
            max_date = arrow.get(date[0]).shift(days=-2).format('YYYY-MM-DD')
            log.info(f'Max date: {max_date}')
            return max_date
    except TypeError:
        log.info("No max date...")
        return arrow.utcnow().shift(months=-3).format('YYYY-MM-DD')
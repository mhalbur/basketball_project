import arrow
import logging
from nba_api.stats.endpoints import leaguegamelog
from etl.connectors.sqlite import SQLite3


log = logging.getLogger(__name__)


def get_max_game_date():
    log.info("Retrieving max date from table")
    try:
        with SQLite3() as db:
            database_obj = db.select_sql(sql=str("select max(game_date) from games"))
        for date in database_obj:
            log.info('Shifting max date -2 days...')
            max_date = arrow.get(date[0]).shift(days=-2).format('YYYY-MM-DD')
            log.info(f'Max date: {max_date}')
            return max_date
    except TypeError:
        log.info("No max date...")
        return arrow.utcnow().shift(months=-3).format('YYYY-MM-DD')


def get_nba_games():
    max_date = get_max_game_date()
    game_logs = leaguegamelog.LeagueGameLog(date_from_nullable=max_date, timeout=120)
    logs = game_logs.get_normalized_dict()
    for game in logs['LeagueGameLog']:
        yield game
        
        
# def get_nba_games():
#     data = []
#     max_date = get_max_game_date()
#     game_logs = leaguegamelog.LeagueGameLog(date_from_nullable=max_date, timeout=120)
#     logs = game_logs.get_normalized_dict()
#     for game in logs['LeagueGameLog']:
#         clean_game = {}
#         clean_game['season_id'] = game['SEASON_ID']
#         clean_game['game_id'] = game['GAME_ID']
#         clean_game['game_date'] = game['GAME_DATE']
#         clean_game['matchup'] = game['MATCHUP']
#         data.append(clean_game)
    
#     return data
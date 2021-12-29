import arrow
from nba_api.stats.endpoints import leaguegamelog
from packages.connectors.sqlite import SQLite3
from generic.infrastructure import coroutine


def get_max_game_date():
    try: 
        with SQLite3() as db:
            database_obj = db.execute_sql(sql="select max(game_date) from games")
        for date in database_obj:
            dt = arrow.get(date[0]) 
            update_dt = dt.shift(days=-2)
            return update_dt.format('YYYY-MM-DD')
    except TypeError:
        return '2021-10-01'


def get_nba_games():
    max_date = get_max_game_date()
    game_logs = leaguegamelog.LeagueGameLog(date_from_nullable=max_date, timeout=120)
    logs = game_logs.get_normalized_dict()
    for game in logs['LeagueGameLog']:
        yield game


@coroutine
def load_nba_games():
    while True:
        row = yield
        with SQLite3() as db:
            db.execute_sql(sql_file_path='projects/games/resources',
                           sql_file_name='games_stage.sql',
                           game_id=row['game_id'],
                           season_id=row['season_id'],
                           game_date=row['game_date'],
                           matchup=row['matchup'])

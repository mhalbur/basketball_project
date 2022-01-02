import arrow
from nba_api.stats.endpoints import leaguegamelog
from packages.connectors.sqlite import SQLite3
from generic.infrastructure import coroutine


def get_max_game_date():
    try:
        with SQLite3() as db:
            database_obj = db.select_sql(sql=str("select max(game_date) from games"))
        for date in database_obj:
            return arrow.get(date[0]).shift(days=-2).format('YYYY-MM-DD')
    except TypeError:
        return arrow.utcnow().shift(months=-3).format('YYYY-MM-DD')


def get_nba_games():
    max_date = get_max_game_date()
    game_logs = leaguegamelog.LeagueGameLog(date_from_nullable=max_date, timeout=120)
    logs = game_logs.get_normalized_dict()
    for game in logs['LeagueGameLog']:
        yield game


@coroutine
def load_nba_games():
    with SQLite3() as db:
        while True:
            row = yield
            print(row)
            row_dict = list(row.values())
            db.sql_file = 'projects/games/resources/games_stage.sql'
            sql = db.read_sql_file(list=row_dict)
            db.execute_sql(sql=sql)

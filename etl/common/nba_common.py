from etl.connectors.sqlite import SQLite3
from nba_api.stats.static import teams

def get_nba_teams():
    nba_teams = teams.get_teams()
    for team in nba_teams:
        yield team


def get_game_ids():
    with SQLite3() as db:
        games = db.select_sql(sql_file_path='etl/resources', sql_file_name="game_select.sql")

    game_ids = []
    for game in games:
        game_ids.append(f"00{game[0]}")

    return game_ids

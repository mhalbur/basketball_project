from nba_api.stats.endpoints import boxscoretraditionalv2
from packages.connectors.sqlite import SQLite3
from generic.infrastructure import coroutine
from generic.infrastructure import formatter
from projects.boxscore_player.config import boxscore_players_fields


def stage_boxscore_player(game_id):
    print("here")
    data = get_boxscore(game_id=game_id)
    format = formatter(data=data, fields=boxscore_players_fields)
    loader = load_boxscore_players()

    for row in format:
        loader.send(row)


def get_boxscore(game_id):
    boxscore_dataset = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=120)
    boxscore = boxscore_dataset.get_normalized_dict()
    for row in boxscore["PlayerStats"]:
        yield row


@coroutine
def load_boxscore_players():
    while True:
        row = yield
        for key in row:
            if row[key] is None:
                row[key] = 0
            elif row[key] == '':
                row[key] = "NULL"
        with SQLite3() as db:
            db.execute_sql(sql_file_path='projects/boxscore_player/resources',
                           sql_file_name='boxscore_player_stage.sql',
                           *boxscore_players_fields)

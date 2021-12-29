from nba_api.stats.endpoints import boxscoretraditionalv2
from packages.connectors.sqlite import SQLite3
from packages.infrastructure import coroutine
from packages.infrastructure import formatter
from projects.boxscore_player.config import boxscore_players_fields


def stage_boxscore_player(game_id):
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
                           game_id=row['game_id'],
                           team_id=row["team_id"],
                           player_id=row["player_id"],
                           start_position=row["start_position"],
                           min=row["min"],
                           fgm=row["fgm"],
                           fga=row["fga"],
                           fg_pct=row["fg_pct"],
                           fg3m=row["fg3m"],
                           fg3a=row["fg3a"],
                           fg3_pct=row["fg3_pct"],
                           ftm=row["ftm"],
                           fta=row["fta"],
                           ft_pct=row["ft_pct"],
                           oreb=row["oreb"],
                           dreb=row["dreb"],
                           reb=row["reb"],
                           ast=row["ast"],
                           stl=row["stl"],
                           blk=row["blk"],
                           to=row["to"],
                           pf=row["pf"],
                           pts=row["pts"],
                           plus_minus=row["plus_minus"])
        print("complete")

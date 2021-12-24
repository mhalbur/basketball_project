from nba_api.stats.endpoints import boxscoretraditionalv2
from packages.connectors.sqlite3 import execute_sql, read_sql_file
from packages.infrastructure import coroutine


def get_boxscore(game_id, type):
    boxscore_dataset = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=f"00{game_id}")
    boxscore = boxscore_dataset.get_normalized_dict()
    for row in boxscore[type]:
        yield row


# IN PROGRESS NOT COMPLETE
@coroutine
def load_boxscore_players():
    while True:
        row = yield
        for key in row:
            if row[key] is None:
                row[key] = 0
            elif row[key] == '':
                row[key] = "NULL"
        load_sql = read_sql_file(file_path='resources/boxscore_player_stage.sql', 
                                 game_id=row['game_id'],
                                 team_id = row["team_id"],
                                 player_id = row["player_id"],
                                 start_position = row["start_position"],
                                 min = row["min"],
                                 fgm = row["fgm"],
                                 fga = row["fga"],
                                 fg_pct = row["fg_pct"],
                                 fg3m = row["fg3m"],
                                 fg3a = row["fg3a"],
                                 fg3_pct = row["fg3_pct"],
                                 ftm = row["ftm"],
                                 fta = row["fta"],
                                 ft_pct = row["ft_pct"],
                                 oreb = row["oreb"],
                                 dreb = row["dreb"],
                                 reb = row["reb"],
                                 ast = row["ast"],
                                 stl = row["stl"],
                                 blk = row["blk"],
                                 to = row["to"],
                                 pf = row["pf"],
                                 pts = row["pts"],
                                 plus_minus = row["plus_minus"])
        execute_sql(sql=load_sql)

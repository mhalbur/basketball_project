from geopy.distance import great_circle
from etl.connectors.sqlite import SQLite3
from etl.common import coroutine


def get_team_info():
    with SQLite3() as db:
        results = db.select_sql(sql_file_path="projects/teams_distance/resources", sql_file_name="teams_select.sql")
    for team1 in results:
        team_distances = {}
        team_distances["team_id"] = team1[0]
        for team2 in results:
            dist = cacluate_distance(team1_lat=team1[1], team1_long=team1[2], team2_lat=team2[1], team2_long=team2[2])
            team_distances[team2[0]] = dist
        yield team_distances


def cacluate_distance(team1_lat, team1_long, team2_lat, team2_long):
    team_1 = (team1_lat, team1_long)
    team_2 = (team2_lat, team2_long)
    return great_circle(team_1, team_2).miles


@coroutine
def load_nba_team_distances():
    while True:
        row = yield
        with SQLite3() as db:
            db.execute_sql(sql_file_path='projects/teams_distance/resources',
                           sql_file_name='teams_distance_st.sql',
                           team_id=row["team_id"],
                           t1610612737=row[1610612737],
                           t1610612738=row[1610612738],
                           t1610612739=row[1610612739],
                           t1610612740=row[1610612740],
                           t1610612741=row[1610612741],
                           t1610612742=row[1610612742],
                           t1610612743=row[1610612743],
                           t1610612744=row[1610612744],
                           t1610612745=row[1610612745],
                           t1610612746=row[1610612746],
                           t1610612747=row[1610612747],
                           t1610612748=row[1610612748],
                           t1610612749=row[1610612749],
                           t1610612750=row[1610612750],
                           t1610612751=row[1610612751],
                           t1610612752=row[1610612752],
                           t1610612753=row[1610612753],
                           t1610612754=row[1610612754],
                           t1610612755=row[1610612755],
                           t1610612756=row[1610612756],
                           t1610612757=row[1610612757],
                           t1610612758=row[1610612758],
                           t1610612759=row[1610612759],
                           t1610612760=row[1610612760],
                           t1610612761=row[1610612761],
                           t1610612762=row[1610612762],
                           t1610612763=row[1610612763],
                           t1610612764=row[1610612764],
                           t1610612765=row[1610612765],
                           t1610612766=row[1610612766])

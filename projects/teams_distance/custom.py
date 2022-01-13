import arrow
from geopy.distance import great_circle

from etl.functions.database import select_sql
from etl.functions.file import write_gzipped_csv_dict
from projects.teams_distance.config import (FIELDS, SQL_FILE_PATH,
                                            WORKING_FILE_PATH)


def team_distance_main():
    results = select_sql(file_path=f"{SQL_FILE_PATH}/teams_select.sql")
    data = get_team_distance(results=results)
    file_path = f'{WORKING_FILE_PATH}/{arrow.now().format("YYYYMMDD")}_team_distance.txt.gz'
    write_gzipped_csv_dict(file_path=file_path, data=data, fields=FIELDS)


def get_team_distance(results):
    teams = []

    for team1 in results:
        team_distances = {}
        team_distances["team_id"] = team1[0]
        for team2 in results:
            dist = cacluate_distance(team1_lat=team1[1], team1_long=team1[2], team2_lat=team2[1], team2_long=team2[2])
            team_distances[team2[0]] = dist
        teams.append(team_distances)
    return teams


def cacluate_distance(team1_lat, team1_long, team2_lat, team2_long):
    team_1 = (team1_lat, team1_long)
    team_2 = (team2_lat, team2_long)
    return great_circle(team_1, team_2).miles

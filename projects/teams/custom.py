import arrow
import csv
import gzip
from geopy.geocoders import Nominatim

from etl.common.transform import format_dict
from etl.functions.nba import get_nba_teams
from projects.teams.config import FIELDS, WORKING_FILE_PATH


def teams_main():
    team_data = get_nba_teams()
    file_path = f'{WORKING_FILE_PATH}/{arrow.now().format("YYYYMMDD")}_teams.txt.gz'
    with gzip.open(file_path, 'wt', compresslevel=6) as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        for team in team_data:
            parsed_team = format_dict(row=team, fields=FIELDS)

            city = parsed_team['city']
            state = parsed_team['state']

            location = get_long_lat(city, state)

            parsed_team['latitude'] = location[0]
            parsed_team['longitude'] = location[1]

            writer.writerow(parsed_team)


def get_long_lat(city, state):
    geolocator = Nominatim(user_agent="nba_basketball")
    location = geolocator.geocode(f"{city} {state}")
    return location.latitude, location.longitude

from geopy.geocoders import Nominatim
from nba_api.stats.static import teams
from packages.connectors.sqlite import SQLite3
from packages.infrastructure import coroutine


def get_nba_teams():
    nba_teams = teams.get_teams()
    for team in nba_teams:
        yield team


def get_long_lat(city, state):
    geolocator = Nominatim(user_agent="nba_basketball")
    location = geolocator.geocode(f"{city} {state}")
    return location.latitude, location.longitude


def format_nba_teams(data):
    for row in data:
        nba_team = {}
        nba_team['team_id'] = row['id']
        nba_team['team_name'] = row['full_name']
        nba_team['abbr'] = row['abbreviation']

        city = row['city']
        state = row['state']

        nba_team['city'] = city
        nba_team['state'] = state

        loc = get_long_lat(city=city, state=state)

        lat = loc[0]
        long = loc[1]

        nba_team['latitude'] = lat
        nba_team['longitude'] = long

        yield nba_team


@coroutine
def load_nba_team():
    while True:
        row = yield
        with SQLite3() as db:
            db.execute_sql(sql_file_path='projects/teams/resources',
                           sql_file_name='teams_stage.sql',
                           team_id=row['team_id'],
                           team_name=row['team_name'],
                           abbr=row['abbr'],
                           city=row['city'],
                           state=row['state'],
                           latitude=row['latitude'],
                           longitude=row['longitude'])

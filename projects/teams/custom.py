from geopy.geocoders import Nominatim


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
import arrow

from database import execute_sql, read_sql_file, clean_table, select_sql
from functools import wraps

from geopy.distance import great_circle
from geopy.geocoders import Nominatim
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster, leaguegamelog


def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer


"""
TEAMS
"""
def get_nba_teams():
    nba_teams = teams.get_teams()
    for team in nba_teams:
        yield team


def get_long_lat(city, state):
    geolocator = Nominatim(user_agent="nba_basketball_practice")
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
        load_sql = read_sql_file(file_path='resources/teams_stage.sql', 
                                 team_id=row['team_id'],
                                 team_name=row['team_name'],
                                 abbr=row['abbr'],
                                 city=row['city'],
                                 state=row['state'],
                                 latitude=row['latitude'],
                                 longitude=row['longitude'])
        execute_sql(database_file="nba_basketball.db", sql=load_sql)
        

"""
PLAYERS
"""
def get_nba_players(data):
    for row in data:
        team_id = row['id']
        roster = commonteamroster.CommonTeamRoster(team_id=team_id);
        roster_dict = roster.get_normalized_dict()
        for nba_player in roster_dict['CommonTeamRoster']:
            yield nba_player
            

def format_nba_players(data):
    for nba_player in data:
        player = {}
        player_name = nba_player['PLAYER'].split(' ')
        player['player_id'] = nba_player['PLAYER_ID']
        player['team_id'] = nba_player['TeamID']
        player['first_name'] = player_name[0]
        player['last_name'] = player_name[1]
        player['jersey_number'] = nba_player['NUM']
        player['position'] = nba_player['POSITION']
        player['age'] = int(nba_player['AGE'])
        player['height'] = nba_player['HEIGHT']
        player['experience'] = nba_player['EXP']
        yield player


@coroutine
def load_nba_players():
    while True:
        row = yield
        load_sql = read_sql_file(file_path='resources/players_stage.sql', 
                                 player_id=row['player_id'],
                                 team_id=row['team_id'],
                                 first_name=row['first_name'],
                                 last_name=row['last_name'],
                                 jersey_number=row['jersey_number'],
                                 position=row['position'],
                                 age=row['age'],
                                 height=row['height'],
                                 experience=row['experience'])
        execute_sql(database_file="nba_basketball.db", sql=load_sql)
        

"""
GAMES
"""
def get_max_game_date():
    try: 
        database_obj = execute_sql(sql="select max(game_date) from nba_games")
        for date in database_obj:
            dt = arrow.get(date[0]) 
            update_dt = dt.shift(days=-2)
            return update_dt.format('YYYY-MM-DD')
    except TypeError as ex:
        return '2021-10-01'


def get_nba_games():
    max_date = get_max_game_date()
    game_logs = leaguegamelog.LeagueGameLog(date_from_nullable=max_date)
    logs = game_logs.get_normalized_dict()
    for game in logs['LeagueGameLog']:
        yield game


def format_nba_games(data):
    for game in data:
        game_log = {}
        game_log["season_id"] = game['SEASON_ID']
        game_log["game_id"] = game['GAME_ID']
        game_log["game_date"] = game["GAME_DATE"]
        game_log["matchup"] = game["MATCHUP"]
        yield game_log


@coroutine
def load_nba_games():
    while True:
        row = yield
        load_sql = read_sql_file(file_path='resources/games_stage.sql', 
                                 game_id=row['game_id'],
                                 season_id=row['season_id'],
                                 game_date=row['game_date'],
                                 matchup = row['matchup'],
                                 )
        execute_sql(database_file="nba_basketball.db", sql=load_sql)
        

"""
TEAMS DISTANCE
"""
def get_team_info():
    results = select_sql(sql_file_path="resources", sql_file_name="teams_select.sql")
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
        load_sql = read_sql_file(file_path='resources/teams_distances_st.sql',
                                 team_id = row["team_id"],
                                 t1610612737 = row[1610612737],
                                 t1610612738 = row[1610612738],
                                 t1610612739 = row[1610612739],
                                 t1610612740 = row[1610612740],
                                 t1610612741 = row[1610612741],
                                 t1610612742 = row[1610612742],
                                 t1610612743 = row[1610612743],
                                 t1610612744 = row[1610612744],
                                 t1610612745 = row[1610612745],
                                 t1610612746 = row[1610612746],
                                 t1610612747 = row[1610612747],
                                 t1610612748 = row[1610612748],
                                 t1610612749 = row[1610612749],
                                 t1610612750 = row[1610612750],
                                 t1610612751 = row[1610612751],
                                 t1610612752 = row[1610612752],
                                 t1610612753 = row[1610612753],
                                 t1610612754 = row[1610612754],
                                 t1610612755 = row[1610612755],
                                 t1610612756 = row[1610612756],
                                 t1610612757 = row[1610612757],
                                 t1610612758 = row[1610612758],
                                 t1610612759 = row[1610612759],
                                 t1610612760 = row[1610612760],
                                 t1610612761 = row[1610612761],
                                 t1610612762 = row[1610612762],
                                 t1610612763 = row[1610612763],
                                 t1610612764 = row[1610612764],
                                 t1610612765 = row[1610612765],
                                 t1610612766 = row[1610612766] 
                                )
        print(load_sql)
        execute_sql(database_file="nba_basketball.db", sql=load_sql)


"""
CALLABLE FUNCTIONS 
"""
def stage_nba_team():
    clean_table(table="working_nba_teams_st")
    team_data = get_nba_teams()
    format_team_data = format_nba_teams(data=team_data)
    loader = load_nba_team()

    for row in format_team_data:
        loader.send(row)


def stage_nba_players():
    clean_table(table="working_nba_players_st")
    team_data = get_nba_teams()
    player_data = get_nba_players(data=team_data)
    format_player_data = format_nba_players(data=player_data)
    loader = load_nba_players()

    for row in format_player_data:
        loader.send(row)


def stage_nba_games():
    clean_table(table="working_nba_games_st")
    game_data = get_nba_games()
    format_game_data = format_nba_games(data=game_data)
    loader = load_nba_games()

    for row in format_game_data:
        loader.send(row)


def stage_nba_teams_distances():
    clean_table(table="working_nba_teams_distance_st")
    team_info = get_team_info()
    loader = load_nba_team_distances()

    for row in team_info:
        loader.send(row)

# stage_nba_teams_distances()
from database import execute_sql, read_sql_file
from functools import wraps

from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster


def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer


def get_nba_teams():
    nba_teams = teams.get_teams()
    for team in nba_teams:
        yield team


def format_nba_teams(data):
    for row in data:
        nba_team = {}
        nba_team['team_id'] = row['id']
        nba_team['team_name'] = row['full_name']
        nba_team['abbr'] = row['abbreviation']
        nba_team['city'] = row['city']
        nba_team['state'] = row['state']
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
                                    state=row['state'])
        execute_sql(database_file="nba_basketball.db", sql=load_sql)
        

def get_nba_players(data):
    for row in data:
        team_id = row['id']
        roster = commonteamroster.CommonTeamRoster(team_id=team_id);
        roster_dict = roster.get_normalized_dict()
        for nba_player in roster_dict['CommonTeamRoster']:
            print(nba_player)
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
        print(player)
        yield player


@coroutine
def load_nba_players():
    while True:
        row = yield
        print(row)
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
        

def stage_nba_team():
    team_data = get_nba_teams()
    format_team_data = format_nba_teams(data=team_data)
    loader = load_nba_team()

    for row in format_team_data:
        loader.send(row)


def stage_nba_players():
    team_data = get_nba_teams()
    player_data = get_nba_players(data=team_data)
    format_player_data = format_nba_players(data=player_data)
    loader = load_nba_players()

    for row in format_player_data:
        loader.send(row)


# TESTING
# stage_nba_players()

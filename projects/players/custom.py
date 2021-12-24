from nba_api.stats.endpoints import commonteamroster
from packages.connectors.sqlite3 import execute_sql, read_sql_file
from packages.common import get_nba_teams
from packages.infrastructure import coroutine


def get_nba_players(data):
    for row in data:
        team_id = row['id']
        roster = commonteamroster.CommonTeamRoster(team_id=team_id, timeout=120)
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
        load_sql = read_sql_file(file_path='projects/players/resources/players_stage.sql', 
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
from etl.common.transform import coroutine
from etl.connectors.sqlite import SQLite3
from nba_api.stats.endpoints import commonteamroster
from etl.functions.nba import get_team_ids


# redo of player api after completing games - commenting out until complete
# def get_players():    
#     for team_id in get_team_ids():
#         player_data = player_api_extract(team_id=team_id)
#         format_player_data = format_nba_players(data=player_data)
        
#         for row in format_player_data:
#             <>.send(row)

    
def player_api_extract(team_id):
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
    file_path = 'projects/players/resources/players_stage.sql'
    with SQLite3() as db:
        while True:
            row = yield
            db.execute_sql(file_path = file_path,
                           player_id=row['player_id'],
                           team_id=row['team_id'],
                           first_name=row['first_name'],
                           last_name=row['last_name'],
                           jersey_number=row['jersey_number'],
                           position=row['position'],
                           age=row['age'],
                           height=row['height'],
                           experience=row['experience'])

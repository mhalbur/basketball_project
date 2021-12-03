import json

from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import commonteamroster


# def get_nba_team():
    


def get_nba_team_id():
    nba_teams = teams.get_teams()
    team_ids = []
    for team in nba_teams:
        team_ids.append(team['id'])

    return team_ids


def get_nba_players():
    players = []
    team_ids = get_nba_team_id()
    for team_id in team_ids:
        roster = commonteamroster.CommonTeamRoster(team_id=team_id);
        roster_dict = roster.get_normalized_dict()
        for nba_player in roster_dict['CommonTeamRoster']:
            print(nba_player)
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
            players.append(player)
            print(player)
        print("\n\n")


get_nba_players()
# get_nba_team_id()
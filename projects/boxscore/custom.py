from nba_api.stats.endpoints import boxscoretraditionalv2
from generic.infrastructure import formatter, loader
from projects.boxscore.config import players_fields, team_fields, resources


def stage_boxscore_player(game_id):
    data = get_boxscore(game_id=game_id)

    # player_data = extract_boxscore_data(data=data, type='PlayerStats')
    team_data = extract_boxscore_data(data=data, type='TeamStats')

    # player_format = formatter(data=player_data, fields=players_fields, none_val=0)
    team_format = formatter(data=team_data, fields=team_fields, none_val=0)

    # player_loader = loader(sql_file=f'{resources}/player_stage.sql')
    team_loader = loader(sql_file=f'{resources}/team_stage.sql')

    # for row in player_format:
        # player_loader.send(row)

    for row in team_format:
        team_loader.send(row)


def get_boxscore(game_id):
    boxscore_dataset = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=120)
    boxscore = boxscore_dataset.get_normalized_dict()
    yield boxscore


def extract_boxscore_data(data, type):
    for row in data:
        for boxscore in row[type]:
            yield boxscore

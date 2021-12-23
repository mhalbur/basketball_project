from nba_api.stats.static import teams


def get_nba_teams():
    nba_teams = teams.get_teams()
    for team in nba_teams:
        yield team
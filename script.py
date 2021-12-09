from database import execute_sql
from unnamed import stage_nba_team, stage_nba_players, stage_nba_games

DDL_FILE_PATH = "resources/ddls"
RESOURCES = "resources"

"""
PLAYERS
"""
# create players staging table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_players_st.sql")

# insert fresh data into staging table
stage_nba_players()

"""
TEAMS
"""
# create teams staging table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_teams_st.sql")

# create teams table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_teams.sql")

# insert fresh data into staging table
stage_nba_team()

# apply teams data
execute_sql(sql_file_path=RESOURCES, sql_file_name="teams_apply.sql")

"""
GAMES
"""
# create games staging table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_games_st.sql")

# create games table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_games.sql")

# insert fresh data into staging table
stage_nba_games()

# apply to games table
# execute_sql(database_file=DATABASE_FILE, sql_file_path=RESOURCES, sql_file_name="games_apply.sql")

from database import execute_sql
from unnamed import stage_nba_team, stage_nba_players, stage_nba_games, stage_nba_teams_distances

DDL_FILE_PATH = "resources/ddls"
RESOURCES = "resources"


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
TEAM'S DISTANCES
"""
# create teams staging table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_teams_distance_st.sql")

# create teams table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_teams_distance.sql")

# insert fresh data into staging table
stage_nba_teams_distances

# apply teams distance data
execute_sql(sql_file_path=RESOURCES, sql_file_name="teams_distances_apply.sql")


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
execute_sql(sql_file_path=RESOURCES, sql_file_name="games_apply.sql")

"""
PLAYERS
"""
# create players staging table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_players_st.sql")

# create players table
execute_sql(sql_file_path=DDL_FILE_PATH, sql_file_name="nba_players.sql")

# insert fresh data into staging table
stage_nba_players()

# delete players data
execute_sql(sql_file_path=RESOURCES, sql_file_name="players_delete.sql")

# apply players data
execute_sql(sql_file_path=RESOURCES, sql_file_name="players_apply.sql")

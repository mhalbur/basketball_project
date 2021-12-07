from database import read_sql_file, execute_sql
from unnamed import stage_nba_team

DATABASE_FILE = "nba_basketball.db"

create_players_table = read_sql_file(file_path="resources/ddls/nba_players_st.sql")
print(create_players_table)
execute_sql(database_file=DATABASE_FILE, sql=create_players_table)

# this will only ever need to be ran once
create_teams_staging_table = read_sql_file(file_path="resources/ddls/nba_teams_st.sql")
execute_sql(database_file=DATABASE_FILE, sql=create_teams_staging_table)
create_teams_table = read_sql_file(file_path="resources/ddls/nba_teams.sql")
execute_sql(database_file=DATABASE_FILE, sql=create_teams_table)
delete_teams_table = read_sql_file(file_path="resources/teams_delete.sql")
execute_sql(database_file=DATABASE_FILE, sql=delete_teams_table)
stage_nba_team()
apply_teams_table = read_sql_file(file_path="resources/teams_apply.sql")
execute_sql(database_file=DATABASE_FILE, sql=apply_teams_table)
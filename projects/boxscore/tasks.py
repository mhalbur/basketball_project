import projects.boxscore.custom as bsp
from concurrent.futures import ThreadPoolExecutor, as_completed
from packages.connectors.sqlite import SQLite3
from projects.boxscore.config import resources, ddl


def install_script():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=ddl, sql_file_name='player_st.sql')
        db.execute_sql(sql_file_path=ddl, sql_file_name='player.sql')
        db.execute_sql(sql_file_path=ddl, sql_file_name='team_st.sql')
        db.execute_sql(sql_file_path=ddl, sql_file_name='team.sql')


def boxscore_api_extract():
    with SQLite3() as db:
        db.clean_table(table="working_boxscore_player_st")
        db.clean_table(table="working_boxscore_team_st")
        games = db.select_sql(sql_file_path=resources, sql_file_name="game_select.sql")

    futures = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        for game in games:
            game_id = f"00{game[0]}"
            futures.append(ex.submit(bsp.stage_boxscore_player, game_id))

        for future in as_completed(futures):
            result = future.result()


def apply_boxscore():
    with SQLite3() as db:
        # db.execute_sql(sql_file_path=resources, sql_file_name='player_apply.sql')
        db.execute_sql(sql_file_path=resources, sql_file_name='team_apply.sql')

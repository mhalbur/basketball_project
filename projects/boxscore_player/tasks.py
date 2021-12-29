from concurrent.futures import ThreadPoolExecutor, as_completed
import projects.boxscore_player.custom as bsp
from packages.connectors.sqlite import SQLite3

RESOURCES = 'projects/boxscore_player/resources'

def install_script():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='boxscore_player_st.sql')
        db.execute_sql(sql_file_path=f'{RESOURCES}/ddls', sql_file_name='boxscore_player.sql')


def boxscore_api_extract():
    # with SQLite3() as db:
    #     db.clean_table(table="working_boxscore_player_st")
    with SQLite3() as db:
        games = db.select_sql(sql_file_path="projects/boxscore_player/resources", sql_file_name="game_select.sql")

    futures = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        for game in games:
            game_id = f"00{game[0]}"
            print(game_id)
            futures.append(ex.submit(bsp.stage_boxscore_player, game_id))

        for future in as_completed(futures):
            result = future.result()
            print(f"completed {game_id}")


def apply_boxscore_player():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=RESOURCES, sql_file_name='boxscore_player_apply.sql')

from concurrent.futures import ThreadPoolExecutor, as_completed

import projects.boxscore.custom as bsp
from etl.common import cleaner
from etl.connectors.sqlite import SQLite3
from projects.boxscore.config import ddl, resources


def install_script():
    with SQLite3() as db:
        db.execute_sql(sql_file_path=ddl, sql_file_name='player_st.sql')
        db.execute_sql(sql_file_path=ddl, sql_file_name='player.sql')
        db.execute_sql(sql_file_path=ddl, sql_file_name='team_st.sql')
        db.execute_sql(sql_file_path=ddl, sql_file_name='team.sql')


def boxscore_api_extract():
    cleaner(tables=['working_boxscore_player_st', 'working_boxscore_team_st'])

    with SQLite3() as db:
        games = db.select_sql(sql_file_path=resources, sql_file_name="game_select.sql")

    player_futures = []
    boxscore_data = []

    for game in games:
        game_id = f"00{game[0]}"
        data = bsp.get_boxscore(game_id=game_id)
        boxscore_data.append(data)

    for boxscore in boxscore_data:
        bsp.stage_boxscore_team(data=boxscore)

    with ThreadPoolExecutor(max_workers=5) as ex:
        for boxscore in boxscore_data:
            player_futures.append(ex.submit(bsp.stage_boxscore_player, boxscore))

    for future in as_completed(player_futures):
        result = future.result()


def apply_boxscore():
    with SQLite3() as db:
        # db.execute_sql(sql_file_path=resources, sql_file_name='player_apply.sql')
        db.execute_sql(sql_file_path=resources, sql_file_name='team_apply.sql')

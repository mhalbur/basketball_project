create table if not exists working_boxscore_player_st(
    GAME_ID int,
    TEAM_ID int,
    PLAYER_ID int,
    START_POSITION string,
    MIN int,
    FGM int,
    FGA int,
    FG_PCT number,
    FG3M int,
    FG3A int,
    FG3_PCT number,
    FTM int,
    FTA int,
    FT_PCT number,
    OREB int,
    DREB int,
    REB int,
    AST int,
    STL int,
    BLK int,
    "TO" int,
    PF int,
    PTS int,
    PLUS_MINUS int
);
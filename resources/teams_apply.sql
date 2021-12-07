insert into nba_teams
select
    team_id,
    team_name,
    abbr,
    city,
    state
from working_nba_teams_st;
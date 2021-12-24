insert into teams
select
    team_id,
    team_name,
    abbr,
    city,
    state,
    latitude,
    longitude
from working_teams_st;
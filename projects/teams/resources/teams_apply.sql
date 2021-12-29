insert into teams
select
    team_id,
    team_name,
    abbr,
    city,
    state,
    latitude,
    longitude
from working_teams_st a
where not exists(
    select 1
    from teams b
    where a.team_id = b.team_id
);
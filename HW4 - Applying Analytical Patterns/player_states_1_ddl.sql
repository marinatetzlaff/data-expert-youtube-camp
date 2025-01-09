CREATE TABLE player_states (
    player_name TEXT,
    first_active_season INTEGER,
    last_active_season INTEGER,
    yearly_active_state TEXT,
    season INTEGER,
    PRIMARY KEY (player_name, season)
)

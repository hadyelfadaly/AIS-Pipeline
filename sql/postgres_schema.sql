CREATE TABLE IF NOT EXISTS vessels (
    user_id           BIGINT PRIMARY KEY,
    name              TEXT,
    ship_type         INTEGER,
    destination       TEXT,
    call_sign         TEXT,
    imo_number        BIGINT,
    draught_m         DOUBLE PRECISION,
    eta_month         INTEGER,
    eta_day           INTEGER,
    eta_hour          INTEGER,
    eta_minute        INTEGER,
    is_cargo_or_tanker BOOLEAN,
    seen_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS positions (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             BIGINT NOT NULL,
    event_time          TIMESTAMP NOT NULL,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    sog                 DOUBLE PRECISION,
    cog                 DOUBLE PRECISION,
    navigational_status INTEGER,
    name                TEXT,
    cell_id             TEXT,
    hour                TEXT,
    wave_height_m       DOUBLE PRECISION,
    current_velocity_kmh DOUBLE PRECISION,
    wind_speed_kmh      DOUBLE PRECISION,
    polled_at           BIGINT
);

CREATE INDEX IF NOT EXISTS positions_event_time_idx ON positions (event_time DESC);
CREATE INDEX IF NOT EXISTS positions_user_id_idx    ON positions (user_id);

CREATE OR REPLACE VIEW live_positions AS
SELECT DISTINCT ON (p.user_id)
       p.user_id,
       v.name,
       v.ship_type,
       v.destination,
       v.is_cargo_or_tanker,
       p.latitude,
       p.longitude,
       p.sog,
       p.cog,
       p.navigational_status,
       p.event_time
FROM positions p
LEFT JOIN vessels v USING (user_id)
ORDER BY p.user_id, p.event_time DESC;

CREATE TABLE IF NOT EXISTS live_vessels (
    user_id             BIGINT PRIMARY KEY,
    name                TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    sog                 DOUBLE PRECISION,
    cog                 DOUBLE PRECISION,
    navigational_status INTEGER,
    event_time          TIMESTAMP
);
CREATE INDEX IF NOT EXISTS live_vessels_event_time_idx ON live_vessels (event_time DESC);

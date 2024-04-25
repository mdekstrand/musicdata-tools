--: columns:
--: - id
--: - name
--: - gender
--: - type

CREATE TABLE mb_artist (
    artist_id UUID NOT NULL PRIMARY KEY,
    name VARCHAR,
    gender VARCHAR,
    type VARCHAR
);
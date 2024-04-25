--: columns:
--: - id
--: - title
--: - artist-credit
CREATE TABLE mb_recording (
    rec_id UUID NOT NULL PRIMARY KEY,
    title VARCHAR,
    artist_credit JSON
);
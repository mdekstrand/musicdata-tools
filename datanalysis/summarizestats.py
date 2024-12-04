import duckdb
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
mlhd_path = os.path.join(current_dir, "../data/mlhd/*/*.parquet")
musicbrainz_path = os.path.join(current_dir, "../data/musicbrainz.db")
stat_path = os.path.join(current_dir, "outputs")

# ensuring data directory exists
os.makedirs(stat_path, exist_ok=True)

conn = duckdb.connect()
brainz_conn = duckdb.connect(musicbrainz_path)

# extract artist gender distribution as play count and unique user count for each gender
gender_count_query = f"""
COPY (
    SELECT 
        a.gender AS gender,
        COUNT(user_id) AS n_plays,  
        COUNT(DISTINCT mlhd.user_id) AS n_users  
    FROM 
        (SELECT UNNEST(artist_ids) AS artist_id, user_id FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    GROUP BY gender
    ORDER BY n_plays DESC
) TO '{stat_path}/gender_count.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(gender_count_query)
print("Gender count saved")

# extract artist gender distribution in active user's interactions
gender_count_active = f"""
COPY (
    SELECT 
        a.gender AS gender,
        COUNT(user_id) AS n_plays,  
        COUNT(DISTINCT mlhd.user_id) AS n_users  
    FROM 
        (
        SELECT UNNEST(artist_ids) AS artist_id, user_id 
        FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    WHERE mlhd.user_id IN (
        SELECT user_id
        FROM read_parquet('{mlhd_path}')
        GROUP BY user_id
        HAVING COUNT(*) >= 5
    )
    GROUP BY gender
    ORDER BY n_plays DESC
) TO '{stat_path}/gender_count_active.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(gender_count_active)
print("Artist gender count for active users saved")

# extract number of unique users and plays for each release 
release_count_query = f"""
COPY ( 
    SELECT 
           release_id, 
           COUNT(user_id) AS n_plays,
           COUNT(DISTINCT user_id) AS n_users 
    FROM 
           read_parquet('{mlhd_path}')
    GROUP BY release_id
    ORDER BY n_plays DESC
) TO '{stat_path}/release_count.parquet'(COMPRESSION zstd);
"""
conn.execute(release_count_query)
print("Number of users and plays for each release saved")

# extract type data
type_count_query = f"""
COPY (
    SELECT 
        a.type AS type,
        COUNT(mlhd.user_id) AS n_plays,  
        COUNT(DISTINCT mlhd.user_id) AS n_users  
    FROM 
        (SELECT UNNEST(artist_ids) AS artist_id, user_id FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    GROUP BY type
    ORDER BY n_plays DESC
) TO '{stat_path}/type_count.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(type_count_query)
print("Type count saved")

# extract genre data
genre_count_query = f"""COPY(
SELECT 
     g.genre_name,
     COUNT(mlhd.user_id) AS n_plays,  
     COUNT(DISTINCT mlhd.user_id) AS n_users  
FROM 
     (SELECT rec_id, user_id FROM read_parquet('{mlhd_path}')) mlhd
LEFT JOIN 
    (SELECT 
        rec_id,
        UNNEST(json_extract(artist_credit,'$[*].artist.genres[*].name')) AS genre_name
    FROM mb_recording) g
USING (rec_id)     
GROUP BY genre_name
ORDER BY n_plays DESC
)TO '{stat_path}/genre_count.parquet' (COMPRESSION zstd);"""

brainz_conn.execute(genre_count_query)
print("Genre data saved")

# extract artist data
artist_count_query = f"""
COPY (
    SELECT 
        a.artist_id,
        COUNT(mlhd.user_id) AS n_plays,  
        COUNT(DISTINCT mlhd.user_id) AS n_users  
    FROM 
        (SELECT UNNEST(artist_ids) AS artist_id, user_id FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    GROUP BY a.artist_id
    ORDER BY n_plays DESC
) TO '{stat_path}/artist_count.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(artist_count_query)
print("Artist count saved")

# extract number of plays of each user
play_count_query = f"""COPY(
SELECT user_id, 
COUNT(rec_id) as play_count
FROM read_parquet('{mlhd_path}') 
GROUP BY user_id
)TO '{stat_path}/play_count.parquet' (COMPRESSION zstd);"""

conn.execute(play_count_query)
print("Number of plays for each user saved")

# extract number of plays for each track 
track_count_query = f"""
COPY ( 
    SELECT 
           rec_id, 
           COUNT(user_id) AS n_plays, 
    FROM 
           read_parquet('{mlhd_path}')
    GROUP BY rec_id
    ORDER BY n_plays DESC
) TO '{stat_path}/track_count.parquet'(COMPRESSION zstd);
"""
conn.execute(track_count_query)
print("Number of plays for each track saved")

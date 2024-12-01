import duckdb
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
mlhd_path = os.path.join(current_dir, "../data/mlhd/*/*.parquet")
musicbrainz_path = os.path.join(current_dir, "../data/musicbrainz.db")
stat_path = os.path.join(current_dir, "outputs")

# ensuring data directory exists
os.makedirs(stat_path, exist_ok=True)

conn = duckdb.connect()
brainz_conn = duckdb.connect(musicbrainz_path)

# extract artist gender distribution
gender_count_query = f"""
COPY (
    SELECT 
        a.gender AS gender,
        COUNT(*) AS non_unique_count,  
        COUNT(DISTINCT mlhd.artist_id) AS unique_count  
    FROM 
        (SELECT UNNEST(artist_ids) AS artist_id FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    GROUP BY gender
    ORDER BY non_unique_count DESC
) TO '{stat_path}/gender_count.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(gender_count_query)
print("Gender count saved")

# extract artist gender distribution in active user's interactions
gender_count_active = f"""
COPY (
    SELECT 
        a.gender AS gender,
        COUNT(*) AS non_unique_count,  
        COUNT(DISTINCT mlhd.artist_id) AS unique_count  
    FROM 
        (SELECT UNNEST(artist_ids) AS artist_id, user_id FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    GROUP BY gender
    HAVING COUNT(user_id) >= 10
    ORDER BY non_unique_count DESC
) TO '{stat_path}/gender_count_active.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(gender_count_active)
print("Artist gender count for active users saved")

# extract number of unique users and plays for each release 
release_count_query = f"""
COPY ( 
    SELECT 
           release_id, 
           COUNT(DISTINCT user_id) AS n_users, 
           COUNT(user_id) AS n_plays
    FROM 
           read_parquet('{mlhd_path}')
    GROUP BY 
           release_id
) TO '{stat_path}/release_count.parquet'(COMPRESSION zstd);
"""
conn.execute(release_count_query)
print("Number of users and plays for each release saved")

# extract type data
type_count_query = f"""
COPY (
    SELECT 
        a.type AS type,
        COUNT(*) AS non_unique_count,  
        COUNT(DISTINCT mlhd.artist_id) AS unique_count  
    FROM 
        (SELECT UNNEST(artist_ids) AS artist_id FROM read_parquet('{mlhd_path}')) mlhd
    LEFT JOIN 
        mb_artist a
    USING (artist_id)
    GROUP BY type
    ORDER BY non_unique_count DESC
) TO '{stat_path}/type_count.parquet' (COMPRESSION zstd);
"""

brainz_conn.execute(type_count_query)
print("Type count saved")

#extract genre data
genre_count_query = f"""COPY(
SELECT 
     g.genre_name,
     COUNT(*) AS non_unique_count,  
     COUNT(DISTINCT mlhd.rec_id) AS unique_count  
FROM 
     (SELECT rec_id FROM read_parquet('{mlhd_path}')) mlhd
LEFT JOIN 
    (SELECT 
        rec_id,
        UNNEST(json_extract(artist_credit,'$[*].artist.genres[*].name')) AS genre_name
    FROM mb_recording) g
USING (rec_id)     
GROUP BY genre_name
ORDER BY non_unique_count DESC
)TO '{stat_path}/genre_count.parquet' (COMPRESSION zstd);"""

brainz_conn.execute(genre_count_query)
print("Genre data saved")

# extract number of ratings of each user
rating_count_query = f"""COPY(
SELECT user_id, 
COUNT(rec_id) as rating_count
FROM read_parquet('{mlhd_path}') 
GROUP BY user_id
)TO '{stat_path}/rating_count.parquet' (COMPRESSION zstd);"""

conn.execute(rating_count_query)
print("Number of ratings for each user saved")

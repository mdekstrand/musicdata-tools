import logging
from datetime import datetime
from pathlib import Path

import duckdb
from tqdm import tqdm

from musicdata.resources import duck_options

from .layout import data_dir

mlhd_dir = data_dir / "mlhd"
musicbrainz_path = data_dir / "musicbrainz.db"
mlhd_ids_path = data_dir / "mlhd_ids.parquet"

log_path = data_dir / "prep_log.txt"
new_mlhd_path = data_dir / "mlhdv2"
new_mlhd_path.mkdir(parents=True, exist_ok=True)

data_start = "2005-01-01"
data_end = "2014-01-01"

conn = duckdb.connect(config=duck_options())
brainz_conn = duckdb.connect(musicbrainz_path, config=duck_options())

_log = logging.getLogger(__name__)


def process():
    data_intial_size = 0
    duplicate_count = 0
    first_group_count = 0
    timestamp_count = 0
    non_person_count = 0
    null_gender_count = 0
    napp_gender_count = 0
    data_final_size = 0

    counter = 1

    ##
    _log.info("creating mlhd ids mapping table")
    conn.execute(f"""create temp table mlhd_ids as
                select * from read_parquet('{mlhd_ids_path}')
                ORDER BY entity_uuid""")
    conn.execute(f"attach '{musicbrainz_path}' as musicbrainz")

    brainz_size = conn.execute("select count(*) from musicbrainz.mb_artist").fetchone()[
        0
    ]
    ##
    _log.info("joining with mlhd_ids")
    conn.execute(f"""
    create temp table artists as
    SELECT
        artist_uuid.id_num AS artist_id,
        mb_artist.gender AS gender,
        mb_artist.type AS type
    FROM musicbrainz.mb_artist as mb_artist
    RIGHT JOIN (SELECT * FROM mlhd_ids WHERE entity_type = 'artist_ids')
                 AS artist_uuid
        ON artist_uuid.entity_uuid = mb_artist.artist_id
        """)

    conn.execute("detach musicbrainz")
    mapped_brainz_size = conn.execute("select count(*) from artists").fetchone()[0]

    # start looping through directories
    segments = [segment for segment in mlhd_dir.iterdir() if segment.is_dir()]

    for mlhd_path in tqdm(segments, desc="Processing Directories", unit="dir"):
        _log.info("getting initial data size")
        data_intial_size += conn.execute(f"""select count(*) from
                                        read_parquet('{mlhd_path}/*.parquet')""").fetchone()[
            0
        ]

        ##
        _log.info("excluding duplicates")
        conn.execute(f"""create or replace temp table mlhd as
                        select distinct(*) from read_parquet('{mlhd_path}/*.parquet') """)

        ##
        data_unique_size = conn.execute("select count(*) from mlhd").fetchone()[0]
        duplicate_count += data_intial_size - data_unique_size

        ##
        _log.info("excluding group artist rows")
        first_group_count += conn.execute(f"""delete from mlhd
                                    where array_length(artist_ids)>1
                                        """).fetchone()[0]

        ##
        _log.info("excluding sparse timestamp range")
        timestamp_count += conn.execute(f"""delete from mlhd
                                    where to_timestamp(timestamp)>=
                                    '{data_end}' """).fetchone()[0]

        ##
        _log.info("excluding non-person artist rows")
        non_person_count += conn.execute("""delete from mlhd where artist_ids[1] not in
                    (select artist_id from artists
                    where type='Person')""").fetchone()[0]

        ##
        _log.info("excluding rows with null artist gender")
        null_gender_count += conn.execute("""delete from mlhd where artist_ids[1] in
                    (select artist_id from artists
                    where gender is NULL)""").fetchone()[0]

        ##
        _log.info("excluding rows with non-applicable artist gender")
        napp_gender_count += conn.execute("""delete from mlhd where artist_ids[1] in
                    (select artist_id from artists
                    where gender = 'Not applicable')""").fetchone()[0]

        ##
        _log.info("converting artist_ids list type to int")
        conn.execute("""create or replace temp table mlhd as
                        select user_id, timestamp, artist_ids[1] as artist_id,
                        release_id, rec_id from mlhd
        """)

        ##
        _log.info("Getting final data row count")
        data_final_size += conn.execute("""select count(*) from
                                        mlhd""").fetchone()[0]

        ##
        _log.info("writing mlhd to parquet")
        conn.execute(f"""
                    COPY (SELECT * FROM mlhd)
                    TO '{new_mlhd_path}/chunk{counter}.parquet' (COMPRESSION zstd);
        """)

        counter += 1

    with open(log_path, "w") as logfile:
        logfile.write(f"""# of rows in musicbrainz: {brainz_size:,}\n""")
        logfile.write(
            f"""# of rows in musicbrainz after mapping: {mapped_brainz_size:,}\n"""
        )
        logfile.write(f"# of rows in initial data: {data_intial_size:,}\n")
        logfile.write(f"# of duplicate rows removed: {duplicate_count:,}\n")
        logfile.write(
            f"""# of rows with group artist removed (1st round): {first_group_count:,}\n"""
        )
        logfile.write(
            f"""# of rows with sparse timestamp removed: {timestamp_count:,}\n"""
        )
        logfile.write(
            f"""# of rows with non-person artist removed (2nd round): {non_person_count:,}\n"""
        )
        logfile.write(
            f"""# of rows with null artist gender removed: {null_gender_count:,}\n"""
        )
        logfile.write(
            f"""# of rows with non-applicable artist gender removed: {napp_gender_count:,}\n"""
        )
        logfile.write(f"""# of rows in final mlhd data: {data_final_size:,}\n""")

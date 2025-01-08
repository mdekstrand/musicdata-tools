import duckdb
import glob
import os
from memory_limits import duck_options

mlhd_path = '../../data/mlhd/*/*.parquet'
musicbrainz_path = '../../data/musicbrainz.db'
final_mlhd_path = '../../data/mlhd.db'

mlhd_conn = duckdb.connect(final_mlhd_path, config=duck_options())

def import_mlhd_db():
    #create mlhd_plays table in mlhd database
    parquet_unfold = glob.glob(mlhd_path)
    mlhd_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS mlhd_plays AS
        SELECT * FROM parquet_scan('{parquet_unfold[0]}') LIMIT 0;
    """)
    print("Table 'mlhd_plays' is created")

    # incrementally process and insert data
    total_files = len(parquet_unfold)
    print(f"Found {total_files} files to process.")

    for i, parquet_file in enumerate(parquet_unfold, start=1):
        print(f"[{i}/{total_files}] Processing {parquet_file}...")
        mlhd_conn.execute(f"""
            INSERT INTO mlhd_plays
            SELECT * FROM parquet_scan('{parquet_file}')
            WHERE array_length(artist_ids) = 1;
        """)

    mlhd_conn.close()
    print(f"All data successfully written to {final_mlhd_path}.")

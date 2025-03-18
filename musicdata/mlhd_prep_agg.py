import duckdb
from memory_limits import duck_options
from .layout import data_dir
import logging
from pathlib import Path
from tqdm import tqdm

mlhd_path = data_dir / "mlhdv2"

log_path = data_dir / "prep_agg_log.txt"
new_mlhd_path = data_dir / "solo-artist-count"
new_mlhd_path.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(config=duck_options())

_log = logging.getLogger(__name__)

def process():
    _log.info("aggregating artist listens")   
    conn.execute(f"""
    create temp table mlhd as
    select 
        user_id, artist_id, count(*),
        min(timestamp) as first_time,
        max(timestamp) as last_time
    from read_parquet('{mlhd_path}/*.parquet')
    group by user_id, artist_id
        """)
    
    ##
    _log.info("writing aggregated mlhd to parquet")
    conn.execute(f"""
          copy (select * from mlhd) 
          TO '{new_mlhd_path}/.parquet' (COMPRESSION zstd);
     """)

 
    

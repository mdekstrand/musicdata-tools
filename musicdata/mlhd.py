import logging
import multiprocessing as mp
import os
import re
import tarfile
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from pathlib import Path
from queue import Queue
from threading import Thread

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import zstandard
from humanize import naturalsize
from manylog import LogListener, init_worker_logging
from progress_api import make_progress

from memory_limits import duck_options

from .layout import data_dir, mlhd_src_dir
from .params import id_ranges

BATCH_SIZE = 20_000_000
_MLHD_FN_RE = re.compile(r"^[a-f0-9]+/([a-f0-9-]+)\.txt\.zst")
_log = logging.getLogger(__name__)

out_dir = data_dir / "mlhd"
mlhd_ids_path = data_dir / "mlhd_ids.parquet"


def extract_unique_entities(file: Path):
    """
    Extract entities from a given file and return them as a dataframe
    """
    unique_entities = {
        "user_id": set(),
        "artist_ids": set(),
        "release_id": set(),
        "rec_id": set(),
    }
    decomp = zstandard.ZstdDecompressor()

    _log.info(f"Processing file {file} for extracting entities")
    with open(file, "rb") as srcf, tarfile.TarFile(fileobj=srcf) as tf:
        for entry in tf:
            if entry.isdir():
                continue

            m = _MLHD_FN_RE.match(entry.name)
            if not m:
                _log.warning(f"Skipping invalid filename: {entry.name}")
                continue

            uid = m[1]
            cstr = tf.extractfile(entry)
            if not cstr:
                _log.info("cstr is empty")
                continue

            with cstr, decomp.stream_reader(cstr) as data:
                tbl = csv.read_csv(
                    data,
                    read_options=csv.ReadOptions(
                        column_names=["timestamp", "artist_ids", "release_id", "rec_id"]
                    ),
                    parse_options=csv.ParseOptions(delimiter="\t"),
                )

                # add entities to shared dictionaries
                unique_entities["user_id"].add(uid)

                artist_lists = tbl.column("artist_ids").to_pylist()
                for artist_list in artist_lists:
                    unique_entities["artist_ids"].update(artist_list.split(","))

                for col in ["release_id", "rec_id"]:
                    unique_entities[col].update(tbl.column(col).to_pylist())

    return unique_entities


def build_mlhd_ids(files, jobs):
    _log.info("building mlhd_ids by parallel extraction and deduplication")

    # First pass: extract unique entities in parallel
    with ProcessPoolExecutor(max_workers=jobs) as executor:
        results = list(executor.map(extract_unique_entities, files))

    _log.info("Merging extracted entities")
    merged = {
        "user_id": set(),
        "artist_ids": set(),
        "release_id": set(),
        "rec_id": set(),
    }
    for d in results:
        for key in merged.keys():
            merged[key].update(d.get(key, set()))

    _log.info("Converting entity sets into a dataframe")
    unique_df = pd.DataFrame(
        [(key, value) for key, value_set in merged.items() for value in value_set],
        columns=["entity_type", "entity_uuid"],
    )

    _log.info("Assigning integer IDs to each entity")
    unique_df["id_num"] = (
        unique_df.groupby("entity_type").cumcount()
        + unique_df["entity_type"].map(id_ranges)
    ).astype("int32")

    _log.info("writing all unique entities with their ids into mlhd_ids...")

    table = pa.Table.from_pandas(unique_df)
    pq.write_table(table, mlhd_ids_path, compression="zstd")
    _log.info("finished building mlhd_ids")


def map_entity_ids(db: duckdb.DuckDBPyConnection):
    _log.info("mapping UUIDs to integer IDs using the mlhd_ids table")

    query = """
    SELECT
        user_map.id_num AS user_id,
        events.timestamp,
        ARRAY(
            SELECT artist_map.id_num
            FROM UNNEST(events.artist_ids) as t(artist_uuid)
            LEFT JOIN mlhd_ids as artist_map
            ON artist_map.entity_uuid = t.artist_uuid
            AND artist_map.entity_type = 'artist_ids'
        ) AS artist_ids,
        release_map.id_num AS release_id,
        rec_map.id_num AS rec_id
    FROM events
    LEFT JOIN mlhd_ids AS user_map
        ON user_map.entity_uuid = events.user_id
        AND user_map.entity_type = 'user_id'
    LEFT JOIN mlhd_ids AS release_map
        ON release_map.entity_uuid = events.release_id
        AND release_map.entity_type = 'release_id'
    LEFT JOIN mlhd_ids AS rec_map
       ON rec_map.entity_uuid = events.rec_id
       AND rec_map.entity_type = 'rec_id'
    """
    return db.sql(query)


def import_mlhd(jobs: int | None = None, use_mapping: bool = False):
    files = sorted(mlhd_src_dir.glob("mlhdplus-complete-*.tar"))
    _log.info("found %d files", len(files))

    _log.info("ensuring output dir %s exists", out_dir)
    out_dir.mkdir(exist_ok=True)

    if jobs is None and "NUM_JOBS" in os.environ:
        jobs = int(os.environ["NUM_JOBS"])
    if jobs is None:
        jobs = max(1, min(mp.cpu_count() // 4, 4))

    if use_mapping:
        build_mlhd_ids(files, jobs * 4)

    fpb = make_progress(_log, "files", total=len(files))
    ctx = mp.get_context("spawn")

    if jobs == 1:
        for file in files:
            import_file(file, use_mapping)
            _log.info("finished file %s", file)
            fpb.update()
    else:
        with LogListener() as ll:
            assert ll.address is not None
            with ProcessPoolExecutor(
                jobs,
                mp_context=ctx,
                initializer=init_worker_logging,
                initargs=(ll.address, _log.getEffectiveLevel()),
            ) as pool:
                for file, _ in zip(
                    files, pool.map(import_file, files, repeat(use_mapping))
                ):
                    _log.info("finished file %s", file)
                    fpb.update()


def import_file(file: Path, use_mapping: bool):
    """
    Import a single MLHD file.
    """
    stat = file.stat()
    _log.info("reading %s: %s", file.name, naturalsize(stat.st_size, binary=True))
    pb = make_progress(_log, file.stem, stat.st_size, "bytes")
    pos = 0
    decomp = zstandard.ZstdDecompressor()
    rec = None

    with open(file, "rb") as srcf, tarfile.TarFile(fileobj=srcf) as tf:
        for entry in tf:
            old = pos
            pos = srcf.tell()
            pb.update(pos - old)

            if entry.isdir():
                if rec:
                    rec.finish()
                _log.info("parsing segment %s", entry.name)
                rec = SegmentRecorder(entry.name, use_mapping=use_mapping)
                rec.start()
                continue

            m = _MLHD_FN_RE.match(entry.name)
            if not m:
                _log.warning("invalid filename: %s", entry.name)
                continue
            uid = m[1]
            cstr = tf.extractfile(entry)

            assert cstr is not None

            with cstr, decomp.stream_reader(cstr) as data:
                tbl = csv.read_csv(
                    data,
                    csv.ReadOptions(
                        column_names=["timestamp", "artist_ids", "release_id", "rec_id"]
                    ),
                    csv.ParseOptions(delimiter="\t"),
                )

            assert rec is not None, "user encountered but no recorder"
            assert rec.is_alive(), "recorder thread has died"
            rec.record_user(uid, tbl)

        pb.update(srcf.tell() - pos)
        if rec is not None:
            rec.finish()

    pb.finish()


class SegmentRecorder(Thread):
    segment: str
    queue: Queue[tuple[str, pa.Table] | None]
    out_dir: Path
    chunks: int

    def __init__(self, segment, use_mapping):
        self.segment = segment
        self.use_mapping = use_mapping
        super().__init__(name=f"writer-{segment}")
        self.queue = Queue(10)
        self.out_dir = out_dir / segment
        self.chunks = 0

    def record_user(self, uid, tbl):
        self.queue.put((uid, tbl))

    def finish(self):
        _log.info("finishing segment %s", self.segment)
        self.queue.put(None)
        _log.debug("waiting for writer thread")
        self.join()

    def run(self):
        self.out_dir.mkdir(exist_ok=True, parents=True)

        with duckdb.connect(config=duck_options(mem_fraction=0.15)) as db:
            # db.execute("PRAGMA disable_progress_bar")
            if self.use_mapping and mlhd_ids_path.exists():
                db.execute(
                    f"CREATE TABLE mlhd_ids AS SELECT * FROM read_parquet('{mlhd_ids_path}') ORDER BY entity_uuid"
                )
                db.execute("CREATE INDEX entity_id_idx ON mlhd_ids (entity_uuid)")

            db.execute("""
                CREATE TABLE events (
                    user_id UUID NOT NULL,
                    timestamp BIGINT NOT NULL,
                    artist_ids UUID[],
                    release_id UUID,
                    rec_id UUID
                )
            """)

            while True:
                try:
                    done = self._pump_item(db)
                except Exception as e:
                    _log.error("segment %s: error in worker: %s", self.segment, e)
                    raise e

                if done:
                    return

    def _pump_item(self, db):
        item = self.queue.get()
        if item is None:
            _log.debug("writer finishing segment %s", self.segment)
            self._maybe_write(db, True)
            return True

        uid, tbl = item
        self._record_user_events(db, uid, tbl)
        self._maybe_write(db)
        return False

    def _record_user_events(
        self, db: duckdb.DuckDBPyConnection, uid: str, tbl: pa.Table
    ) -> None:
        _log.debug("recording user %s", uid)
        src = db.from_arrow(tbl)  # noqa: F841
        proj = db.sql(
            f"select '{uid}', timestamp, string_split(artist_ids, ','), release_id, rec_id from src"
        )
        proj.insert_into("events")

    def _maybe_write(self, db: duckdb.DuckDBPyConnection, force: bool = False):
        db.execute("SELECT COUNT(*) FROM events")
        res = db.fetchone()
        assert res is not None
        (count,) = res

        if count < BATCH_SIZE and not force:
            return

        if count == 0:
            return

        self.chunks += 1
        fn = self.out_dir / f"chunk-{self.chunks}.parquet"

        _log.debug("segment %s: writing %d rows to %s", self.segment, count, fn)

        if self.use_mapping:
            mapped = map_entity_ids(db)
            mapped.write_parquet(os.fspath(fn), compression="zstd")
        else:
            db.table("events").write_parquet(os.fspath(fn), compression="zstd")

        db.execute("TRUNCATE events")
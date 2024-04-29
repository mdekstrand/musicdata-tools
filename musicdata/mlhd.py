import logging
import multiprocessing as mp
import os
import re
import tarfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from queue import Queue
from threading import Thread

import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import zstandard
from humanize import naturalsize
from manylog import LogListener, init_worker_logging
from progress_api import make_progress

from .layout import data_dir, mlhd_src_dir

BATCH_SIZE = 20_000_000
_MLHD_FN_RE = re.compile(r"^[a-f0-9]+/([a-f0-9-]+)\.txt\.zst")
_log = logging.getLogger(__name__)

out_dir = data_dir / "mlhd"


def import_mlhd(jobs: int | None = None):
    files = sorted(mlhd_src_dir.glob("mlhdplus-complete-*.tar"))
    _log.info("found %d files", len(files))

    _log.info("ensuring output dir %s exists", out_dir)
    out_dir.mkdir(exist_ok=True)

    fpb = make_progress(_log, "files", total=len(files))

    ctx = mp.get_context("spawn")

    if jobs is None and "NUM_JOBS" in os.environ:
        jobs = int(os.environ["NUM_JOBS"])

    if jobs is None:
        jobs = max(1, min(mp.cpu_count() // 4, 4))

    if jobs == 1:
        for file in files:
            import_file(file)
            _log.info("finished file %s", file)
            fpb.update()
    else:
        with LogListener() as ll:
            assert ll.address is not None
            with ProcessPoolExecutor(
                jobs,
                ctx,
                initializer=init_worker_logging,
                initargs=(ll.address, _log.getEffectiveLevel()),
            ) as pool:
                for file, res in zip(files, pool.map(import_file, files)):
                    _log.info("finished file %s", file)
                    fpb.update()


def import_file(file: Path):
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
                rec = SegmentRecorder(entry.name)
                rec.start()
                continue

            m = _MLHD_FN_RE.match(entry.name)
            if not m:
                _log.warn("invalid filename: %s", entry.name)
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

    def __init__(self, segment):
        self.segment = segment
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

        with duckdb.connect() as db:
            db.execute("PRAGMA disable_progress_bar")
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
        db.table("events").write_parquet(os.fspath(fn), compression="zstd")
        db.execute("TRUNCATE events")

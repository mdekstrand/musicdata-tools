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
from pyarrow.parquet import ParquetWriter

from .layout import data_dir, mlhd_src_dir

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
        with LogListener() as ll, ProcessPoolExecutor(
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
                    rec.save()
                _log.info("parsing segment %s", entry.name)
                rec = SegmentRecorder(entry.name)
                continue

            m = _MLHD_FN_RE.match(entry.name)
            if not m:
                _log.warn("invalid filename: %s", entry.name)
                continue
            uid = m[1]
            with tf.extractfile(entry) as cstr, decomp.stream_reader(cstr) as data:
                tbl = csv.read_csv(
                    data,
                    csv.ReadOptions(
                        column_names=["timestamp", "artist_ids", "release_id", "rec_id"]
                    ),
                    csv.ParseOptions(delimiter="\t"),
                )

            rec.record_user(uid, tbl)

        pb.update(srcf.tell() - pos)
        rec.save()

    pb.finish()


class SegmentRecorder:
    segment: str
    db: duckdb.DuckDBPyConnection

    def __init__(self, segment):
        self.segment = segment
        self.thread = WriterThread(segment)
        self.thread.start()
        self.db = duckdb.connect()
        self.db.execute("PRAGMA disable_progress_bar")

    def record_user(self, uid, tbl):
        _log.debug("recording user %s", uid)
        tbl = self.db.from_arrow(tbl)
        proj = self.db.sql(f"""
            SELECT CAST('{uid}' AS UUID) AS user_id,
                timestamp,
                CAST(string_split(artist_ids, ',') AS UUID[]) AS artist_ids,
                CAST(release_id AS UUID) AS release_id,
                CAST(rec_id AS UUID) AS rec_id
            FROM tbl
        """)
        out = proj.to_arrow_table()
        self.thread.queue.put(out)

    def save(self):
        outf = out_dir / f"{self.segment}.parquet"
        _log.info("finishing %s", outf)
        self.thread.queue.put(None)
        _log.debug("waiting for writer thread")
        self.thread.join()
        self.db.close()
        del self.db, self.thread


class WriterThread(Thread):
    file: Path
    writer: ParquetWriter
    queue: Queue[pa.Table | None]

    def __init__(self, segment):
        self.file = out_dir / f"{segment}.parquet"
        _log.info("opening output file %s", self.file)
        self.writer = ParquetWriter(
            os.fspath(self.file), compression="zstd", compression_level=9
        )
        self.queue = Queue(10)

    def run(self):
        while True:
            item = self.queue.get()
            if item is None:
                _log.info("finishing output file %s", self.file)
                self.writer.close()
                return

            self.writer.write_table(item)

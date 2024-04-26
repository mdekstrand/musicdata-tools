import logging
import multiprocessing as mp
import os
import re
import tarfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import pyarrow.csv as csv
import zstandard
from humanize import naturalsize
from manylog import LogListener, init_worker_logging
from progress_api import make_progress

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
                _log.info("parsing segment %s", entry.name)
                if rec:
                    rec.save()
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

            tbl = rec.db.from_arrow(tbl)
            proj = rec.db.sql(
                f"select '{uid}', timestamp, string_split(artist_ids, ','), release_id, rec_id from tbl"
            )
            proj.insert_into("events")
            _log.debug("inserted user %s", uid)

        pb.update(srcf.tell() - pos)
        rec.save()

    pb.finish()


class SegmentRecorder:
    segment: str
    db: duckdb.DuckDBPyConnection

    def __init__(self, segment):
        self.segment = segment
        self.db = duckdb.connect()
        self.db.execute("""
            CREATE TABLE events (
                user_id UUID NOT NULL,
                timestamp BIGINT NOT NULL,
                artist_ids UUID[],
                release_id UUID,
                rec_id UUID
            )
        """)
        self.db.execute("PRAGMA disable_progress_bar")

    def save(self):
        outf = out_dir / f"{self.segment}.parquet"
        _log.info("saving %s", outf)
        self.db.table("events").write_parquet(os.fspath(outf), compression="zstd")
        self.db.close()
        del self.db

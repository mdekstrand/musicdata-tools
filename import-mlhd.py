"""
Import the MLHD+ files to Parquet that can be queried with DuckDB.

Usage:
    import-mlhd.py [options]

Options:
    -v, --verbose   enable verbose log output
"""

import argparse
import logging
import multiprocessing as mp
import os
import pickle
import re
import tarfile
import threading
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import enlighten
import pyarrow.csv as csv
import zstandard
from gbsim.script_util import init_logging
from humanize import naturalsize

_log = logging.getLogger("import-mlhd")
progress: enlighten.Manager
pg_queue: mp.Queue

_MLHD_FN_RE = re.compile(r"^[a-f0-9]+/([a-f0-9-]+)\.txt\.zst")

COUNT_BAR_FORMAT = (
    "{desc}{desc_pad}{percentage:3.0f}%|{bar}| "
    "{count:{len_total}d}/{total:d} "
    "[{elapsed}<{eta}, {rate:.2f}{unit_pad}{unit}/s]"
)

FILE_BAR_FORMAT = (
    "{desc}{desc_pad}{percentage:3.0f}%|{bar}| {count:!.2j}{unit} / {total:!.2j}{unit} "
    "[{elapsed}<{eta}, {rate:!.2j}{unit}/s]"
)

src_dir = Path("data/mlhd-source")
out_dir = Path("data/mlhd")


def main(args: argparse.Namespace):
    global progress, pg_queue
    level = init_logging(args)
    progress = enlighten.get_manager()
    _log.info("importing MLHD")

    out_dir.mkdir(exist_ok=True)
    files = sorted(src_dir.glob("mlhdplus-complete-*.tar"))
    _log.info("found %d files", len(files))
    fpb = progress.counter(
        total=len(files), desc="Input files", unit="files", bar_format=COUNT_BAR_FORMAT
    )

    ctx = mp.get_context("spawn")
    pg_queue = ctx.SimpleQueue()
    listen = ProgressListener(pg_queue)
    listen.start()

    jobs = args.jobs
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
        with ProcessPoolExecutor(
            jobs, ctx, initializer=_init_logging, initargs=(pg_queue, level)
        ) as pool:
            for file, res in zip(files, pool.map(import_file, files)):
                _log.info("finished file %s", file)
                fpb.update()

    fpb.close()
    progress.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        prog="import-mlhd", description="Import MLHD and MusicBrainz data."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable verbose logging"
    )
    parser.add_argument("-J", "--jobs", type=int, help="enable verbose logging")
    return parser.parse_args()


def import_file(file: Path):
    """
    Import a single MLHD file.
    """
    stat = file.stat()
    _log.info("reading %s: %s", file.name, naturalsize(stat.st_size, binary=True))
    pg_queue.put(("start", (file.stem, stat.st_size)))
    pos = 0
    decomp = zstandard.ZstdDecompressor()
    rec = None
    with open(file, "rb") as srcf, tarfile.TarFile(fileobj=srcf) as tf:
        for entry in tf:
            old = pos
            pos = srcf.tell()
            pg_queue.put(("update", (file.stem, pos - old)))
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

        pg_queue.put(("update", (file.stem, srcf.tell() - pos)))
        rec.save()

    pg_queue.put("close", file.stem)


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


def _init_logging(queue: mp.SimpleQueue, level: int):
    global pg_queue
    pg_queue = queue
    logging.getLogger().setLevel(level)
    logging.getLogger().addHandler(QueueHandler(pg_queue))


class QueueHandler(logging.Handler):
    queue: mp.Queue

    def __init__(self, queue: mp.Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record: logging.LogRecord) -> bool:
        self.queue.put(("log", (record.name, pickle.dumps(record))))


class ProgressListener(threading.Thread):
    queue: mp.Queue
    counters: dict[str, enlighten.Counter]

    def __init__(self, queue):
        super().__init__(daemon=True)
        self.queue = queue
        self.counters = {}

    def run(self):
        while True:
            action, payload = self.queue.get()
            match action:
                case "start":
                    name, size = payload
                    self.counters[name] = progress.counter(
                        total=float(size),
                        desc=name,
                        unit="B",
                        bar_format=FILE_BAR_FORMAT,
                    )
                case "update":
                    name, incr = payload
                    try:
                        ctr = self.counters[name]
                    except KeyError:
                        _log.warn("unknown counter %s", name)
                        continue

                    ctr.update(incr)
                case "close":
                    try:
                        ctr = self.counters[payload]
                    except KeyError:
                        _log.warn("unknown counter %s", payload)
                        continue
                    ctr.close()
                    del self.counters[payload]
                case "log":
                    name, data = payload
                    record = pickle.loads(data)
                    logging.getLogger(name).handle(record)
                case _:
                    _log.warn("invalid action %s", action)


if __name__ == "__main__":
    args = parse_args()
    main(args)

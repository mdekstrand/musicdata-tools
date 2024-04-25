import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from duckdb import DuckDBPyConnection, connect

from .dbutils import parse_sqlinfo
from .layout import data_dir, mb_src_dir, sql_dir

_log = logging.getLogger(__name__)

MB_TABLES = [
    "artist",
    "recording",
]

db_fn = data_dir / "musicbrainz.db"


def import_mb(table: Optional[str]):
    tables = [table] if table else MB_TABLES
    with connect(os.fspath(db_fn)) as db, TemporaryDirectory(
        prefix="music-import-"
    ) as tmp:
        for table in tables:
            import_table(db, table, tmp)


def import_table(db: DuckDBPyConnection, table: str, tmp: Path):
    _log.info("preparing to import %s", table)
    sql_fn = sql_dir / f"mb_{table}.sql"
    meta, sql = parse_sqlinfo(sql_fn)

    fifo = tmp / f"decompress-{table}.fifo"
    _log.info("creating pipe %s", fifo)
    os.mkfifo(fifo)

    _log.info("maybe dropping table %s", table)
    db.execute(f"DROP TABLE IF EXISTS mb_{table}")
    _log.info("creating table %s", table)
    db.execute(sql)
    _log.info("opening input")
    mb_fn = mb_src_dir / f"{table}.tar.xz"
    if not mb_fn.exists():
        _log.error("%s does not exist", mb_fn)
        raise RuntimeError("missing input file")
    pid = os.fork()
    if pid == 0:
        _log.debug("opening FIFO %s", fifo)
        out = os.open(fifo, os.O_WRONLY)
        _log.debug("spawning process")
        os.dup2(out, 1)
        os.close(out)
        os.execvp("bsdtar", ["bsdtar", "xf", os.fspath(mb_fn), "-O", f"mbdump/{table}"])
        raise RuntimeError("could not spawn tar")

    _log.info("inserting JSON")
    cols = ", ".join(f'"{col}"' for col in meta["columns"])
    insert = f"""
        INSERT INTO mb_{table}
        SELECT {cols} FROM read_json('{fifo}', format='newline_delimited', maximum_object_size=67108864)
    """
    _log.debug("INSERT statement: %s", insert)
    db.execute(insert)

    _log.info("cleaning up")
    _pid, code = os.waitpid(pid, 0)
    if code != 0:
        _log.error("decompression failed with status %s", code)

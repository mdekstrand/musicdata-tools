"""
Ingest source data to DuckDB-usable formats.

Usage:
    ingest.py [-v] --musicbrainz [-p PART]
    ingest.py [-v] --mlhd

Options:
    -v, --verbose   enable verbose log output
    -p PART, --part=PART
                    only import data segment PART

Modes:
    --musicbrainz   import MusicBrainz metadata
    --mlhd          import MLHD+ listening data
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import logging
import re
import argparse

from docopt import docopt
from yaml import safe_load
from duckdb import connect, DuckDBPyConnection

from gbsim.script_util import init_logging

_log = logging.getLogger('import-musicbrainz')
_YAML_RE = re.compile(r'^--:\s+(.*)')

MB_TABLES = [
    'artist',
    'recording',
]

sql_dir = Path('sql')
mb_dir = Path('data/musicbrainz')
db_path = Path('data/musicbrainz.db')

def main(args: argparse.Namespace):
    init_logging(args)

    mb_tables = []
    if args.mb_table:
        mb_tables = [args.mb_table]
    else:
        mb_tables = MB_TABLES

    with connect(os.fspath(db_path)) as duck, TemporaryDirectory(prefix='ml-import') as tmp:
        tmp = Path(tmp)
        for table in mb_tables:
            import_musicbrainz(duck, table, tmp)


def parse_args():
    parser = argparse.ArgumentParser(
        prog='import-mlhd',
        description='Import MLHD and MusicBrainz data.'
    )
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose logging')
    parser.add_argument('--mb-table', metavar='TABLE', help='import MusicBrains TABLE')
    return parser.parse_args()


def import_musicbrainz(db: DuckDBPyConnection, table: str, tmp: Path):
    sql_fn = sql_dir / f'mb_{table}.sql'
    meta, sql = read_sql(sql_fn)

    _log.info('creating FIFO in %s', tmp)
    fifo = tmp / f'decompress-{table}.fifo'
    os.mkfifo(fifo)

    _log.info('maybe dropping table %s', table)
    db.execute(f'DROP TABLE IF EXISTS mb_{table}')
    _log.info('creating table %s', table)
    db.execute(sql)
    _log.info('opening input')
    mb_fn = mb_dir / f'{table}.tar.xz'
    if not mb_fn.exists():
        _log.error('%s does not exist', mb_fn)
        raise RuntimeError('missing input file')
    pid = os.fork()
    if pid == 0:
        _log.debug('opening FIFO %s', fifo)
        out = os.open(fifo, os.O_WRONLY)
        _log.debug('spawning process')
        os.dup2(out, 1)
        os.close(out)
        os.execvp('tar', ['tar', 'xf', os.fspath(mb_fn), '-O', f'mbdump/{table}'])
        raise RuntimeError('could not spawn tar')

    _log.info('inserting JSON')
    cols = ', '.join(f'"{col}"' for col in meta['columns'])
    insert = f"""
        INSERT INTO mb_{table}
        SELECT {cols} FROM read_json('{fifo}', format='newline_delimited', maximum_object_size=67108864)
    """
    _log.debug('INSERT statement: %s', insert)
    db.execute(insert)

    _log.info("cleaning up")
    _pid, code = os.waitpid(pid, 0)
    if code != 0:
        _log.error('decompression failed with status %s', code)


def read_sql(fn: Path) -> tuple[dict, str]:
    """
    Read an SQL script along with its metadata.
    """
    text = fn.read_text()
    yaml_lines = []
    sql_lines = []
    for line in text.splitlines():
        m = _YAML_RE.match(line)
        if m:
            yaml_lines.append(m[1])
        else:
            sql_lines.append(line)

    yaml = '\n'.join(yaml_lines)
    sql = '\n'.join(sql_lines)
    if yaml.strip():
        meta = safe_load(yaml)
    else:
        meta = {}
    return meta, sql


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)

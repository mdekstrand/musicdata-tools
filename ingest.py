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

import logging
import os
import re
from pathlib import Path

from docopt import ParsedOptions, docopt
from duckdb import DuckDBPyConnection
from sandal.cli import setup_logging
from yaml import safe_load

from musicdata import musicbrainz

_log = logging.getLogger("ingest")

MB_TABLES = [
    "artist",
    "recording",
]

sql_dir = Path("sql")
mb_dir = Path("data/musicbrainz")
db_path = Path("data/musicbrainz.db")


def main(args: ParsedOptions):
    setup_logging(args["--verbose"])

    if args["--musicbrainz"]:
        musicbrainz.import_mb(args["--part"])


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)

"""
Ingest source data to DuckDB-usable formats.

Usage:
    ingest.py [options] --musicbrainz [-p PART]
    ingest.py [options] --mlhd

Options:
    -v, --verbose       enable verbose log output
    --log-file=FILE     write log file to FILE
    -p PART, --part=PART
                        only import data segment PART

Modes:
    --musicbrainz       import MusicBrainz metadata
    --mlhd              import MLHD+ listening data
"""

import logging

from docopt import ParsedOptions, docopt
from sandal.cli import setup_logging

from musicdata import musicbrainz
from musicdata.layout import data_dir

_log = logging.getLogger("ingest")


def main(args: ParsedOptions):
    setup_logging(args["--verbose"], args["--log-file"], True)

    _log.info("ensuring data directory exists")
    data_dir.mkdir(exist_ok=True)

    if args["--musicbrainz"]:
        _log.info("starting MusicBrainz import")
        musicbrainz.import_mb(args["--part"])


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)

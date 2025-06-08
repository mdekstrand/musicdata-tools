"""
Ingest source data to DuckDB-usable formats.

Usage:
    ingest.py [options] --musicbrainz [-p PART]
    ingest.py [options] --mlhd [--use-mapping]

Options:
    -v, --verbose       enable verbose log output
    --log-file=FILE     write log file to FILE
    -p PART, --part=PART
                        only import data segment PART
    --use-mapping       enable mapping for UUIDs to integers in MLHD+
Modes:
    --musicbrainz       import MusicBrainz metadata
    --mlhd              import MLHD+ listening data
"""

import logging
import sys

from docopt import ParsedOptions, docopt
from sandal.cli import setup_logging

from musicdata import mlhd, musicbrainz
from musicdata.layout import data_dir

_log = logging.getLogger("ingest")


def main(args: ParsedOptions):
    setup_logging(args["--verbose"], "ingest.log", True)

    _log.info("ensuring data directory exists")
    data_dir.mkdir(exist_ok=True)

    if args["--musicbrainz"]:
        _log.info("starting MusicBrainz import")
        musicbrainz.import_mb(args["--part"])
    elif args["--mlhd"]:
        use_mapping = args["--use-mapping"]
        _log.info(f"starting MLHD+ import with use_mapping={use_mapping}")
        mlhd.import_mlhd(use_mapping=use_mapping)
    else:
        _log.error("no valid action specified")
        sys.exit(2)


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)
    
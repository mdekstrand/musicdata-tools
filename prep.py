"""
Preprocess MLHD+ Data

Usage:
    prep.py [options]

Options:
    -v, --verbose       enable verbose log output
    --log-file=FILE     write log file to FILE
"""

import logging
from docopt import ParsedOptions, docopt
from sandal.cli import setup_logging
from musicdata import mlhd_prep
from musicdata.layout import data_dir

_log = logging.getLogger("ingest")


def main(args: ParsedOptions):
    setup_logging(args["--verbose"], "prep.log", True)

    _log.info("ensuring data directory exists")
    data_dir.mkdir(exist_ok=True)

    _log.info(f"starting MLHD+ preprocess")
    mlhd_prep.process()


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)
    
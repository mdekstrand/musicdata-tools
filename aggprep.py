"""
Aggregate Artist-User Pairs in Preprocessed MLHD+ Data

Usage:
    aggprep.py [options]

Options:
    -v, --verbose       enable verbose log output
    --log-file=FILE     write log file to FILE
"""

import logging
from docopt import ParsedOptions, docopt
from sandal.cli import setup_logging
from musicdata import mlhd_prep_agg
from musicdata.layout import data_dir

_log = logging.getLogger("ingest")


def main(args: ParsedOptions):
    setup_logging(args["--verbose"], "aggprep.log", True)

    _log.info(f"starting MLHD+ preprocess")
    mlhd_prep_agg.process()


if __name__ == "__main__":
    args = docopt(__doc__)
    main(args)
    
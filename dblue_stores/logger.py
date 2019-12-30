import logging

import sys

logger = logging.getLogger('dblue.store')


def configure_logger(verbose):
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s %(message)s',
        level=log_level,
        stream=sys.stdout
    )

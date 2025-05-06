#!/usr/bin/env python3
## @file logger_configurator.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 02 May 2025

####################################################################################################
import logging
####################################################################################################
def configure_logging(log_file, debug=False):
    """Configure logging

    Arguments:
    log_file -- name for log file
    debug    -- activate or not debug level
    """

    # Set log level and format based on debug mode
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = (
        '%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
        if debug else
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()
        ]
    )

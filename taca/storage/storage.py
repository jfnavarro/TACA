"""
Storage methods and utilities
"""
import logging
import os
import re
import shutil
import time
from taca.utils.config import CONFIG
from taca.utils import filesystem

logger = logging.getLogger(__name__)

# This is used by many of the functions in this module
finished_run_indicator = CONFIG.get('storage', {}).get('finished_run_indicator', 'RTAComplete.txt')

def cleanup_nas(seconds):
    """
    Will move the finished runs in NASes to nosync directory.
    :param int seconds: Days/hours converted as second to consider a run to be old
    """
    check_demux = CONFIG.get('storage', {}).get('check_demux', False)
    dirs = CONFIG.get('storage').get('data_dirs')
    dirs = dirs if isinstance(dirs, list) else [dirs]
    for data_dir in dirs:
        logger.info('Moving old runs in {}'.format(data_dir))
        with filesystem.chdir(data_dir):
            for run in [r for r in os.listdir(data_dir) if re.match(filesystem.RUN_RE, r)]:
                rta_file = os.path.join(run, finished_run_indicator)
                if os.path.exists(rta_file):
                    if check_demux:
                        logger.info('Moving run {} to nosync directory'.format(os.path.basename(run)))
                        shutil.move(run, 'nosync')
                    else:
                        if os.stat(rta_file).st_mtime < time.time() - seconds:
                            logger.info('Moving run {} to nosync directory'.format(os.path.basename(run)))
                            shutil.move(run, 'nosync')
                        else:
                            logger.info('{} file exists but is not older than given time, skipping run {}'
                                        .format(finished_run_indicator, run))

def cleanup_processing(seconds):
    """
    Cleanup runs in processing server.
    :param int seconds: Days/hours converted as second to consider a run to be old
    """
    try:
        # Remove old runs from archiving dirs
        dirs = CONFIG.get('storage').get('archive_dirs')
        dirs = dirs if isinstance(dirs, list) else [dirs]
        for archive_dir in dirs:
            logger.info('Removing old runs in {}'.format(archive_dir))
            with filesystem.chdir(archive_dir):
                for run in [r for r in os.listdir(archive_dir) if re.match(filesystem.RUN_RE, r)]:
                    rta_file = os.path.join(run, finished_run_indicator)
                    if os.path.exists(rta_file):
                        if os.stat(rta_file).st_mtime < time.time() - seconds:
                            logger.info('Removing run {} to nosync directory'.format(os.path.basename(run)))
                            shutil.rmtree(run)
                        else:
                            logger.info('{} file exists but is not older than given time, skipping run {}'.format(
                                        finished_run_indicator, run))
    except IOError:
        logger.error("Could not find transfer.tsv file, so I cannot decide if I should "
                     "archive any run or not.")


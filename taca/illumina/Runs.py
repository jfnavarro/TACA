import os
import re
import csv
import logging
import subprocess
import shutil
from datetime import datetime

from taca.utils import misc

logger = logging.getLogger(__name__)

class Run(object):
    """ 
    Defines an Illumina run
    """
    def __init__(self, run_dir, configuration):
        if not os.path.exists(run_dir):
            raise RuntimeError('Could not locate run directory {}'.format(run_dir))
        
        if 'analysis_server' not in configuration or \
            'bcl2fastq' not in configuration or \
            'samplesheets_dir' not in configuration:
            raise RuntimeError("configuration missing required entries "
                               "(analysis_server, bcl2fastq, samplesheets_dir)")
        
        if not os.path.exists(os.path.join(run_dir, 'runParameters.xml')) \
        and os.path.exists(os.path.join(run_dir, 'RunParameters.xml')):
            # In NextSeq runParameters is named RunParameters
            logger.warning("Renaming RunParameters.xml to runParameters.xml")
            os.rename(os.path.join(run_dir, 'RunParameters.xml'), os.path.join(run_dir, 'runParameters.xml'))
        elif not os.path.exists(os.path.join(run_dir, 'runParameters.xml')):
            raise RuntimeError('Could not locate runParameters.xml in run directory {}'.format(run_dir))
              
        self.run_dir = os.path.abspath(run_dir)
        self.id = os.path.basename(os.path.normpath(run_dir))
        pattern = r'(\d{6})_([ST-]*\w+\d+)_\d+_([AB]?)([A-Z0-9\-]+)'
        m = re.match(pattern, self.id)
        self.date = m.group(1)
        self.instrument = m.group(2)
        self.position = m.group(3)
        self.flowcell_id = m.group(4)
        self.CONFIG = configuration
        self._set_demux_folder(configuration)
        # This flag tells TACA to move demultiplexed files to the analysis server
        self.transfer_to_analysis_server = True
        # Probably worth to add the samplesheet name as a variable too
        
    def demultiplex_run(self):
        raise NotImplementedError("Please Implement this method")

    def check_run_status(self):
        raise NotImplementedError("Please Implement this method")

    def _set_run_type(self):
        raise NotImplementedError("Please Implement this method")

    def get_run_type(self):
        if self.run_type:
            return self.run_type
        else:
            raise RuntimeError("run_type not yet available!!")

    def _set_sequencer_type(self, configuration):
        raise NotImplementedError("Please Implement this method")

    def _get_sequencer_type(self):
        if self.sequencer_type:
            return self.sequencer_type
        else:
            raise RuntimeError("sequencer_type not yet available!!")

    def _set_demux_folder(self, configuration):
        self.demux_dir = "Demultiplexing"
        for option in self.CONFIG['bcl2fastq']['options']:
            if isinstance(option, dict) and option.get('output-dir'):
                _demux_dir = option.get('output-dir')

    def _get_demux_folder(self):
        if self.demux_dir:
            return self.demux_dir
        else:
            raise RuntimeError("demux_folder not yet available!!")

    def _get_samplesheet(self):
        raise NotImplementedError("Please Implement this method")

    def _is_demultiplexing_done(self):
        return os.path.exists(os.path.join(self.run_dir,
                                           self._get_demux_folder(), 
                                           'Stats',
                                           'DemultiplexingStats.xml'))

    def _is_demultiplexing_started(self):
        return os.path.exists(os.path.join(self.run_dir, self._get_demux_folder()))

    def _is_sequencing_done(self):
        return os.path.exists(os.path.join(self.run_dir, 'RTAComplete.txt'))

    def get_run_status(self):
        """ Return the status of the run, that is the trello card where it needs to be placed
        """
        demux_started = self._is_demultiplexing_started() # True if demux is ongoing
        demux_done = self._is_demultiplexing_done() # True if demux is done
        sequencing_done = self._is_sequencing_done() # True if sequencing is done
        if sequencing_done and demux_done:
            return 'COMPLETED' # run is done, transfer might be ongoing.
        elif sequencing_done and demux_started and not demux_done:
            return 'IN_PROGRESS'
        elif sequencing_done and not demux_started:
            return 'TO_START'
        elif not sequencing_done:
            return 'SEQUENCING'
        else:
            raise RuntimeError('Unexpected status in get_run_status')

    def transfer_run(self, t_file, analysis):
        """ Transfer a run to the analysis server. Will add group R/W permissions to
            the run directory in the destination server so that the run can be processed
            by any user/account in that group (i.e a functional account...). 
            :param str t_file: File where to put the transfer information
            :param bool analysis: Trigger analysis on remote server
        """
        # TODO: check the run type and build the correct rsync command
        # The option -a implies -o and -g which is not the desired behaviour
        command_line = ['rsync', '-Lav', '--no-o', '--no-g']
        # Add R/W permissions to the group
        command_line.append('--chmod=g+rw')
        # This horrible thing here avoids data dup when we use multiple indexes in a lane/FC
        command_line.append("--exclude=Demultiplexing_*/*_*") 
        command_line.append("--include=*/")
        for to_include in self.CONFIG['analysis_server']['sync']['include']:
            command_line.append("--include={}".format(to_include))
        command_line.extend(["--exclude=*", "--prune-empty-dirs"])
        r_user = self.CONFIG['analysis_server']['user']
        r_host = self.CONFIG['analysis_server']['host']
        r_dir = self.CONFIG['analysis_server']['sync']['data_archive']
        remote = "{}@{}:{}".format(r_user, r_host, r_dir)
        command_line.extend([self.run_dir, remote])

        # Create temp file indicating that the run is being transferred
        try:
            open(os.path.join(self.run_dir, 'transferring'), 'w').close()
        except IOError as e:
            logger.error("Cannot create a file in {}. "
                         "Check the run name, and the permissions.".format(self.id))
            raise e
        started = ("Started transfer of run {} on {}".format(self.id, datetime.now()))
        logger.info(started)
        # In this particular case we want to capture the exception because we want
        # to delete the transfer file
        try:
            misc.call_external_command(command_line, with_log_files=True, 
                                       prefix="", log_dir=self.run_dir)
        except subprocess.CalledProcessError as exception:
            os.remove(os.path.join(self.run_dir, 'transferring'))
            raise exception

        logger.info('Adding run {} to {}'.format(self.id, t_file))
        with open(t_file, 'a') as tranfer_file:
            tsv_writer = csv.writer(tranfer_file, delimiter='\t')
            tsv_writer.writerow([self.id, str(datetime.now())])
        os.remove(os.path.join(self.run_dir, 'transferring'))

        if analysis:
            # This needs to pass the runtype (i.e., Xten or HiSeq) and start the correct pipeline
            self.trigger_analysis()
        
    def archive_run(self, destination):
        """ Move run to the archive folder
            :param str destination: the destination folder
        """
        if destination and os.path.isdir(destination):
            logger.info('archiving run {}'.format(self.id))
            shutil.move(self.run_dir, os.path.join(destination, self.id))
        else:
            logger.warning("Cannot move run to archive, destination does not exist")

    def is_transferred(self, transfer_file):
        """ Checks wether a run has been transferred to the analysis server or not.
            Returns true in the case in which the tranfer is ongoing.
            :param str run: Run directory
            :param str transfer_file: Path to file with information about transferred runs
        """
        try:
            with open(transfer_file, 'r') as file_handle:
                t_f = csv.reader(file_handle, delimiter='\t')
                for row in t_f:
                    # Rows have two columns: run and transfer date
                    if row[0] == os.path.basename(self.id):
                        return True
            if os.path.exists(os.path.join(self.run_dir, 'transferring')):
                return True
            return False
        except IOError:
            return False

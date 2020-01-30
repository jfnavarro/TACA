import os
from datetime import datetime

from flowcell_parser.classes import SampleSheetParser
from taca.utils.filesystem import chdir
from taca.illumina.Runs import Run
from taca.utils import misc

import logging
logger = logging.getLogger(__name__)

class NextSeq_Run(Run):

    def __init__(self,  path_to_run, configuration):
        # Constructor, it returns a NextSeq object only 
        # if the NextSeq run belongs to NGI or ST facility, i.e., contains
        # Application or Production in the Description
        super(NextSeq_Run, self).__init__( path_to_run, configuration)
        # In the NextSeq the sample sheet is created by the operator
        # and placed in the run root folder.
        self.ssname = os.path.join(self.run_dir, "SampleSheet.csv")
        self.sequencer_type = "NextSeq"
        self._set_run_type()

    def _set_run_type(self):
        if not os.path.exists(self.ssname):
            logger.error("Could not find the Sample Sheet")
            self.run_type = "NON-NGI-RUN"
        else:
            # it sample sheet exists try to see if it is a NGI-run
            try:
                ssparser = SampleSheetParser(self.ssname)
            except:
                logger.error("Error parsing the Sample Sheet")
                self.run_type = "NON-NGI-RUN"
            else:
                #TODO Key error can perfectly occur here
                if ssparser.header['Description'] == "Production" \
                or ssparser.header['Description'] == "Application" \
                or ssparser.header['Description'] == "Private":
                    self.run_type = "NGI-RUN"
                else:
                    # otherwise this is a non NGI run
                    self.run_type = "NON-NGI-RUN"
     
    def check_run_status(self):
        return

    def demultiplex_run(self): 
        """ Demultiplex a NextSeq run:
            - define if necessary the bcl2fastq commands (if indexes are not of size 8, i.e. neoprep)
            - run bcl2fastq conversion
        """
        # Samplesheet need to be positioned in the FC directory with name SampleSheet.csv (Illumina default)
        # Make the demux call
        with chdir(self.run_dir):
            cl = [self.CONFIG.get('bcl2fastq')['bin']]
            if self.CONFIG.get('bcl2fastq').has_key('options'):
                cl_options = self.CONFIG['bcl2fastq']['options']
                # Append all options that appear in the configuration file to the main command.
                for option in cl_options:
                    if isinstance(option, dict):
                        opt, val = option.items()[0]
                        cl.extend(['--{}'.format(opt), str(val)])
                    else:
                        cl.append('--{}'.format(option))
            logger.info(("BCL to FASTQ conversion and demultiplexing started for "
                 " run {} on {}".format(os.path.basename(self.id), datetime.now())))
            try:
                misc.call_external_command_detached(cl, with_log_files=True)
            except:
                logger.error("There was an error running bcl2fasq")
                raise
        return True
        







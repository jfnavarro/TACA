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
        # if the NextSeq run belongs to NGI facility, i.e., contains
        # Application or Production in the Description
        super(NextSeq_Run, self).__init__( path_to_run, configuration)
        # In the NextSeq the sample sheet is created by the operator
        # and placed in the run root folder.
        self.ssname = os.path.join(self.run_dir, "SampleSheet.csv")
        self.sequencer_type = "NextSeq"
        self._set_run_type()

    def _set_run_type(self):
        if not os.path.exists(self.ssname):
            # Case in which no samplesheet is found, assume it is a non NGI run
            self.run_type = "NON-NGI-RUN"
        else:
            # it sample sheet exists try to see if it is a NGI-run
            try:
                ssparser = SampleSheetParser(self.ssname)
            except:
                logger.error("Error parsing the samplessheet")
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
        # if this is not the case then create it and take special care of modification to be done on the SampleSheet
        #samplesheet_dest = os.path.join(self.run_dir, "SampleSheet.csv")
        # Check that the samplesheet is not already present. In this case go the next step
        #if not os.path.exists(samplesheet_dest):
        #    try:
        #        with open(samplesheet_dest, 'wb') as fcd:
        #            fcd.write(self._generate_clean_samplesheet(ssparser))
        #    except Exception as e:
        #        if os.path.exists(samplesheet_dest):
        #            os.remove(samplesheet_dest)
        #        logger.error(e)
        #        return False
        #    logger.info(("Created SampleSheet.csv for Flowcell {} in {} "
        #                 .format(self.id, samplesheet_dest)))
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
        
    def _generate_clean_samplesheet(self, ssparser):
        """ Will generate a 'clean' samplesheet, for bcl2fastq2.17
        """
        output = ""
        # Header
        output += "[Header]{}".format(os.linesep)
        for field in ssparser.header:
            output += "{},{}".format(field.rstrip(), ssparser.header[field].rstrip())
            output += os.linesep
        # now parse the data section
        data = []
        # NextSeq has always 4 lanes (Assuming the Lane info is not in the samplesheet)
        # Therefore, the data sections must be duplicated 4 times, one for each lane
        for lane in xrange(1,5):
            for line in ssparser.data:
                entry = {}
                for field, value in line.iteritems():
                    if 'Sample_ID' in field:
                        entry[field] ='Sample_{}'.format(value)
                    elif 'Sample_Project' in field:
                        entry[field] = value.replace(".", "_")
                    else:
                        entry[field] = value     
                entry['Lane'] = str(lane)
                data.append(entry)

        fields_to_output = ['Lane', 'Sample_ID', 'Sample_Name', 'index', 'Sample_Project']
        # now create the new SampleSheet data section
        output += "[Data]{}".format(os.linesep)
        for field in ssparser.datafields:
            if field not in fields_to_output:
                fields_to_output.append(field)
        output += ",".join(fields_to_output)
        output += os.linesep
        # now process each data entry and output it
        for entry in data:
            line = []
            for field in fields_to_output:
                if field in entry:
                    line.append(entry[field])
            output += ",".join(line)
            output += os.linesep
        return output







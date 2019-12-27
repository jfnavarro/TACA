"""
Analysis methods for TACA
"""
import glob
import logging
import os

from taca.illumina.NextSeq_Runs import NextSeq_Run
from taca.utils.config import CONFIG

from flowcell_parser.classes import RunParametersParser

logger = logging.getLogger(__name__)

def get_runObj(run):
    """ Tries to read runParameters.xml to parse the type of sequencer
        and then return the respective Run object (MiSeq, HiSeq..)
        :param run: run name identifier
        :type run: string
        :rtype: Object
        :returns: returns the sequencer type object,
        None if the sequencer type is unknown of there was an error
    """

    if os.path.exists(os.path.join(run, 'runParameters.xml')):
        run_parameters_file = "runParameters.xml"
    elif os.path.exists(os.path.join(run, 'RunParameters.xml')):
        run_parameters_file = "RunParameters.xml"
    else:
        logger.error("Cannot find RunParameters.xml or runParameters.xml in "
                     "the run folder for run {}".format(run))
        return

    rppath = os.path.join(run, run_parameters_file)
    try:
        rp = RunParametersParser(os.path.join(run, run_parameters_file))
    except OSError:
        logger.warn("Problems parsing the runParameters.xml file at {}. "
                    "This is quite unexpected. please archive the run {} manually".format(rppath, run))
    else:
        # This information about the run type (with HiSeq2.5 applicationaName does not work anymore,
        # but as for a long time we will have instruments not updated I need to find out something that works
        try:
            # Works for recent control software
            runtype = rp.data['RunParameters']["Setup"]["Flowcell"]
        except KeyError:
            # Use this as second resource but print a warning in the logs
            logger.warn("Parsing runParameters to fecth instrument type, "
                        "not found Flowcell information in it. Using ApplicaiotnName")
            # here makes sense to use get with default value "" ->
            # so that it doesn't raise an exception in the next lines
            # (in case ApplicationName is not found, get returns None)
            runtype = rp.data['RunParameters']["Setup"].get("ApplicationName", "")

        if "NextSeq" in runtype:
            return NextSeq_Run(run, CONFIG["analysis"]["NextSeq"])
        else:
            logger.warn("Unrecognized run type {}, cannot archive the run {}. "
                        "Someone as likely bought a new sequencer without telling "
                        "it to the bioinfo team".format(runtype, run))

def transfer_run(run_dir, analysis):
    """ Interface for click to force a transfer a run to uppmax
        :param: string run_dir: the run to tranfer
        :param bool analysis: if trigger or not the analysis
    """
    runObj = get_runObj(run_dir)
    if runObj is None:
        # Maybe throw an exception if possible?
        logger.error("Trying to force a transfer of run {} but the sequencer was not recognized.".format(run_dir))
    else:
        runObj.transfer_run("nosync", 
                            os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv'),
                            analysis) 

def run_preprocessing(run, force_trasfer=True):
    """ Run demultiplexing in all data directories
        :param str run: Process a particular run instead of looking for runs
        :param bool force_tranfer: if set to True the FC is transferred also if fails QC
    """
    def _process(run):
        """ Process a run/flowcell and transfer to analysis server
            :param taca.illumina.Run run: Run to be processed and transferred
        """
        logger.info('Checking run {}'.format(run.id))
        t_file = os.path.join(CONFIG['analysis']['status_dir'], 'transfer.tsv')
        if run.is_transferred(t_file):
            # In this case I am either processing a run that is in transfer
            # or that has been already transferred. Do nothing.
            # time to time this situation is due to runs that are copied back from NAS after a reboot.
            # This check avoid failures
            logger.info('Run {} already transferred to analysis server, skipping it'.format(run.id))
            return

        if run.get_run_status() == 'SEQUENCING':
            # Check status files and say i.e Run in second read, maybe something
            # even more specific like cycle or something
            logger.info('Run {} is not finished yet'.format(run.id))
        elif run.get_run_status() == 'TO_START':
            if run.get_run_type() == 'NON-NGI-RUN':
                # For now MiSeq specific case. Process only NGI-run, skip all the others (PhD student runs)
                logger.warn("Run {} marked as {}, "
                            "TACA will skip this and move the run to "
                            "no-sync directory".format(run.id, run.get_run_type()))
                # Archive the run if indicated in the config file
                if 'storage' in CONFIG:
                    run.archive_run(CONFIG['storage']['archive_dirs'][run.sequencer_type])
                return
            # Otherwise it is fine, process it
            logger.info(("Starting BCL to FASTQ conversion and demultiplexing for run {}".format(run.id)))
            try:
                run.demultiplex_run()
            except:
                logger.info(("Error demultiplexing for run {}".format(run.id)))
                pass
        elif run.get_run_status() == 'IN_PROGRESS':
            logger.info(("BCL conversion and demultiplexing process in "
                         "progress for run {}, skipping it".format(run.id)))
        elif run.get_run_status() == 'COMPLETED':
            logger.info(("Preprocessing of run {} is finished, transferring it".format(run.id)))

            # Transfer to analysis server if flag is True
            if run.transfer_to_analysis_server:
                logger.info('Transferring run {} to {} into {}'
                            .format(run.id,
                                    run.CONFIG['analysis_server']['host'],
                                    run.CONFIG['analysis_server']['sync']['data_archive']))
                run.transfer_run(t_file,  False) # Do not trigger analysis

            # Archive the run if indicated in the config file
            if 'storage' in CONFIG:
                run.archive_run(CONFIG['storage']['archive_dirs'][run.sequencer_type])

    if run:
        # Needs to guess what run type I have (HiSeq, MiSeq, HiSeqX, NextSeq)
        runObj = get_runObj(run)
        if not runObj:
            logger.warning("Unrecognized instrument type or incorrect run folder {}".format(run))
            raise RuntimeError("Unrecognized instrument type or incorrect run folder {}".format(run))
        else:
            _process(runObj, force_trasfer)
    else:
        data_dirs = CONFIG.get('analysis').get('data_dirs')
        for data_dir in data_dirs:
            # Run folder looks like DATE_*_*_*, the last section is the FC name. 
            # See Courtesy information from illumina of 10 June 2016 (no more XX at the end of the FC)
            runs = glob.glob(os.path.join(data_dir, '[1-9]*_*_*_*'))
            for _run in runs:
                runObj = get_runObj(_run)
                if not runObj:
                    logger.warning("Unrecognized instrument type or incorrect run folder {}".format(_run))
                    raise RuntimeError("Unrecognized instrument type or incorrect run folder {}".format(_run))
                else:
                    try:
                        _process(runObj, force_trasfer)
                    except:
                        # this function might throw and exception,
                        # it is better to continue processing other runs
                        logger.warning("There was an error processing the run {}".format(_run))
                        pass





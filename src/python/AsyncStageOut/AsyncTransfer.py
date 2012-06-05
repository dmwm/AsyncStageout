#!/usr/bin/env python
"""
Checks for files to transfer
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.TransferDaemon import TransferDaemon
from AsyncStageOut.LFNSourceDuplicator import LFNSourceDuplicator
from AsyncStageOut.StatisticDaemon import StatisticDaemon
from AsyncStageOut.AnalyticsDaemon import AnalyticsDaemon
from AsyncStageOut.FilesCleaner import FilesCleaner
import subprocess, os, errno
import time, datetime
import logging
import threading

def execute_command(command):
    """
    _execute_command_
    Function to manage commands.
    """
    proc = subprocess.Popen(
           ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
           stdout=subprocess.PIPE,
           stderr=subprocess.PIPE,
           stdin=subprocess.PIPE,
    )
    proc.stdin.write(command)
    stdout, stderr = proc.communicate()
    rc = proc.returncode

    return stdout, stderr, rc

class AsyncTransfer(Harness):
    """
    _AsyncTransfer_
    AsyncTransfer main class. Call workers to do following work:
    1- Duplicate lfn's from a source into the AsyncTransfer CouchDB
    2- Transfer LFN in the local AsyncTransfer CouchDB
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("AsyncTransfer.__init__")

    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        logging.debug(self.config)

        # in case nothing was configured we have a fallback.
        myThread = threading.currentThread()

        # Archiving logs
        log_dir = '%s/logs' % self.config.AsyncTransfer.componentDir
        if os.path.exists(log_dir):

            archive_dir = '%s/archive/%s/%s/%s' % ( self.config.AsyncTransfer.componentDir, \
str(datetime.datetime.now().year), str(datetime.datetime.now().month), str(datetime.datetime.now().day) )
            try:
                os.makedirs(archive_dir)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else: raise
            command = 'tar -czf %s/logs_%s.tar.gz %s/* ; rm -rf %s/*' % ( archive_dir, str(time.time()), log_dir, log_dir )
            out, error, retcode = execute_command(command)
            if retcode != 0 :
                msg = "Error when archiving %s : %s"\
                       % (log_dir, error)
                raise Exception(msg)

        logging.debug("Setting DB source poll interval to %s seconds" \
                      %str(self.config.AsyncTransfer.pollViewsInterval) )

        myThread.workerThreadManager.addWorker( \
                              LFNSourceDuplicator(self.config), \
                              self.config.AsyncTransfer.pollViewsInterval \
                            )

        logging.debug("Setting poll interval to %s seconds" \
                      %str(self.config.AsyncTransfer.pollInterval) )


        myThread.workerThreadManager.addWorker( \
                              TransferDaemon(self.config), \
                              self.config.AsyncTransfer.pollInterval \
                            )

        logging.debug("Setting poll statistic interval to %s seconds" \
                       %str(self.config.AsyncTransfer.pollStatInterval) )

        myThread.workerThreadManager.addWorker( \
                              StatisticDaemon(self.config), \
                              self.config.AsyncTransfer.pollStatInterval \
                            )

        logging.debug("Setting Analytics polling interval to %s seconds" \
                       %str(self.config.AsyncTransfer.analyticsPollingInterval) )

        myThread.workerThreadManager.addWorker( \
                              AnalyticsDaemon(self.config), \
                              self.config.AsyncTransfer.analyticsPollingInterval \
                            )

        myThread.workerThreadManager.addWorker( \
                              FilesCleaner(self.config), \
                              self.config.AsyncTransfer.filesCleaningPollingInterval \
                            )

        logging.debug("Setting Analytics polling interval to %s seconds" \
                       %str(self.config.AsyncTransfer.filesCleaningPollingInterval) )

        return

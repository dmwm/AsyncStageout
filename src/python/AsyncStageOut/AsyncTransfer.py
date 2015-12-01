#!/usr/bin/env python
"""
Checks for files to transfer
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.TransferDaemon import TransferDaemon
# from AsyncStageOut.LFNSourceDuplicator import LFNSourceDuplicator
from AsyncStageOut import execute_command
import os
import errno
import time
import datetime
import logging
import threading


class AsyncTransfer(Harness):
    """
    _AsyncTransfer_
    AsyncTransfer main class. Call workers to do following work:
    1- Duplicate lfn's from a source into the AsyncTransfer CouchDB
    2- Transfer LFN in the local AsyncTransfer CouchDB
    """

    def __init__(self, config):
        # call the base class
        self.config = config
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
        logDir = '%s/logs' % self.config.AsyncTransfer.componentDir
        if os.path.exists(logDir):

            archiveDir = '%s/archive/%s/%s/%s' % (self.config.AsyncTransfer.componentDir, str(datetime.datetime.now().year),
                                                  str(datetime.datetime.now().month), str(datetime.datetime.now().day))
            try:
                os.makedirs(archiveDir)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            command = 'tar -czf %s/logs_%s.tar.gz %s/* ; rm -rf %s/*' % (archiveDir, str(time.time()), logDir, logDir)
            dummyout, error, retcode = execute_command(command)
            if retcode != 0:
                msg = "Error when archiving %s : %s" % (logDir, error)
                raise Exception(msg)

#        logging.debug("Setting DB source poll interval to %s seconds" \
#                      %str(self.config.AsyncTransfer.pollViewsInterval) )

#        myThread.workerThreadManager.addWorker( \
#                              LFNSourceDuplicator(self.config), \
#                              self.config.AsyncTransfer.pollViewsInterval \
#                            )

        logging.debug("Setting poll interval to %s seconds", self.config.AsyncTransfer.pollInterval)

        myThread.workerThreadManager.addWorker(
            TransferDaemon(self.config),
            self.config.AsyncTransfer.pollInterval
        )

        return

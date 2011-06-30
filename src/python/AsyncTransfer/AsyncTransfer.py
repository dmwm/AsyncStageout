#!/usr/bin/env python
"""
Checks for files to transfer
"""
import logging
import threading

from WMCore.Agent.Harness import Harness

from TransferDaemon import TransferDaemon
from LFNSourceDuplicator import LFNSourceDuplicator
from StatisticDaemon import StatisticDaemon

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
        logging.info(self.config)

        # in case nothing was configured we have a fallback.
        myThread = threading.currentThread()

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


        return


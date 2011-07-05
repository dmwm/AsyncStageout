#!/usr/bin/env python
"""
Checks for files to process
"""
import logging
import threading

from WMCore.Agent.Harness import Harness

from AsyncStageOut_t.fakeDaemon import fakeDaemon

class AsyncTransferTest(Harness):
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

        logging.debug("Setting poll interval to %s seconds" \
                      %str(self.config.AsyncTransferTest.pollInterval) )


        myThread.workerThreadManager.addWorker( \
                              fakeDaemon(self.config), \
                              self.config.AsyncTransferTest.pollInterval \
                            )


        return

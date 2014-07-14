#!/usr/bin/env python
"""
Checks for files to transfer
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.ReporterDaemon import ReporterDaemon
from AsyncStageOut import execute_command
import os, errno
import time, datetime
import logging
import threading

class Reporter(Harness):
    """
_AsyncTransfer_
AsyncTransfer main class. Call workers to do following work:
1- Duplicate lfn's from a source into the AsyncTransfer CouchDB
2- Transfer LFN in the local AsyncTransfer CouchDB
"""

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("Reporter.__init__")

    def preInitialization(self):
        """
Add required worker modules to work threads
"""
        logging.debug(self.config)

        # in case nothing was configured we have a fallback.
        myThread = threading.currentThread()

        myThread.workerThreadManager.addWorker( \
                              ReporterDaemon(self.config), \
                              self.config.AsyncTransfer.pollInterval \
                            )

        return

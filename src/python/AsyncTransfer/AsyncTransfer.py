#!/usr/bin/env python
"""
Checks for files to transfer
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from AsyncTransfer.TransferDaemon import TransferDaemon

class AsyncTransfer(Harness):
    """
    _FileTransfer_
    
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
                      %str(self.config.AsyncTransfer.pollInterval) )


        myThread.workerThreadManager.addWorker( \
                              TransferDaemon(self.config), \
                              self.config.AsyncTransfer.pollInterval \
                            )


        return  


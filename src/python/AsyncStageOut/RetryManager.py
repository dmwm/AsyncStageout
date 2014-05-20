#!/usr/bin/env python
"""
RetryManager
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.RetryManagerDaemon import RetryManagerDaemon
import logging
import threading

class RetryManager(Harness):
    """
    _RetryManager_
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("RetryManager.__init__")

    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        logging.debug(self.config)
        myThread = threading.currentThread()
        logging.debug("Setting RetryManager polling interval to %s seconds" \
                       %str(self.config.RetryManager.pollInterval) )
        myThread.workerThreadManager.addWorker( \
                              RetryManagerDaemon(self.config), \
                              self.config.RetryManager.pollInterval \
                            )


        return

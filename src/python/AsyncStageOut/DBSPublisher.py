#!/usr/bin/env python
"""
Checks for files to publish
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.PublisherDaemon import PublisherDaemon
import logging
import threading

class DBSPublisher(Harness):
    """
    _DBSPublisher_
    DBSPublisher main class. Call workers by user to publish files into DBS
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("DBSPublisher.__init__")

    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        logging.debug(self.config)
        myThread = threading.currentThread()
        logging.debug("Setting component poll interval to %s seconds" \
                      %str(self.config.DBSPublisher.pollInterval) )
        myThread.workerThreadManager.addWorker( \
                              PublisherDaemon(self.config), \
                              self.config.DBSPublisher.pollInterval \
                            )

        return

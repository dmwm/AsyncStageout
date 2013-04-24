#!/usr/bin/env python
"""
Checks for files to publish
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.AnalyticsDaemon import AnalyticsDaemon
import logging
import threading

class Analytics(Harness):
    """
    _Analytics_
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("Analytics.__init__")

    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        logging.debug(self.config)
        myThread = threading.currentThread()
        logging.debug("Setting Analytics polling interval to %s seconds" \
                       %str(self.config.Analytics.analyticsPollingInterval) )
        myThread.workerThreadManager.addWorker( \
                              AnalyticsDaemon(self.config), \
                              self.config.Analytics.analyticsPollingInterval \
                            )

        return

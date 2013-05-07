#!/usr/bin/env python
"""
Build stat docs.
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.StatisticDaemon import StatisticDaemon 
import logging
import threading

class Statistics(Harness):
    """
    _Statistics_
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("Statistics.__init__")

    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        logging.debug(self.config)
        myThread = threading.currentThread()
        logging.debug("Setting component poll interval to %s seconds" \
                      %str(self.config.Statistics.pollStatInterval) )
        myThread.workerThreadManager.addWorker( \
                              StatisticDaemon(self.config), \
                              self.config.Statistics.pollStatInterval \
                            )

        return

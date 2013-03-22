#!/usr/bin/env python
"""
Checks for files to publish
"""
from WMCore.Agent.Harness import Harness
from AsyncStageOut.CleanerDaemon import CleanerDaemon
from AsyncStageOut import execute_command
import logging
import threading
import os, errno
import time, datetime

class FilesCleaner(Harness):
    """
    _DBSPublisher_
    DBSPublisher main class. Call workers by user to publish files into DBS
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        logging.info("FilesCleaner.__init__")

    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        logging.debug(self.config)
        myThread = threading.currentThread()
        # Archiving logs
        log_dir = '%s/logs' % self.config.FilesCleaner.componentDir
        if os.path.exists(log_dir):

            archive_dir = '%s/archive/%s/%s/%s' % ( self.config.FilesCleaner.componentDir, \
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

        logging.debug("Setting Cleaning polling interval to %s seconds" \
                       %str(self.config.FilesCleaner.filesCleaningPollingInterval) )
        myThread.workerThreadManager.addWorker( \
                              CleanerDaemon(self.config), \
                              self.config.FilesCleaner.filesCleaningPollingInterval \
                            )


        return

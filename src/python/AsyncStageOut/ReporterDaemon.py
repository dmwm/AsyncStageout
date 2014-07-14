#pylint: disable=C0103,W0105

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
"""
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from AsyncStageOut.ReporterWorker import ReporterWorker
from multiprocessing import Pool
from WMCore.WMFactory import WMFactory
#import random
import logging
#import time
import os

result_list = []
current_running = []

def reporter(user, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the reporter worker")
    try:
        worker = ReporterWorker(user, config)
    except Exception, e:
        logging.debug("Reporter Worker cannot be created!:" %e)
        return user
    if worker.init:
        logging.debug("Starting %s" % worker)
        try:
            worker ()
        except Exception, e:
            logging.debug("Reporter Worker cannot start!:" %e)
            return user
    else:
       logging.debug("Worker cannot be initialized!")
    return user

def log_result(result):
    """
    Each worker executes this callback.
    """
    result_list.append(result)
    current_running.remove(result)

class ReporterDaemon(BaseWorkerThread):
    """
    _TransferDaemon_
    Call multiprocessing library to instantiate a TransferWorker for each user.
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        #Need a better way to test this without turning off this next line
        BaseWorkerThread.__init__(self)
        #logging.basicConfig(format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt = '%m-%d %H:%M')
        #self.logger = logging.getLogger()
        # self.logger is set up by the BaseWorkerThread, we just set it's level

        self.config = config.AsyncTransfer
        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)
        self.pool = Pool(processes=self.config.pool_size)
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.schedAlgoDir, namespace = self.config.schedAlgoDir)
        self.dropbox_dir = '%s/dropbox/inputs' % self.config.componentDir
        if not os.path.isdir(self.dropbox_dir):
            try:
                os.makedirs(self.dropbox_dir)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    self.logger.error('Unknown error in mkdir' % e.errno)
                    raise
        result_list = []
        current_running = []

    # Over riding setup() is optional, and not needed here
    def algorithm(self, parameters = None):
        """
        1. Get a list of users with files to transfer from the FS
        2. Submit the report to a subprocess
       """
        users = []
        for user_dir in os.listdir(self.dropbox_dir):
            if os.path.isdir(os.path.join(self.dropbox_dir, user_dir)) and os.listdir(os.path.join(self.dropbox_dir, user_dir)):
                users.append(user_dir)

        self.logger.info('Active users %s' % len(users))
        self.logger.debug('Active users %s' % users)

        self.logger.info('Current reporter running %s' % len(current_running))
        self.logger.debug('Current reporter running %s' % current_running)

        for u in users:
            self.logger.debug('kicking off pool')
            if u not in current_running:
                self.logger.debug('New reporter for %s' % u)
                current_running.append(u)
                self.pool.apply_async(reporter,(u, self.config), callback = log_result)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

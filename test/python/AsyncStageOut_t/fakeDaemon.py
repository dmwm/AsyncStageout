#pylint: disable=C0103

"""
Spawn fake workers
"""

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from multiprocessing import Pool

import logging

def ftscp(user, tfc_map, config):
    """
    Fake ftscp function.
    """
    logging.debug("Work done using %s %s %s!..." %(user, tfc_map, config))
    return True


class fakeDaemon(BaseWorkerThread):
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.logger = logging.getLogger()

        self.config = config.AsyncTransferTest
        self.logger.setLevel(self.config.log_level)
        self.logger.debug('Configuration loaded')

    def algorithm(self, parameters = None):
        """
        Give a list of users, sites and an empty tfc map
        to pool workers and object. The aim is to use the same
        mulprocessing call used in the AsyncStageOut
        """
        users = ['user1']
        sites = [u'T2_IT_Bari', u'T2_IT_Pisa']
        self.logger.debug('Active sites are : %s ' %sites)
        site_tfc_map = {}

        self.logger.debug( 'kicking off pool with size %s' %self.config.pool_size )
        pool = Pool(processes=self.config.pool_size)
        r = [pool.apply_async(ftscp, (u, site_tfc_map, self.config)) for u in users]
        pool.close()
        pool.join()

        for result in r:
            if result.ready():
                self.logger.info(result.get(1))

        return r

#pylint: disable=C0103,W0105

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
4. spawn a process per user that publish their files
"""
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from AsyncStageOut.PublisherWorker import PublisherWorker
from multiprocessing import Pool
import random

def publish(user, config):
    """
    Each worker executes this function.
    """
    worker = PublisherWorker(user, config)
    return worker()

class PublisherDaemon(BaseWorkerThread):
    """
    _PublisherDaemon_
    Call multiprocessing library to instantiate a PublisherWorker for each user.
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

        self.config = config.DBSPublisher
        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)

        self.logger.debug('Configuration loaded')

        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to CouchDB')
        self.pool = Pool(processes=self.config.publication_pool_size)

    # Over riding setup() is optional, and not needed here
    def algorithm(self, parameters = None):
        """

        1. Get a list of users with files to publish from the couchdb instance
        2. For each user get a suitably sized input for publish
        3. Submit the publish to a subprocess

        """
        users = self.active_users(self.db)
        self.logger.debug('kicking off pool %s' %users)
        r = [self.pool.apply_async(publish, (u, self.config)) for u in users]
        for result in r:
            self.logger.info(result.get())

    def active_users(self, db):
        """
        Query a view for users with files to publish.
        """
        query = {'group': True, 'group_level':3}
        users = db.loadView('DBSPublisher', 'publish', query)
        active_users = []
        if len(users['rows']) <= self.config.publication_pool_size:
            active_users = users['rows']
        else:
            #TODO: have a plugin algorithm here...
            active_users = random.sample(users['rows'], self.config.publication_pool_size)
        def keys_map(inputDict):
            """
            Map function.
            """
            return inputDict['key']

        return map(keys_map, active_users)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

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
from WMCore.WMFactory import WMFactory

result_list = []
current_running = []

def publish(user, config):
    """
    Each worker executes this function.
    """
    worker = PublisherWorker(user, config)
    return worker()

def log_result(result):
    """
    Each worker executes this callback.
    """
    result_list.append(result)
    current_running.remove(result)

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
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.schedAlgoDir, namespace = self.config.schedAlgoDir)
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
        for u in users:
            self.logger.debug('current_running %s' %current_running)
            if u not in current_running:
                self.logger.debug('processing %s' %u)
                current_running.append(u)
                self.logger.debug('processing %s' %current_running)
                self.pool.apply_async(ftscp,(u, self.config), callback = log_result)

    def active_users(self, db):
        """
        Query a view for users with files to transfer. Get this from the
        following view:
            ftscp?group=true&group_level=1
        """
        query = {'group': True, 'group_level': 3, 'stale': 'ok'}
        try:
            users = db.loadView('DBSPublisher', 'publish', query)
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)
            return []

        active_users = []
        if len(users['rows']) <= self.config.pool_size:
            active_users = users['rows']
            def keys_map(inputDict):
                """
                Map function.
                """
                return inputDict['key']
            active_users = map(keys_map, active_users)
        else:
            #TODO: have a plugin algorithm here...
            users = self.factory.loadObject(self.config.algoName, args = [self.config, self.logger, users['rows'], self.config.pool_size], getFromCache = False, listFlag = True)
            active_users = users()
            self.logger.debug("users %s" % active_users)

        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)

        return active_users

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

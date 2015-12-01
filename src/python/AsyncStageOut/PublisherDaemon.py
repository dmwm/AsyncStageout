# pylint: disable=C0103,W0105

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
4. spawn a process per user that publish their files
"""
import logging
from multiprocessing import Pool

from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer

from AsyncStageOut.BaseDaemon import BaseDaemon
from AsyncStageOut.PublisherWorker import PublisherWorker

result_list = []
current_running = []


def publish(user, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the worker")
    try:
        worker = PublisherWorker(user, config)
    except Exception as e:
        logging.debug("Worker cannot be created!: %s", e)
        return user
    if worker.init:
        logging.debug("Starting %s", worker)
        try:
            worker()
        except Exception as e:
            logging.debug("Worker cannot start!: %s", e)
            return user
    return user


def log_result(result):
    """
    Each worker executes this callback.
    """
    result_list.append(result)
    current_running.remove(result)


class PublisherDaemon(BaseDaemon):
    """
    _PublisherDaemon_
    Call multiprocessing library to instantiate a PublisherWorker for each user.
    """

    def __init__(self, config):
        """
        Initialise class members
        """
        # Need a better way to test this without turning off this next line
        BaseDaemon.__init__(self, config, 'DBSPublisher')

        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to CouchDB')
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.schedAlgoDir, namespace=self.config.schedAlgoDir)
        self.pool = Pool(processes=self.config.publication_pool_size)

    def algorithm(self, parameters=None):
        """
        1. Get a list of users with files to publish from the couchdb instance
        2. For each user get a suitably sized input for publish
        3. Submit the publish to a subprocess
        """
        del parameters
        users = self.active_users(self.db)
        self.logger.debug('kicking off pool %s', users)
        for u in users:
            self.logger.debug('current_running %s', current_running)
            if u not in current_running:
                self.logger.debug('processing %s', u)
                current_running.append(u)
                self.logger.debug('processing %s', current_running)
                self.pool.apply_async(publish, (u, self.config), callback=log_result)

    def active_users(self, db):
        """
        Query a view for users with files to transfer. Get this from the
        following view:
            publish?group=true&group_level=1
        """
        # TODO: Remove stale=ok for now until tested
        # query = {'group': True, 'group_level': 3, 'stale': 'ok'}
        query = {'group': True, 'group_level': 3}
        try:
            users = db.loadView('DBSPublisher', 'publish', query)
        except Exception as e:
            self.logger.exception('A problem occured when contacting couchDB: %s', e)
            return []

        active_users = []
        if len(users['rows']) <= self.config.publication_pool_size:
            active_users = users['rows']

            def keys_map(inputDict):
                """
                Map function.
                """
                return inputDict['key']
            active_users = map(keys_map, active_users)
        else:
            sorted_users = self.factory.loadObject(self.config.algoName, args=[self.config, self.logger, users['rows'],
                                                                               self.config.publication_pool_size], getFromCache=False, listFlag=True)
            # active_users = random.sample(users['rows'], self.config.publication_pool_size)
            active_users = sorted_users()[:self.config.publication_pool_size]
        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)

        return active_users

    def terminate(self, parameters=None):
        """
        Called when thread is being terminated.
        """
        del parameters
        self.pool.close()
        self.pool.join()

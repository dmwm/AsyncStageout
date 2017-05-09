#pylint: disable=C0103,W0105,broad-except,logging-not-lazy

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

from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping
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
        logging.debug("Worker cannot be created!:" %e)
        return user
    if worker.init:
        logging.debug("Starting %s" %worker)
        try:
            worker()
        except Exception as e:
            logging.debug("Worker cannot start!:" %e)
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
        #Need a better way to test this without turning off this next line
        BaseDaemon.__init__(self, config, 'DBSPublisher')

        try:
            config_server = CouchServer(dburl=self.config.config_couch_instance)
            self.config_db = config_server.connectDatabase(self.config.config_database)
            self.logger.debug('Connected to config CouchDB')
        except:
            self.logger.exception('Failed when contacting local couch')
            raise
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.schedAlgoDir,
                                 namespace=self.config.schedAlgoDir)
        self.pool = Pool(processes=self.config.publication_pool_size)

        try:
            self.oracleDB = HTTPRequests(self.config.oracleDB,
                                         self.config.opsProxy,
                                         self.config.opsProxy)
            self.logger.debug('Contacting OracleDB:' + self.config.oracleDB)
        except:
            self.logger.exception('Failed when contacting Oracle')
            raise

    def algorithm(self, parameters=None):
        """
        1. Get a list of users with files to publish from the couchdb instance
        2. For each user get a suitably sized input for publish
        3. Submit the publish to a subprocess
        """
        if self.config.isOracle:
	    users = self.active_users(self.oracleDB)
        else:
            users = self.active_users(self.db)
        self.logger.debug('kicking off pool %s' %users)
        for u in users:
            self.logger.debug('current_running %s' %current_running)
            if u not in current_running:
                self.logger.debug('processing %s' %u)
                current_running.append(u)
                self.logger.debug('processing %s' %current_running)
                self.pool.apply_async(publish, (u, self.config),
                                      callback=log_result)

    def active_users(self, db):
        """
        Query a view for users with files to transfer. Get this from the
        following view:
            publish?group=true&group_level=1
        """
        if self.config.isOracle:
            active_users = []

            fileDoc = {}
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'acquirePublication'

            self.logger.debug("Retrieving publications from oracleDB")

            results = ''
            try:
                results = db.post(self.config.oracleFileTrans,
                                  data=encodeRequest(fileDoc))
            except Exception as ex:
                self.logger.error("Failed to acquire publications \
                                  from oracleDB: %s" %ex)
                return []
                
            fileDoc = dict()
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'acquiredPublication'
            fileDoc['grouping'] = 0

            self.logger.debug("Retrieving acquired puclications from oracleDB")

            result = []

            try:
                results = db.get(self.config.oracleFileTrans,
                                 data=encodeRequest(fileDoc))
                result.extend(oracleOutputMapping(results))
            except Exception as ex:
                self.logger.error("Failed to acquire publications \
                                  from oracleDB: %s" %ex)
                return []

            self.logger.debug("%s acquired puclications retrieved" % len(result))
            #TODO: join query for publisher (same of submitter)
            unique_users = [list(i) for i in set(tuple([x['username'], x['user_group'], x['user_role']]) for x in result 
                                                 if x['transfer_state'] == 3)]
            return unique_users
        else:
            # TODO: Remove stale=ok for now until tested
            # query = {'group': True, 'group_level': 3, 'stale': 'ok'}
            query = {'group': True, 'group_level': 3}
            try:
                users = db.loadView('DBSPublisher', 'publish', query)
            except Exception as e:
                self.logger.exception('A problem occured \
                                      when contacting couchDB: %s' % e)
                return []

            if len(users['rows']) <= self.config.publication_pool_size:
                active_users = users['rows']
                active_users = [x['key'] for x in active_users]
            else:
                pool_size=self.config.publication_pool_size
                sorted_users = self.factory.loadObject(self.config.algoName,
                                                       args=[self.config,
                                                             self.logger,
                                                             users['rows'],
                                                             pool_size],
                                                       getFromCache=False,
                                                       listFlag = True)
                active_users = sorted_users()[:self.config.publication_pool_size]
            self.logger.info('%s active users' % len(active_users))
            self.logger.debug('Active users are: %s' % active_users)

            return active_users

    def terminate(self):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

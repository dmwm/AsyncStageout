#pylint: disable=C0103,W0105,W0703,W1201,W0141
"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. get active sites and build up a dictionary of TFC's
4. create a multiprocessing Pool of size N
5. spawn a process per user that
    a. makes rest copyjob
    b. submits to FTS
"""
import os
import logging
from multiprocessing import Pool

from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping

from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC

from AsyncStageOut.BaseDaemon import BaseDaemon
from AsyncStageOut.TransferWorker import TransferWorker
import errno

result_list = []
current_running = []


def ftscp(user, tfc_map, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the worker")
    try:
        worker = TransferWorker(user, tfc_map, config)
    except Exception as e:
        logging.debug("Worker cannot be created!:" %e)
        return user
    logging.debug("Worker created and init %s" % worker.init)
    if worker.init:
        logging.debug("Starting %s" %worker)
        try:
            worker()
        except Exception as e:
            logging.debug("Worker cannot start!:" %e)
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


class TransferDaemon(BaseDaemon):
    """
    _TransferDaemon_
    Call multiprocessing library to instantiate a TransferWorker for each user.
    """
    def __init__(self, config):
        """
        Initialise class members:
            1. check and create dropbox dir
            2. define oracle and couch (config and file instance) server connection
            3. PhEDEx connection
            4. Setup wmcore factory
        """

        self.doc_acq = ''
        # Need a better way to test this without turning off this next line
        BaseDaemon.__init__(self, config, 'AsyncTransfer')

        self.dropbox_dir = '%s/dropbox/outputs' % self.config.componentDir

        if not os.path.isdir(self.dropbox_dir):
            try:
                os.makedirs(self.dropbox_dir)
            except OSError as e:
                if not e.errno == errno.EEXIST:
                    self.logger.exception('Unknown error in mkdir' % e.errno)
                    raise

        if not os.path.isdir("/tmp/DashboardReport"):
            try:
                os.makedirs("/tmp/DashboardReport")
            except OSError as e:
                if not e.errno == errno.EEXIST:
                    self.logger.exception('Unknown error in mkdir' % e.errno)
                    raise

        config_server = CouchServer(dburl=self.config.config_couch_instance)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        if self.config.isOracle:    
            self.oracleDB = HTTPRequests(self.config.oracleDB,
                                         self.config.opsProxy,
                                         self.config.opsProxy)
        else:
            server = CouchServer(dburl=self.config.couch_instance,
                                 ckey=self.config.opsProxy,
                                 cert=self.config.opsProxy)
            self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to CouchDB')
        self.pool = Pool(processes=self.config.pool_size)
        try:
            self.phedex = PhEDEx(responseType='xml',
                                 dict={'key':self.config.opsProxy,
                                       'cert':self.config.opsProxy})
        except Exception as e:
            self.logger.exception('PhEDEx exception: %s' % e)
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.schedAlgoDir,
                                 namespace=self.config.schedAlgoDir)

    # Over riding setup() is optional, and not needed here
    def algorithm(self, parameters=None):
        """
        1  Get transfer config from couchdb config instance 
        2. Get a list of users with files to transfer from the db instance 
                                                    (oracle or couch, by config flag)
        3. For each user get a suitably sized input for submission (call to a list)
        4. Submit to a subprocess
        """

        if self.config.isOracle:
            sites, users = self.oracleSiteUser(self.oracleDB)
        else:
            users = self.active_users(self.db)

            sites = self.active_sites()
            self.logger.info('%s active sites' % len(sites))
            self.logger.debug('Active sites are: %s' % sites)

        site_tfc_map = {}
        for site in sites:
            if site and str(site) != 'None' and str(site) != 'unknown':
                site_tfc_map[site] = self.get_tfc_rules(site)
                self.logger.debug('tfc site: %s %s' % (site, self.get_tfc_rules(site)))
        self.logger.debug('kicking off pool')
        for u in users:
            self.logger.debug('current_running %s' % current_running)
            if u not in current_running:
                self.logger.debug('processing %s' % u)
                current_running.append(u)
                self.logger.debug('processing %s' % current_running)
                self.pool.apply_async(ftscp, (u, site_tfc_map, self.config),
                                      callback=log_result)

    def oracleSiteUser(self, db):
        """
        1. Acquire transfers from DB
        2. Get acquired users and destination sites
        """
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquireTransfers'

        self.logger.debug("Retrieving transfers from oracleDB")

        try:
            result = db.post(self.config.oracleFileTrans,
                             data=encodeRequest(fileDoc))
        except Exception as ex:
            self.logger.error("Failed to acquire transfers \
                              from oracleDB: %s" %ex)
            pass

        self.doc_acq = str(result)

        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquiredTransfers'
        fileDoc['grouping'] = 0

        self.logger.debug("Retrieving users from oracleDB")

        try:
            results = db.get(self.config.oracleFileTrans,
                             data=encodeRequest(fileDoc))
        except Exception:
            self.logger.exception("Failed to get acquired transfers \
                              from oracleDB.")
            results = None
            pass

        documents = oracleOutputMapping(results)
       
        for doc in documents:
            if doc['user_role'] is None:
                doc['user_role'] = ""
            if doc['user_group'] is None:
                doc['user_group'] = ""

        unique_users = []
        try:
            unique_users = [list(i) for i in set(tuple([x['username'],
                                                        x['user_group'],
                                                        x['user_role']]) for x in documents)]
        except Exception as ex:
            self.logger.error("Failed to map active users: %s" %ex) 

        if len(unique_users) <= self.config.pool_size:
            active_users = unique_users
        else:  
            active_users = unique_users[:self.config.pool_size]

        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)

        active_sites_dest = [x['destination'] for x in documents]
        active_sites = active_sites_dest + [x['source'] for x in documents]

        self.logger.debug('Active sites are: %s' % list(set(active_sites)))
        return list(set(active_sites)), active_users

    def active_users(self, db):
        """
        Query a view for users with files to transfer.
        get this from the following view:
              ftscp?group=true&group_level=1
        """
        query = {'group': True, 'group_level': 3}
        try:
            users = db.loadView(self.config.ftscp_design, 'ftscp_all', query)
        except Exception as e:
            self.logger.exception('A problem occured when\
                                  contacting couchDB: %s' % e)
            return []

        if len(users['rows']) <= self.config.pool_size:
            active_users = [x['key'] for x in users['rows']]
        else:
            sorted_users = self.factory.loadObject(self.config.algoName,
                                                   args=[self.config,
                                                         self.logger,
                                                         users['rows'],
                                                         self.config.pool_size],
                                                   getFromCache=False,
                                                   listFlag=True)
            active_users = sorted_users()[:self.config.pool_size]
        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)
        return active_users

    def  active_sites(self):
        """
        Get a list of all sites involved in transfers.
        """
        query = {'group': True, 'stale': 'ok'}
        try:
            sites = self.db.loadView('AsyncTransfer', 'sites', query)
        except Exception as e:
            self.logger.exception('A problem occured \
                                  when contacting couchDB: %s' % e)
            return []

        def keys_map(inputDict):
            """
            Map function.
            """
            return inputDict['key']

        return map(keys_map, sites['rows'])

    def get_tfc_rules(self, site):
        """
        Get the TFC regexp for a given site.
        """
        tfc_file = None
        try:
            self.phedex.getNodeTFC(site)
        except Exception as e:
            self.logger.exception('PhEDEx exception: %s' % e)
        try:
            tfc_file = self.phedex.cacheFileName('tfc',
                                                 inputdata={'node': site})
        except Exception as e:
            self.logger.exception('PhEDEx cache exception: %s' % e)
        return readTFC(tfc_file)

    def terminate(self, parameters=None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

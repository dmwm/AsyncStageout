#pylint: disable=C0103,W0105

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. get active sites and build up a dictionary of TFC's
4. create a multiprocessing Pool of size N
5. spawn a process per user that
    a. makes the ftscp copyjob
    b. submits ftscp
    c. deletes successfully transferred files
"""
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from AsyncStageOut.TransferWorker import TransferWorker
from multiprocessing import Pool
from WMCore.WMFactory import WMFactory
#import random
import logging
#import time
import os

result_list = []
current_running = []

def ftscp(user, tfc_map, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the worker")
    try:
        worker = TransferWorker(user, tfc_map, config)
    except Exception, e:
        logging.debug("Worker cannot be created!:" %e)
        return user
    logging.debug("Worker created and init %s" % worker.init)
    if worker.init:
       logging.debug("Starting %s" %worker)
       try:
           worker ()
       except Exception, e:
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

class TransferDaemon(BaseWorkerThread):
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

        self.logger.debug('Configuration loaded')
        self.dropbox_dir = '%s/dropbox/outputs' % self.config.componentDir
        if not os.path.isdir(self.dropbox_dir):
            try:
                os.makedirs(self.dropbox_dir)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    self.logger.error('Unknown error in mkdir' % e.errno)
                    raise
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        config_server = CouchServer(dburl=self.config.config_couch_instance)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.logger.debug('Connected to CouchDB')
        self.pool = Pool(processes=self.config.pool_size)
        try:
            self.phedex = PhEDEx(responseType='xml', dict = {'key': self.config.opsProxy, 'cert': self.config.opsProxy})
        except Exception, e:
            self.logger.exception('PhEDEx exception: %s' % e)
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.schedAlgoDir, namespace = self.config.schedAlgoDir)

        result_list = []
        current_running = []

    # Over riding setup() is optional, and not needed here
    def algorithm(self, parameters = None):
        """
        1. Get a list of users with files to transfer from the couchdb instance
        2. For each user get a suitably sized input for ftscp (call to a list)
        3. Submit the ftscp to a subprocess
        """
        query = {'stale':'ok'}
        try:
            params = self.config_db.loadView('asynctransfer_config', 'GetTransferConfig', query)
            self.config.max_files_per_transfer = params['rows'][0]['key'][1]
            self.config.algoName = params['rows'][0]['key'][2]
        except IndexError:
            self.logger.exception('Config data could not be retrieved from the config database. Fallback to the config file')
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)

        users = self.active_users(self.db)

        sites = self.active_sites()
        self.logger.info('%s active sites' % len(sites))
        self.logger.debug('Active sites are: %s' % sites)

        site_tfc_map = {}
        for site in sites:
            # TODO: Remove this check once the ASO request will be validated before the upload.
            if site and str(site) != 'None' and str(site) != 'unknown':
                site_tfc_map[site] = self.get_tfc_rules(site)
        self.logger.debug('kicking off pool')
        for u in users:
            self.logger.debug('current_running %s' %current_running)
            if u not in current_running:
                self.logger.debug('processing %s' %u)
                current_running.append(u)
                self.logger.debug('processing %s' %current_running)
                self.pool.apply_async(ftscp,(u, site_tfc_map, self.config), callback = log_result)

    def active_users(self, db):
        """
        Query a view for users with files to transfer. Get this from the
        following view:
            ftscp?group=true&group_level=1
        """
        #TODO: Remove stale=ok for now until tested
        #query = {'group': True, 'group_level': 3, 'stale': 'ok'}
        query = {'group': True, 'group_level': 3}
        try:
            users = db.loadView('AsyncTransfer', 'ftscp_all', query)
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
            sorted_users = self.factory.loadObject(self.config.algoName, args = [self.config, self.logger, users['rows'], self.config.pool_size], getFromCache = False, listFlag = True)
            #active_users = random.sample(users['rows'], self.config.pool_size)
            active_users = sorted_users()[:self.config.pool_size]
        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)
        return active_users

    def active_sites(self):
        """
        Get a list of all sites involved in transfers.
        """
        query = {'group': True, 'stale': 'ok'}
        try:
            sites = self.db.loadView('AsyncTransfer', 'sites', query)
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)
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
        except Exception, e:
            self.logger.exception('PhEDEx exception: %s' % e)
        try:
            tfc_file = self.phedex.cacheFileName('tfc', inputdata={'node': site})
        except Exception, e:
            self.logger.exception('PhEDEx cache exception: %s' % e)
        return readTFC(tfc_file)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

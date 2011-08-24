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

from WMCore.Configuration import loadConfigurationFile
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from AsyncStageOut.TransferWorker import TransferWorker
import time, datetime
import subprocess, os, errno
from multiprocessing import Pool

import random

def ftscp(user, tfc_map, config):
    """
    Each worker executes this function.
    """
    worker = TransferWorker(user, tfc_map, config)
    return worker()

def execute_command(command):
    """
    _execute_command_
    Function to manage commands.
    """
    proc = subprocess.Popen(
           ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
           stdout=subprocess.PIPE,
           stderr=subprocess.PIPE,
           stdin=subprocess.PIPE,
    )
    proc.stdin.write(command)
    stdout, stderr = proc.communicate()
    rc = proc.returncode

    return stdout, stderr, rc


class TransferDaemon(BaseWorkerThread):
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

        self.logger.info('Archiving logs')

        # Archiving logs
        log_dir = '%s/logs' % self.config.componentDir
        if os.path.exists(log_dir):

            archive_dir = '%s/archive/%s/%s/%s' % ( self.config.componentDir, \
str(datetime.datetime.now().year), str(datetime.datetime.now().month), str(datetime.datetime.now().day) )
            try:
                os.makedirs(archive_dir)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else: raise

            command = 'tar -czf %s/logs_%s.tar.gz %s/* ; rm -rf %s/*' % ( archive_dir, str(time.time()), log_dir, log_dir )

            self.logger.debug( "tarring logs using %s" %command )
            out, error, retcode = execute_command(command)

            if retcode != 0 :
                msg = "Error when archiving %s : %s"\
                       % (log_dir, error)
                raise Exception(msg)

        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to CouchDB')
        self.pool = Pool(processes=self.config.pool_size)

        self.phedex = PhEDEx(responseType='xml')

    # Over riding setup() is optional, and not needed here

    def algorithm(self, parameters = None):
        """

        1. Get a list of users with files to transfer from the couchdb instance
        2. For each user get a suitably sized input for ftscp (call to a list)
        3. Submit the ftscp to a subprocess

        """
        users = self.active_users(self.db)

        sites = self.active_sites()
        self.logger.info('%s active sites' % len(sites))
        self.logger.debug('Active sites are: %s' % sites)

        site_tfc_map = {}
        for site in sites:
            site_tfc_map[site] = self.get_tfc_rules(site)

        self.logger.debug('kicking off pool')
        r = [self.pool.apply_async(ftscp, (u, site_tfc_map, self.config)) for u in users]
        for result in r:
            self.logger.info(result.get())

    def active_users(self, db):
        """
        Query a view for users with files to transfer. Get this from the
        following view:
            ftscp?group=true&group_level=1
        """
        query = {'group': True, 'group_level':1}
        users = db.loadView('AsyncTransfer', 'ftscp', query)

        active_users = []
        if len(users['rows']) <= self.config.pool_size:
            active_users = users['rows']
        else:
            #TODO: have a plugin algorithm here...
            active_users = random.sample(users['rows'], self.config.pool_size)

        def keys_map(inputDict):
            """
            Map function.
            """
            return inputDict['key'][0]

        return map(keys_map, active_users)

    def active_sites(self):
        """
        Get a list of all sites involved in transfers.
        """
        query = {'group': True}
        sites = self.db.loadView('AsyncTransfer', 'sites', query)

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
        self.phedex.getNodeTFC(site)
        tfc_file = self.phedex.cacheFileName('tfc', inputdata={'node': site})

        return readTFC(tfc_file)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()

if __name__ == '__main__':
    """
    Something temporary while I write the unit tests...
    """
    import sys
    cfg_file = sys.path[0].replace('AsyncTransfer', 'DefaultConfig.py')
    config = loadConfigurationFile(cfg_file)

    d = TransferDaemon(config)
    #while True:
    d.algorithm([])

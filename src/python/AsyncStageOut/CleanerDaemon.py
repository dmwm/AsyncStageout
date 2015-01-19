#!/usr/bin/env
#pylint: disable-msg=W0613,C0103
"""
It populates user_monitoring_db database
with the details of users transfers from
files_database.
"""
import time, datetime
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
import subprocess, os, errno
import time
import traceback
from AsyncStageOut import getDNFromUserName
from AsyncStageOut import getProxy

def execute_command( command, logger, timeout ):
    """
    _execute_command_
    Funtion to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
    )
    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return rc, stdout, stderr

        time.sleep(0.1)
    stdout, stderr = proc.communicate()
    rc = proc.returncode
    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))
    return rc, stdout, stderr

class CleanerDaemon(BaseWorkerThread):
    """
    _FilesCleaner_
    Clean transferred outputs from /store/temp.
    """
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config.FilesCleaner
        self.logger.debug('Configuration loaded')

        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)
        self.logger.debug('Configuration loaded')
        config_server = CouchServer(dburl=self.config.config_couch_instance)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.logger.debug('Connected to files DB')
        self.phedex = PhEDEx(responseType='xml')
        self.log_dir = '%s/logs/%s/%s/%s' % ( self.config.componentDir, \
 str(datetime.datetime.now().month), str(datetime.datetime.now().year), "Ops")
        try:
            os.makedirs(self.log_dir)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                self.logger.error('Unknown error in mkdir' % e.errno)
                raise
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.opsProxy = self.config.opsProxy
        self.site_tfc_map = {}
        self.defaultDelegation = { 'logger': self.logger,
                                   'credServerPath' : \
                                   self.config.credentialDir,
                                   # It will be moved to be getfrom couchDB
                                   'myProxySvr': 'myproxy.cern.ch',
                                   'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                   'serverDN' : self.config.serverDN,
                                   'uisource' : self.uiSetupScript,
                                   'cleanEnvironment' : getattr(self.config, 'cleanEnvironment', False)
                                 }
        os.environ['X509_USER_PROXY'] = self.opsProxy
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        try:
            self.db = server.connectDatabase(self.config.files_database)
        except Exception, e:
            self.logger.exception('A problem occured when connecting to couchDB: %s' % e)

    def algorithm(self, parameters = None):
        """
        a. Get the lastCleaningTime from files_db
        b. Get the lastDBMonitoringUpdate from the monitoring database
        c. Call the view LastUpdatePerLFN to remove only the outputs of files updated between a. and b.
        d. Update the lastCleaningTime in files_db
        """
        sites = self.active_sites()
        self.logger.info('%s active sites' % len(sites))
        self.logger.debug('Active sites are: %s' % sites)
        for site in sites:
            if str(site) != 'None' and str(site)!= 'unknown':
                self.site_tfc_map[site] = self.get_tfc_rules(site)
        if sites:
            query = {}
            try:
                since = self.config_db.loadView('asynctransfer_config', 'lastFilesCleaningTime', query)['rows'][0]['key']
            except IndexError:
                self.logger.debug('No records to determine last cleanning time, waiting for next iteration')
                return
            except KeyError:
                self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
                return
            except Exception, e:
                self.logger.exception('A problem occured when contacting couchDB to determine last cleanning time: %s' % e)
                return

            end_time = time.time()
            query = { 'startkey': since, 'endkey': end_time, 'stale': 'ok'}
            try:
                all_LFNs = self.db.loadView('AsyncTransfer', 'LFNSiteByLastUpdate', query)['rows']
            except Exception, e:
                self.logger.exception('A problem occured when contacting couchDB to retrieve LFNs: %s' % e)
                return

            updateUri = "/" + self.config_db.name + "/_design/asynctransfer_config/_update/lastCleaningTime/LAST_CLEANING_TIME"
            updateUri += "?last_cleaning_time=%d" % end_time
            try:
                self.config_db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception, e:
                self.logger.exception('A problem occured when contacting couchDB to update last cleanning time: %s' % e)
                return

            self.logger.info('LFNs to remove: %s' % len(all_LFNs))
            self.logger.debug('LFNs to remove: %s' % all_LFNs)

            lfn_to_proxy = {}
            for lfnDetails in all_LFNs:
                lfn = lfnDetails['value']['lfn']
                user = lfn.split('/')[4].split('.')[0]
                if not lfn_to_proxy.has_key(user):
                    valid_proxy = False
                    try:
                        userDN = getDNFromUserName(user, self.logger, ckey = self.config.opsProxy, cert = self.config.opsProxy)
                        valid_proxy, userProxy = getProxy(userDN, "", "", self.defaultDelegation, self.logger)
                    except Exception, ex:
                        msg = "Error getting the user proxy"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                    if valid_proxy:
                        lfn_to_proxy[user] = userProxy
                    else:
                        lfn_to_proxy[user] = self.opsProxy
                userProxy = lfn_to_proxy[user]
                location = lfnDetails['value']['location']
                self.logger.info("Removing %s from %s" %(lfn, location))
                pfn = self.apply_tfc_to_lfn( '%s:%s' %(location, lfn))
                if pfn:
                    command = 'env -i X509_USER_PROXY=%s gfal-rm -v -t 180 %s'  % \
                              (userProxy, pfn)
                    self.logger.debug("Running remove command %s" % command)
                    rc, stdout, stderr = execute_command(command, self.logger, 3600)
                    if rc:
                        self.logger.info("Deletion command failed with output %s and error %s" %(stdout, stderr))
                    else:
                        self.logger.info("File Deleted.")
                else:
                    self.logger.info("Removing %s from %s failed when retrieving the PFN" %(lfn, location))
        return

    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        try:
            site, lfn = tuple(file.split(':'))
        except Exception, e:
            self.logger.error('It does not seem to be an lfn %s' %file.split(':'))
            return None
        if self.site_tfc_map.has_key(site):
            pfn = self.site_tfc_map[site].matchLFN('srmv2', lfn)
            # TODO: improve fix for wrong tfc on sites
            try:
                if pfn.find("\\") != -1: pfn = pfn.replace("\\","")
                if pfn.split(':')[0] != 'srm' and pfn.split(':')[0] != 'gsiftp' :
                    self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                    return None
            except IndexError:
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
            except AttributeError:
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
            return pfn
        else:
            self.logger.error('Wrong site %s!' % site)
            return None

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
        self.phedex.getNodeTFC(site)
        try:
            tfc_file = self.phedex.cacheFileName('tfc', inputdata={'node': site})
        except Exception, e:
            self.logger.exception('A problem occured when getting the TFC regexp: %s' % e)
            return None
        return readTFC(tfc_file)

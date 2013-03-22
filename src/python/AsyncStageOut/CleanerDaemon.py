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
        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to local couchDB')
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
        monitoring_server = CouchServer(self.config.couch_user_monitoring_instance)
        self.monitoring_db = monitoring_server.connectDatabase(self.config.user_monitoring_db)
        self.logger.debug('Connected to user_monitoring_db in couchDB')
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.opsProxy = self.config.opsProxy
        sites = self.active_sites()
        self.logger.info('%s active sites' % len(sites))
        self.logger.debug('Active sites are: %s' % sites)
        self.site_tfc_map = {}
        sites = self.active_sites()
        for site in sites:
            self.site_tfc_map[site] = self.get_tfc_rules(site)

    def algorithm(self, parameters = None):
        """
        a. Get the lastCleaningTime from files_db
        b. Get the lastDBMonitoringUpdate from the monitoring database
        c. Call the view LastUpdatePerLFN to remove only the outputs of files updated between a. and b.
        d. Update the lastCleaningTime in files_db
        """
        try:
            query = {}
            since = self.db.loadView('AsyncTransfer', 'lastFilesCleaningTime', query)['rows'][0]['key']
            end_time = self.monitoring_db.loadView('UserMonitoring', 'LastSummariesUpdate', query)['rows'][0]['key']
        except IndexError:
            self.logger.debug('No records to determine end time, waiting for next iteration')
            return
        except KeyError:
            self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
            return
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)
            return
        updateUri = "/" + self.db.name + "/_design/asynctransfer_config/_update/lastCleaningTime/LAST_CLEANING_TIME"
        updateUri += "?last_cleaning_time=%d" % (end_time + 1)
        self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
        query = { 'startkey': since, 'endkey': end_time + 1 }
        all_LFNs = self.db.loadView('AsyncTransfer', 'LFNSiteByLastUpdate', query)['rows']
        self.logger.debug('LFNs to remove: %s' %all_LFNs)
        for lfnDetails in all_LFNs:
            lfn = lfnDetails['value']['lfn']
            location = lfnDetails['value']['location']
            pfn = self.apply_tfc_to_lfn( '%s:%s' %( location, lfn ) )
            logfile = open('%s/%s_%s.lcg-del.log' % ( self.log_dir, location, str(time.time()) ), 'w')
            command = 'export X509_USER_PROXY=%s ; source %s ; lcg-del -l %s'  % \
                      (self.opsProxy, self.uiSetupScript, pfn)
            self.logger.debug("Running remove command %s" % command)
            self.logger.debug("log file: %s" % logfile.name)
            proc = subprocess.Popen(
                    ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                    stdout=logfile,
                    stderr=logfile,
                    stdin=subprocess.PIPE,
                    )
            proc.stdin.write(command)
            stdout, stderr = proc.communicate()
            rc = proc.returncode
            logfile.close()

        return

    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        site, lfn = tuple(file.split(':'))
        pfn = self.site_tfc_map[site].matchLFN('srmv2', lfn)

        #TODO: improve fix for wrong tfc on sites
        try:
            if pfn.split(':')[0] != 'srm':
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
        except IndexError:
            self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None
        except AttributeError:
            self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None

        return pfn

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


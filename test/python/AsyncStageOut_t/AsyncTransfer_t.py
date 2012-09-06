#!/usr/bin/env python
#pylint: disable=C0103,E1103
"""
AsyncTransfer test
It requires X509_USER_PROXY, COUCHURL vars, and ftscp command.
"""

import os
import logging
import unittest
import time
import threading
import subprocess
import urllib

from AsyncStageOut.TransferDaemon import TransferDaemon
from AsyncStageOut.PublisherDaemon import PublisherDaemon
from AsyncStageOut.LFNSourceDuplicator import LFNSourceDuplicator
from AsyncStageOut.TransferWorker import TransferWorker
from AsyncStageOut.AnalyticsDaemon import AnalyticsDaemon
from AsyncStageOut.StatisticDaemon import StatisticDaemon
from WMCore.Configuration import loadConfigurationFile
from WMCore.Database.CMSCouch import CouchServer
from WMQuality.TestInitCouchApp import CouchAppTestHarness
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.WMInit import getWMBASE
from WMCore.Storage.TrivialFileCatalog import readTFC
from WMQuality.TestInitCouchApp import TestInitCouchApp

from AsyncStageOut_t.fakeDaemon import fakeDaemon
from AsyncStageOut_t.AsyncTransferTest import AsyncTransferTest
from AsyncStageOut import getHashLfn
import datetime
import hashlib

import random

def apply_tfc(site_file, site_tfc_map, site):
    """
    Take a CMS_NAME:lfn string and make a pfn
    """
    site_tfc_map[site] = get_tfc_rules(site)
    site, lfn = tuple(site_file.split(':'))

    return site_tfc_map[site].matchLFN('srmv2', lfn)

def get_tfc_rules(site):
    """
    Get the TFC regexp for a given site.
    """
    phedex = PhEDEx(responseType='xml')
    phedex.getNodeTFC(site)
    tfc_file = phedex.cacheFileName('tfc', inputdata={'node': site})

    return readTFC(tfc_file)

class AsyncTransfer_t(unittest.TestCase):
    """
    TestCase for TestAsyncTransfer module
    """

    def setUp(self):
        """
        setup for test.
        """
        self.myThread = threading.currentThread()
        self.myThread.dbFactory = None
        self.myThread.logger = None
        self.database_interface = None
        if hasattr(self.myThread, 'dbi'):
            self.database_interface = self.myThread.dbi
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.config = self.getConfig()
        self.testInit.setupCouch("wmagent_jobdump/fwjrs", "FWJRDump")
        self.testInit.setupCouch("agent_database", "Agent")
        couchapps = "../../../src/couchapp"
        self.async_couchapp = "%s/AsyncTransfer" % couchapps
        self.publication_couchapp = "%s/DBSPublisher" % couchapps
        self.monitor_couchapp = "%s/monitor" % couchapps
        self.user_monitoring_couchapp = "%s/UserMonitoring" % couchapps
        self.stat_couchapp = "%s/stat" % couchapps
        harness = CouchAppTestHarness("asynctransfer")
        harness.create()
        harness.pushCouchapps(self.async_couchapp)
        harness.pushCouchapps(self.publication_couchapp)
        harness.pushCouchapps(self.monitor_couchapp)
        harness_user_mon = CouchAppTestHarness("user_monitoring_asynctransfer")
        harness_user_mon.create()
        harness_user_mon.pushCouchapps(self.user_monitoring_couchapp)
        harness_stat = CouchAppTestHarness("asynctransfer_stat")
        harness_stat.create()
        harness_stat.pushCouchapps(self.stat_couchapp)

        # Connect to db
        self.async_server = CouchServer( os.getenv("COUCHURL") )
        self.db = self.async_server.connectDatabase( "asynctransfer" )
        self.monitoring_db = self.async_server.connectDatabase( "user_monitoring_asynctransfer" )
        self.dbStat = self.async_server.connectDatabase( "asynctransfer_stat" )
        self.dbSource = self.async_server.connectDatabase( "wmagent_jobdump/fwjrs" )
        doc = {"_id": "T1_IT_INFN",
               "state": "running",
               "countries": [
                   "IT",
                   "AT",
                   "HU",
                   "PL"
               ],
               "url": "https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer",
               "couchapp": {
               }
            }
        self.db.queue(doc, True)
        self.db.commit()
        doc = {
           "_id": "MONITORING_DB_UPDATE",
           "db_update": 1,
           "couchapp": {
           }
        }
        self.monitoring_db.queue(doc, True)
        self.monitoring_db.commit()
        self.config = self.getConfig()
        self.testConfig = self.getTestConfig()
        self.users = ['fred', 'barney', 'wilma', 'betty']
        self.sites = ['T2_IT_Pisa', 'T2_IT_Rome', 'T2_IT_Bari']
        self.lfn = ['/this/is/a/lfnA', '/this/is/a/lfnB', '/this/is/a/lfnC', '/this/is/a/lfnD', '/this/is/a/lfnE']

        return

    def tearDown(self):
        """
        Delete database
        """
        self.testInit.tearDownCouch(  )
        if  self.database_interface:
            self.myThread.dbi = self.database_interface
        self.async_server.deleteDatabase( "asynctransfer" )
        self.async_server.deleteDatabase( "asynctransfer_stat" )
        self.async_server.deleteDatabase( "user_monitoring_asynctransfer" )


    def getConfig(self):
        """
        _createConfig_
        General config file
        """
        config = loadConfigurationFile('../../../configuration/Example.py')
        config.CoreDatabase.connectUrl = os.getenv("COUCHURL") + "/agent_database"
        config.AsyncTransfer.couch_instance = os.getenv("COUCHURL")
        config.AsyncTransfer.couch_statinstance = os.getenv("COUCHURL")
        config.AsyncTransfer.couch_user_monitoring_instance = os.getenv("COUCHURL")
        config.AsyncTransfer.files_database = "asynctransfer"
        config.AsyncTransfer.statitics_database = "asynctransfer_stat"
        config.AsyncTransfer.user_monitoring_db = "user_monitoring_asynctransfer"
        config.AsyncTransfer.data_source = os.getenv("COUCHURL")
        config.AsyncTransfer.couch_statinstance = os.getenv("COUCHURL")
        config.AsyncTransfer.db_source = "wmagent_jobdump/fwjrs"
        config.AsyncTransfer.log_level = logging.DEBUG
        config.AsyncTransfer.pluginName = "JSM"
        config.AsyncTransfer.max_retry = 2
        config.AsyncTransfer.expiration_days = 1
        config.AsyncTransfer.pluginDir = "AsyncStageOut.Plugins"
        config.AsyncTransfer.serviceCert = os.getenv('X509_USER_PROXY')
        config.AsyncTransfer.componentDir = self.testInit.generateWorkDir(config)
        config.DBSPublisher.pollInterval = 10
        config.DBSPublisher.publication_pool_size = 1
        config.DBSPublisher.componentDir = self.testInit.generateWorkDir(config)
        config.DBSPublisher.namespace = 'AsyncStageOut.DBSPublisher'
        config.DBSPublisher.log_level = logging.DEBUG
        config.DBSPublisher.files_database = "asynctransfer_1"
        config.DBSPublisher.couch_instance = os.getenv("COUCHURL")
        config.DBSPublisher.publication_max_retry = 0
        config.DBSPublisher.serviceCert = os.getenv('X509_USER_PROXY')
        config.DBSPublisher.max_files_per_block = 100
        config.DBSPublisher.workflow_expiration_time = 3

        return config

    def getTestConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration( connectUrl = os.getenv("COUCHURL") + "/agent_database" )
        config.component_("AsyncTransferTest")
        config.AsyncTransferTest.couch_instance = os.getenv("COUCHURL")
        config.AsyncTransferTest.files_database = "asynctransfer"
        config.AsyncTransferTest.data_source = os.getenv("COUCHURL")
        config.AsyncTransferTest.db_source = "wmagent_jobdump/fwjrs"
        config.AsyncTransferTest.log_level = logging.DEBUG
        config.AsyncTransferTest.pluginName = "JSM"
        config.AsyncTransferTest.pluginDir = "AsyncStageOut.Plugins"
        config.AsyncTransferTest.max_files_per_transfer = 10
        config.AsyncTransferTest.pool_size = 3
        config.AsyncTransferTest.max_retry = 2
        config.AsyncTransferTest.max_retry = 1000
        config.AsyncTransferTest.pollInterval = 10
        config.AsyncTransferTest.serviceCert = os.getenv('X509_USER_PROXY')
        config.AsyncTransferTest.componentDir = self.testInit.generateWorkDir(config)

        return config

    def createTestDocinFilesDB(self, site = None):
        """
        Creates a test document in files_db

        """
        doc = {}
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['jobid'] = '1'
        doc['retry_count'] = []
        doc['source'] = random.choice(self.sites)
        if not site:
            doc['destination'] = random.choice(self.sites)
        else:
            doc['destination'] = site
        doc['user'] = random.choice(self.users)
        doc['group'] = 'someGroup'
        doc['role'] = 'someRole'
        doc['lfn'] = '/store/user/riahi/TestUnit'
        doc['state'] = 'new'
        doc['workflow'] = 'someWorkflow'
        doc['checksums'] = 'someChecksums'
        doc['start_time'] = str(datetime.datetime.now())
        doc['end_time'] = str(datetime.datetime.now())
        doc['job_end_time'] = str(time.time())
        doc['dbSource_url'] = 'someUrl'
        self.db.queue(doc, True)
        self.db.commit()

        return doc

    def createFileDocinFilesDB(self, doc_id = '', state = 'new', publication_state = 'not_published'):
        """
        Creates a test document in files_db
        """
        doc = {}
        lfn = random.choice(self.lfn) + doc_id
        doc['_id'] = getHashLfn(lfn)
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['workflow'] = 'someWorkflow'
        doc['jobid'] = '1'
        doc['lfn'] = lfn
        doc['retry_count'] = []
        doc['source'] = random.choice(self.sites)
        doc['destination'] = random.choice(self.sites)
        doc['user'] = random.choice(self.users)
        doc['group'] = 'someGroup'
        doc['role'] = 'someRole'
        doc['state'] = state
        doc['checksums'] = 'someChecksums'
        doc['start_time'] = str(datetime.datetime.now())
        doc['end_time'] = str(datetime.datetime.now())
        doc['dbSource_url'] = 'someUrl'
        doc['size'] = 1000
        doc['end_time'] = 10000
        doc['last_update'] = 10000
        doc['job_end_time'] = 10000
        doc['publication_state'] = publication_state
        doc['publication_retry_count'] = []
        doc['publish_dbs_url'] = 'https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet'
        doc['inputdataset'] = '/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO'
        doc['dbs_url'] = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
        self.db.queue(doc, True)
        self.db.commit()

        return doc

    def createTestFileFinishedYesterdayinFilesDB( self ):
        """
        Creates a test document in files_db

        """
        doc = {}
        doc['_id'] = getHashLfn("/this/is/a/lfnA")
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['workflow'] = 'someWorkflow'
        doc['size'] = 999999
        doc['jobid'] = '1'
        doc['lfn'] = '/this/is/a/lfnA'
        doc['retry_count'] = []
        doc['source'] = random.choice(self.sites)
        doc['destination'] = random.choice(self.sites)
        doc['user'] = random.choice(self.users)
        doc['state'] = 'done'
        doc['start_time'] = str(datetime.datetime.now()).\
replace(str(datetime.datetime.now()).split(" ")[0].split("-")[2], \
str(int(str(datetime.datetime.now()).split(" ")[0].split("-")[2]) - 3))
        doc['end_time'] = str(datetime.datetime.now()).\
replace(str(datetime.datetime.now()).split(" ")[0].split("-")[2], \
str(int(str(datetime.datetime.now()).split(" ")[0].split("-")[2]) - 2))
        doc['job_end_time'] = str(time.time())
        doc['dbSource_url'] = 'someUrl'
        self.db.queue(doc, True)
        self.db.commit()

        return doc

    def DeleteTestDocinFilesDB(self, doc):
        """
        Remove the test documents in files_db
        """
        document = self.db.document( doc )
        self.db.queueDelete(document)
        self.db.commit()

        return

    def DeleteTestDocinDBStat(self, doc):
        """
        Remove the test documents from statdb
        """
        document = self.dbStat.document( doc )
        self.db.queueDelete(document)
        self.db.commit()

        return

    def createTestDocinDBSource(self, doc_id = ''):
        """
        Creates a JSM document
        """
        doc = {\
   "timestamp": time.time(),\
   "jobid": 7,\
   "retrycount": 0,\
   "fwjr": {\
       "task": "/CmsRunAnalysis-22_121713/Analysis",\
       "steps": {\
           "logArch1": {\
               "status": 0,\
               "logs": {\
               },\
               "stop": 1290425610,\
               "site": {\
               },\
               "input": {\
               },\
               "errors": [\
               ],\
               "parameters": {\
               },\
               "analysis": {\
               },\
               "start": 1290425601,\
               "cleanup": {\
               },\
               "output": {\
                   "logArchive": [\
                       {\
                           "runs": {\
                           },\
"lfn": "/store/user/riahi/lfnB"+doc_id,\
"pfn": "srm://this/is/a/pfnB",\
                           "module_label": "logArchive",\
                           "location": "T2_IT_Bari",\
                           "events": 0,\
                           "size": 0\
                       }\
                   ]\
               }\
           },\
           "cmsRun1": {\
               "status": 0,\
               "logs": {\
               },\
               "stop": 1290425590,\
               "site": {\
               },\
               "input": {\
                   "source": [\
                       {\
                           "runs": {\
                               "1": [\
                                   39,\
                                   60,\
                                   73,\
                                   78,\
                                   80,\
                                   112\
                               ]\
                           },\
                           "input_source_class": "PoolSource",\
                           "input_type": "primaryFiles",\
                           "lfn": "/store/user/riahi/lfnB"+doc_id,\
                           "pfn": "file:/this/is/a/pfnB",\
                           "module_label": "source",\
                           "guid": "D005BB56-CA2B-DF11-BA08-0030487C60AE",\
                           "events": 600\
                       }\
                   ]\
               },\
               "errors": [\
               ],\
               "parameters": {\
               },\
               "analysis": {\
               },\
               "start": 1290425561,\
               "cleanup": {\
               },\
               "output": {\
                   "output": [\
                       {\
                           "branch_hash": "8dbc25d29c96c171aa2700e3c3249274",\
                           "user_dn": "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi",\
                           "lfn": "/store/temp/user/riahi/lfnB"+doc_id,\
                           "dataset": {\
                               "applicationName": "cmsRun",\
                               "applicationVersion": "CMSSW_3_6_1_patch7",\
                               "dataTier": "USER",\
                           },\
"InputPFN": "/home/cmsint/globus-tmp.fermi11.13080.0/\
https_3a_2f_2fcert-rb-01.cnaf.infn.it_3a9000_2frYxxZQTCljEAG6Q6-QgmhQ/job/\
WMTaskSpace/cmsRun1/output.root",\
                           "checksums": {\
                               "adler32": "fc729f97",\
                               "cksum": "2529793973"\
                           },\
                           "guid": "5CD0D341-2CF6-DF11-9A92-0030487DA364",\
                           "size": 16621775,\
                           "location": "T2_IT_Bari",\
                           "async_dest": "T2_IT_Pisa",\
                           "events": 600,\
                           "ouput_module_class": "PoolOutputModule",\
                           "pfn": "/this/is/a/pfn",\
                           "catalog": "",\
                           "module_label": "output",\
                           "input": [\
"/store/mc/JobRobot/RelValProdTTbar/GEN-SIM-DIGI-RECO/MC_3XY_V24_JobRobot-v1/0000/D005BB56-CA2B-DF11-BA08-0030487C60AE.root"\
                           ],\
                           "StageOutCommand": "srmv2-lcg",\
                           "runs": {\
                               "1": [\
                                   39,\
                                   60,\
                                   73,\
                                   78,\
                                   80,\
                                   112\
                               ]\
                           },\
"OutputPFN": "storm-se-01.ba.infn.it:8444/srm/managerv2?SFN=/cms/store/user/grandi/22_121713/0000/5CD0D341-2CF6-DF11-9A92-0030487DA364.root"\
                       }\
                   ]\
               }\
           },\
           "stageOut1": {\
               "status": 0,\
               "logs": {\
               },\
               "stop": 1290425601,\
               "site": {\
               },\
               "input": {\
               },\
               "errors": [\
               ],\
               "parameters": {\
               },\
               "analysis": {\
               },\
               "start": 1290425591,\
               "cleanup": {\
               },\
               "output": {\
               }\
           }\
       }\
   },\
   "type": "fwjr"\
}


        self.dbSource.queue(doc)
        self.dbSource.commit()

        return doc

    def DeleteTestDocinDBSource(self, doc):
        """
        Deletes test docs from DB source
        """
        document = self.dbSource.document( doc )
        self.dbSource.queueDelete( document )
        self.dbSource.commit()

        return

    def testA_BasicTest_testLoadFtscpView(self):
        """
       _BasicFunctionTest_
        Tests the components, by seeing if the AsyncTransfer view load correctly files from couch.
        """
        doc = self.createTestDocinFilesDB()
        query = {'reduce':False,
         'key':[doc['user'], doc['group'], doc['role'], doc['destination'], doc['source'], doc['dn']]}
        active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']
        assert len(active_files) == 1
        for i in range(1, 5):
            self.createTestDocinFilesDB()
        query = {'reduce':False}
        all_active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']

        assert len(all_active_files) == 5

    def testA_BasicTest_testFileTransfer(self):
        """
        _BasicFunctionTest_
        Tests the components, by seeing if they can process documents.
        """
        self.createFileDocinFilesDB()
        Transfer = TransferDaemon(config = self.config)
        Transfer.algorithm( )
        query = {'reduce':False}
        files_acquired = self.db.loadView('monitor', 'filesAcquired', query)['rows']
        query = {'reduce':False}
        files_new = self.db.loadView('monitor', 'filesNew', query)['rows']

        assert ( len(files_acquired) + len(files_new) ) == 1

        for i in range(1, 5):
            self.createFileDocinFilesDB( str(i) )
        query = {'reduce':False}
        files_acquired = self.db.loadView('monitor', 'filesAcquired', query)['rows']
        query = {'reduce':False}
        files_new = self.db.loadView('monitor', 'filesNew', query)['rows']

        assert ( len(files_acquired) + len(files_new) ) == 5

    def testB_InteractionWithTheSource_testDocumentDuplicationAndThenTransfer(self):
        """
        _testB_InteractionWithTheSource_testDocumentDuplication_
        Tests the components: gets data from DB source and duplicate
        them in files_db and see if the component can process them.
        """
        self.createTestDocinDBSource()
        LFNDuplicator = LFNSourceDuplicator(config = self.config)
        LFNDuplicator.algorithm( )
        time.sleep(10)
        query = { 'reduce':False }
        active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']

        assert len(active_files) == 1

        Transfer = TransferDaemon(config = self.config)
        Transfer.algorithm( )
        query = {'reduce':False}
        files_acquired = self.db.loadView('monitor', 'filesAcquired', query)['rows']
        query = {'reduce':False}
        files_new = self.db.loadView('monitor', 'filesNew', query)['rows']

        assert ( len(files_acquired) + len(files_new) ) == 1

        for i in range(1, 5):
            self.createTestDocinDBSource( str(i) )
        LFNDuplicator_1 = LFNSourceDuplicator(config = self.config)
        LFNDuplicator_1.algorithm( )
        time.sleep(20)
        query = {'reduce':False }
        active1_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']

        assert len(active1_files) == 5

        Transfer_1 = TransferDaemon(config = self.config)
        Transfer_1.algorithm( )
        query = {'reduce':False}
        files_acquired = self.db.loadView('monitor', 'filesAcquired', query)['rows']
        query = {'reduce':False}
        files_new = self.db.loadView('monitor', 'filesNew', query)['rows']

        assert ( len(files_acquired) + len(files_new) ) == 5

    def testC_StatWork_testDocRemovalFromRuntimeDB(self):
        """
        _StatWork_BasicFunctionTest_
        Test statisticWorker, by seeing if it can remove an expired doc from runtimeDB.
        """
        doc = self.createTestFileFinishedYesterdayinFilesDB( )
        statWorker = StatisticDaemon(config = self.config)
        statWorker.algorithm( )
        query = {'reduce':False,
         'key':[doc['user'], doc['destination'], doc['source'], doc['dn'] ] }
        active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']

        assert len(active_files) == 0

    def testD_StatWork_testNumberOfDocsPerIteration(self):
        """
        _StatWork_testNumberOfDocsPerIteration_
        Test if the stat daemon creates a new document per iteration
        """
        self.createTestFileFinishedYesterdayinFilesDB( )
        statWorker = StatisticDaemon(config = self.config)
        statWorker.algorithm( )
        self.createTestFileFinishedYesterdayinFilesDB( )
        statWorker = StatisticDaemon(config = self.config)
        statWorker.algorithm( )
        query = {}
        serverRows = self.dbStat.loadView('stat', 'ftservers', query)['rows']

        assert len(serverRows) == 2

    def testD_InteractionWithTheSource_testUpdateFWJR(self):
        """
        _testD_InteractionWithTheSource_testUpdateFWJR_
        Tests the components: gets data from DB source and duplicate
        them in files_db and see if the component can update the fwjr when the transfer is done.
        """
        self.createTestDocinDBSource()
        LFNDuplicator = LFNSourceDuplicator(config = self.config)
        LFNDuplicator.algorithm( )
        time.sleep(10)
        # Run the daemon
        Transfer = TransferDaemon(config = self.config)
        Transfer.algorithm( )
        query = {'reduce':False}
        files_acquired = self.db.loadView('monitor', 'filesAcquired', query)['rows']
        # Get files acuired
        document = self.db.document(files_acquired[0]['id'])
        sites = self.sites
        site_tfc_map = {}
        for site in sites:
            site_tfc_map[site] = get_tfc_rules(site)
        # Mark the document as good
        worker = TransferWorker([document['user'], None, None], site_tfc_map, self.config.AsyncTransfer)
        worker.mark_good([document['lfn']])
        query = { 'reduce':False, 'key':[ document['jobid'] , document['job_end_time'] ] }
        result = self.dbSource.loadView('FWJRDump', 'fwjrByJobIDTimestamp', query)['rows']
        docSource = self.dbSource.document(result[0]['id'])

        assert docSource['fwjr']['steps'].has_key('asyncStageOut1') == True

    def testE_FixBug1196_PoolWorkersFromAgent_FunctionTest(self):
        """
        _BasicPoolWorkers_FunctionTest_
        Tests the components, by seeing if it can spawn process
        using the multiprocessing without problems
        """
        myThread = threading.currentThread()
        self.createTestDocinFilesDB()
        Transfer = AsyncTransferTest(config = self.testConfig)
        Transfer.prepareToStart()
        # Set sleep time to 3 days and you will reproduce the
        # problem described in #1196
        time.sleep(30)
        myThread.workerThreadManager.terminateWorkers()
        while threading.activeCount() > 1:
            time.sleep(1)

    def testF_TestIfgetHashLfnHashCorrectlyLFNs(self):
        """
        _testF_TestIfgetHashLfnHashCorrectlyLFNs
        Tests if the getHashLfn function of the AsyncStageOut module module hashs correctly LFNs.
        """
        lfn = "/My/lfn/path"
        hashedLfn = getHashLfn(lfn)
        assert hashlib.sha224(lfn).hexdigest() == hashedLfn

    def testG_PrePostTransferCleaning(self):
        """
        _testG_PrePostTransferCleaning
        Tests if the cleanSpace method removes correctly files.
        """
        file_doc = self.createTestDocinFilesDB('T2_IT_Rome')
        sites = self.sites
        site_tfc_map = {}
        for site in sites:
            site_tfc_map[site] = get_tfc_rules(site)
        pfn = apply_tfc('T2_IT_Rome'+':'+file_doc['lfn'], site_tfc_map, 'T2_IT_Rome')
        emptyFile = os.getcwd() + '/__init__.py'
        command = 'srmcp -debug=true file:///' + emptyFile + ' ' + pfn + ' -2'
        log_dir = '%s/logs/%s' % (self.config.AsyncTransfer.componentDir, file_doc['user'])
        try:
            os.makedirs(log_dir)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else: raise
        stdout_log = open('%s/%s.srmcp_out_log' % (log_dir, file_doc['destination']), 'w')
        stderr_log = open('%s/%s.srmcp_err_log' % (log_dir, file_doc['destination']), 'w')
        proc = subprocess.Popen(
                        ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                        stdout=stdout_log,
                        stderr=stderr_log,
                        stdin=subprocess.PIPE,
                            )
        proc.stdin.write(command)
        stdout, stderr = proc.communicate()
        rc = proc.returncode
        stdout_log.close()
        stderr_log.close()
        worker = TransferWorker([file_doc['user'], None, None], site_tfc_map, self.config.AsyncTransfer)
        to_clean = {(str(pfn), str(pfn)): 'T2_IT_Rome'}
        worker.cleanSpace(to_clean)
        commandLs = 'srmls ' + pfn
        stdoutls_log = open('%s/%s.srmls_out_log' % (log_dir, 'T2_IT_Rome'), 'w')
        stderrls_log = open('%s/%s.srmls_err_log' % (log_dir, 'T2_IT_Rome'), 'w')
        procls = subprocess.Popen(
                        ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                        stdout=stdoutls_log,
                        stderr=stderrls_log,
                        stdin=subprocess.PIPE,
                            )
        procls.stdin.write(commandLs)
        stdoutls, stderrls = procls.communicate()
        rcls = procls.returncode
        stdoutls_log.close()
        stderrls_log.close()
        assert rcls == 1

    def testH_AnalyticsCompMethods_tests(self):
        """
        _testH_AnalyticsCompMethods_tests_
        Tests the Analytics component methods
        """
        self.createFileDocinFilesDB()
        self.config.AsyncTransfer.max_retry = 0
        Analytics = AnalyticsDaemon(config = self.config)
        Analytics.updateWorkflowSummaries()
        query = {}
        state = self.monitoring_db.loadView('UserMonitoring', 'StatesByWorkflow', query)['rows'][0]['value']
        assert state["new"] == 1
        # Run the daemon
        Transfer = TransferDaemon(config = self.config)
        Transfer.algorithm( )
        Analytics.updateWorkflowSummaries()
        state = self.monitoring_db.loadView('UserMonitoring', 'StatesByWorkflow', query)['rows'][0]['value']
        assert state["failed"] == 1
        self.createFileDocinFilesDB( state = 'done', publication_state = 'published' )
        Analytics.updateJobSummaries()
        query = { 'reduce':True }
        numberWorkflow = self.monitoring_db.loadView('UserMonitoring', 'FilesByWorkflow', query)['rows'][0]['value']
        assert numberWorkflow == 1
        query = { }
        Analytics.updateWorkflowSummaries()
        state = self.monitoring_db.loadView('UserMonitoring', 'StatesByWorkflow', query)['rows'][0]['value']
        assert state["published"] == 1

    def testI_DBSPublisher_testLoadPublishView(self):
        """
        _testI_DBSPublisher_testLoadPublishView_
        Tests Publish view used by the DBSPublisher to load the files to publish.
        """
        doc = self.createFileDocinFilesDB( state = 'done', publication_state = 'not_published' )
        query = {'reduce':False,
         'key':[doc['user'], doc['group'], doc['role'], doc['dn'], doc['workflow']]}
        active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        assert len(active_files) == 1
        for i in range(1, 5):
            self.createFileDocinFilesDB( doc_id = str(i), state = 'done', publication_state = 'not_published' )
        query = {'reduce':False}
        all_active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        assert len(all_active_files) == 5
        Publisher = PublisherDaemon( config = self.config )
        Publisher.algorithm( )
        all_active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        assert len(all_active_files) == 0

if __name__ == '__main__':
    unittest.main()


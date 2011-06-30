#!/usr/bin/env python
#pylint: disable=C0103,E1103
"""
AsyncTransfer test
"""

import os
import logging
import unittest
import time
import threading

from WMQuality.TestInit   import TestInit

from AsyncTransfer.TransferDaemon import TransferDaemon
from AsyncTransfer.LFNSourceDuplicator import LFNSourceDuplicator
from AsyncTransfer.StatisticDaemon import StatisticDaemon
from WMCore.Configuration import loadConfigurationFile
from WMCore.Database.CMSCouch import CouchServer
from WMQuality.TestInitCouchApp import CouchAppTestHarness

from WMCore.WMInit import getWMBASE
from fakeDaemon import fakeDaemon
from AsyncTransferTest import AsyncTransferTest
import datetime

import random

class AsyncTransfer_t(unittest.TestCase):
    """
    TestCase for TestAsyncTransfer module
    """

    def setUp(self):
        """
        setup for test.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.MsgService", "WMCore.ThreadPool","WMCore.Agent.Database"],
                                useDefault = False)

        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction = False)

        self.config = self.getConfig()
        self.testConfig = self.getTestConfig()

        # Connect to files db
        self.server_filesdb = CouchServer(self.config.AsyncTransfer.couch_instance)
        self.server_filesdb.createDatabase(self.config.AsyncTransfer.files_database)
        self.db = self.server_filesdb.connectDatabase(self.config.AsyncTransfer.files_database)

        # Connect to couchDB source
        self.server_jsmdb = CouchServer(self.config.AsyncTransfer.data_source)
        self.server_jsmdb.createDatabase(self.config.AsyncTransfer.jsm_db)
        self.dbSource = self.server_jsmdb.connectDatabase(self.config.AsyncTransfer.jsm_db)

        # Connect to statDB
        self.server_statdb = CouchServer(self.config.AsyncTransfer.couch_statinstance)
        self.server_statdb.createDatabase(self.config.AsyncTransfer.statitics_database)
        self.dbStat = self.server_statdb.connectDatabase(self.config.AsyncTransfer.statitics_database)


        couchapps = "../../../src/couchapp"
        couchapps_jsm = "%s/src/couchapps" % getWMBASE()

        self.async_couchapp = "%s/AsyncTransfer" % couchapps
        self.monitor_couchapp = "%s/monitor" % couchapps
        self.jsm_couchapp = "%s/FWJRDump" % couchapps_jsm
        self.stat_couchapp = "%s/stat" % couchapps

        self.users = ['fred', 'barney', 'wilma', 'betty']
        self.sites = ['T2_CH_CAF', 'T2_CH_CSCS', 'T2_DE_DESY',
                      'T2_DE_RWTH', 'T2_ES_CIEMAT', 'T2_ES_IFCA',
                      'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU',
                      'T2_FR_IPHC', 'T2_IT_Bari', 'T2_FR_GRIF_LLR',
                      'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome',
                      'T2_UK_London_Brunel', 'T2_UK_London_IC', 'T2_TW_Taiwan',
                      'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech',
                      'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska',
                      'T2_US_UCSD', 'T2_US_Wisconsin', 'T2_US_Purdue']

        self.lfn = ['/this/is/a/lfnA', '/this/is/a/lfnB', '/this/is/a/lfnC', '/this/is/a/lfnD', '/this/is/a/lfnE']

        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase(modules = ["WMCore.MsgService", "WMCore.ThreadPool","WMCore.Agent.Database"])
        self.testInit.delWorkDir()

        self.server_filesdb.deleteDatabase(self.config.AsyncTransfer.files_database)
        self.server_jsmdb.deleteDatabase(self.config.AsyncTransfer.jsm_db)
        self.server_statdb.deleteDatabase(self.config.AsyncTransfer.statitics_database)

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()

        config = loadConfigurationFile('../../../src/python/DefaultConfig.py')

        #First the general stuff
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        #Now the CoreDatabase information
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")
        config.CoreDatabase.dialect = os.getenv("DIALECT")

        #Now the AsyncTransfer stuff
        config.AsyncTransfer.serviceCert = os.getenv('X509_USER_PROXY')
        config.AsyncTransfer.logDir                = os.path.join(self.testDir, 'logs')
        config.AsyncTransfer.componentDir          = os.getcwd()

        return config


    def getTestConfig(self):
        """
        _createConfig_

        General config file
        """
        config = self.testInit.getConfiguration()

        config = loadConfigurationFile('../../../src/python/DefaultConfig.py')

        #First the general stuff
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        #Now the CoreDatabase information
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")
        config.CoreDatabase.dialect = os.getenv("DIALECT")

        config.component_("AsyncTransferTest")
        config.AsyncTransferTest.couch_instance = config.AsyncTransfer.couch_instance
        config.AsyncTransferTest.files_database = config.AsyncTransfer.files_database
        config.AsyncTransferTest.data_source = config.AsyncTransfer.data_source
        config.AsyncTransferTest.jsm_db = config.AsyncTransfer.jsm_db
        config.AsyncTransferTest.log_level = logging.DEBUG
        config.AsyncTransferTest.pluginName = config.AsyncTransfer.pluginName
        config.AsyncTransferTest.pluginDir = config.AsyncTransfer.pluginDir
        config.AsyncTransferTest.max_files_per_transfer = 10
        config.AsyncTransferTest.pool_size = 3
        config.AsyncTransferTest.max_retry = 1000
        config.AsyncTransferTest.pollInterval = 10
        config.AsyncTransferTest.serviceCert = os.getenv('X509_USER_PROXY')
        config.AsyncTransferTest.map_FTSserver = \
{'PT' : 'https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'ES' : 'https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'IT' : 'https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'UK' : 'https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'FR' : 'https://cclcgftsprod.in2p3.fr:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'CH' : 'https://prod-fts-ws.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'DE' : 'https://fts-fzk.gridka.de:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'TW' : 'https://w-fts.grid.sinica.edu.tw:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'US' : 'https://cmsfts1.fnal.gov:8443/glite-data-transfer-fts/services/FileTransfer' ,\
 'defaultServer' : 'https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer'}

        config.AsyncTransferTest.logDir                = os.path.join(self.testDir, 'logs')
        config.AsyncTransferTest.componentDir          = os.getcwd()

        return config

    def createTestDocinFilesDB(self):
        """
        Creates a test document in files_db

        """
        doc = {}
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['workflow'] = 'someWorkflow'
        doc['jobid'] = '1'
        doc['retry_count'] = []
        doc['source'] = random.choice(self.sites)
        doc['destination'] = random.choice(self.sites)
        doc['user'] = random.choice(self.users)
        doc['state'] = 'new'
        doc['start_time'] = str(datetime.datetime.now())
        doc['end_time'] = str(datetime.datetime.now())
        doc['dbSource_update'] = str(time.time())


        self.db.queue(doc, True)
        self.db.commit()

        return doc

    def createFileDocinFilesDB(self, doc_id = '' ):
        """
        Creates a test document in files_db

        """
        doc = {}

        doc['_id'] = random.choice(self.lfn) + doc_id
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['workflow'] = 'someWorkflow'
        doc['jobid'] = '1'
        doc['retry_count'] = []
        doc['source'] = random.choice(self.sites)
        doc['destination'] = random.choice(self.sites)
        doc['user'] = random.choice(self.users)
        doc['state'] = 'new'
        doc['start_time'] = str(datetime.datetime.now())
        doc['end_time'] = str(datetime.datetime.now())
        doc['dbSource_update'] = str(time.time())


        self.db.queue(doc, True)
        self.db.commit()

        return doc

    def createTestFileFinishedYesterdayinFilesDB( self ):
        """
        Creates a test document in files_db

        """
        doc = {}

        doc['_id'] = "/this/is/a/lfnA"
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['task'] = 'someWorkflow'
        doc['size'] = 999999
        doc['jobid'] = '1'
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
        doc['dbSource_update'] = str(time.time())


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
                           "location": "gridse3.pg.infn.it",\
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
                           "lfn": "/store/user/riahi/lfnB"+doc_id,\
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
                           "location": "gridse3.pg.infn.it",\
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
"OutputPFN": "srm://gridse3.pg.infn.it:8443/srm/managerv2?SFN=/cms//store/user/grandi/22_121713/0000/5CD0D341-2CF6-DF11-9A92-0030487DA364.root"\
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
        harness = CouchAppTestHarness(self.config.AsyncTransfer.files_database, self.config.AsyncTransfer.couch_instance)
        harness.create()
        harness.pushCouchapps(self.async_couchapp)

        doc = self.createTestDocinFilesDB()
        query = {'reduce':False,
         'key':[doc['user'], doc['destination'], doc['source'], doc['dn'] ] }

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
        harness = CouchAppTestHarness(self.config.AsyncTransfer.files_database, self.config.AsyncTransfer.couch_instance)
        harness.create()
        harness.pushCouchapps(self.async_couchapp, self.monitor_couchapp)

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
        harness = CouchAppTestHarness(self.config.AsyncTransfer.files_database, self.config.AsyncTransfer.couch_instance)
        harness.create()
        harness.pushCouchapps(self.async_couchapp, self.monitor_couchapp)

        harness1 = CouchAppTestHarness(self.config.AsyncTransfer.jsm_db, self.config.AsyncTransfer.data_source)
        harness1.create()
        harness1.pushCouchapps(self.jsm_couchapp)

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
        harness = CouchAppTestHarness(self.config.AsyncTransfer.statitics_database, self.config.AsyncTransfer.couch_statinstance)
        harness.create()
        harness.pushCouchapps(self.stat_couchapp)

        harness1 = CouchAppTestHarness(self.config.AsyncTransfer.files_database, self.config.AsyncTransfer.couch_instance)
        harness1.create()
        harness1.pushCouchapps(self.async_couchapp, self.monitor_couchapp)

        doc = self.createTestFileFinishedYesterdayinFilesDB( )

        statWorker = StatisticDaemon(config = self.config)
        statWorker.algorithm( )

        query = {'reduce':False,
         'key':[doc['user'], doc['destination'], doc['source'], doc['dn'] ] }

        active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']
        assert len(active_files) == 0

    def testD_FixBug1196_BasicPoolWorkers_FunctionTest(self):
        """
        _BasicPoolWorkers_FunctionTest_

        Tests the class used by the component, by seeing if it can spawn process
        using the multiprocessing without problems
        """
        self.createTestDocinFilesDB()

        Transfer = fakeDaemon(config = self.testConfig)
        counter = 0

        while ( counter < 10 ):

            result = Transfer.algorithm( )
            assert len(result) == 1

            counter += 1

    def testD_FixBug1196_PoolWorkersFromAgent_FunctionTest(self):
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

if __name__ == '__main__':
    unittest.main()


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

from fakeDaemon import fakeDaemon
from AsyncTransferTest import AsyncTransferTest
import datetime

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


        self.docsInFilesDB = []
        self.docsInDBSource = []
        self.docsInStatDB = []

        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction = False)

        self.config = self.getConfig()
        self.testConfig = self.getTestConfig()

        # Connect to files db
        server = CouchServer(self.config.AsyncTransfer.couch_instance)
        self.db = server.connectDatabase(self.config.AsyncTransfer.files_database)
        print('Connected to async couchDB')

        # Connect to couchDB source
        server = CouchServer(self.config.AsyncTransfer.data_source)
        self.dbSource = server.connectDatabase(self.config.AsyncTransfer.jsm_db)
        print('Connected to couchDB source')

        # Connect to statDB
        server = CouchServer(self.config.AsyncTransfer.couch_statinstance)
        self.dbStat = server.connectDatabase(self.config.AsyncTransfer.statitics_database)
        print('Connected to statDB')

        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase(modules = ["WMCore.MsgService", "WMCore.ThreadPool","WMCore.Agent.Database"])
        self.testInit.delWorkDir()

        # Remove test docs in couchDB
        for doc in self.docsInFilesDB:
            self.DeleteTestDocinFilesDB(doc)

        for doc in self.docsInDBSource:
            self.DeleteTestDocinDBSource(doc)

        for doc in self.docsInStatDB:
            self.DeleteTestDocinDBStat(doc)

        return


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
{ 'PT' : 'https://fts.pic.es:8443/glite-data-transfer-fts/services/FileTransfer' ,\
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

    def createTestDocinFilesDB(self, doc = {} ):
        """
        Creates a test document in files_db

        """

        doc['_id'] = "/this/is/a/lfnA"
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['workflow'] = 'someWorkflow'
        doc['jobid'] = '1'
        doc['retry_count'] = []
        doc['source'] = 'T2_IT_Bari'
        doc['destination'] = "T2_IT_Pisa"
        doc['user'] = 'riahi'
        doc['state'] = 'new'
        doc['start_time'] = str(datetime.datetime.now())
        doc['end_time'] = str(time.time())
        doc['dbSource_update'] = str(time.time())


        self.db.queue(doc, True)
        self.db.commit()

        print('new doc added')

        self.docsInFilesDB.append( doc['_id'] )

        return

    def createTestFileFinishedYesterdayinFilesDB(self, config = None, doc = {} ):
        """
        Creates a test document in files_db

        """

        doc['_id'] = "/this/is/a/lfnA"
        doc['dn'] = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi"
        doc['task'] = 'someWorkflow'
        doc['size'] = 999999
        doc['jobid'] = '1'
        doc['retry_count'] = []
        doc['source'] = 'T2_IT_Bari'
        doc['destination'] = "T2_IT_Pisa"
        doc['user'] = 'riahi'
        doc['state'] = 'done'
        doc['start_time'] = str(datetime.datetime.now()).replace(str(datetime.datetime.now()).split(" ")[0].split("-")[2],str(int(str(datetime.datetime.now()).split(" ")[0].split("-")[2]) - 3))
        doc['end_time'] = str(datetime.datetime.now()).replace(str(datetime.datetime.now()).split(" ")[0].split("-")[2],str(int(str(datetime.datetime.now()).split(" ")[0].split("-")[2]) - 2))
        doc['dbSource_update'] = str(time.time())


        self.db.queue(doc, True)
        self.db.commit()

        self.docsInStatDB.append( config.map_FTSserver[ doc['destination'].split("_")[1] ]+"_"+str(datetime.datetime.now()).replace(str(datetime.datetime.now()).split(" ")[0].split("-")[2],str(int(str(datetime.datetime.now()).split(" ")[0].split("-")[2]) - 3)).split(" ")[0] )

        return


    def DeleteTestDocinFilesDB(self, doc):
        """
        Remove the test documents in files_db
        """
        document = self.db.document( doc )
        self.db.queueDelete(document)
        self.db.commit()

        print('deleted %s' %doc)

        return

    def DeleteTestDocinDBStat(self, doc):
        """
        Remove the test documents from statdb
        """
        document = self.dbStat.document( doc )
        self.db.queueDelete(document)
        self.db.commit()

        print('deleted %s from stat' %doc)

        return


    def createTestDocinDBSource(self):
        """
        Creates a JSM document
        """

        doc = {\
   "_id": "12345",\
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
"lfn": "/store/user/riahi/lfnB",\
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
                           "lfn": "/store/user/riahi/lfnB",\
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
                           "lfn": "/store/user/riahi/lfnB",\
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
        print('Duplication done')

        self.docsInDBSource.append( doc['_id'] )
        self.docsInFilesDB.append( '/store/user/riahi/lfnB' )

        return


    def DeleteTestDocinDBSource(self, doc):
        """
        Deletes test docs from DB source

        """
        document = self.dbSource.document( doc )
        self.dbSource.queueDelete( document )
        self.dbSource.commit()

        print('deleted %s' %doc)
        return


    def testA_BasicFunctionTest(self):
        """
        _BasicFunctionTest_

        Tests the components, by seeing if they can process a simple document
        """
        self.createTestDocinFilesDB()

        Transfer = TransferDaemon(config = self.config)
        Transfer.algorithm( )

        return


    def testB_DuplicateDataFromJSM_BasicFunctionTest(self):
        """
        _DuplicateDataFromJSM_BasicFunctionTest_

        Tests the components: gets data from DB source and duplicate
        it in files_db and see if the component can process it.
        """

        self.createTestDocinDBSource()

        LFNDuplicator = LFNSourceDuplicator(config = self.config)
        LFNDuplicator.algorithm( )

        time.sleep(10)

        Transfer_1 = TransferDaemon(config = self.config)
        Transfer_1.algorithm( )

        return

    def testC_BasicPoolWorkers_FunctionTest(self):
        """
        _BasicPoolWorkers_FunctionTest_

        Tests the class used by the component, by seeing if it can spawn process
        using the multiprocessing without problems
        """
        self.createTestDocinFilesDB()

        Transfer = fakeDaemon(config = self.testConfig)
        counter = 0

        while ( counter < 10 ):

            Transfer.algorithm( )
            counter += 1

    def testC_PoolWorkersFromAgent_FunctionTest(self):
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
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        return

    def testD_StatWork_BasicFunctionTest(self):
        """
        _StatWork_BasicFunctionTest_
        Test statisticWorker, by seeing if it can remove a doc more than expire time
        old from runtimeDB and create a new stat doc in statDB
        """

        self.createTestFileFinishedYesterdayinFilesDB(config = self.config.AsyncTransfer)

        statWorker = StatisticDaemon(config = self.config)
        statWorker.algorithm( )

        return


if __name__ == '__main__':
    unittest.main()


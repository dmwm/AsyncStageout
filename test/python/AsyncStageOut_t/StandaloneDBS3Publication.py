"""
This module is meant to be a stand-alone way to the DBS3 publication code.
"""

import os
import time
import json
import logging
import unittest

from WMCore.Configuration import loadConfigurationFile

import AsyncStageOut.PublisherWorker

orig_x509_user_proxy = os.getenv("X509_USER_PROXY")
if not orig_x509_user_proxy:
    orig_x509_user_proxy = os.path.join("/tmp", "x509up_u%d" % os.geteuid())
if not os.path.exists(orig_x509_user_proxy):
    raise RuntimeError("X509 proxy, %s, does not exist" % orig_x509_user_proxy)

TEST_USER, TEST_GROUP, TEST_ROLE = 'bbockelm', 'uscms', 'cmsuser'
os.environ["TEST_ASO"] = 'true'

#TEST_DBS3_READER = "https://cmsweb.cern.ch/dbs/prod/global/"
#TEST_DBS3_WRITER = "https://cmsweb.cern.ch/dbs/prod/phys03/"
TEST_DBS3_READER = "https://cmsweb-testbed.cern.ch/dbs/int/global/"
TEST_DBS3_WRITER = "https://cmsweb-testbed.cern.ch/dbs/int/phys03/"
#TEST_DBS3_READER = "https://dbs3-dev01.cern.ch/dbs/dev/phys03/"
#TEST_DBS3_WRITER = "https://dbs3-dev01.cern.ch/dbs/dev/phys03/"
TEST_WORKFLOW = "140107_205325_crab3test:bbockelm_crab_lxplus414_int_5"
TEST_SE_NAME = "se.example.com"
TEST_INPUT_DATASET = "/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO"
TEST_INPUT_MCDATASET = "/CRAB3_PrivateMC"

class TestDBS3Publication(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.config = self.getConfig()

    def setUp(self):
        self.publisher = AsyncStageOut.PublisherWorker.PublisherWorker((TEST_USER, TEST_GROUP, TEST_ROLE), self.config.DBSPublisher)

    def getConfig(self):
        """
        _createConfig_
        General config file
        """
        config = loadConfigurationFile('configuration/Example.py')
        config.DBSPublisher.serviceCert = orig_x509_user_proxy
        config.DBSPublisher.serviceKey = orig_x509_user_proxy
        config.DBSPublisher.opsProxy = orig_x509_user_proxy
        config.DBSPublisher.algoName = 'FIFOPriority'
        config.DBSPublisher.pollInterval = 10
        config.DBSPublisher.publication_pool_size = 1
        config.DBSPublisher.componentDir = "test/data"
        config.DBSPublisher.namespace = 'AsyncStageOut.DBSPublisher'
        config.DBSPublisher.log_level = logging.DEBUG
        config.DBSPublisher.files_database = "asynctransfer_1"
        config.DBSPublisher.couch_instance = os.getenv("COUCHURL")
        config.DBSPublisher.publication_max_retry = 0
        config.DBSPublisher.serviceCert = orig_x509_user_proxy
        config.DBSPublisher.max_files_per_block = 10
        config.DBSPublisher.workflow_expiration_time = 3

        return config

    def test_setup(self):
        # Just verify the PublisherWorker object can be created in setUp
        pass

    def test_dbs3(self):
        return self.dbs3_publish_helper(TEST_INPUT_DATASET)

    def test_dbs3_mc(self):
        return self.dbs3_publish_helper(TEST_INPUT_MCDATASET)

    def dbs3_publish_helper(self, input_dataset):
        test_data_path = os.path.join(self.config.DBSPublisher.componentDir, "edm_files_to_publish.json")
        res = json.load(open(test_data_path))
        toPublish = {}
        for files in res['result']:
            outdataset = str(files['outdataset'])
            outdataset_info = outdataset.split("-")
            outdataset_info[1] += "_%d" % int(time.time())
            outdataset = "-".join(outdataset_info)
            if toPublish.has_key(outdataset):
                toPublish[outdataset].append(files)
            else:
                toPublish[outdataset] = []
                toPublish[outdataset].append(files)
            lfn_dir, name = os.path.split(files['lfn'])
            name = name.split(".")
            name = ".".join(name[:-1]) + "_%d.%s" % (int(time.time()), name[-1])
            files['lfn'] = os.path.join(lfn_dir, name)

        # Publish all given files
        lfn_ready = [i['lfn'] for i in res['result']]
        fail_files, toPublish = self.publisher.clean(lfn_ready, toPublish)
        #self.publisher.logger.debug('to_publish %s' % toPublish)
        self.publisher.logger.info("Starting data publication for: " + str(TEST_WORKFLOW))
        self.publisher.not_expired_wf = False
        failed, done, dbsResults = self.publisher.publishInDBS3(userdn=self.publisher.userDN, sourceURL=TEST_DBS3_READER,
                                                                inputDataset=input_dataset, toPublish=toPublish,
                                                                destURL=TEST_DBS3_WRITER, targetSE=TEST_SE_NAME, workflow=TEST_WORKFLOW)
        self.publisher.logger.debug("DBS publication results %s" % dbsResults)
        self.assertFalse(failed)


if __name__ == '__main__':
    unittest.main()

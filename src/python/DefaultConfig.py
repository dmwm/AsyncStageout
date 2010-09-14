import logging
from WMCore.Configuration import Configuration

config = Configuration()

config.section_("General")
config.General.workDir = "/tmp"

config.component_("AsyncTransfer")
config.AsyncTransfer.namespace = "AsyncTransfer.AsyncTransfer"
config.AsyncTransfer.componentDir  = config.General.workDir + "/AsyncTransfer"
config.AsyncTransfer.logLevel = "DEBUG"
config.AsyncTransfer.pollInterval = 2
config.AsyncTransfer.couch_instance = 'http://admin:password@localhost:5984'
config.AsyncTransfer.couch_database = 'crabserver/asynctransfer'
config.AsyncTransfer.max_files_per_transfer = 1000
config.AsyncTransfer.pool_size = 2
config.AsyncTransfer.log_level = logging.DEBUG
config.AsyncTransfer.ftscp = '../test/python/AsyncTransfer/ftscp_fake'
config.AsyncTransfer.defaultProxy = '/home/crab/gridcert/proxy.cert'

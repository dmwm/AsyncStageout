import logging
from WMCore.Configuration import Configuration

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('AsyncTransfer')

config.AsyncTransfer.couch_instance = 'http://admin:password@localhost:5984'

config.AsyncTransfer.couch_database = 'crab/asynctransfer'

config.AsyncTransfer.log_level = logging.DEBUG

config.AsyncTransfer.max_files_per_transfer = 1000

config.AsyncTransfer.pool_size = 2

config.AsyncTransfer.ftscp = '../test/python/AsyncTransfer/ftscp_fake'
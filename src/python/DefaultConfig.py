import logging
from WMCore.Configuration import Configuration

config = Configuration()

config.section_("General")
config.General.workDir = "/tmp"

config.component_("AsyncTransfer")
config.AsyncTransfer.namespace = "AsyncTransfer.AsyncTransfer"
config.AsyncTransfer.componentDir  = config.General.workDir + "/AsyncTransfer"
config.AsyncTransfer.pollInterval = 2
config.AsyncTransfer.couch_instance = 'http://localhost:5984'
config.AsyncTransfer.files_database = 'async/files'
config.AsyncTransfer.statitics_database = 'async/statitics'
config.AsyncTransfer.requests_database = 'async/requests'
config.AsyncTransfer.data_source = 'http://localhost:5984/'
config.AsyncTransfer.jsm_db = 'jsm'
config.AsyncTransfer.pluginDir = "AsyncTransfer.Plugins"
config.AsyncTransfer.pluginName = "DummySource"
config.AsyncTransfer.pollViewsInterval = 10
config.AsyncTransfer.max_files_per_transfer = 1000
config.AsyncTransfer.pool_size = 2
config.AsyncTransfer.log_level = logging.INFO
config.AsyncTransfer.ftscp = '../test/python/AsyncTransfer/ftscp_fake'
config.AsyncTransfer.serviceCert = '/home/crab/service.cert'
config.AsyncTransfer.map_FTSserver = { 
    'IT' : 'https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'UK' : 'https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'FR' : 'https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'CH' : 'https://prod-fts-ws.cern.ch:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'DE' : 'https://fts-fzk.gridka.de:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'TW' : 'https://w-fts.grid.sinica.edu.tw:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'US' : 'https://cmsfts1.fnal.gov:8443/glite-data-transfer-fts/services/FileTransfer' , 
    'defaultServer' : 'https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer'}
# Complete for other special sites (India, russia...)


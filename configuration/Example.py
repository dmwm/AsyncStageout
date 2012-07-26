#!/usr/bin/env python
"""
Example configuration for AsyncStageOut
"""

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration
import logging

serverHostName = "HOST_NAME"
hostDN = "Your host DN"
workDirectory = "/AsyncStageOut/worgkin/dir"
databaseUrl = "http://user:password@host:port/agent_database"
couchUrl = "http://user:passwd@host:port"
statCouchUrl = "http://user:passwd@host:port"
userMonitoringCouchUrl = "http://user:passwd@host:port"
couchURLSource = "http://user:passwd@host:port"
files_database = "asynctransfer"
statitics_database = "asynctransfer_stat"
requests_database = "request_database"
database_src = "analysis_wmstats"
user_monitoring_db = "user_monitoring_asynctransfer"
serviceCert = "/path/to/valid/host-cert"
userEmail = "Your mail address"
agentName = "Agent name"
teamName = "Your team name"
credentialDir = "/tmp/credentials/"
file_cache_endpoint = "https://cmsweb-testbed.cern.ch/crabcache/"
ui_script = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'

config = Configuration()
config.section_('General')
config.General.workDir = workDirectory
config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl

config.section_("Agent")
config.Agent.contact = userEmail
config.Agent.agentName = agentName
config.Agent.hostName = serverHostName
config.Agent.teamName = teamName

config.component_("AsyncTransfer")
config.AsyncTransfer.log_level = logging.INFO
config.AsyncTransfer.namespace = "AsyncStageOut.AsyncTransfer"
config.AsyncTransfer.componentDir  = config.General.workDir
config.AsyncTransfer.pollInterval = 10
config.AsyncTransfer.pollViewsInterval = 10
config.AsyncTransfer.couch_instance = couchUrl
config.AsyncTransfer.files_database = files_database
config.AsyncTransfer.statitics_database = statitics_database
config.AsyncTransfer.requests_database = requests_database
config.AsyncTransfer.data_source = couchURLSource
config.AsyncTransfer.db_source = database_src
config.AsyncTransfer.pluginName = "CentralMonitoring"
config.AsyncTransfer.pluginDir = "AsyncStageOut.Plugins"
config.AsyncTransfer.max_files_per_transfer = 1000
config.AsyncTransfer.pool_size = 3
config.AsyncTransfer.max_retry = 3
config.AsyncTransfer.credentialDir = credentialDir
config.AsyncTransfer.UISetupScript = ui_script
config.AsyncTransfer.transfer_script = 'ftscp'
config.AsyncTransfer.serverDN = hostDN
config.AsyncTransfer.pollStatInterval = 86400
config.AsyncTransfer.expiration_days = 30
config.AsyncTransfer.couch_statinstance = statCouchUrl
config.AsyncTransfer.serviceCert = serviceCert
config.AsyncTransfer.serviceKey = "/path/to/valid/host-key"
config.AsyncTransfer.cleanEnvironment = True
config.AsyncTransfer.user_monitoring_db = user_monitoring_db
config.AsyncTransfer.couch_user_monitoring_instance = userMonitoringCouchUrl
config.AsyncTransfer.analyticsPollingInterval = 1800
config.AsyncTransfer.filesCleaningPollingInterval = 14400
config.AsyncTransfer.summaries_expiration_days = 30
config.component_('DBSPublisher')
config.DBSPublisher.pollInterval = 10
config.DBSPublisher.publication_pool_size = 1
config.DBSPublisher.componentDir = config.General.workDir
config.DBSPublisher.UISetupScript = ui_script
config.DBSPublisher.namespace = 'AsyncStageOut.DBSPublisher'
config.DBSPublisher.log_level = logging.INFO
config.DBSPublisher.files_database = files_database
config.DBSPublisher.couch_instance = couchUrl
config.DBSPublisher.publication_max_retry = 3
config.DBSPublisher.userFileCacheEndpoint = file_cache_endpoint
config.DBSPublisher.credentialDir = credentialDir
config.DBSPublisher.serverDN = hostDN
config.DBSPublisher.serviceCert = serviceCert
config.DBSPublisher.serviceKey =  "/path/to/valid/host-key"
config.DBSPublisher.max_files_per_block = 100
config.DBSPublisher.workflow_expiration_time = 3


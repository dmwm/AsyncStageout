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
config_database = "asynctransfer_config"
jobs_database = "jobs"
serviceCert = "/path/to/valid/host-cert"
serviceKey = "/path/to/valid/host-key"
opsProxy = "/path/to/ops/proxy"
userEmail = "Your mail address"
agentName = "Agent name"
teamName = "Your team name"
credentialDir = "/tmp/credentials/"
cache_area = "url_to_CS_cache"
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
config.AsyncTransfer.requests_database = requests_database
config.AsyncTransfer.data_source = couchURLSource
config.AsyncTransfer.db_source = database_src
config.AsyncTransfer.pluginName = "CentralMonitoring"
config.AsyncTransfer.pluginDir = "AsyncStageOut.Plugins"
config.AsyncTransfer.max_files_per_transfer = 100
config.AsyncTransfer.pool_size = 80
config.AsyncTransfer.max_retry = 3
config.AsyncTransfer.credentialDir = credentialDir
config.AsyncTransfer.UISetupScript = ui_script
config.AsyncTransfer.transfer_script = 'ftscp'
config.AsyncTransfer.serverDN = hostDN
config.AsyncTransfer.expiration_days = 30
config.AsyncTransfer.serviceCert = serviceCert
config.AsyncTransfer.serviceKey = serviceKey
config.AsyncTransfer.opsProxy = opsProxy
config.AsyncTransfer.cleanEnvironment = True
config.AsyncTransfer.user_monitoring_db = user_monitoring_db
config.AsyncTransfer.couch_user_monitoring_instance = userMonitoringCouchUrl
config.AsyncTransfer.summaries_expiration_days = 30
config.AsyncTransfer.config_database = config_database
config.AsyncTransfer.jobs_database = jobs_database
config.AsyncTransfer.schedAlgoDir = 'AsyncStageOut.SchedPlugins'
config.AsyncTransfer.algoName = 'FIFOPriority'
config.AsyncTransfer.config_couch_instance = couchUrl
config.AsyncTransfer.cache_area = cache_area
config.component_('Reporter')
config.Reporter.namespace = 'AsyncStageOut.Reporter'
config.Reporter.componentDir = config.General.workDir
config.component_('DBSPublisher')
config.DBSPublisher.pollInterval = 600 
config.DBSPublisher.publication_pool_size = 80
config.DBSPublisher.componentDir = config.General.workDir
config.DBSPublisher.UISetupScript = ui_script
config.DBSPublisher.namespace = 'AsyncStageOut.DBSPublisher'
config.DBSPublisher.log_level = logging.INFO
config.DBSPublisher.files_database = files_database
config.DBSPublisher.config_database = config_database
config.DBSPublisher.config_couch_instance = couchUrl
config.DBSPublisher.couch_instance = couchUrl
config.DBSPublisher.publication_max_retry = 3
config.DBSPublisher.cache_area = cache_area
config.DBSPublisher.credentialDir = credentialDir
config.DBSPublisher.serverDN = hostDN
config.DBSPublisher.serviceCert = serviceCert
config.DBSPublisher.serviceKey = serviceKey
config.DBSPublisher.opsProxy = opsProxy
config.DBSPublisher.max_files_per_block = 100
config.DBSPublisher.workflow_expiration_time = 3
config.DBSPublisher.schedAlgoDir = 'AsyncStageOut.SchedPlugins'
config.DBSPublisher.algoName = 'FIFOPriority'
config.DBSPublisher.block_closure_timeout = 18800
config.DBSPublisher.publish_dbs_url = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter'
#config.component_('Analytics')
#config.Analytics.user_monitoring_db = user_monitoring_db
#config.Analytics.couch_user_monitoring_instance = userMonitoringCouchUrl
#config.Analytics.analyticsPollingInterval = 900
#config.Analytics.log_level = logging.INFO
#config.Analytics.componentDir = config.General.workDir
#config.Analytics.namespace = 'AsyncStageOut.Analytics'
#config.Analytics.files_database = files_database
#config.Analytics.config_database = config_database
#config.Analytics.config_couch_instance = couchUrl
#config.Analytics.couch_instance = couchUrl
#config.Analytics.config_couch_instance = couchUrl
#config.Analytics.summaries_expiration_days = 6
#config.Analytics.amq_auth_file = '/path/to/amq/auth/file'
config.component_('FilesCleaner')
config.FilesCleaner.log_level = logging.INFO
config.FilesCleaner.componentDir = config.General.workDir
config.FilesCleaner.namespace = 'AsyncStageOut.FilesCleaner'
config.FilesCleaner.files_database = files_database
config.FilesCleaner.UISetupScript = ui_script
config.FilesCleaner.couch_instance = couchUrl
config.FilesCleaner.filesCleaningPollingInterval = 14400
config.FilesCleaner.opsProxy = opsProxy
config.FilesCleaner.config_database = config_database
config.FilesCleaner.config_couch_instance = couchUrl
config.FilesCleaner.credentialDir = credentialDir
config.FilesCleaner.serverDN = hostDN 
config.component_('Statistics')
config.Statistics.log_level = logging.INFO
config.Statistics.componentDir = config.General.workDir
config.Statistics.namespace = 'AsyncStageOut.Statistics'
config.Statistics.files_database = files_database
config.Statistics.config_database = config_database
config.Statistics.config_couch_instance = couchUrl
config.Statistics.couch_instance = couchUrl
config.Statistics.pollStatInterval = 1800
config.Statistics.couch_statinstance = statCouchUrl
config.Statistics.expiration_days = 7
config.Statistics.statitics_database = statitics_database
config.Statistics.opsProxy = opsProxy
config.Statistics.mon_database = files_database
config.component_("RetryManager")
config.RetryManager.namespace = "AsyncStageOut.RetryManager"
config.RetryManager.componentDir = config.General.workDir
config.RetryManager.log_level = 10
config.RetryManager.pollInterval = 300
config.RetryManager.files_database = files_database
config.RetryManager.couch_instance = couchUrl
config.RetryManager.opsProxy = opsProxy
config.RetryManager.retryAlgoDir = 'AsyncStageOut.RetryPlugins'
config.RetryManager.algoName = 'DefaultRetryAlgo'
config.RetryManager.cooloffTime = 7200

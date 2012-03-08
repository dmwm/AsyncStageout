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
couchURLSource = "http://user:passwd@host:port"
files_database = "asynctransfer"
statitics_database = "asynctransfer_stat"
requests_database = "request_database"
jsm_database = "wmagent_jobdump/fwjrs"
serviceCert = "/path/to/valid/host-cert"
userEmail = "Your mail address"
agentName = "Agent name"
teamName = "Your team name"
credentialDir = "/tmp/credentials/"

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
config.AsyncTransfer.db_source = jsm_database
config.AsyncTransfer.pluginName = "JSM"
config.AsyncTransfer.pluginDir = "AsyncStageOut.Plugins"
config.AsyncTransfer.max_files_per_transfer = 1000
config.AsyncTransfer.pool_size = 3
config.AsyncTransfer.max_retry = 3
config.AsyncTransfer.credentialDir = credentialDir
config.AsyncTransfer.UISetupScript = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'
config.AsyncTransfer.serverDN = hostDN
config.AsyncTransfer.pollStatInterval = 86400
config.AsyncTransfer.expiration_days = 7
config.AsyncTransfer.couch_statinstance = statCouchUrl
config.AsyncTransfer.serviceCert = serviceCert
config.AsyncTransfer.serviceKey = "/path/to/valid/host-key"
config.AsyncTransfer.cleanEnvironment = True

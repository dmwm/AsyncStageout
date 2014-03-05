#!/usr/bin/env
#pylint: disable-msg=C0103
'''
The TransferWorker does the following:

    a. make the ftscp copyjob
    b. submit ftscp and watch
    c. delete successfully transferred files from the database

There should be one worker per user transfer.


'''
from WMCore.Database.CMSCouch import CouchServer

import time
import logging
import subprocess, os, errno
import tempfile
import datetime
import traceback
from WMCore.WMFactory import WMFactory
import urllib
import re
from WMCore.Credential.Proxy import Proxy
from AsyncStageOut import getHashLfn
from AsyncStageOut import getFTServer
from AsyncStageOut import getDNFromUserName
import json
#import json
#import socket
#import stomp
from time import strftime

def execute_command( command, logger, timeout ):
    """
    _execute_command_
    Funtion to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
    )

    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))

    return stdout, rc

def getProxy(userdn, group, role, defaultDelegation, logger):
    """
    _getProxy_
    """

    logger.debug("Retrieving proxy for %s" % userdn)
    config = defaultDelegation
    config['userDN'] = userdn
    config['group'] = group
    config['role'] = role
    proxy = Proxy(defaultDelegation)
    proxyPath = proxy.getProxyFilename( True )
    timeleft = proxy.getTimeLeft( proxyPath )
    if timeleft is not None and timeleft > 3600:
        return (True, proxyPath)
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft( proxyPath )
    if timeleft is not None and timeleft > 0:
        return (True, proxyPath)
    return (False, None)


class ReporterWorker:

    def __init__(self, user, config):
        """
        store the user and tfc the worker
        """
        self.user = user
        self.config = config
        self.dropbox_dir = '%s/dropbox/inputs' % self.config.componentDir
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Reporter-%s' % self.user)
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.cleanEnvironment = ''
        self.userDN = ''
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        # TODO: improve how the worker gets a log
        self.logger.debug("Trying to get DN")
        try:
            self.userDN = getDNFromUserName(self.user, self.logger)
        except Exception, ex:
            msg =  "Error retrieving the user DN"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            self.init = False
            return
        if not self.userDN:
            self.init = False
            return
        defaultDelegation = {
                                  'logger': self.logger,
                                  'credServerPath' : \
                                      self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN' : self.config.serverDN,
                                  'uisource' : self.uiSetupScript,
                                  'cleanEnvironment' : getattr(self.config, 'cleanEnvironment', False)
                            }
        if hasattr(self.config, "cache_area"):
            try:
                defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(self.config.cache_area)[0]
            except IndexError:
                self.logger.error('MyproxyAccount parameter cannot be retrieved from %s' % self.config.cache_area)
                pass
        if getattr(self.config, 'serviceCert', None):
            defaultDelegation['server_cert'] = self.config.serviceCert
        if getattr(self.config, 'serviceKey', None):
            defaultDelegation['server_key'] = self.config.serviceKey

        self.valid = False
        try:

            self.valid, proxy = getProxy(self.userDN, "", "", defaultDelegation, self.logger)

        except Exception, ex:

            msg =  "Error getting the user proxy"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

        if self.valid:
            self.userProxy = proxy
        else:
            # Use the operator's proxy when the user proxy in invalid.
            # This will be moved soon
            self.logger.error('Did not get valid proxy. Setting proxy to ops proxy')
            self.userProxy = config.opsProxy

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)
        self.commandTimeout = 1200
        self.max_retry = config.max_retry

    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """
        self.logger.info("Retrieving files for %s" % self.user)
        files_to_update = self.files_for_update()
        self.logger.info("%s files to process" % len(files_to_update))
        self.logger.debug("%s files to process" % files_to_update)   
        for input_file in files_to_update:
            failed_lfns = []
            failure_reason = []
            good_lfns = [] 
            self.logger.info("Updating %s" % input_file)
            with open(input_file) as data_file: 
                try:
                    json_data = json.load(data_file)
                except ValueError, e:
                    self.logger.error("Error loading %s" % e)
                    continue  
            if json_data:
                self.logger.debug('Content %s %s %s' % (json_data['LFNs'], json_data['transferStatus'], json_data['failure_reason']))
                self.logger.debug('Failure index %s' % json_data['transferStatus'].index('failed'))

                # Sort failed files
                failed_indexes = [i for i,x in enumerate(json_data['transferStatus']) if x == 'failed']  
                self.logger.info('failed indexes %s' % len(failed_indexes))  
                self.logger.debug('failed indexes %s' % failed_indexes)
                for i in failed_indexes:
                    failed_lfns.append(json_data['LFNs'][i]) 
                    failure_reason.append(json_data['failure_reason'][i]) 
                self.logger.debug('Marking failed %s %s' %(failed_lfns,failure_reason)) 
                self.mark_failed(failed_lfns, failure_reason) 

                # Sort good files
                good_indexes = [i for i,x in enumerate(json_data['transferStatus']) if x == 'done']
                self.logger.info('good indexes %s' % len(good_indexes))
                self.logger.debug('good indexes %s' % good_indexes)
                for i in good_indexes:
                    good_lfns.append(json_data['LFNs'][i])
                self.logger.info('Marking good %s' %(good_lfns))
                self.mark_good(good_lfns)

                # Remove the json file
                self.logger.debug('Removing %s' % input_file)
                os.unlink( input_file ) 
            else:
                continue
        self.logger.info('Update completed')
        return

    def files_for_update(self):
        """
        Retrieve the list of files to update.
        """
        files_to_update = []
        user_dir = os.path.join(self.dropbox_dir, self.user)
        self.logger.info('Looking into %s' % user_dir)
        for user_file in os.listdir(user_dir):
            files_to_update.append(os.path.join(self.dropbox_dir, self.user, user_file))
        return files_to_update

    def mark_acquired(self, files=[], good_logfile=None):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        for lfn in files:
            if lfn['value'].find('temp') > 1:
                docId = getHashLfn(lfn['value'])
                self.logger.debug("Marking acquired %s" % docId)
                # Load document to get the retry_count
                try:
                    document = self.db.document( docId )
                except Exception, ex:
                    msg =  "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                if (document['state'] == 'new' or document['state'] == 'retry'):
                    data = {}
                    data['state'] = 'acquired'
                    data['last_update'] = time.time()
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                    updateUri += "?" + urllib.urlencode(data)
                    try:
                        self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                    except Exception, ex:
                        msg = "Error updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                    self.logger.debug("Marked acquired %s of %s" % (docId,lfn))
                    lfn_in_transfer.append(lfn)

                else:
                    continue
            else:
                good_lfn = lfn['value'].replace('store', 'store/temp', 1)
                self.mark_good([good_lfn])
        return lfn_in_transfer

    def mark_good(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        for lfn in files:
            try:
                document = self.db.document( getHashLfn(lfn) )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            if document['state'] != 'killed':
                outputLfn = document['lfn'].replace('store/temp', 'store', 1)
                try:
                    now = str(datetime.datetime.now())
                    last_update = time.time()
                    data = {}
                    data['end_time'] = now
                    data['state'] = 'done'
                    data['lfn'] = outputLfn
                    data['last_update'] = last_update
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + getHashLfn(lfn)
                    updateUri += "?" + urllib.urlencode(data)
                    self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                except Exception, ex:
                    msg =  "Error updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                try:
                    self.db.commit()
                except Exception, ex:
                    msg =  "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
        self.logger.debug("transferred file updated")

    def mark_failed(self, files=[], failures_reasons = [], force_fail = False ):
        """
        Something failed for these files so increment the retry count
        """
        for lfn in files:
            data = {}
            if not isinstance(lfn, dict):
                if 'temp' not in lfn:
                    temp_lfn = lfn.replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn
                perm_lfn = lfn
            else:
                if 'temp' not in lfn['value']:
                    temp_lfn = lfn['value'].replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn['value']
                perm_lfn = lfn['value']
            self.logger.debug("LFN to update %s" % temp_lfn)
            docId = getHashLfn(temp_lfn)
            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            if document['state'] != 'killed':
                now = str(datetime.datetime.now())
                last_update = time.time()
                # Prepare data to update the document in couch
                if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                    data['state'] = 'failed'
                    data['end_time'] = now
                else:
		    data['state'] = 'retry'
                    
                data['failure_reason'] = failures_reasons[files.index(lfn)] 
                data['last_update'] = last_update
                data['retry'] = now
                # Update the document in couch
                self.logger.debug("Marking failed %s" % docId)
                try:
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                    updateUri += "?" + urllib.urlencode(data)
                    self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                except Exception, ex:
                    msg =  "Error in updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                try:
                    self.db.commit()
                except Exception, ex:
                    msg =  "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
        self.logger.debug("failed file updated")

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

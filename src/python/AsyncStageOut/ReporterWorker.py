#!/usr/bin/env
#pylint: disable-msg=C0103
'''
The TransferWorker does the following:

a. make the ftscp copyjob
b. submit ftscp and watch
c. delete successfully transferred files from the database

There should be one worker per user transfer.


'''
import os
import re
import json
import time
import urllib
import logging
import datetime
import traceback
import subprocess

from WMCore.WMFactory import WMFactory
from WMCore.Credential.Proxy import Proxy
from WMCore.Database.CMSCouch import CouchServer

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Storage.TrivialFileCatalog import readTFC
from AsyncStageOut import getHashLfn
from AsyncStageOut import getDNFromUserName
from AsyncStageOut import getCommonLogFormatter

from RESTInteractions import HTTPRequests
from ServerUtilities import getHashLfn, generateTaskName,\
        PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping


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
    proxyPath = proxy.getProxyFilename(True)
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 3600:
        return True, proxyPath
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 0:
        return True, proxyPath
    return False, None


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
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return rc, stdout, stderr

        time.sleep(0.1)
    stdout, stderr = proc.communicate()
    rc = proc.returncode
    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))
    return rc, stdout, stderr


class ReporterWorker:

    def __init__(self, user, config):
        """
        store the user and tfc the worker
        """
        self.user = user
        self.config = config
        self.kibana_file = open(self.config.kibana_dir+"/"+self.user+"/"+"ended.json", 'a')
        self.dropbox_dir = '%s/dropbox/inputs' % self.config.componentDir
        logging.basicConfig(level=config.log_level)
        self.site_tfc_map = {}
        self.logger = logging.getLogger('AsyncTransfer-Reporter-%s' % self.user)
        formatter = getCommonLogFormatter(self.config)
        for handler in logging.getLogger().handlers:
            handler.setFormatter(formatter)
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
        except Exception as ex:
            msg = "Error retrieving the user DN"
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
                                  'credServerPath': self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN': self.config.serverDN,
                                  'uisource': self.uiSetupScript,
                                  'cleanEnvironment': getattr(self.config, 'cleanEnvironment', False)
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
        except Exception as ex:
            msg = "Error getting the user proxy"
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

        if self.config.isOracle:
            try:
                self.oracleDB = HTTPRequests(self.config.oracleDB,
                                             config.opsProxy,
                                             config.opsProxy)
            except Exception:
                self.logger.exception()
                raise
        else:
            server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
            self.db = server.connectDatabase(self.config.files_database)

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)
        self.commandTimeout = 1200
        self.max_retry = config.max_retry
        # Proxy management in Couch
        os.environ['X509_USER_PROXY'] = self.userProxy
        try:
            self.phedex = PhEDEx(responseType='xml',
                                 dict={'key':self.config.opsProxy,
                                       'cert':self.config.opsProxy})
        except Exception as e:
            self.logger.exception('PhEDEx exception: %s' % e)

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
            remove_good = True
            remove_failed = True
            failed_lfns = []
            failure_reason = []
            good_lfns = []
            self.logger.info("Updating %s" % input_file)
            if os.path.basename(input_file).startswith('Reporter'):
                try:
                    json_data = json.loads(open(input_file).read())
                except ValueError as e:
                    self.logger.error("Error loading %s" % e)
                    self.logger.debug('Removing %s' % input_file)
                    os.unlink(input_file)
                    continue
                except Exception as e:
                    self.logger.error("Error loading %s" % e)
                    self.logger.debug('Removing %s' % input_file)
                    os.unlink(input_file)
                    continue
                if json_data:
                    self.logger.debug('Inputs: %s %s %s' % (json_data['LFNs'], json_data['transferStatus'], json_data['failure_reason']))

                    if 'FAILED' or 'abandoned' or 'CANCELED' or 'lost' in json_data['transferStatus']:
                        # Sort failed files
                        failed_indexes = [i for i, x in enumerate(json_data['transferStatus']) if x == 'FAILED' or x == 'CANCELED']
                        abandoned_indexes = [i for i, x in enumerate(json_data['transferStatus']) if x == 'abandoned']
                        failed_indexes.extend(abandoned_indexes)
                        self.logger.info('failed indexes %s' % len(failed_indexes))
                        self.logger.debug('failed indexes %s' % failed_indexes)
                        for i in failed_indexes:
                            failed_lfns.append(json_data['LFNs'][i])
                            failure_reason.append(json_data['failure_reason'][i])
                        self.logger.debug('Marking failed %s %s' %(failed_lfns, failure_reason))
                        updated_failed_lfns = self.mark_failed(failed_lfns, failure_reason)

                        try:
                            self.kibana_file.write(str(time.time())+"\tFailed:\t"+str(len(updated_failed_lfns))+"\n")
                        except Exception as ex:
                            self.logger.error(ex)

                        if len(updated_failed_lfns) != len(failed_lfns):
                            remove_failed = False

                    if 'Done' or 'FINISHED' in json_data['transferStatus']:
                        # Sort good files
                        good_indexes = [i for i, x in enumerate(json_data['transferStatus']) if (x == 'Done' or x == 'FINISHED' or x == 'Finishing') ]
                        self.logger.info('good indexes %s' % len(good_indexes))
                        self.logger.debug('good indexes %s' % good_indexes)
                        for i in good_indexes:
                            good_lfns.append(json_data['LFNs'][i])
                        self.logger.info('Marking good %s' %(good_lfns))
                        updated_good_lfns = self.mark_good(good_lfns)

                        try:
                            self.kibana_file.write(str(time.time())+"\tFailed:\t"+str(len(updated_good_lfns))+"\n")
                        except Exception as ex:
                            self.logger.error(ex)

                        if len(updated_good_lfns) != len(good_lfns):
                            remove_good = False

                    if remove_good and remove_failed:
                        # Remove the json file
                        self.logger.debug('Removing %s' % input_file)
                        os.unlink( input_file )

                else:
                    self.logger.info('Empty file %s' % input_file)
                    continue
            else:
                self.logger.info('File not for the Reporter %s' % input_file)
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

    def mark_good(self, files):
        """
        Mark the list of files as tranferred
        """
        updated_lfn = []
        good_ids = []
        for it, lfn in enumerate(files):
            hash_lfn = getHashLfn(lfn)
            self.logger.info("Marking good %s" % hash_lfn)
            self.logger.debug("Marking good %s" % lfn)
            if not self.config.isOracle:
                try:
                    document = self.db.document(hash_lfn)
                except Exception as ex:
                    msg = "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
            self.logger.info("Doc %s Loaded" % hash_lfn)
            try:
                now = str(datetime.datetime.now())
                last_update = time.time()
                if self.config.isOracle:
                    docId = getHashLfn(lfn)
                    good_ids.append(docId)
                    updated_lfn.append(lfn)
                else:
                    if document['state'] != 'killed' and document['state'] != 'done' and document['state'] != 'failed':
                        outputLfn = document['lfn'].replace('store/temp', 'store', 1)
                        data = dict()
                        data['end_time'] = now
                        data['state'] = 'done'
                        data['lfn'] = outputLfn
                        data['last_update'] = last_update
                        updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + getHashLfn(lfn)
                        updateUri += "?" + urllib.urlencode(data)
                        self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                        updated_lfn.append(lfn)
                        self.logger.debug("Marked good %s" % lfn)
                    else: 
                        updated_lfn.append(lfn)
                    try:
                        self.db.commit()
                    except Exception as ex:
                        msg = "Error commiting documents in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue   
            except Exception as ex:
                msg = "Error updating document"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if self.config.isOracle:
                try:
                    data = dict()
                    data['asoworker'] = self.config.asoworker
                    data['subresource'] = 'updateTransfers'
                    data['list_of_ids'] = good_ids
                    data['list_of_transfer_state'] = ["DONE" for x in good_ids]
                    result = self.oracleDB.post(self.config.oracleFileTrans,
                                                data=encodeRequest(data))
                    self.logger.debug("Marked good %s" % good_ids)
                except Exception:
                    self.logger.exeption('Error updating document')
                    
            self.logger.info("Transferred file %s updated, removing now source file" %docId)
            try:
                docbyId = self.oracleDB.get(self.config.oracleFileTrans.replace('filetransfers','fileusertransfers'),
                                            data=encodeRequest({'subresource': 'getById', 'id': docId}))
                document = oracleOutputMapping(docbyId, None)[0]
            except Exception:
                msg = "Error getting file from source"
                self.logger.exception(msg)
                raise
            if document["source"] not in self.site_tfc_map:
                self.logger.debug("site not found... gathering info from phedex")
                self.site_tfc_map[document["source"]] = self.get_tfc_rules(document["source"])
            pfn = self.apply_tfc_to_lfn( '%s:%s' %(document["source"], lfn))
            self.logger.debug("File has to be removed now from source site: %s" %pfn)
            self.remove_files(self.userProxy, pfn)
            self.logger.debug("Transferred file removed from source")
        return updated_lfn

    def remove_files(self, userProxy, pfn):

        command = 'env -i X509_USER_PROXY=%s gfal-rm -v -t 180 %s'  % \
                  (userProxy, pfn)
        logging.debug("Running remove command %s" % command)
        try:
            rc, stdout, stderr = execute_command(command, self.logger, 3600)
        except Exception as ex:
            self.logger.error(ex)
            raise
        if rc:
            logging.info("Deletion command failed with output %s and error %s" %(stdout, stderr))
        else:
            logging.info("File Deleted.")
        return

    def get_tfc_rules(self, site):
        """
        Get the TFC regexp for a given site.
        """
        self.phedex.getNodeTFC(site)
        try:
            tfc_file = self.phedex.cacheFileName('tfc', inputdata={'node': site})
        except Exception:
            self.logger.exception('A problem occured when getting the TFC regexp: %s')
            return None
        return readTFC(tfc_file)

    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        try:
            site, lfn = tuple(file.split(':'))
        except Exception:
            self.logger.exception('It does not seem to be an lfn %s' %file.split(':'))
            return None
        if site in self.site_tfc_map:
            pfn = self.site_tfc_map[site].matchLFN('srmv2', lfn)
            # TODO: improve fix for wrong tfc on sites
            try:
                if pfn.find("\\") != -1: pfn = pfn.replace("\\","")
                if pfn.split(':')[0] != 'srm' and pfn.split(':')[0] != 'gsiftp' :
                    self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                    return None
            except IndexError:
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
            except AttributeError:
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
            return pfn
        else:
            self.logger.error('Wrong site %s!' % site)
            return None

    def mark_failed(self, files=[], failures_reasons=[], force_fail=False):
        """
        Something failed for these files so increment the retry count
        """
        updated_lfn = []
        for lfn in files:
            data = {}
            self.logger.debug("Document: %s" % lfn)
            if not isinstance(lfn, dict):
                if 'temp' not in lfn:
                    temp_lfn = lfn.replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn
            else:
                if 'temp' not in lfn['value']:
                    temp_lfn = lfn['value'].replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn['value']
            docId = getHashLfn(temp_lfn)
            # Load document to get the retry_count
            if self.config.isOracle:
                try:
                    self.logger.debug("Document: %s" %docId)
                    docbyId = self.oracleDB.get(self.config.oracleFileTrans.replace('filetransfers',
                                                                                    'fileusertransfers'),
                                                data=encodeRequest({'subresource': 'getById', 'id': docId}))
                    document = oracleOutputMapping(docbyId)[0]
                    data = dict()
                    data['asoworker'] = self.config.asoworker
                    data['subresource'] = 'updateTransfers'
                    data['list_of_ids'] = docId

                    if force_fail or document['transfer_retry_count'] + 1 > self.max_retry:
                        data['list_of_transfer_state'] = 'FAILED'
                        data['list_of_retry_value'] = 0
                    else:
                        data['list_of_transfer_state'] = 'RETRY'
                        fatal_error = self.determine_fatal_error(failures_reasons[files.index(lfn)])
                        if fatal_error:
                            data['list_of_transfer_state'] = 'FAILED'
                    data['list_of_failure_reason'] = failures_reasons[files.index(lfn)]
                    data['list_of_retry_value'] = 0

                    self.logger.debug("update: %s" % data)
                    result = self.oracleDB.post(self.config.oracleFileTrans,
                                                data=encodeRequest(data))
                    updated_lfn.append(lfn)
                    self.logger.debug("Marked failed %s" % lfn)
                except Exception as ex:
                    self.logger.error("Error updating document status: %s" %ex)
                    continue
            else:
                try:
                    document = self.db.document( docId )
                except Exception as ex:
                    msg = "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                if document['state'] != 'killed' and document['state'] != 'done' and document['state'] != 'failed':
                    now = str(datetime.datetime.now())
                    last_update = time.time()
                    # Prepare data to update the document in couch
                    if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                        data['state'] = 'failed'
                        data['end_time'] = now
                    else:
                        data['state'] = 'retry'
                        fatal_error = self.determine_fatal_error(failures_reasons[files.index(lfn)])
                        if fatal_error:
                            data['state'] = 'failed'
                            data['end_time'] = now

                    self.logger.debug("Failure list: %s" % failures_reasons)
                    self.logger.debug("Files: %s" % files)
                    self.logger.debug("LFN %s" % lfn)

                    data['failure_reason'] = failures_reasons[files.index(lfn)]
                    data['last_update'] = last_update
                    data['retry'] = now
                    # Update the document in couch
                    self.logger.debug("Marking failed %s" % docId)
                    try:
                        updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                        updateUri += "?" + urllib.urlencode(data)
                        self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                        updated_lfn.append(docId)
                        self.logger.debug("Marked failed %s" % docId)
                    except Exception as ex:
                        msg = "Error in updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                    try:
                        self.db.commit()
                    except Exception as ex:
                        msg = "Error commiting documents in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                else: updated_lfn.append(docId)
        self.logger.debug("failed file updated")
        return updated_lfn

    def determine_fatal_error(self, failure=""):
        """
        Determine if transfer error is fatal or not.
        """
        permanent_failure_reasons = [
                             ".*canceled because it stayed in the queue for too long.*",
                             ".*permission denied.*",
                             ".*disk quota exceeded.*",
                             ".*operation not permitted*",
                             ".*mkdir\(\) fail.*",
                             ".*open/create error.*",
                             ".*mkdir\: cannot create directory.*",
                             ".*does not have enough space.*"
                                    ]
        failure = str(failure).lower()
        for permanent_failure_reason in permanent_failure_reasons:
            if re.match(permanent_failure_reason, failure):
                return True
        return False

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

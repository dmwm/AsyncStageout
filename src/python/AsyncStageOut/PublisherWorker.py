#!/usr/bin/env
#pylint: disable-msg=C0103
'''
The Publisherworker does the following:

    a. looks to UserFileCacheEndpoint to get the workflow details
    b. looks to local database to get transferred files
    c. publish trnasferred files

There should be one worker per user transfer.
'''
import time
import logging
import subprocess, os, errno
import tempfile
import datetime
import traceback
import tarfile
import urllib
import shutil
import json
import types
import copy
from contextlib import contextmanager
from WMComponent.DBSUpload.DBSInterface import createProcessedDataset, createAlgorithm, insertFiles
from WMComponent.DBSUpload.DBSInterface import createPrimaryDataset,   createFileBlock, closeBlock
from WMComponent.DBSUpload.DBSInterface import createDBSFileFromBufferFile
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import DbsException
from DBSAPI.dbsMigrateApi import DbsMigrateApi
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WMFactory import WMFactory
from WMCore.Credential.Proxy import Proxy
from AsyncStageOut import getHashLfn
from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from WMCore.DataStructs.Run import Run
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

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

@contextmanager
def working_directory(directory):
    """
    This function is intended to be used as a ``with`` statement context
    manager. It allows you to replace code like this: 1
        original_directory = os.getcwd()
        try:
            os.chdir(some_dir)
            ... bunch of code ...
        finally:
            os.chdir(original_directory)

        with something simpler:

        with working_directory(some_dir):
            ... bunch of code ...
    """
    original_directory = os.getcwd()
    try:
        os.chdir(directory)
        yield directory
    finally:
        os.chdir(original_directory)

class PublisherWorker:

    def __init__(self, user, config):
        """
        store the user and tfc the worker
        """
        self.user = user[0]
        self.group = user[1]
        self.role = user[2]
        self.config = config
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('DBSPublisher-Worker-%s' % self.user)
        self.pfn_to_lfn_mapping = {}
        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.max_retry = config.publication_max_retry
        self.userFileCacheEndpoint = config.userFileCacheEndpoint
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.proxyDir = config.credentialDir
        self.myproxyServer = 'myproxy.cern.ch'
        query = {'group': True,
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}
        self.userDN = self.db.loadView('DBSPublisher', 'publish', query)['rows'][0]['key'][3]
        defaultDelegation = {
                                  'logger': self.logger,
                                  'credServerPath' : \
                                      self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN' : self.config.serverDN,
                                  'uisource' : self.uiSetupScript,
                            }
        if getattr(self.config, 'serviceCert', None):
            defaultDelegation['server_cert'] = self.config.serviceCert
        if getattr(self.config, 'serviceKey', None):
            defaultDelegation['server_key'] = self.config.serviceKey
        valid = False
        try:
            valid, proxy = getProxy(self.userDN, self.group, self.role, defaultDelegation, self.logger)
        except Exception, ex:
            msg =  "Error getting the user proxy"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        if valid:
            self.userProxy = proxy
        else:
            # Use the operator's proxy when the user proxy in invalid.
            # This will be moved soon
            self.logger.error('Did not get valid proxy. Setting proxy to host cert')
            self.userProxy = config.serviceCert
        self.ufc = UserFileCache({'endpoint': self.userFileCacheEndpoint,
                                  'cert': self.userProxy,
                                  'key': self.userProxy})
        os.environ['X509_USER_PROXY'] = self.userProxy
        self.phedexApi = PhEDEx(responseType='json')
        self.max_files_per_block = self.config.max_files_per_block


    def __call__(self):
        """
        1- check the nubmer of files in wf to publish if it is < max_files_per_block
	2- check in wf if now - last_finished_job > max_publish_time
        3- then call publish, mark_good, mark_failed for each wf
        """
        query = {'group':True}
        active_workflows = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        self.logger.debug('actives wfs %s' %active_workflows)
        for wf in active_workflows:
            lfn_ready = []
            wf_jobs_endtime = []
            workToDo = False
            query = {'reduce':False, 'key': wf['key']}
            active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
            self.logger.debug('actives files %s' %active_files)
            for file in active_files:
                wf_jobs_endtime.append(file['value'][5])
                lfn_ready.append(file['value'][1])
                self.logger.debug('LFNs ready %s' %lfn_ready)
            # If the number of files < max_files_per_block then check the oldness of the workflow
            if wf['value'] < self.max_files_per_block:
                wf_jobs_endtime.sort()
                if (( time.time() - wf_jobs_endtime[0] )/3600) < self.config.workflow_expiration_time:
                    continue
            if lfn_ready:
                try:
                    seName = self.phedexApi.getNodeSE( str(file['value'][0]) )
                    if not seName:
                        continue
                except Exception, ex:
                    msg =  "SE of %s cannot be retrieved" % str(file['value'][0])
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                failed_files, good_files = self.publish( str(file['key'][4]), str(file['value'][2]), str(file['key'][3]), str(file['key'][0]), str(file['value'][3]), str(file['value'][4]), str(seName), lfn_ready )
                self.mark_failed( failed_files )
                self.mark_good( good_files )

        self.logger.info('Publication algorithm completed')

    def mark_good(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        last_update = int(time.time())
        for lfn in files:
            try:
                data = {}
                data['publication_state'] = 'published'
                data['last_update'] = last_update
                updateUri = "/" + self.db.name + "/_design/DBSPublisher/_update/updateFile/" + getHashLfn(lfn.replace('store', 'store/temp', 1))
                updateUri += "?" + urllib.urlencode(data)
                self.logger.info(updateUri)
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

    def mark_failed( self, files=[] ):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        last_update = int(time.time())
        for lfn in files:
            data = {}
            docId = getHashLfn(lfn)
            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            # Prepare data to update the document in couch
            if len(document['publication_retry_count']) + 1 > self.max_retry:
                data['publication_state'] = 'publication_failed'
            else:
                data['publication_state'] = 'publishing'
            data['last_update'] = last_update
            data['retry'] = now
            # Update the document in couch
            try:
                updateUri = "/" + self.db.name + "/_design/DBSPublisher/_update/updateFile/" + docId
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

    def publish(self, workflow, dbsurl, userdn, userhn, inputDataset, sourceurl, targetSE, lfn_ready):
        """Perform the data publication of the workflow result.
          :arg str workflow: a workflow name
          :arg str dbsurl: the DBS URL endpoint where to publish
          :arg str inputDataset
          :arg str sourceurl
          :return: the publication status or result"""
        try:
            toPublish = self.readToPublishFiles(workflow, userhn, lfn_ready)
        except (tarfile.ReadError, RuntimeError):
            self.logger.error("Unable to read publication description files. ")
            return lfn_ready, []
        if not toPublish: return lfn_ready, []
        self.logger.info("Starting data publication for: " + str(workflow))
        failed, done, dbsResults = self.publishInDBS(userdn=userdn, sourceURL=sourceurl,
                                                     inputDataset=inputDataset, toPublish=toPublish,
                                                     destURL=dbsurl, targetSE=targetSE)
        self.logger.debug("DBS publication results %s" % dbsResults)
        return failed, done

    def readToPublishFiles(self, workflow, userhn, lfn_ready):
        """
        Download and read the files describing
        what needs to be published
        """
        def decodeAsString(a):
            """
            DBS is stupid and doesn't understand that unicode is a string: (if type(obj) == type(''))
            So best to convert as much of the decoded JSON to str as possible. Some is left over and handled by
            PoorMansBufferFile
            """
            newDict = {}
            for key, value in a.iteritems():
                if type(key) == types.UnicodeType:
                    key = str(key)
                if type(value) == types.UnicodeType:
                    value = str(value)
                newDict.update({key : value})
            return newDict

        tmpDir = tempfile.mkdtemp()
        toPublish = {}

        try:
            with working_directory(tmpDir):
                tgzName = '%s_publish.tgz' % workflow
                self.logger.info('Workflow ready for publication %s' % tgzName)
                tgzPath = self.ufc.download(name=tgzName, output=tgzName)
                tgz = tarfile.open(tgzName, 'r')
                for member in tgz.getmembers():
                    jsonFile = tgz.extractfile(member)
                    requestPublish = json.load(jsonFile, object_hook=decodeAsString)
                    for datasetName in requestPublish:
                        if toPublish.get(datasetName, None):
                            for lfn in requestPublish[datasetName]:
                                toPublish[datasetName].append(lfn)
                        else:
                            toPublish[datasetName] = copy.deepcopy(requestPublish[datasetName])
                # Clean toPublish keeping only completed files
                fail_files, toPublish = self.clean(lfn_ready, toPublish)
                tgz.close()
            shutil.rmtree(tmpDir, ignore_errors=True)
        except Exception, ex:
            msg =  "Error error when reading publication details for %s" %workflow
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        self.logger.debug('to_publish %s' %toPublish)
        return toPublish

    def clean(self, lfn_ready, toPublish):
        """
        Clean before publishing
        """
        fail_files = []
        new_toPublish = {}
        files_to_publish = []
        lfn_to_publish = []
        for ready in lfn_ready:
            done = False
            for datasetPath, files in toPublish.iteritems():
                new_temp_files = []
                lfn_dict = {}
                for lfn in files:
                    if lfn['lfn'] == ready:
                        lfn_to_publish.append(lfn['lfn'])
                        lfn_dict = lfn
                        lfn_dict['lfn'] = lfn['lfn'].replace('store/temp', 'store', 1)
                        new_temp_files.append(lfn_dict)
                        done = True
                        break
                if done:
                    break
            if new_temp_files:
                if new_toPublish.has_key(datasetPath):
                    new_toPublish[datasetPath].extend(new_temp_files)
                else:
                    new_toPublish[datasetPath] = new_temp_files
            files_to_publish.extend(lfn_to_publish)

        # Fail files that user does not ask to publish
        fail_files = filter(lambda x: x not in files_to_publish, lfn_ready)
        return fail_files, new_toPublish

    def dbsFiles_to_failed(self, dbsFiles):
        """
        Create failed files list from dbsFiles.
        """
        failed = []
        for file in dbsFiles:
            failed.append(file['LogicalFileName'])
        return failed

    def publishInDBS(self, userdn, sourceURL, inputDataset, toPublish, destURL, targetSE):
        """
        Actually do the publishing
        """
        class PoorMansBufferFile(dict):
            """
            The JSON we recover is almost a BufferFile, just needs a couple of methods
            to match the signature we need
            """

            def getRuns(self):
                """
                _getRuns_

                Retrieve a list of WMCore.DataStructs.Run objects that represent which
                run/lumi sections this file contains.
                """
                runs = []
                for run, lumis in self.get("runInfo", {}).iteritems():
                    runs.append(Run(run, *lumis))
                return runs

            def getParentLFNs(self):
                """
                _getParentLFNs_

                Retrieve a list of parent LFNs in dictionary form
                """
                parents = []

                for lfn in self.get("parentLFNs", []):
                    parents.append({'LogicalFileName': str(lfn)})
                return parents

        publish_next_iteration = []
        failed = []
        published = []
        results = {}

        # Start of publishInDBS
        blockSize = self.max_files_per_block
        migrateAPI = DbsMigrateApi(sourceURL, destURL)
        sourceApi = DbsApi({'url' : sourceURL, 'version' : 'DBS_2_0_9', 'mode' : 'POST'})
        destApi   = DbsApi({'url' : destURL,   'version' : 'DBS_2_0_9', 'mode' : 'POST'})

        try:
            migrateAPI.migrateDataset(inputDataset)
        except Exception, ex:
            msg =  "Error migrating dataset"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            return failed, published, results

        for datasetPath, files in toPublish.iteritems():
            results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0,}
            if not files:
                continue

            # Pull some info out of files[0]
            appName = files[0]["appName"]
            appVer  = files[0]["appVer"]
            appFam  = files[0]["appFam"]
            seName  = targetSE

            empty, primName, procName, tier =  datasetPath.split('/')

            # Find any files already in the dataset so we can skip them
            try:
                existingDBSFiles = destApi.listFiles(path=datasetPath)
                existingFiles = [x['LogicalFileName'] for x in existingDBSFiles]
                results[datasetPath]['existingFiles'] = len(existingFiles)
            except DbsException, ex:
                existingDBSFiles = []
                existingFiles = []
                msg =  "Error when listing files in DBS"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.debug(msg)

            workToDo = False

            # Is there anything to do?
            for file in files:
                if not file['lfn'] in existingFiles:
                    workToDo = True
                    break

            if not workToDo:
                self.logger.info("Nothing uploaded, %s has these files already or not enough files" % datasetPath)
                for file in files:
                    published.append(file['lfn'])
                continue

            # This must exist because we just migrated it
            primary = destApi.listPrimaryDatasets(primName)[0]
            algo = createAlgorithm(apiRef=destApi, appName=appName, appVer=appVer, appFam=appFam)
            processed = createProcessedDataset(algorithm=algo, apiRef=destApi, primary=primary, processedName=procName,
                                               dataTier=tier, group='Analysis', parent=inputDataset)

            # Convert JSON Buffer-like files into DBSFiles
            dbsFiles = []
            for file in files:
                if not file['lfn'] in existingFiles:
                    pmbFile = PoorMansBufferFile(file)
                    dbsFile = createDBSFileFromBufferFile(pmbFile, processed)
                    dbsFiles.append(dbsFile)
                published.append(file['lfn'])

            count = 0
            blockCount = 0
            if len(dbsFiles) < self.max_files_per_block:
                try:
                    block = createFileBlock(apiRef=destApi, datasetPath=processed, seName=seName)
                    status = insertFiles(apiRef=destApi, datasetPath=str(datasetPath), files=dbsFiles[count:count+blockSize], block=block, maxFiles=blockSize)
                    count += blockSize
                    blockCount += 1
                    status = closeBlock(apiRef=destApi, block=block)
                except Exception, ex:
                    failed = self.dbsFiles_to_failed(dbsFiles)
                    msg =  "Error when publishing"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
            else:
                while count < len(dbsFiles):
                    try:
                        if len(dbsFiles[count:len(dbsFiles)]) < self.max_files_per_block:
                            for file in dbsFiles[count:len(dbsFiles)]:
                                publish_next_iteration.append(file['LogicalFileName'])
                            count += blockSize
                            continue
                        block = createFileBlock(apiRef=destApi, datasetPath=processed, seName=seName)
                        status = insertFiles(apiRef=destApi, datasetPath=str(datasetPath), files=dbsFiles[count:count+blockSize], block=block, maxFiles=blockSize)
                        count += blockSize
                        blockCount += 1
                        status = closeBlock(apiRef=destApi, block=block)
                    except Exception, ex:
                        failed = self.dbsFiles_to_failed(dbsFiles)
                        msg =  "Error when publishing"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)

            results[datasetPath]['files'] = len(dbsFiles)
            results[datasetPath]['blocks'] = blockCount
        published = filter(lambda x: x not in failed + publish_next_iteration, published)
        self.logger.info("end of publication failed %s published %s publish_next_iteration %s results %s" %(failed, published, publish_next_iteration, results))
        return failed, published, results

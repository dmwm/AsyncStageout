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
import os
import datetime
import traceback
import tarfile
import urllib
import json
import types
import re
import uuid
import pprint
from WMComponent.DBSUpload.DBSInterface import createProcessedDataset, createAlgorithm, insertFiles
from WMComponent.DBSUpload.DBSInterface import createPrimaryDataset,   createFileBlock, closeBlock
from WMComponent.DBSUpload.DBSInterface import createDBSFileFromBufferFile
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import DbsException
from DBSAPI.dbsMigrateApi import DbsMigrateApi
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Credential.Proxy import Proxy
from AsyncStageOut import getHashLfn
from WMCore.DataStructs.Run import Run
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
import dbs.apis.dbsClient as dbsClient
import pycurl
import cStringIO
from AsyncStageOut import getDNFromUserName

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
        # If we're just testing publication, we skip the DB connection.
        if os.getenv("TEST_ASO"):
            self.db = None
        else:
            server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
            self.db = server.connectDatabase(self.config.files_database)
        self.max_retry = config.publication_max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.file_catalog = getattr(self.config, 'file_catalog', 'DBS3')
        self.proxyDir = config.credentialDir
        self.myproxyServer = 'myproxy.cern.ch'
        self.init = True
        self.userDN = ''
        query = {'group': True,
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}
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
        defaultDelegation = {
                                  'logger': self.logger,
                                  'credServerPath' : \
                                      self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN' : self.config.serverDN,
                                  'uisource' : self.uiSetupScript
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
        valid = False
        try:
            if not os.getenv("TEST_ASO"):
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
            self.logger.error('Did not get valid proxy. Setting proxy to ops proxy')
            info = {'server_key': self.config.opsProxy, 'server_cert': self.config.opsProxy, 'logger': self.logger}
            self.logger.info("Ops proxy info: %s" % str(info))
            opsProxy = Proxy({'server_key': self.config.opsProxy, 'server_cert': self.config.opsProxy, 'logger': self.logger})
            self.userDN = opsProxy.getSubject()
            self.userProxy = self.config.opsProxy
        self.cache_area = self.config.cache_area
        os.environ['X509_USER_PROXY'] = self.userProxy
        self.phedexApi = PhEDEx(responseType='json')
        self.max_files_per_block = self.config.max_files_per_block

    def __call__(self):
        """
        1- check the nubmer of files in wf to publish if it is < max_files_per_block
	2- check in wf if now - last_finished_job > max_publish_time
        3- then call publish, mark_good, mark_failed for each wf
        """
        active_user_workflows = []
        self.lfn_map = {}
        query = {'group':True}
        try:
            active_workflows = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        except Exception, e:
            self.logger.error('A problem occured when contacting couchDB to get the list of active WFs: %s' %e)
        for wf in active_workflows:
            if wf['key'][0] == self.user:
                active_user_workflows.append(wf)
        self.logger.debug('actives user wfs %s' % active_user_workflows)
        self.logger.info('actives files %s' %len(active_files))
        for user_wf in active_user_workflows:
            self.not_expired_wf = False
            self.forceFailure = False
            lfn_ready = []
            active_files = []
            wf_jobs_endtime = []
            workToDo = False
            query = {'reduce':False, 'key': user_wf['key']}
            try:
                active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
            except Exception, e:
                self.logger.error('A problem occured when contacting couchDB to get the list of active files for %s: %s' %(self.user, e))
            self.logger.info('actives files %s' %len(active_files))
            self.logger.debug('actives files %s' %active_files)
            for file in active_files:
                wf_jobs_endtime.append(int(time.mktime(time.strptime(\
                                       str(file['value'][5]), '%Y-%m-%d %H:%M:%S'))) \
                                       - time.timezone)
                # To move once the view is fixed
                lfn_hash = file['value'][1].replace('store/temp', 'store', 1)
                lfn_orig = lfn_hash.replace('.' + file['value'][1].split('.', 1)[1].split('/', 1)[0], '', 1)
                self.lfn_map[lfn_orig] = file['value'][1]
                lfn_ready.append(lfn_orig)
            self.logger.debug('LFNs %s ready %s %s' %(len(lfn_ready), lfn_ready, user_wf['value']))
            # If the number of files < max_files_per_block then check the oldness of the workflow
            if user_wf['value'] <= self.max_files_per_block:
                wf_jobs_endtime.sort()
                if (( time.time() - wf_jobs_endtime[0] )/3600) < self.config.workflow_expiration_time:
                    continue
                else:#check the ASO queue
                    if (( time.time() - wf_jobs_endtime[0] )/3600) < (self.config.workflow_expiration_time * 5):
                        workflow_expired = user_wf['key'][3]
                        query = {'reduce':True, 'group': True, 'key':workflow_expired}
                        try:
                            active_jobs = self.db.loadView('AsyncTransfer', 'JobsSatesByWorkflow', query)['rows']
                            if active_jobs[0]['value']['new'] != 0 or active_jobs[0]['value']['acquired'] != 0:
                                self.logger.info('Queue is not empty for workflow %s. Waiting next cycle' % workflow_expired)
                                continue
                        except Exception, e:
                            self.logger.error('A problem occured when contacting couchDB for the workflow status: %s' % e)
                            continue
                    else:
                        self.logger.info('Publish number of files minor of max_files_per_block.')
                        self.forceFailure = True
            else:
                if (( time.time() - wf_jobs_endtime[0] )/3600) < (self.config.workflow_expiration_time * 10):
                    self.not_expired_wf = True
                else:
                    self.logger.debug('Unexpected problem! Force the publication failure if it still cannot publish.')
                    self.forceFailure = True
            if lfn_ready:
                self.logger.debug("Retrieving SE Name from Phedex %s" %str(file['value'][0]))
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
                failed_files, good_files = self.publish( str(file['key'][3]), str(file['value'][2]), \
                                                         self.userDN, str(file['key'][0]), \
                                                         str(file['value'][3]), str(file['value'][4]), \
                                                         str(seName), lfn_ready )
                self.mark_failed( failed_files )
                self.mark_good( good_files )
        self.logger.info('Publications completed')

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
                lfn_db = self.lfn_map[lfn]
                updateUri = "/" + self.db.name + "/_design/DBSPublisher/_update/updateFile/" + \
                            getHashLfn(lfn_db)
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

    def mark_failed( self, files=[], forceFailure=False ):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        last_update = int(time.time())
        for lfn in files:
            data = {}
            lfn_db = self.lfn_map[lfn]
            docId = getHashLfn(lfn_db)
            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            # Prepare data to update the document in couch
            if len(document['publication_retry_count']) + 1 > self.max_retry or forceFailure:
                data['publication_state'] = 'publication_failed'
            else:
                data['publication_state'] = 'publishing'
            data['last_update'] = last_update
            data['retry'] = now
            # Update the document in couch
            try:
                updateUri = "/" + self.db.name + "/_design/DBSPublisher/_update/updateFile/" + docId
                updateUri += "?" + urllib.urlencode(data)
                self.logger.info(updateUri)
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
        if not toPublish:
            if self.forceFailure:
                self.logger.error("FWJR seems not in cache! Forcing the failure")
                self.mark_failed(lfn_ready, True)
            else:
                return [], []
        self.logger.info("Starting data publication in %s of %s" % (self.file_catalog, str(workflow)))
        if self.file_catalog == 'DBS3':
            failed, done, dbsResults = self.publishInDBS3(userdn=userdn, sourceURL=sourceurl,
                                                     inputDataset=inputDataset, toPublish=toPublish,
                                                     destURL=dbsurl, targetSE=targetSE, workflow=workflow)
        else:
            failed, done, dbsResults = self.publishInDBS2(userdn=userdn, sourceURL=sourceurl,
                                                     inputDataset=inputDataset, toPublish=toPublish,
                                                     destURL=dbsurl, targetSE=targetSE, workflow=workflow)
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
        self.logger.info("Starting Read cache...")
        buf = cStringIO.StringIO()
        res = []
        # TODO: input sanitization
        url = self.cache_area + '?taskname=' + workflow + '&filetype=EDM'
        try:
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.SSL_VERIFYPEER, 0)
            c.setopt(c.SSLKEY, self.userProxy)
            c.setopt(c.SSLCERT, self.userProxy)
            c.setopt(c.WRITEFUNCTION, buf.write)
            c.perform()
        except Exception, ex:
            msg =  "Error reading data from cache"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            return {}
        self.logger.info("Data read from cache...")
        try:
            json_string = buf.getvalue()
            buf.close()
            res = json.loads(json_string)
        except Exception, ex:
            msg =  "Error loading results. Trying next time!"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            return {}
        self.logger.info("%s records got from cache" % len(res['result']))
        self.logger.debug("results %s..." % res['result'])
        toPublish = {}
        for files in res['result']:
            outdataset = str(files['outdataset'])
            if toPublish.has_key(outdataset):
                toPublish[outdataset].append(files)
            else:
                toPublish[outdataset] = []
                toPublish[outdataset].append(files)
        fail_files, toPublish = self.clean(lfn_ready, toPublish)
        self.logger.info('to_publish %s files' %len(toPublish))
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
            if toPublish:
                for datasetPath, files in toPublish.iteritems():
                    new_temp_files = []
                    lfn_dict = {}
                    for lfn in files:
                        if lfn['lfn'] == ready:
                            lfn_to_publish.append(lfn['lfn'])
                            lfn_dict = lfn
                            lfn_dict['lfn'] = lfn['lfn'].replace('store/temp', 'store', 1)
                            new_temp_files.append(lfn_dict)
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

    def format_files(self, files):
        """
        Create failed files list from dbsFiles.
        """
        self.logger.info("Formatting FILES...")
        DBSFilesFormat = []
        for f in files:
            temp_dbs_file = {}
            temp_dbs_file['status'] = 'NOTUPLOADED'
            temp_dbs_file['appVer'] = f['swversion']
            temp_dbs_file['events'] = f['inevents']
            temp_dbs_file['checksums'] = {}
            temp_dbs_file['checksums']['adler32'] = f['adler32']
            temp_dbs_file['checksums']['cksum'] = f['cksum']
            temp_dbs_file['datasetpath'] = f['outdataset']
            temp_dbs_file['globalTag'] = f['globaltag']
            temp_dbs_file['appName'] = 'cmsRun'
            temp_dbs_file['configContent'] = 'https://crab3-test/couchdb;;6263396ded191a1c851f5b5e440d0641'
            temp_dbs_file['appFam'] = 'out'
            temp_dbs_file['psetHash'] = f['publishname'].split('-')[1]
            temp_dbs_file['processing_ver'] = 'null'
            temp_dbs_file['acquisition_era'] = 'null'
            temp_dbs_file['id'] = f['pandajobid']
            temp_dbs_file['lfn'] = f['lfn']
            temp_dbs_file['runInfo'] = f['runlumi']
            temp_dbs_file['parentLFNs'] = f['parents']
            temp_dbs_file['size'] = f['filesize']
            temp_dbs_file['acquisitionera'] = f['acquisitionera']
            temp_dbs_file['location'] = []
            temp_dbs_file['location'].append(str(f['location']))
            DBSFilesFormat.append(temp_dbs_file)
        return DBSFilesFormat


    def publishInDBS2(self, userdn, sourceURL, inputDataset, toPublish, destURL, targetSE, workflow):
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
        # Start of publishInDBS2
        blockSize = self.max_files_per_block
        migrateAPI = DbsMigrateApi(sourceURL, destURL)
        sourceApi = DbsApi({'url' : sourceURL, 'version' : 'DBS_2_0_9', 'mode' : 'POST'})
        destApi   = DbsApi({'url' : destURL,   'version' : 'DBS_2_0_9', 'mode' : 'POST'})
        try:
            if inputDataset and inputDataset != 'NULL':
                migrateAPI.migrateDataset(inputDataset)
            else:
                inputDataset = None
                self.logger.info('Processing Monte Carlo production workflow! No input dataset')
        except Exception, ex:
            msg =  "Error migrating dataset"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            return failed, published, results
        self.logger.info("Data migrated...")
        for datasetPath, files in toPublish.iteritems():
            results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0,}
            if not files:
                continue
            appName = 'cmsRun'
            appVer  = files[0]["swversion"]
            appFam  = 'output'
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
                self.logger.error(msg)
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
            # This must exist for DATA because we just migrated it
            primary = {}
            if inputDataset:
                self.logger.debug("Listing migrated data from %s" %primName)
                primary = destApi.listPrimaryDatasets(primName)[0]
            else:
                primary = createPrimaryDataset(apiRef=destApi, primaryName=primName)
            algo = createAlgorithm(apiRef=destApi, appName=appName, appVer=appVer, appFam=appFam)
            processed = createProcessedDataset(algorithm=algo, apiRef=destApi, primary=primary, processedName=procName,
                                               dataTier=tier, group='Analysis', parent=inputDataset)
            # Convert JSON Buffer-like files into DBSFiles
            self.logger.info("The publication of %s is effectively starting here..." %len(files))
            self.logger.debug("The publication of %s is effectively starting here..." %files)
            # Format files accordingly before publishing
            files_to_publish = self.format_files(files)
            self.logger.debug("Got formatted files %s" %files_to_publish)
            dbsFiles = []
            for file in files_to_publish:
                if not file['lfn'] in existingFiles:
                    pmbFile = PoorMansBufferFile(file)
                    dbsFile = createDBSFileFromBufferFile(pmbFile, processed)
                    dbsFiles.append(dbsFile)
                published.append(file['lfn'])
            count = 0
            blockCount = 0
            self.logger.debug("Blocks creation of %s is effectively starting here..." %dbsFiles)
            if len(dbsFiles) < self.max_files_per_block:
                self.logger.info("WF %s is not expired: %s" %(workflow, self.not_expired_wf))
                if not self.not_expired_wf:
                    try:
                        block = createFileBlock(apiRef=destApi, datasetPath=processed, seName=seName)
                        status = insertFiles(apiRef=destApi, datasetPath=str(datasetPath), \
                                             files=dbsFiles[count:count+blockSize], \
                                             block=block, maxFiles=blockSize)
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
                    self.logger.debug("WF %s is not expired: %s" %(workflow, self.not_expired_wf))
                    return [], [], []
            else:
                while count < len(dbsFiles):
                    try:
                        if len(dbsFiles[count:len(dbsFiles)]) < self.max_files_per_block:
                            for file in dbsFiles[count:len(dbsFiles)]:
                                publish_next_iteration.append(file['LogicalFileName'])
                            count += blockSize
                            continue
                        block = createFileBlock(apiRef=destApi, datasetPath=processed, seName=seName)
                        status = insertFiles(apiRef=destApi, datasetPath=str(datasetPath), \
                                             files=dbsFiles[count:count+blockSize], \
                                             block=block, maxFiles=blockSize)
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
        self.logger.info("end of publication failed %s published %s publish_next_iteration %s results %s" \
                         %(failed, published, publish_next_iteration, results))
        return failed, published, results


    def format_file_3(self, file, output_config, dataset):
        nf = {'logical_file_name': file['lfn'],
              'file_type': 'EDM',
              'check_sum': unicode(file['cksum']),
              'event_count': file['inevents'],
              'file_size': file['filesize'],
              'adler32': file['adler32'],
              'file_parent_list': [{'file_parent_lfn': i} for i in file['parents']],
             }
        file_lumi_list = []
        for run, lumis in file['runlumi'].items():
            for lumi in lumis:
                file_lumi_list.append({'lumi_section_num': int(lumi), 'run_num': int(run)})
        nf['file_lumi_list'] = file_lumi_list
        if file.get("md5") != "asda" and file.get("md5") != "NOTSET": # asda is the silly value that MD5 defaults to
            nf['md5'] = file['md5']
        return nf


    def migrateDBS3(self, migrateApi, destReadApi, sourceURL, inputDataset):

        # TODO: include support of private MC WF publication
        try:
            existing_datasets = destReadApi.listDatasets(dataset=inputDataset, detail=True)
        except HTTPError, he:
            msg = str(he)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            self.logger.error("Request to list invalid dataset %s failed." % inputDataset)
            return []
        should_migrate = False
        if not existing_datasets or (existing_datasets[0]['dataset'] != inputDataset):
            should_migrate = True
            self.logger.debug("Dataset %s must be migrated; not in the destination DBS." % inputDataset)
        if not should_migrate:
            # The dataset exists in the destination; make sure source and destination
            # have the same blocks.
            existing_blocks = set([i['block_name'] for i in destReadApi.listBlocks(dataset=inputDataset)])
            proxy = os.environ.get("SOCKS5_PROXY")
            sourceApi = dbsClient.DbsApi(url=sourceURL, proxy=proxy)
            source_blocks = set([i['block_name'] for i in sourceApi.listBlocks(dataset=inputDataset)])
            blocks_to_migrate = source_blocks - existing_blocks
            self.logger.debug("Dataset %s in destination DBS with %d blocks; %d blocks in source." % (inputDataset, len(existing_blocks), len(source_blocks)))
            if blocks_to_migrate:
                self.logger.debug("%d blocks (%s) must be migrated to destination dataset %s." % (len(existing_blocks), ", ".join(existing_blocks), inputDataset) )
                should_migrate = True
        if should_migrate:
            data = {'migration_url': sourceURL, 'migration_input': inputDataset}
            self.logger.debug("About to submit migrate request for %s" % str(data))
            try:
                result = migrateApi.submitMigration(data)
            except HTTPError, he:
                if he.msg.find("Requested dataset %s is already in destination" % inputDataset) >= 0:
                    self.logger.info("Migration API believes this dataset has already been migrated.")
                    return destReadApi.listDatasets(dataset=inputDataset, detail=True)
                self.logger.exception("Request to migrate %s failed." % inputDataset)
                return []
            self.logger.info("Result of migration request %s" % str(result))
            id = result.get("migration_details", {}).get("migration_request_id")
            if id == None:
                self.logger.error("Migration request failed to submit.")
                self.logger.error("Migration request results: %s" % str(result))
                return []
            self.logger.debug("Migration ID: %s" % id)
            time.sleep(1)
            # Wait for up to 60 seconds, then return to the main loop.  Note we don't
            # fail or cancel anything.  Just retry later.
            # States:
            # 0=PENDING
            # 1=IN PROGRESS
            # 2=COMPLETED
            # 3=FAILED
            for i in range(60):
                self.logger.debug("About to get migration request for %s." % id)
                status = migrateApi.statusMigration(migration_rqst_id=id)
                state = status.get("migration_status")
                self.logger.debug("Migration status: %s" % state)
                if state == 0 or state == 1:
                    time.sleep(1)

            if state == 0 or state == 1:
                self.logger.info("Migration of %s has taken too long - will delay publication." % inputDataset)
                return []
            if state == 3:
                self.logger.info("Migration of %s has failed.  Full status: %s" % (inputDataset, str(status)))
                return []
            self.logger.info("Migration of %s is complete." % inputDataset)
            existing_datasets = destReadApi.api.listDatasets(dataset=inputDataset, detail=True)
        return existing_datasets

    def createBulkBlock(self, output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files):
        file_conf_list = []
        file_parent_list = []
        for file in files:
            file_conf = output_config.copy()
            file_conf_list.append(file_conf)
            file_conf['lfn'] = file['logical_file_name']
            for parent_lfn in file.get('file_parent_list', []):
                file_parent_list.append({'logical_file_name': file['logical_file_name'], 'parent_logical_file_name': parent_lfn['file_parent_lfn']})
            del file['file_parent_list']
        blockDump = { \
            'dataset_conf_list': [output_config],
            'file_conf_list': file_conf_list,
            'files': files,
            'processing_era': processing_era_config,
            'primds': primds_config,
            'dataset': dataset_config,
            'acquisition_era': acquisition_era_config,
            'block': block_config,
            'file_parent_list': file_parent_list,
        }
        blockDump['block']['file_count'] = len(files)
        blockDump['block']['block_size'] = sum([int(file[u'file_size']) for file in files])
        return blockDump

    def publishInDBS3(self, userdn, sourceURL, inputDataset, toPublish, destURL, targetSE, workflow):
        """
        Publish files into DBS3
        """
        publish_next_iteration = []
        failed = []
        published = []
        results = {}
        blockSize = self.max_files_per_block

        # Normalize URLs
        WRITE_PATH = "/DBSWriter"
        MIGRATE_PATH = "/DBSMigrate"
        READ_PATH = "/DBSReader"
        if destURL.endswith(WRITE_PATH):
            destReadURL = destURL[:-len(WRITE_PATH)] + READ_PATh
            migrateURL = destURL[:-len(WRITE_PATH)] + MIGRATE_PATH
        else:
            migrateURL = destURL + MIGRATE_PATH
            destReadURL = destURL + READ_PATH
            destURL += WRITE_PATH
        if not sourceURL.endswith(READ_PATH):
            sourceURL += READ_PATH

        self.logger.debug("Migrate datasets")
        # TODO: Migration of datasets.
        proxy = os.environ.get("SOCKS5_PROXY")
        destApi = dbsClient.DbsApi(url=destURL, proxy=proxy)
        destReadApi = dbsClient.DbsApi(url=destReadURL, proxy=proxy)
        migrateApi = dbsClient.DbsApi(url=migrateURL, proxy=proxy)

        self.logger.debug("Submit migration %s" %sourceURL)
        # Submit migration
        sourceApi = dbsClient.DbsApi(url=sourceURL)
        try:
            existing_datasets = self.migrateDBS3(migrateApi, destReadApi, sourceURL, inputDataset)
        except Exception, ex:
            msg = str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            # TODO: Include correctly the publication of private MC WF
            existing_datasets = []
        if not existing_datasets:
            self.logger.info("Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, sourceURL, migrateURL))
            return [], [], []

        # Get basic data about the parent dataset
        if not (existing_datasets and existing_datasets[0]['dataset'] == inputDataset):
            self.logger.error("Inconsistent state: %s migrated, but listDatasets didn't return any information")
            return [], [], []
        primary_ds_type = existing_datasets[0]['primary_ds_type']
        acquisition_era_name = existing_datasets[0]['acquisition_era_name']
        acquisition_era_name = "CRAB"

        # There's little chance this is correct, but it's our best guess for now.
        existing_output = destApi.listOutputConfigs(dataset=inputDataset)
        if not existing_output:
            self.logger.error("Unable to list output config for input dataset %s." % inputDataset)
        existing_output = existing_output[0]
        global_tag = existing_output['global_tag']

        processing_era_config = {'processing_version': 1, 'description': 'CRAB3_processing_era'}

        self.logger.debug("iterate for publication")
        for datasetPath, files in toPublish.iteritems():
            results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0,}
            dbsDatasetPath = datasetPath

            if not files:
                continue

            appName = 'cmsRun'
            appVer  = files[0]["swversion"]
            appFam  = 'output'
            seName  = targetSE
            # TODO: this is invalid:
            pset_hash = files[0]['publishname'].split("-")[-1]
            gtag = str(files[0]['globaltag'])
            if gtag == "None":
                gtag = global_tag
            acquisitionera = str(files[0]['acquisitionera'])
            if acquisitionera == "null":
                acquisitionera = acquisition_era_name
            empty, primName, procName, tier =  dbsDatasetPath.split('/')
            # Change bbockelm-name-pset to bbockelm_name-pset
            procName = "_".join(procName.split("-")[:2]) + "-" + "-".join(procName.split("-")[2:])
            # NOTE: DBS3 currently chokes if we don't include a processing version / acquisition era
            procName = "%s-%s-v%d" % (acquisition_era_name, procName, processing_era_config['processing_version'])
            dbsDatasetPath = "/".join([empty, primName, procName, tier])

            primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
            self.logger.debug("About to insert primary dataset: %s" % str(primds_config))
            destApi.insertPrimaryDataset(primds_config)
            self.logger.debug("Successfully inserting primary dataset %s" % primName)

            # Find any files already in the dataset so we can skip them
            try:
                existingDBSFiles = destApi.listFiles(dataset=dbsDatasetPath)
                existingFiles = [x['logical_file_name'] for x in existingDBSFiles]
                results[datasetPath]['existingFiles'] = len(existingFiles)
            except DbsException, ex:
                existingDBSFiles = []
                existingFiles = []
                msg =  "Error when listing files in DBS"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
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

            acquisition_era_config = {'acquisition_era_name': acquisitionera, 'start_date': 0}

            output_config = {'release_version': appVer,
                             'pset_hash': pset_hash,
                             'app_name': appName,
                             'output_module_label': 'o', #TODO
                             'global_tag': global_tag,
                            }
            self.logger.debug("Published output config.")

            dataset_config = {'dataset': dbsDatasetPath,
                              'processed_ds_name': procName,
                              'data_tier_name': tier,
                              'acquisition_era_name': acquisitionera,
                              'dataset_access_type': '*', # TODO
                              'physics_group_name': 'CRAB3',
                              'last_modification_date': int(time.time()),
                             }
            self.logger.info("About to insert dataset: %s" % str(dataset_config))
            #destApi.insertDataset(dataset_config)
            del dataset_config['acquisition_era_name']
            #dataset_config['acquisition_era'] = acquisition_era_config

            dbsFiles = []
            for file in files:
                if not file['lfn'] in existingFiles:
                    dbsFiles.append(self.format_file_3(file, output_config, dbsDatasetPath))
                published.append(file['lfn'])
            count = 0
            blockCount = 0
            if len(dbsFiles) < self.max_files_per_block:
                self.logger.info("WF %s is not expired: %s" %(workflow, self.not_expired_wf))
                if not self.not_expired_wf:
                    block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
                    files_to_publish = dbsFiles[count:count+blockSize]
                    try:
                        block_config = {'block_name': block_name, 'origin_site_name': seName, 'open_for_writing': 0}
                        self.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                        blockDump = self.createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                        destApi.insertBulkBlock(blockDump)
                        count += blockSize
                        blockCount += 1
                    except Exception, ex:
                        failed += [i['logical_file_name'] for i in files_to_publish]
                        msg =  "Error when publishing"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                else:
                    self.logger.debug("WF %s is not expired: %s" %(workflow, self.not_expired_wf))
                    return [], [], []
            else:
                while count < len(dbsFiles):
                    block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
                    files_to_publish = dbsFiles[count:count+blockSize]
                    try:
                        if len(dbsFiles[count:len(dbsFiles)]) < self.max_files_per_block:
                            for file in dbsFiles[count:len(dbsFiles)]:
                                publish_next_iteration.append(file['logical_file_name'])
                            count += blockSize
                            continue
                        block_config = {'block_name': block_name, 'origin_site_name': seName, 'open_for_writing': 0}
                        #self.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                        blockDump = self.createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, acquisition_era_config, block_config, files_to_publish)
                        self.logger.debug("Block to insert: %s\n" % pprint.pformat(blockDump))
                        destApi.insertBulkBlock(blockDump)

                        count += blockSize
                        blockCount += 1
                    except Exception, ex:
                        failed += [i['logical_file_name'] for i in files_to_publish]
                        msg =  "Error when publishing (%s) " % ", ".join(failed)
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        count += blockSize
            results[datasetPath]['files'] = len(dbsFiles) - len(failed)
            results[datasetPath]['blocks'] = blockCount
        published = filter(lambda x: x not in failed + publish_next_iteration, published)
        self.logger.info("End of publication status: failed %s, published %s, publish_next_iteration %s, results %s" \
                         % (failed, published, publish_next_iteration, results))
        return failed, published, results



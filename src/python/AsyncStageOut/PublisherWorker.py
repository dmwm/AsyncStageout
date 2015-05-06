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
from WMCore.Database.CMSCouch import CouchServer
from AsyncStageOut import getHashLfn
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
import dbs.apis.dbsClient as dbsClient
from WMCore.Services.pycurl_manager import RequestHandler
import cStringIO
from AsyncStageOut import getDNFromUserName
from AsyncStageOut import getProxy

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
        self.max_retry = config.publication_max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.proxyDir = config.credentialDir
        self.myproxyServer = 'myproxy.cern.ch'
        self.init = True
        self.userDN = ''
        query = {'group': True,
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}
        self.logger.debug("Trying to get DN")
        try:
            self.userDN = getDNFromUserName(self.user, self.logger)
        except Exception as ex:
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
                defaultDelegation['userDN'] = self.userDN
                defaultDelegation['group'] = self.group
                defaultDelegation['role'] = self.role
                valid, proxy = getProxy(defaultDelegation, self.logger)
        except Exception as ex:
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
        # If we're just testing publication, we skip the DB connection.
        if os.getenv("TEST_ASO"):
            self.db = None
        else:
            server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
            self.db = server.connectDatabase(self.config.files_database)
        self.phedexApi = PhEDEx(responseType='json')
        self.max_files_per_block = self.config.max_files_per_block
        self.block_publication_timeout = self.config.block_closure_timeout
        self.publish_dbs_url = self.config.publish_dbs_url

        WRITE_PATH = "/DBSWriter"
        MIGRATE_PATH = "/DBSMigrate"
        READ_PATH = "/DBSReader"

        if self.publish_dbs_url.endswith(WRITE_PATH):
            self.publish_read_url = self.publish_dbs_url[:-len(WRITE_PATH)] + READ_PATH
            self.publish_migrate_url = self.publish_dbs_url[:-len(WRITE_PATH)] + MIGRATE_PATH
        else:
            self.publish_migrate_url = self.publish_dbs_url + MIGRATE_PATH
            self.publish_read_url = self.publish_dbs_url + READ_PATH
            self.publish_dbs_url += WRITE_PATH

        try:
            self.connection = RequestHandler(config={'timeout': 300, 'connecttimeout' : 300})
        except Exception as ex:
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.debug(msg)

    def __call__(self):
        """
        1- check the nubmer of files in wf to publish if it is < max_files_per_block
	2- check in wf if now - last_finished_job > max_publish_time
        3- then call publish, mark_good, mark_failed for each wf
        """
        active_user_workflows = []
        self.lfn_map = {}
        query = {'group':True, 'startkey':[self.user, self.group, self.role], 'endkey':[self.user, self.group, self.role, {}]}
        try:
            active_user_workflows = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        except Exception as e:
            self.logger.error('A problem occured when contacting couchDB to get the list of active WFs: %s' %e)
            self.logger.info('Publications for %s will be retried next time' % self.user)
            return
        self.logger.debug('active user wfs %s' % active_user_workflows)
        self.logger.info('active user wfs %s' %len(active_user_workflows))
        now = time.time()
        last_publication_time = 0
        for user_wf in active_user_workflows:
            self.forceFailure = False
            lfn_ready = []
            active_files = []
            wf_jobs_endtime = []
            workflow_status = ''
            workToDo = False
            query = {'reduce':False, 'key': user_wf['key']}#'stale': 'ok'}
            try:
                active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
            except Exception as e:
                self.logger.error('A problem occured to retrieve the list of active files for %s: %s' %(self.user, e))
                self.logger.info('Publications of %s will be retried next time' % user_wf['key'])
                continue
            self.logger.info('active files in %s: %s' %(user_wf['key'], len(active_files)))
            for file in active_files:
                if file['value'][4]:
                    # Get the list of jobs end_time for each WF.
                    wf_jobs_endtime.append(int(time.mktime(time.strptime(\
                                           str(file['value'][4]), '%Y-%m-%d %H:%M:%S'))) \
                                           - time.timezone)
                # To move once the remote stageout from WN is fixed.
                if "/temp/temp" in file['value'][1]:
                    lfn_hash = file['value'][1].replace('store/temp/temp', 'store', 1)
                else:
                    lfn_hash = file['value'][1].replace('store/temp', 'store', 1)
                if lfn_hash.startswith("/store/user"):
                    lfn_orig = lfn_hash.replace('.' + file['value'][1].split('.', 1)[1].split('/', 1)[0], '', 1)
                else:
                    lfn_orig = lfn_hash
                if "/temp/temp" in file['value'][1]:
                    self.lfn_map[lfn_orig] = file['value'][1].replace('temp/temp', 'temp', 1)
                else:
                    self.lfn_map[lfn_orig] = file['value'][1]
                lfn_ready.append(lfn_orig)
            self.logger.info('%s LFNs ready in %s active files' %(len(lfn_ready), user_wf['value']))

            # Check if the workflow is still valid
            if wf_jobs_endtime:
                self.logger.debug('jobs_endtime of %s: %s' % (user_wf, wf_jobs_endtime))
                wf_jobs_endtime.sort()
                workflow_duration = (now - wf_jobs_endtime[0]) / 86400
                if workflow_duration > self.config.workflow_expiration_time:
                    self.logger.info('Workflow %s expired since %s days! Force the publication failure.' % (\
                                      user_wf['key'], (workflow_duration - self.config.workflow_expiration_time)))
                    self.forceFailure = True

            # If the number of files < max_files_per_block check first workflow status and then the last publication time
            workflow = user_wf['key'][3]
            if user_wf['value'] <= self.max_files_per_block:
                url = '/'.join(self.cache_area.split('/')[:-1]) + '/workflow'
                self.logger.info("Starting retrieving the status of %s from %s ." % (workflow, url))
                buf = cStringIO.StringIO()
                data = {'workflow': workflow}
                header = {"Content-Type ":"application/json"}
                try:
                    response, res_ = self.connection.request(url, data, header, doseq=True, ckey=self.userProxy, cert=self.userProxy)#, verbose=True)# for debug
                except Exception as ex:
                    msg = "Error reading the status of %s from cache." % workflow
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                self.logger.info("Status of %s read from cache..." % workflow)
                try:
                    buf.close()
                    res = json.loads(res_)
                    workflow_status = res['result'][0]['status']
                    self.logger.info("Workflow status is %s" % workflow_status)
                except ValueError:
                    self.logger.error("Workflow %s is removed from WM" % workflow)
                    workflow_status = 'REMOVED'
                except Exception as ex:
                    msg = "Error loading the status of %s !" % workflow
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue

                if workflow_status not in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED']:
                    query = {'reduce':True, 'key': user_wf['key']}
                    try:
                        last_publication_time = self.db.loadView('DBSPublisher', 'last_publication', query)['rows']
                    except Exception as e:
                        self.logger.error('Cannot get last publication time from Couch for %s: %s' %(user_wf['key'], e))
                        continue
                    self.logger.debug('last_publication_time of %s: %s' % (user_wf['key'], last_publication_time))
                    if last_publication_time:
                        self.logger.debug('last published block: %s' % last_publication_time[0]['value']['max'])
                        wait_files = now - last_publication_time[0]['value']['max']
                        if wait_files > self.block_publication_timeout:
                            self.logger.info('Forse the publication since the last publication was %s ago' % wait_files)
                            self.logger.info('Publish number of files minor of max_files_per_block for %s.' % user_wf['key'])
                        elif not self.forceFailure:
                            continue
                        else:
                            pass
                else:
                    self.logger.info('Closing the publication of %s since it is completed' % workflow)

            if lfn_ready:
                pnn = str(file['value'][0])
                failed_files, failure_reason, good_files = self.publish( str(file['key'][3]), \
                                                         self.userDN, str(file['key'][0]), \
                                                         str(file['value'][2]), str(file['value'][3]), \
                                                         str(pnn), lfn_ready )
                self.mark_failed( failed_files, failure_reason )
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
            except Exception as ex:
                msg =  "Error updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
        try:
            self.db.commit()
        except Exception as ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def mark_failed( self, files=[], failure_reason="", forceFailure=False ):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        last_update = int(time.time())
        for lfn in files:
            self.logger.info("Marking failed %s" % lfn)
            data = {}
            lfn_db = self.lfn_map[lfn]
            docId = getHashLfn(lfn_db)
            self.logger.info("Marking failed %s of %s" % (docId, lfn_db))
            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception as ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            # Prepare data to update the document in couch
            if len(document['publication_retry_count']) + 1 > self.max_retry or forceFailure:
                data['publication_state'] = 'publication_failed'
            else:
                data['publication_state'] = 'publishing'
            data['last_update'] = last_update
            data['retry'] = now
            data['publication_failure_reason'] = failure_reason

            # Update the document in couch
            try:
                updateUri = "/" + self.db.name + "/_design/DBSPublisher/_update/updateFile/" + docId
                updateUri += "?" + urllib.urlencode(data)
                self.logger.info(updateUri)
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception as ex:
                msg =  "Error in updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
        try:
            self.db.commit()
        except Exception as ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def publish(self, workflow, userdn, userhn, inputDataset, sourceurl, pnn, lfn_ready):
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
                msg = "FWJR not found in cache! Forcing the failure"
                self.logger.error(msg)
                self.mark_failed(lfn_ready, msg, True)
            else:
                return [], "", []
        self.logger.info("Starting data publication in %s of %s" % ("DBS", str(workflow)))
        failed, failure_reason, done, dbsResults = self.publishInDBS3(userdn=userdn, sourceURL=sourceurl,
                                                      inputDataset=inputDataset, toPublish=toPublish,
                                                      pnn=pnn, workflow=workflow)
        self.logger.debug("DBS publication results %s" % dbsResults)
        return failed, failure_reason, done

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
        self.logger.info("Starting Read cache for %s..." % workflow)
        buf = cStringIO.StringIO()
        res = []
        # TODO: input sanitization
        header = {"Content-Type ":"application/json"}
        data = {'taskname': workflow, 'filetype': 'EDM'}
        url = self.cache_area
        try:
            response, res_ = self.connection.request(url, data, header, doseq=True, ckey=self.userProxy, cert=self.userProxy)#, verbose=True)# for debug
        except Exception as ex:
            msg =  "Error reading data from cache"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            return {}
        self.logger.info("Data read from cache...")
        try:
            buf.close()
            res = json.loads(res_)
        except Exception as ex:
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
        # FIXME: the following code takes the last user dataset referred in the cache area.
        # So it will not work properly if there are more than 1 user dataset to publish.
        if toPublish:
            self.logger.info("Checking the metadata of %s LFNs ready in %s records got from cache" %(len(lfn_ready), len(toPublish[outdataset])))
            fail_files, toPublish = self.clean(lfn_ready, toPublish)
            if toPublish.has_key(outdataset):
                self.logger.info('to_publish %s files in %s' % (len(toPublish[outdataset]), outdataset))
            else:
                self.logger.error('No files to publish in %s' % outdataset)
                self.logger.error('The metadata is still not available in the cache area or \
                                   there are more than 1 dataset to publish for the same workflow')
            self.logger.debug('to_publish %s' % toPublish)
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
        self.logger.info('Cleaning ends')
        return fail_files, new_toPublish

    def format_file_3(self, file):
        nf = {'logical_file_name': file['lfn'],
              'file_type': 'EDM',
              'check_sum': unicode(file['cksum']),
              'event_count': file['inevents'],
              'file_size': file['filesize'],
              'adler32': file['adler32'],
              'file_parent_list': [{'file_parent_lfn': i} for i in set(file['parents'])],
             }
        file_lumi_list = []
        for run, lumis in file['runlumi'].items():
            for lumi in lumis:
                file_lumi_list.append({'lumi_section_num': int(lumi), 'run_num': int(run)})
        nf['file_lumi_list'] = file_lumi_list
        if file.get("md5") != "asda" and file.get("md5") != "NOTSET": # asda is the silly value that MD5 defaults to
            nf['md5'] = file['md5']
        return nf


    def migrateByBlockDBS3(self, migrateApi, destReadApi, sourceApi, inputDataset, inputBlocks = None):
        """
        Submit one migration request for each block that needs to be migrated.
        If inputBlocks is missing, migrate the full dataset.
        :return: - the output of destReadApi.listDatasets(dataset = inputDataset)
                   if all migrations have finished ok,
                 - an empty list otherwise.
        """
        if inputBlocks:
            blocksToMigrate = set(inputBlocks)
        else:
            ## This is for the case to migrate the whole dataset, which we don't do
            ## at this point Feb/2015 (we always pass inputBlocks).
            ## Make a set with the blocks that need to be migrated.
            blocksInDestDBS = set([block['block_name'] for block in destReadApi.listBlocks(dataset = inputDataset)])
            blocksInSourceDBS = set([block['block_name'] for block in sourceApi.listBlocks(dataset = inputDataset)])
            blocksToMigrate = blocksInSourceDBS - blocksInDestDBS
            msg = "Dataset %s in destination DBS with %d blocks; %d blocks in source DBS."
            msg = msg % (inputDataset, len(blocksInDestDBS), len(blocksInSourceDBS))
            self.logger.info(msg)
        numBlocksToMigrate = len(blocksToMigrate)
        if numBlocksToMigrate == 0:
            self.logger.info("No migration needed.")
        else:
            msg = "Have to migrate %d blocks from %s to %s."
            msg = msg % (numBlocksToMigrate, sourceApi.url, destReadApi.url)
            self.logger.info(msg)
            msg = "List of blocks to migrate:\n%s." % (", ".join(blocksToMigrate))
            self.logger.debug(msg)
            msg = "Submitting %d block migration requests to DBS3 ..." % (numBlocksToMigrate)
            self.logger.info(msg)
            numBlocksAtDestination = 0
            numQueuedUnkwonIds = 0
            numFailedSubmissions = 0
            migrationIdsInProgress = []
            for block in list(blocksToMigrate):
                ## Submit migration request for this block.
                (id, atDestination, alreadyQueued) = self.requestBlockMigration(migrateApi, sourceApi, block)
                ## If the block is already in the destination DBS instance, we don't need
                ## to monitor its migration status. If the migration request failed to be
                ## submitted, we retry it next time. Otherwise, save the migration request
                ## id in the list of migrations in progress.
                if id == None:
                    blocksToMigrate.remove(block)
                    if atDestination:
                        numBlocksAtDestination += 1
                    elif alreadyQueued:
                        numQueuedUnkwonIds += 1
                    else:
                        numFailedSubmissions += 1
                else:
                    migrationIdsInProgress.append(id)
            if numBlocksAtDestination > 0:
                msg = "%d blocks already in destination DBS." % (numBlocksAtDestination)
                self.logger.info(msg)
            if numFailedSubmissions > 0:
                msg  = "%d block migration requests failed to be submitted." % (numFailedSubmissions)
                msg += " Will retry them later."
                self.logger.info(msg)
            if numQueuedUnkwonIds > 0:
                msg  = "%d block migration requests were already queued," % (numQueuedUnkwonIds)
                msg += " but could not retrieve their request id."
                self.logger.info(msg)
            numMigrationsInProgress = len(migrationIdsInProgress)
            if numMigrationsInProgress == 0:
                msg = "No migrations in progress."
                self.logger.info(msg)
            else:
                msg = "%d block migration requests successfully submitted." % (numMigrationsInProgress)
                self.logger.info(msg)
                msg = "List of migration requests ids: %s" % (migrationIdsInProgress)
                self.logger.info(msg)
                ## Wait for up to 300 seconds, then return to the main loop. Note that we
                ## don't fail or cancel any migration request, but just retry it next time.
                ## Migration states:
                ##   0 = PENDING
                ##   1 = IN PROGRESS
                ##   2 = SUCCESS
                ##   3 = FAILED (failed migrations are retried up to 3 times automatically)
                ##   9 = Terminally FAILED
                ## In the case of failure, we expect the publisher daemon to try again in
                ## the future.
                numFailedMigrations = 0
                numSuccessfulMigrations = 0
                waitTime = 30
                numTimes = 10
                msg = "Will monitor their status for up to %d seconds." % (waitTime * numTimes)
                self.logger.info(msg)
                for i in range(numTimes):
                    msg  = "%d block migrations in progress." % (numMigrationsInProgress)
                    msg += " Will check migrations status in %d seconds." % (waitTime)
                    self.logger.info(msg)
                    time.sleep(waitTime)
                    ## Check the migration status of each block migration request.
                    ## If a block migration has succeeded or terminally failes, remove the
                    ## migration request id from the list of migration requests in progress.
                    for id in list(migrationIdsInProgress):
                        try:
                            status = migrateApi.statusMigration(migration_rqst_id = id)
                            state = status[0].get('migration_status')
                            retry = status[0].get('retry_count')
                        except Exception as ex:
                            msg = "Could not get status for migration id %d:\n%s" % (id, ex.msg)
                            self.logger.error(msg)
                        else:
                            if state == 2:
                                msg = "Migration id %d succeeded." % (id)
                                self.logger.info(msg)
                                migrationIdsInProgress.remove(id)
                                numSuccessfulMigrations += 1
                            if state == 9:
                                msg = "Migration id %d terminally failed." % (id)
                                self.logger.info(msg)
                                msg = "Full status for migration id %d:\n%s" % (id, str(status))
                                self.logger.debug(msg)
                                migrationIdsInProgress.remove(id)
                                numFailedMigrations += 1
                            if state == 3:
                                if retry < 3:
                                    msg = "Migration id %d failed (retry %d), but should be retried." % (id, retry)
                                    self.logger.info(msg)
                                else:
                                    msg = "Migration id %d failed (retry %d)." % (id, retry)
                                    self.logger.info(msg)
                                    msg = "Full status for migration id %d:\n%s" % (id, str(status))
                                    self.logger.debug(msg)
                                    migrationIdsInProgress.remove(id)
                                    numFailedMigrations += 1
                    numMigrationsInProgress = len(migrationIdsInProgress)
                    ## Stop waiting if there are no more migrations in progress.
                    if numMigrationsInProgress == 0:
                        break
                ## If after the 300 seconds there are still some migrations in progress,
                ## return an empty list (the caller function should interpret this as
                ## "migration has failed or is not complete").
                if numMigrationsInProgress > 0:
                    msg = "Migration of %s has taken too long - will delay the publication." % (inputDataset)
                    self.logger.info(msg)
                    return []
            msg = "Migration of %s has finished." % (inputDataset)
            self.logger.info(msg)
            msg  = "Migration status summary (from %d input blocks to migrate):" % (numBlocksToMigrate)
            msg += " at destination = %d," % (numBlocksAtDestination)
            msg += " succeeded = %d," % (numSuccessfulMigrations)
            msg += " failed = %d," % (numFailedMigrations)
            msg += " submission failed = %d," % (numFailedSubmissions)
            msg += " queued with unknown id = %d." % (numQueuedUnkwonIds)
            self.logger.info(msg)
            ## If there were failed migrations, return an empty list (the caller
            ## function should interpret this as "migration has failed or is not
            ## complete").
            if numFailedMigrations > 0 or numFailedSubmissions > 0:
                msg = "Some blocks failed to be migrated."
                self.logger.info(msg)
                return []
            if numQueuedUnkwonIds > 0:
                msg = "Some block migrations were already queued, but failed to retrieve the request id."
                self.logger.info(msg)
                return []
            if (numBlocksAtDestination + numSuccessfulMigrations) == numBlocksToMigrate:
                msg = "Migration was successful."
                self.logger.info(msg)
        existingDatasets = destReadApi.listDatasets(dataset = inputDataset, detail = True, dataset_access_type = '*')
        return existingDatasets


    def requestBlockMigration(self, migrateApi, sourceApi, block):
        """
        Submit migration request for one block, checking the request output.
        """
        atDestination = False
        alreadyQueued = False
        id = None
        msg = "Submiting migration request for block %s ..." % (block)
        self.logger.info(msg)
        sourceURL = sourceApi.url
        data = {'migration_url': sourceURL, 'migration_input': block}
        try:
            result = migrateApi.submitMigration(data)
        except HTTPError as he:
            if "is already at destination" in he.msg:
                msg = "Block is already at destination."
                self.logger.info(msg)
                atDestination = True
            else:
                msg  = "Request to migrate %s failed." % (block)
                msg += "\nRequest detail: %s" % (data)
                msg += "\nDBS3 exception: %s" % (he.msg)
                self.logger.error(msg)
        if not atDestination:
            msg = "Result of migration request: %s" % (str(result))
            self.logger.debug(msg)
            id = result.get('migration_details', {}).get('migration_request_id')
            report = result.get('migration_report')
            if id == None:
                msg  = "Migration request failed to submit."
                msg += "\nMigration request results: %s" % (str(result))
                self.logger.error(msg)
            if "REQUEST ALREADY QUEUED" in report:
                ## Request could be queued in another thread, then there would be
                ## no id here, so look by block and use the id of the queued request.
                alreadyQueued = True
                try:
                    status = migrateApi.statusMigration(block_name = block)
                    id = status[0].get('migration_request_id')
                except Exception:
                    msg = "Could not get status for already queued migration of block %s." % (block)
                    self.logger.error(msg)
        return (id, atDestination, alreadyQueued)


    def createBulkBlock(self, output_config, processing_era_config, primds_config, \
                        dataset_config, acquisition_era_config, block_config, files):
        file_conf_list = []
        file_parent_list = []
        for file in files:
            file_conf = output_config.copy()
            file_conf_list.append(file_conf)
            file_conf['lfn'] = file['logical_file_name']
            for parent_lfn in file.get('file_parent_list', []):
                file_parent_list.append({'logical_file_name': file['logical_file_name'], \
                                         'parent_logical_file_name': parent_lfn['file_parent_lfn']})
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

    def publishInDBS3(self, userdn, sourceURL, inputDataset, toPublish, pnn, workflow):
        """
        Publish files into DBS3
        """
        publish_next_iteration = []
        failed = []
        published = []
        results = {}
        failure_reason = ""
        blockSize = self.max_files_per_block

        # In the case of MC input (or something else which has no 'real' input dataset),
        # we simply call the input dataset by the primary DS name (/foo/).
        noInput = len(inputDataset.split("/")) <= 3

        READ_PATH = "/DBSReader"
        READ_PATH_1 = "/DBSReader/"

        if not sourceURL.endswith(READ_PATH) and not sourceURL.endswith(READ_PATH_1):
            sourceURL += READ_PATH

        ## When looking up parents may need to look in global DBS as well.
        globalURL = sourceURL
        globalURL = globalURL.replace('phys01', 'global')
        globalURL = globalURL.replace('phys02', 'global')
        globalURL = globalURL.replace('phys03', 'global')
        globalURL = globalURL.replace('caf', 'global')

        proxy = os.environ.get("SOCKS5_PROXY")
        self.logger.debug("Source API URL: %s" % sourceURL)
        sourceApi = dbsClient.DbsApi(url=sourceURL, proxy=proxy)
        self.logger.debug("Global API URL: %s" % globalURL)
        globalApi = dbsClient.DbsApi(url=globalURL, proxy=proxy)
        self.logger.debug("Destination API URL: %s" % self.publish_dbs_url)
        destApi = dbsClient.DbsApi(url=self.publish_dbs_url, proxy=proxy)
        self.logger.debug("Destination read API URL: %s" % self.publish_read_url)
        destReadApi = dbsClient.DbsApi(url=self.publish_read_url, proxy=proxy)
        self.logger.debug("Migration API URL: %s" % self.publish_migrate_url)
        migrateApi = dbsClient.DbsApi(url=self.publish_migrate_url, proxy=proxy)

        if not noInput:
            existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            primary_ds_type = existing_datasets[0]['primary_ds_type']
            # There's little chance this is correct, but it's our best guess for now.
            # CRAB2 uses 'crab2_tag' for all cases
            existing_output = destReadApi.listOutputConfigs(dataset=inputDataset)
            if not existing_output:
                self.logger.error("Unable to list output config for input dataset %s." % inputDataset)
                global_tag = 'crab3_tag'
            else:
                global_tag = existing_output[0]['global_tag']
        else:
            self.logger.info("This publication appears to be for private MC.")
            primary_ds_type = 'mc'
            global_tag = 'crab3_tag'

        acquisition_era_name = "CRAB"
        processing_era_config = {'processing_version': 1, 'description': 'CRAB3_processing_era'}

        self.logger.debug("Starting iteration through files for publication")
        for datasetPath, files in toPublish.iteritems():
            results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0}
            dbsDatasetPath = datasetPath

            if not files:
                continue

            appName = 'cmsRun'
            appVer  = files[0]["swversion"]
            appFam  = 'output'
            pset_hash = files[0]['publishname'].split("-")[-1]
            gtag = str(files[0]['globaltag'])
            if gtag == "None":
                gtag = global_tag
            acquisitionera = str(files[0]['acquisitionera'])
            if acquisitionera == "null":
                acquisitionera = acquisition_era_name
            empty, primName, procName, tier =  dbsDatasetPath.split('/')

            primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
            self.logger.debug("About to insert primary dataset: %s" % str(primds_config))
            destApi.insertPrimaryDataset(primds_config)
            self.logger.debug("Successfully inserted primary dataset %s" % primName)

            ## Find all (valid) files already published in this dataset.
            try:
                existingDBSFiles = destReadApi.listFiles(dataset = dbsDatasetPath, detail = True)
                existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
                existingFilesValid = [f['logical_file_name'] for f in existingDBSFiles if f['is_file_valid']]
                msg  = "Dataset %s already contains %d files" % (datasetPath, len(existingFiles))
                msg += " (%d valid, %d invalid)." % (len(existingFilesValid), len(existingFiles) - len(existingFilesValid))
                self.logger.info(msg)
                results[datasetPath]['existingFiles'] = len(existingFiles)
            except Exception as ex:
                msg  = "Error when listing files in DBS: %s" % (str(ex))
                msg += "\n%s" % (str(traceback.format_exc()))
                self.logger.error(msg)
                return [], [], {}

            ## Is there anything to do?
            workToDo = False
            for file in files:
                if file['lfn'] not in existingFilesValid:
                    workToDo = True
                    break
            ## If there is no work to do (because all the files that were requested
            ## to be published are already published and in VALID state), put the
            ## files in the list of published files and continue with the next dataset.
            if not workToDo:
                msg = "Nothing uploaded, %s has these files already or not enough files." % (datasetPath)
                self.logger.info(msg)
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
                              'dataset_access_type': 'VALID', # TODO
                              'physics_group_name': 'CRAB3',
                              'last_modification_date': int(time.time()),
                             }
            self.logger.info("About to insert dataset: %s" % str(dataset_config))
            del dataset_config['acquisition_era_name']

            ## List of all files that must (and can) be published.
            dbsFiles = []
            ## Set of all the parent files from all the files requested to be published.
            parentFiles = set()
            ## Set of parent files for which the migration to the destination DBS instance
            ## should be skipped (because they were not found in DBS).
            parentsToSkip = set()
            ## Set of parent files to migrate from the source DBS instance
            ## to the destination DBS instance.
            localParentBlocks = set()
            ## Set of parent files to migrate from the global DBS instance
            ## to the destination DBS instance.
            globalParentBlocks = set()

            ## Loop over all files to publish.
            for file in files:
                ## Check if this file was already published and if it is valid.
                if (file['lfn'] not in existingFiles) or (file['lfn'] in invalidExistingFiles):
                    ## We have a file to publish.
                    ## Get the parent files and for each parent file do the following:
                    ## 1) Add it to the list of parent files.
                    ## 2) Find the block to which it belongs and insert that block name in
                    ##    (one of) the set of blocks to be migrated to the destination DBS.
                    for parentFile in list(file['parents']):
                        if parentFile not in parentFiles:
                            parentFiles.add(parentFile)
                            ## Is this parent file already in the destination DBS instance?
                            ## (If yes, then we don't have to migrate this block.)
                            blocksDict = destReadApi.listBlocks(logical_file_name = parentFile)
                            if not blocksDict:
                                ## No, this parent file is not in the destination DBS instance.
                                ## Maybe it is in the same DBS instance as the input dataset?
                                blocksDict = sourceApi.listBlocks(logical_file_name = parentFile)
                                if blocksDict:
                                    ## Yes, this parent file is in the same DBS instance as the input dataset.
                                    ## Add the corresponding block to the set of blocks from the source DBS
                                    ## instance that have to be migrated to the destination DBS.
                                    localParentBlocks.add(blocksDict[0]['block_name'])
                                else:
                                    ## No, this parent file is not in the same DBS instance as input dataset.
                                    ## Maybe it is in global DBS instance?
                                    blocksDict = globalApi.listBlocks(logical_file_name = parentFile)
                                    if blocksDict:
                                        ## Yes, this parent file is in global DBS instance.
                                        ## Add the corresponding block to the set of blocks from global DBS
                                        ## instance that have to be migrated to the destination DBS.
                                        globalParentBlocks.add(blocksDict[0]['block_name'])
                            ## If this parent file is not in the destination DBS instance, is not
                            ## the source DBS instance, and is not in global DBS instance, then it
                            ## means it is not known to DBS and therefore we can not migrate it.
                            ## Put it in the set of parent files for which migration should be skipped.
                            if not blocksDict:
                                parentsToSkip.add(parentFile)
                        ## If this parent file should not be migrated because it is not known to DBS,
                        ## we remove it from the list of parents in the file-to-publish info dictionary
                        ## (so that when publishing, this "parent" file will not appear as a parent).
                        if parentFile in parentsToSkip:
                            msg = "Skipping parent file %s, as it doesn't seem to be known to DBS." % (parentFile)
                            self.logger.info(msg)
                            if parentFile in file['parents']:
                                file['parents'].remove(parentFile)
                    ## Add this file to the list of files to be published.
                    dbsFiles.append(self.format_file_3(file))
                published.append(file['lfn'])

            ## Print a message with the number of files to publish.
            msg = "Found %d files not already present in DBS which will be published." % len(dbsFiles)
            self.logger.info(msg)

            ## If there are no files to publish, continue with the next dataset.
            if len(dbsFiles) == 0:
                self.logger.info("Nothing to do for this dataset.")
                continue

            ## Migrate parent blocks before publishing.
            ## First migrate the parent blocks that are in the same DBS instance
            ## as the input dataset.
            if localParentBlocks:
                msg = "List of parent blocks that need to be migrated from %s:\n%s" % (sourceApi.url, localParentBlocks)
                self.logger.info(msg)
                existingDatasets = self.migrateByBlockDBS3(migrateApi, destReadApi, sourceApi, inputDataset, localParentBlocks)
                if not existingDatasets:
                    msg = "Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, sourceApi.url, destReadApi.url)
                    self.logger.info(msg)
                    failed += [i['logical_file_name'] for i in dbsFiles]
                    failure_reason = msg
                    return failed, failure_reason, [], {}
                if not existingDatasets[0]['dataset'] == inputDataset:
                    msg = "ERROR: Inconsistent state: %s migrated, but listDatasets didn't return any information." % (inputDataset)
                    self.logger.info(msg)
                    failed += [i['logical_file_name'] for i in dbsFiles]
                    failure_reason = msg
                    return failed, failure_reason, [], {}
            ## Then migrate the parent blocks that are in the global DBS instance.
            if globalParentBlocks:
                msg = "List of parent blocks that need to be migrated from %s:\n%s" % (globalApi.url, globalParentBlocks)
                self.logger.info(msg)
                existingDatasets = self.migrateByBlockDBS3(migrateApi, destReadApi, globalApi, inputDataset, globalParentBlocks)
                if not existingDatasets:
                    msg = "Failed to migrate %s from %s to %s; not publishing any files." % (inputDataset, globalApi.url, destReadApi.url)
                    self.logger.info(msg)
                    failed += [i['logical_file_name'] for i in dbsFiles]
                    failure_reason = msg
                    return failed, failure_reason, [], {}
                if not existingDatasets[0]['dataset'] == inputDataset:
                    msg = "ERROR: Inconsistent state: %s migrated, but listDatasets didn't return any information" % (inputDataset)
                    self.logger.info(msg)
                    failed += [i['logical_file_name'] for i in dbsFiles]
                    failure_reason = msg
                    return failed, failure_reason, [], {}

            count = 0
            blockCount = 0
            if len(dbsFiles) < self.max_files_per_block:
                block_name = "%s#%s" % (dbsDatasetPath, str(uuid.uuid4()))
                files_to_publish = dbsFiles[count:count+blockSize]
                try:
                    block_config = {'block_name': block_name, 'origin_site_name': pnn, 'open_for_writing': 0}
                    self.logger.debug("Inserting files %s into block %s." % ([i['logical_file_name'] for i in files_to_publish], block_name))
                    blockDump = self.createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, \
                                                     acquisition_era_config, block_config, files_to_publish)
                    destApi.insertBulkBlock(blockDump)
                    count += blockSize
                    blockCount += 1
                except Exception as ex:
                    failed += [i['logical_file_name'] for i in files_to_publish]
                    msg =  "Error when publishing"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    failure_reason = str(ex)
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
                        block_config = {'block_name': block_name, 'origin_site_name': pnn, 'open_for_writing': 0}
                        blockDump = self.createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, \
                                                         acquisition_era_config, block_config, files_to_publish)
                        self.logger.debug("Block to insert: %s\n" % pprint.pformat(blockDump))
                        destApi.insertBulkBlock(blockDump)

                        count += blockSize
                        blockCount += 1
                    except Exception as ex:
                        failed += [i['logical_file_name'] for i in files_to_publish]
                        msg =  "Error when publishing (%s) " % ", ".join(failed)
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        failure_reason = str(ex)
                        count += blockSize
            results[datasetPath]['files'] = len(dbsFiles) - len(failed)
            results[datasetPath]['blocks'] = blockCount
        published = filter(lambda x: x not in failed + publish_next_iteration, published)
        self.logger.info("End of publication status: failed %s, published %s, publish_next_iteration %s, results %s" \
                         % (failed, published, publish_next_iteration, results))
        return failed, failure_reason, published, results

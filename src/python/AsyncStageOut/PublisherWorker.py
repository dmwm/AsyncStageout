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
        except Exception, ex:
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
        except Exception, e:
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
            query = {'reduce':False, 'key': user_wf['key'], 'stale': 'ok'}
            try:
                active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
            except Exception, e:
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
                except Exception, ex:
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
                except Exception, ex:
                    msg = "Error loading the status of %s !" % workflow
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue

                if workflow_status not in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED']:
                    query = {'reduce':True, 'key': user_wf['key']}
                    try:
                        last_publication_time = self.db.loadView('DBSPublisher', 'last_publication', query)['rows']
                    except Exception, e:
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
                failed_files, good_files = self.publish( str(file['key'][3]), \
                                                         self.userDN, str(file['key'][0]), \
                                                         str(file['value'][2]), str(file['value'][3]), \
                                                         str(pnn), lfn_ready )
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
            self.logger.info("Marking failed %s" % lfn)
            data = {}
            lfn_db = self.lfn_map[lfn]
            docId = getHashLfn(lfn_db)
            self.logger.info("Marking failed %s of %s" % (docId, lfn_db))
            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
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
                self.logger.error("FWJR seems not in cache! Forcing the failure")
                self.mark_failed(lfn_ready, True)
            else:
                return [], []
        self.logger.info("Starting data publication in %s of %s" % ("DBS", str(workflow)))
        failed, done, dbsResults = self.publishInDBS3(userdn=userdn, sourceURL=sourceurl,
                                                 inputDataset=inputDataset, toPublish=toPublish,
                                                 pnn=pnn, workflow=workflow)
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
        self.logger.info("Starting Read cache for %s..." % workflow)
        buf = cStringIO.StringIO()
        res = []
        # TODO: input sanitization
        header = {"Content-Type ":"application/json"}
        data = {'taskname': workflow, 'filetype': 'EDM'}
        url = self.cache_area
        try:
            response, res_ = self.connection.request(url, data, header, doseq=True, ckey=self.userProxy, cert=self.userProxy)#, verbose=True)# for debug
        except Exception, ex:
            msg =  "Error reading data from cache"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            return {}
        self.logger.info("Data read from cache...")
        try:
            buf.close()
            res = json.loads(res_)
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

    def format_file_3(self, file, output_config, dataset):
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


    def migrateDBS3(self, migrateApi, destReadApi, sourceURL, inputDataset):

        try:
            existing_datasets = destReadApi.listDatasets(dataset=inputDataset, detail=True)
        except HTTPError, he:
            msg = str(he)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            self.logger.error("Request to list input dataset %s failed." % inputDataset)
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
            self.logger.debug("Dataset %s in destination DBS with %d blocks; %d blocks in source." % (inputDataset, \
                                                                                                      len(existing_blocks), \
                                                                                                      len(source_blocks)))
            if blocks_to_migrate:
                self.logger.debug("%d blocks (%s) must be migrated to destination dataset %s." % (len(blocks_to_migrate), \
                                                                                                  ", ".join(blocks_to_migrate), \
                                                                                                  inputDataset))
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
            # Wait for up to 300 seconds, then return to the main loop.  Note we don't
            # fail or cancel anything.  Just retry later.
            # States:
            # 0=PENDING
            # 1=IN PROGRESS
            # 2=COMPLETED
            # *=FAILED
            # 9=Terminally FAILED
            #
            # In the case of failure, we expect the publisher daemon to try again in
            # the future.
            #
            wait_time = 30
            for i in range(10):
                self.logger.debug("About to get migration request for %s." % id)
                status = migrateApi.statusMigration(migration_rqst_id=id)
                state = status[0].get("migration_status")
                self.logger.debug("Migration status: %s" % state)
                if state != 9 or state != 2:
                    time.sleep(wait_time)

            if state == 9:
                self.logger.info("Migration of %s has failed.  Full status: %s" % (inputDataset, str(status)))
                return []
            elif state == 2:
                self.logger.info("Migration of %s is complete." % inputDataset)
                existing_datasets = destReadApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            else:
                self.logger.info("Migration of %s has taken too long - will delay publication." % inputDataset)
                return []
        return existing_datasets

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
        blockSize = self.max_files_per_block

        # In the case of MC input (or something else which has no 'real' input dataset),
        # we simply call the input dataset by the primary DS name (/foo/).
        noInput = len(inputDataset.split("/")) <= 3

        READ_PATH = "/DBSReader"
        READ_PATH_1 = "/DBSReader/"

        if not sourceURL.endswith(READ_PATH) and not sourceURL.endswith(READ_PATH_1):
            sourceURL += READ_PATH

        self.logger.debug("Migrate datasets")
        proxy = os.environ.get("SOCKS5_PROXY")
        self.logger.debug("Destination API URL: %s" % self.publish_dbs_url)
        destApi = dbsClient.DbsApi(url=self.publish_dbs_url, proxy=proxy)
        self.logger.debug("Destination read API URL: %s" % self.publish_read_url)
        destReadApi = dbsClient.DbsApi(url=self.publish_read_url, proxy=proxy)
        self.logger.debug("Migration API URL: %s" % self.publish_migrate_url)
        migrateApi = dbsClient.DbsApi(url=self.publish_migrate_url, proxy=proxy)

        if not noInput:
            self.logger.debug("Submit migration from source DBS %s to destination %s." % (sourceURL, self.publish_migrate_url))
            # Submit migration
            sourceApi = dbsClient.DbsApi(url=sourceURL)
            try:
                existing_datasets = self.migrateDBS3(migrateApi, destReadApi, sourceURL, inputDataset)
            except Exception, ex:
                msg = str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                existing_datasets = []
            if not existing_datasets:
                self.logger.info("Failed to migrate %s from %s to %s; not publishing any files." % \
                                 (inputDataset, sourceURL, self.publish_migrate_url))
                return [], [], []
            # Get basic data about the parent dataset
            if not existing_datasets[0]['dataset'] == inputDataset:
                self.logger.error("Inconsistent state: %s migrated, but listDatasets didn't return any information" % inputDataset)
                return [], [], []
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

        self.logger.debug("Starting iteratation through files for publication")
        for datasetPath, files in toPublish.iteritems():
            results[datasetPath] = {'files': 0, 'blocks': 0, 'existingFiles': 0,}
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
            self.logger.debug("Successfully inserting primary dataset %s" % primName)

            # Find any files already in the dataset so we can skip them
            try:
                existingDBSFiles = destApi.listFiles(dataset=dbsDatasetPath)
                existingFiles = [x['logical_file_name'] for x in existingDBSFiles]
                results[datasetPath]['existingFiles'] = len(existingFiles)
            except Exception, ex:
                msg =  "Error when listing files in DBS"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.exception(msg)
                return [], [], []

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
                              'dataset_access_type': 'VALID', # TODO
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
                except Exception, ex:
                    failed += [i['logical_file_name'] for i in files_to_publish]
                    msg =  "Error when publishing"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
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

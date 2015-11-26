#!/usr/bin/env
#pylint: disable-msg=C0103
'''
The Publisherworker does the following:

    a. looks to UserFileCacheEndpoint to get the workflow details
    b. looks to local database to get transferred files
    c. publish trnasferred files

There should be one worker per user transfer.
'''
import os
import re
import json
import time
import uuid
#import pprint
import urllib
import tarfile
import logging
import datetime
import traceback
import cStringIO

import dbs.apis.dbsClient as dbsClient

from RestClient.ErrorHandling.RestClientExceptions import HTTPError

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.pycurl_manager import RequestHandler

from AsyncStageOut import getProxy
from AsyncStageOut import getHashLfn
from AsyncStageOut import getDNFromUserName
from AsyncStageOut import getCommonLogFormatter


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
        formatter = getCommonLogFormatter(self.config)
        for handler in logging.getLogger().handlers:
            handler.setFormatter(formatter)
        self.pfn_to_lfn_mapping = {}
        self.max_retry = config.publication_max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.proxyDir = config.credentialDir
        self.myproxyServer = 'myproxy.cern.ch'
        self.init = True
        self.userDN = ''
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
        defaultDelegation = {'logger': self.logger,
                             'credServerPath': self.config.credentialDir,
                             # It will be moved to be getfrom couchDB
                             'myProxySvr': 'myproxy.cern.ch',
                             'min_time_left': getattr(self.config, 'minTimeLeft', 36000),
                             'serverDN': self.config.serverDN,
                             'uisource': self.uiSetupScript
                            }
        # If we're just testing publication, we skip the DB connection.
        if os.getenv("TEST_ASO"):
            self.db = None
        else:
            server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
            self.db = server.connectDatabase(self.config.files_database)
        if hasattr(self.config, "cache_area"):
            try:
                defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(self.config.cache_area)[0]
                self.cache_area = self.config.cache_area
            except IndexError:
                self.logger.error('MyproxyAccount parameter cannot be retrieved from %s . Fallback to user cache_area  ' % (self.config.cache_area))
		query = {'key':self.user}
           	try:
                    self.user_cache_area = self.db.loadView('DBSPublisher', 'cache_area', query)['rows']
		except Exception as ex:
                    msg = "Error getting user cache_area"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                try:
                    self.cache_area = self.user_cache_area[0]['value'][0]+self.user_cache_area[0]['value'][1]
                    defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(self.cache_area)[0]
                except IndexError:
                    self.logger.error('MyproxyAccount parameter cannot be retrieved from %s' % (self.cache_area))
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
            self.userProxy = self.config.opsProxy
        #self.cache_area = self.config.cache_area
        self.phedexApi = PhEDEx(responseType='json')
        self.max_files_per_block = max(1, self.config.max_files_per_block)
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
            msg = "Error initializing the connection"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.debug(msg)


    def __call__(self):
        """
        1- check the nubmer of files in wf to publish if it is < max_files_per_block
        2- check in wf if now - last_finished_job > max_publish_time
        3- then call publish, mark_good, mark_failed for each wf
        """
        ## Get the list of user workflows which contain at least one file ready to
        ## publish (i.e. the file has been transferred, but not yet published -nor its
        ## publication has failed-). We call them "active" files.
        active_user_workflows = []
        query = {'group': True, 'startkey': [self.user, self.group, self.role], 'endkey': [self.user, self.group, self.role, {}]}
        try:
            active_user_workflows = self.db.loadView('DBSPublisher', 'publish', query)['rows']
        except Exception as ex:
            self.logger.error('A problem occured when contacting couchDB to get the list of active WFs: %s' % ex)
            self.logger.info('Publications for %s will be retried next time' % self.user)
            return
        self.logger.debug('active user wfs: %s' % active_user_workflows)
        self.logger.info('number of active user wfs: %s' % len(active_user_workflows))
        now = time.time()
        self.lfn_map = {}
        ## Loop over the user workflows.
        for user_wf in active_user_workflows:
            workflow = str(user_wf['key'][3])
            wfnamemsg = "%s: " % (workflow)
            ## This flag is to force calling the publish method (e.g. because the workflow
            ## status is terminal) even if regular criteria would say not to call it (e.g.
            ## because there was already a publication done recently for this workflow and
            ## there are not enough files yet for another block publication).
            self.force_publication = False
            ## Get the list of active files in the workflow.
            active_files = []
            query = {'reduce': False, 'key': user_wf['key']}#'stale': 'ok'}
            try:
                active_files = self.db.loadView('DBSPublisher', 'publish', query)['rows']
            except Exception as e:
                msg = "A problem occured to retrieve the list of active files for %s: %s" % (self.user, e)
                self.logger.error(wfnamemsg+msg)
                msg = "Publications will be retried next time."
                self.logger.info(wfnamemsg+msg)
                continue
            msg = "Number of active files: %s." % (len(active_files))
            self.logger.info(wfnamemsg+msg)
            ## If there are no files to publish, continue with the next workflow.
            if not active_files:
                msg = "Continuing with next workflow/user in the loop."
                self.logger.info(wfnamemsg+msg)
                continue
            ## Get the job endtime, destination site, input dataset and input DBS URL for
            ## the active files in the workflow. Put the destination LFNs in a list of ready
            ## files grouped by output dataset.
            lfn_ready = {}
            wf_jobs_endtime = []
            pnn, input_dataset, input_dbs_url = "", "", ""
            for active_file in active_files:
                job_end_time = active_file['value'][5]
                if job_end_time:
                    wf_jobs_endtime.append(int(time.mktime(time.strptime(str(job_end_time), '%Y-%m-%d %H:%M:%S'))) - time.timezone)
                source_lfn = active_file['value'][1]
                dest_lfn = active_file['value'][2]
                ## TODO: The next if is needed only for backward compatibility, because old
                ## documents were injected with source_lfn not including 'temp' when direct
                ## stageout was done. This has been changed in CRAB 3.3.1508. So in October we
                ## can remove the next if and just leave:
                #self.lfn_map[dest_lfn] = source_lfn
                if not source_lfn.startswith('/store/temp/'):
                    self.lfn_map[dest_lfn] = source_lfn.replace('/store/', '/store/temp/', 1)
                else:
                    self.lfn_map[dest_lfn] = source_lfn
                if not pnn or not input_dataset or not input_dbs_url:
                    pnn = str(active_file['value'][0])
                    input_dataset = str(active_file['value'][3])
                    input_dbs_url = str(active_file['value'][4])
                ## Group the destination LFNs by output dataset. We don't know the dataset names
                ## yet, but we can use the fact that there is a one-to-one correspondence
                ## between the original (without the jobid) filenames and the dataset names.
                filename = os.path.basename(dest_lfn)
                left_piece, jobid_fileext = filename.rsplit('_', 1)
                if '.' in jobid_fileext:
                    fileext = jobid_fileext.rsplit('.', 1)[-1]
                    orig_filename = left_piece + '.' + fileext
                else:
                    orig_filename = left_piece
                lfn_ready.setdefault(orig_filename, []).append(dest_lfn)
            msg = "There are %s ready files in %s active files." % (sum(map(len, lfn_ready.values())), user_wf['value'])
            self.logger.info(wfnamemsg+msg)
            msg = "List of jobs end time (len = %s): %s" % (len(wf_jobs_endtime), wf_jobs_endtime)
            self.logger.debug(wfnamemsg+msg)
            ## Check if the workflow has expired. If it has, force the publication for the
            ## available ready files and mark the rest of the files as 'publication failed'.
            self.force_failure = False
            self.publication_failure_msg = ""
            if wf_jobs_endtime:
                wf_jobs_endtime.sort()
                msg = "Oldest job end time: %s. Now: %s." % (wf_jobs_endtime[0], now)
                self.logger.debug(wfnamemsg+msg)
                workflow_duration = (now - wf_jobs_endtime[0])
                workflow_expiration_time = self.config.workflow_expiration_time * 24*60*60
                if workflow_duration > workflow_expiration_time:
                    self.force_publication = True
                    self.force_failure = True
                    time_since_expiration = workflow_duration - workflow_expiration_time
                    hours = int(time_since_expiration/60/60)
                    minutes = int((time_since_expiration - hours*60*60)/60)
                    seconds = int(time_since_expiration - hours*60*60 - minutes*60)
                    self.publication_failure_msg = "Workflow %s expired since %sh:%sm:%ss!" % (workflow, hours, minutes, seconds)
                    msg = self.publication_failure_msg
                    msg += " Will force the publication if possible or fail it otherwise."
                    self.logger.info(wfnamemsg+msg)
            ## List with the number of ready files per dataset.
            lens_lfn_ready = map(lambda x: len(x), lfn_ready.values())
            msg = "Number of ready files per dataset: %s." % (lens_lfn_ready)
            self.logger.info(wfnamemsg+msg)
            ## List with booleans that tell if there are more than max_files_per_block to
            ## publish per dataset.
            enough_lfn_ready = map(lambda x: x >= self.max_files_per_block, lens_lfn_ready)
            ## Auxiliary flag.
            enough_lfn_ready_in_all_datasets = not False in enough_lfn_ready
            ## If for any of the datasets there are less than max_files_per_block to publish,
            ## check for other conditions to decide whether to publish that dataset or not.
            if enough_lfn_ready_in_all_datasets:
                ## TODO: Check how often we are on this situation. I suspect it is not so often,
                ## in which case I would remove the 'if enough_lfn_ready_in_all_datasets' and
                ## always retrieve the workflow status as seems to me it is cleaner and makes
                ## the code easier to understand. (Comment from Andres Tanasijczuk)
                msg  = "All datasets have more than %s ready files." % (self.max_files_per_block)
                msg += " No need to retrieve task status nor last publication time."
                self.logger.info(wfnamemsg+msg)
            else:
                msg  = "At least one dataset has less than %s ready files." % (self.max_files_per_block)
                self.logger.info(wfnamemsg+msg)
                ## Retrieve the workflow status. If the status can not be retrieved, continue
                ## with the next workflow.
                workflow_status = ''
                url = '/'.join(self.cache_area.split('/')[:-1]) + '/workflow'
                msg = "Retrieving status from %s" % (url)
                self.logger.info(wfnamemsg+msg)
                buf = cStringIO.StringIO()
                data = {'workflow': workflow}
                header = {"Content-Type ":"application/json"}
                try:
                    _, res_ = self.connection.request(url, data, header, doseq=True, ckey=self.userProxy, cert=self.userProxy)#, verbose=True)# for debug
                except Exception as ex:
                    msg  = "Error retrieving status from cache."
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(wfnamemsg+msg)
                else:
                    msg = "Status retrieved from cache. Loading task status."
                    self.logger.info(wfnamemsg+msg)
                    try:
                        buf.close()
                        res = json.loads(res_)
                        workflow_status = res['result'][0]['status']
                        msg = "Task status is %s." % (workflow_status)
                        self.logger.info(wfnamemsg+msg)
                    except ValueError:
                        msg = "Workflow removed from WM."
                        self.logger.error(wfnamemsg+msg)
                        workflow_status = 'REMOVED'
                    except Exception as ex:
                        msg  = "Error loading task status!"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(wfnamemsg+msg)
                ## If the workflow status is terminal, go ahead and publish all the ready files
                ## in the workflow.
                if workflow_status in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED']:
                    self.force_publication = True
                    msg = "Considering task status as terminal. Will force publication."
                    self.logger.info(wfnamemsg+msg)
                ## Otherwise...
                else:
                    msg = "Task status is not considered terminal."
                    self.logger.info(wfnamemsg+msg)
                    msg = "Getting last publication time."
                    self.logger.info(wfnamemsg+msg)
                    ## Get when was the last time a publication was done for this workflow (this
                    ## should be more or less independent of the output dataset in case there are
                    ## more than one).
                    query = {'reduce': True, 'key': user_wf['key']}
                    try:
                        last_publication_time = self.db.loadView('DBSPublisher', 'last_publication', query)['rows']
                    except Exception as ex:
                        msg = "Cannot get last publication time for %s: %s" % (user_wf['key'], ex)
                        self.logger.error(wfnamemsg+msg)
                    else:
                        msg = "Last publication time: %s." % (last_publication_time)
                        self.logger.debug(wfnamemsg+msg)
                        ## If this is the first time a publication would be done for this workflow, go
                        ## ahead and publish.
                        if not last_publication_time:
                            self.force_publication = True
                            msg = "There was no previous publication. Will force publication."
                            self.logger.info(wfnamemsg+msg)
                        ## Otherwise...
                        else:
                            msg = "Last published block: %s" % (last_publication_time[0]['value']['max'])
                            self.logger.debug(wfnamemsg+msg)
                            ## If the last publication was long time ago (> our block publication timeout),
                            ## go ahead and publish.
                            time_since_last_publication = now - last_publication_time[0]['value']['max']
                            hours = int(time_since_last_publication/60/60)
                            minutes = int((time_since_last_publication - hours*60*60)/60)
                            timeout_hours = int(self.block_publication_timeout/60/60)
                            timeout_minutes = int((self.block_publication_timeout - timeout_hours*60*60)/60)
                            msg = "Last publication was %sh:%sm ago" % (hours, minutes)
                            if time_since_last_publication > self.block_publication_timeout:
                                self.force_publication = True
                                msg += " (more than the timeout of %sh:%sm)." % (timeout_hours, timeout_minutes)
                                msg += " Will force publication."
                            else:
                                msg += " (less than the timeout of %sh:%sm)." % (timeout_hours, timeout_minutes)
                                msg += " Not enough to force publication."
                            self.logger.info(wfnamemsg+msg)
            ## Call the publish method with the lists of ready files to publish for this
            ## workflow grouped by datasets.
            result = self.publish(workflow, input_dataset, input_dbs_url, pnn, lfn_ready)
            for dataset in result.keys():
                published_files = result[dataset].get('published', [])
                if published_files:
                    self.mark_good(workflow, published_files)
                failed_files = result[dataset].get('failed', [])
                if failed_files:
                    failure_reason = result[dataset].get('failure_reason', "")
                    force_failure = result[dataset].get('force_failure', False)
                    self.mark_failed(workflow, failed_files, failure_reason, force_failure)

        self.logger.info("Publications for user %s (group: %s, role: %s) completed." % (self.user, self.group, self.role))


    def mark_good(self, workflow, files=[]):
        """
        Mark the list of files as tranferred
        """
        wfnamemsg = "%s: " % (workflow)
        last_update = int(time.time())
        for lfn in files:
            data = {}
            source_lfn = self.lfn_map[lfn]
            docId = getHashLfn(source_lfn)
            msg  = "Marking file %s as published." % (lfn)
            msg += " Document id: %s (source LFN: %s)." % (docId, source_lfn)
            self.logger.info(wfnamemsg+msg)
            data['publication_state'] = 'published'
            data['last_update'] = last_update
            try:
                updateUri = "/" + self.db.name + "/_design/DBSPublisher/_update/updateFile/" + getHashLfn(source_lfn)
                updateUri += "?" + urllib.urlencode(data)
                self.logger.info(wfnamemsg+"URI: %s" % updateUri)
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception as ex:
                msg  = "Error updating document in Couch."
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(wfnamemsg+msg)
        try:
            self.db.commit()
        except Exception as ex:
            msg  = "Error committing documents in Couch."
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(wfnamemsg+msg)


    def mark_failed(self, workflow, files=[], failure_reason="", force_failure=False):
        """
        Something failed for these files so increment the retry count
        """
        wfnamemsg = "%s: " % (workflow)
        now = str(datetime.datetime.now())
        last_update = int(time.time())
        for lfn in files:
            data = {}
            source_lfn = self.lfn_map[lfn]
            docId = getHashLfn(source_lfn)
            msg  = "Marking file %s as failed." % (lfn)
            msg += " Document id: %s (source LFN: %s)." % (docId, source_lfn)
            self.logger.info(wfnamemsg+msg)
            # Load document to get the retry_count
            try:
                document = self.db.document(docId)
            except Exception as ex:
                msg  = "Error loading document from Couch."
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(wfnamemsg+msg)
                continue
            # Prepare data to update the document in couch
            if len(document['publication_retry_count']) + 1 > self.max_retry or force_failure:
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
                self.logger.info(wfnamemsg+"URI: %s" % updateUri)
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception as ex:
                msg  = "Error updating document in Couch."
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(wfnamemsg+msg)
        try:
            self.db.commit()
        except Exception as ex:
            msg  = "Error committing documents in Couch."
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(wfnamemsg+msg)


    def publish(self, workflow, inputDataset, sourceURL, pnn, lfn_ready):
        """Perform the data publication of the workflow result.
           :arg str workflow: a workflow name
           :arg str inputDataset
           :arg str sourceURL
           :arg str pnn
           :arg dict lfn_ready
           :return: the publication status or result"""
        wfnamemsg = "%s: " % (workflow)
        retdict = {}
        ## Don't publish anything if there are not enough ready files to make a block in
        ## any of the datasets and publication was not forced. This is a first filtering
        ## so to not retrieve the filemetadata unnecesarily.
        if not False in map(lambda x: len(x) < self.max_files_per_block, lfn_ready.values()) and not self.force_publication:
            msg = "Skipping publication as there are not enough ready files in any of the datasets (and publication was not forced)."
            self.logger.info(wfnamemsg+msg)
            return retdict
        ## Get the filemetada for this workflow.
        msg = "Retrieving publication description files."
        self.logger.info(wfnamemsg+msg)
        lfn_ready_list = []
        for v in lfn_ready.values():
            lfn_ready_list.extend(v)
        try:
            publDescFiles_list = self.getPublDescFiles(workflow)
        except (tarfile.ReadError, RuntimeError):
            msg = "Error retrieving publication description files."
            self.logger.error(wfnamemsg+msg)
            retdict = {'unknown_datasets': {'failed': lfn_ready_list, 'failure_reason': msg, 'force_failure': False, 'published': []}}
            return retdict
        msg = "Number of publication description files: %s" % (len(publDescFiles_list))
        self.logger.info(wfnamemsg+msg)
        ## Group the filemetadata according to the output dataset.
        msg = "Grouping publication description files according to output dataset."
        self.logger.info(wfnamemsg+msg)
        publDescFiles = {}
        for publDescFile in publDescFiles_list:
            dataset = str(publDescFile['outdataset'])
            publDescFiles.setdefault(dataset, []).append(publDescFile)
        msg = "Publication description files: %s" % (publDescFiles)
        self.logger.debug(wfnamemsg+msg)
        ## Discard ready files for which there is no filemetadata.
        msg = "Discarding ready files for which there is no publication description file available (and vice versa)."
        self.logger.info(wfnamemsg+msg)
        toPublish = self.clean(lfn_ready_list, publDescFiles)
        msg = "Number of publication description files to publish: %s" % (sum(map(lambda x: len(x), toPublish.values())))
        self.logger.info(wfnamemsg+msg)
        msg = "Publication description files to publish: %s" % (toPublish)
        self.logger.debug(wfnamemsg+msg)
        ## If there is nothing to publish, return.
        if not toPublish:
            if self.force_failure:
                msg = "Publication description files not found! Will force publication failure."
                self.logger.error(wfnamemsg+msg)
                if self.publication_failure_msg:
                    msg += " %s" % (self.publication_failure_msg)
                retdict = {'unknown_datasets': {'failed': lfn_ready_list, 'failure_reason': msg, 'force_failure': True, 'published': []}}
            return retdict
        ## Don't publish datasets for which there are not enough ready files to make a
        ## block, unless publication was forced.
        for dataset in toPublish.keys():
            files = toPublish[dataset]
            if len(files) < self.max_files_per_block and not self.force_publication:
                msg  = "There are only %s (less than %s) files to publish in dataset %s." % (len(files), self.max_files_per_block, dataset)
                msg += " Will skip publication in this dataset (publication was not forced)."
                self.logger.info(wfnamemsg+msg)
                toPublish.pop(dataset)
        ## Finally... publish if there is something left to publish.
        if not toPublish:
            msg = "Nothing to publish."
            self.logger.info(wfnamemsg+msg)
            return retdict
        for dataset in toPublish.keys():
            msg = "Will publish %s files in dataset %s." % (len(toPublish[dataset]), dataset)
            self.logger.info(wfnamemsg+msg)
        msg = "Starting publication in DBS."
        self.logger.info(wfnamemsg+msg)
        failed, failure_reason, published, dbsResults = self.publishInDBS3(workflow, sourceURL, inputDataset, toPublish, pnn)
        msg = "DBS publication results: %s" % (dbsResults)
        self.logger.debug(wfnamemsg+msg)
        for dataset in toPublish.keys():
            retdict.update({dataset: {'failed': failed.get(dataset, []), 'failure_reason': failure_reason.get(dataset, ""), 'published': published.get(dataset, [])}})
        return retdict


    def getPublDescFiles(self, workflow):
        """
        Download and read the files describing
        what needs to be published
        """
        wfnamemsg = "%s: " % (workflow)
        buf = cStringIO.StringIO()
        res = []
        # TODO: input sanitization
        header = {"Content-Type ": "application/json"}
        data = {'taskname': workflow, 'filetype': 'EDM'}
        url = self.cache_area
        msg = "Retrieving data from %s" % (url)
        self.logger.info(wfnamemsg+msg)
        try:
            _, res_ = self.connection.request(url, data, header, doseq=True, ckey=self.userProxy, cert=self.userProxy)#, verbose=True)# for debug
        except Exception as ex:
            msg  = "Error retrieving data."
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(wfnamemsg+msg)
            return {}
        msg = "Loading results."
        self.logger.info(wfnamemsg+msg)
        try:
            buf.close()
            res = json.loads(res_)
        except Exception as ex:
            msg  = "Error loading results. Trying next time!"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(wfnamemsg+msg)
            return {}
        return res['result']


    def clean(self, lfn_ready_list, publDescFiles):
        """
        Discard ready files that have no filematadata (and vice versa).
        """
        publDescFiles_filtered = {}
        for dataset, outfiles_metadata in publDescFiles.iteritems():
            for outfile_metadata in outfiles_metadata:
                dest_lfn = outfile_metadata['lfn']
                if dest_lfn in lfn_ready_list:
                    publDescFiles_filtered.setdefault(dataset, []).append(outfile_metadata)
        return publDescFiles_filtered


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


    def migrateByBlockDBS3(self, workflow, migrateApi, destReadApi, sourceApi, dataset, blocks = None):
        """
        Submit one migration request for each block that needs to be migrated.
        If blocks argument is not specified, migrate the whole dataset.
        """
        wfnamemsg = "%s: " % (workflow)
        if blocks:
            blocksToMigrate = set(blocks)
        else:
            ## This is for the case to migrate the whole dataset, which we don't do
            ## at this point Feb/2015 (we always pass blocks).
            ## Make a set with the blocks that need to be migrated.
            blocksInDestDBS = set([block['block_name'] for block in destReadApi.listBlocks(dataset = dataset)])
            blocksInSourceDBS = set([block['block_name'] for block in sourceApi.listBlocks(dataset = dataset)])
            blocksToMigrate = blocksInSourceDBS - blocksInDestDBS
            msg = "Dataset %s in destination DBS with %d blocks; %d blocks in source DBS."
            msg = msg % (dataset, len(blocksInDestDBS), len(blocksInSourceDBS))
            self.logger.info(wfnamemsg+msg)
        numBlocksToMigrate = len(blocksToMigrate)
        if numBlocksToMigrate == 0:
            self.logger.info(wfnamemsg+"No migration needed.")
        else:
            msg = "Have to migrate %d blocks from %s to %s." % (numBlocksToMigrate, sourceApi.url, destReadApi.url)
            self.logger.info(wfnamemsg+msg)
            msg = "List of blocks to migrate:\n%s." % (", ".join(blocksToMigrate))
            self.logger.debug(wfnamemsg+msg)
            msg = "Submitting %d block migration requests to DBS3 ..." % (numBlocksToMigrate)
            self.logger.info(wfnamemsg+msg)
            numBlocksAtDestination = 0
            numQueuedUnkwonIds = 0
            numFailedSubmissions = 0
            migrationIdsInProgress = []
            for block in list(blocksToMigrate):
                ## Submit migration request for this block.
                (reqid, atDestination, alreadyQueued) = self.requestBlockMigration(workflow, migrateApi, sourceApi, block)
                ## If the block is already in the destination DBS instance, we don't need
                ## to monitor its migration status. If the migration request failed to be
                ## submitted, we retry it next time. Otherwise, save the migration request
                ## id in the list of migrations in progress.
                if reqid == None:
                    blocksToMigrate.remove(block)
                    if atDestination:
                        numBlocksAtDestination += 1
                    elif alreadyQueued:
                        numQueuedUnkwonIds += 1
                    else:
                        numFailedSubmissions += 1
                else:
                    migrationIdsInProgress.append(reqid)
            if numBlocksAtDestination > 0:
                msg = "%d blocks already in destination DBS." % (numBlocksAtDestination)
                self.logger.info(wfnamemsg+msg)
            if numFailedSubmissions > 0:
                msg  = "%d block migration requests failed to be submitted." % (numFailedSubmissions)
                msg += " Will retry them later."
                self.logger.info(wfnamemsg+msg)
            if numQueuedUnkwonIds > 0:
                msg  = "%d block migration requests were already queued," % (numQueuedUnkwonIds)
                msg += " but could not retrieve their request id."
                self.logger.info(wfnamemsg+msg)
            numMigrationsInProgress = len(migrationIdsInProgress)
            if numMigrationsInProgress == 0:
                msg = "No migrations in progress."
                self.logger.info(wfnamemsg+msg)
            else:
                msg = "%d block migration requests successfully submitted." % (numMigrationsInProgress)
                self.logger.info(wfnamemsg+msg)
                msg = "List of migration requests ids: %s" % (migrationIdsInProgress)
                self.logger.info(wfnamemsg+msg)
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
                self.logger.info(wfnamemsg+msg)
                for _ in range(numTimes):
                    msg  = "%d block migrations in progress." % (numMigrationsInProgress)
                    msg += " Will check migrations status in %d seconds." % (waitTime)
                    self.logger.info(wfnamemsg+msg)
                    time.sleep(waitTime)
                    ## Check the migration status of each block migration request.
                    ## If a block migration has succeeded or terminally failes, remove the
                    ## migration request id from the list of migration requests in progress.
                    for reqid in list(migrationIdsInProgress):
                        try:
                            status = migrateApi.statusMigration(migration_rqst_id = reqid)
                            state = status[0].get('migration_status')
                            retry = status[0].get('retry_count')
                        except Exception as ex:
                            msg = "Could not get status for migration id %d:\n%s" % (reqid, ex.msg)
                            self.logger.error(wfnamemsg+msg)
                        else:
                            if state == 2:
                                msg = "Migration id %d succeeded." % (reqid)
                                self.logger.info(wfnamemsg+msg)
                                migrationIdsInProgress.remove(reqid)
                                numSuccessfulMigrations += 1
                            if state == 9:
                                msg = "Migration id %d terminally failed." % (reqid)
                                self.logger.info(wfnamemsg+msg)
                                msg = "Full status for migration id %d:\n%s" % (reqid, str(status))
                                self.logger.debug(wfnamemsg+msg)
                                migrationIdsInProgress.remove(reqid)
                                numFailedMigrations += 1
                            if state == 3:
                                if retry < 3:
                                    msg = "Migration id %d failed (retry %d), but should be retried." % (reqid, retry)
                                    self.logger.info(wfnamemsg+msg)
                                else:
                                    msg = "Migration id %d failed (retry %d)." % (reqid, retry)
                                    self.logger.info(wfnamemsg+msg)
                                    msg = "Full status for migration id %d:\n%s" % (reqid, str(status))
                                    self.logger.debug(wfnamemsg+msg)
                                    migrationIdsInProgress.remove(reqid)
                                    numFailedMigrations += 1
                    numMigrationsInProgress = len(migrationIdsInProgress)
                    ## Stop waiting if there are no more migrations in progress.
                    if numMigrationsInProgress == 0:
                        break
                ## If after the 300 seconds there are still some migrations in progress, return
                ## with status 1.
                if numMigrationsInProgress > 0:
                    msg = "Migration of %s is taking too long - will delay the publication." % (dataset)
                    self.logger.info(wfnamemsg+msg)
                    return (1, "Migration of %s is taking too long." % (dataset))
            msg = "Migration of %s has finished." % (dataset)
            self.logger.info(wfnamemsg+msg)
            msg  = "Migration status summary (from %d input blocks to migrate):" % (numBlocksToMigrate)
            msg += " at destination = %d," % (numBlocksAtDestination)
            msg += " succeeded = %d," % (numSuccessfulMigrations)
            msg += " failed = %d," % (numFailedMigrations)
            msg += " submission failed = %d," % (numFailedSubmissions)
            msg += " queued with unknown id = %d." % (numQueuedUnkwonIds)
            self.logger.info(wfnamemsg+msg)
            ## If there were failed migrations, return with status 2.
            if numFailedMigrations > 0 or numFailedSubmissions > 0:
                msg = "Some blocks failed to be migrated."
                self.logger.info(wfnamemsg+msg)
                return (2, "Migration of %s failed." % (dataset))
            ## If there were no failed migrations, but we could not retrieve the request id
            ## from some already queued requests, return with status 3.
            if numQueuedUnkwonIds > 0:
                msg = "Some block migrations were already queued, but failed to retrieve their request id."
                self.logger.info(wfnamemsg+msg)
                return (3, "Migration of %s in unknown status." % (dataset))
            if (numBlocksAtDestination + numSuccessfulMigrations) != numBlocksToMigrate:
                msg  = "Something unexpected has happened."
                msg += " The numbers in the migration summary are not consistent."
                msg += " Make sure there is no bug in the code."
                self.logger.info(wfnamemsg+msg)
                return (4, "Migration of %s in some inconsistent status." % (dataset))
            msg = "Migration completed."
            self.logger.info(wfnamemsg+msg)
        migratedDataset = destReadApi.listDatasets(dataset = dataset, detail = True, dataset_access_type = '*')
        if not migratedDataset or migratedDataset[0].get('dataset', None) != dataset:
            return (4, "Migration of %s in some inconsistent status." % (dataset))
        return (0, "")


    def requestBlockMigration(self, workflow, migrateApi, sourceApi, block):
        """
        Submit migration request for one block, checking the request output.
        """
        wfnamemsg = "%s: " % (workflow)
        atDestination = False
        alreadyQueued = False
        reqid = None
        msg = "Submiting migration request for block %s ..." % (block)
        self.logger.info(wfnamemsg+msg)
        sourceURL = sourceApi.url
        data = {'migration_url': sourceURL, 'migration_input': block}
        try:
            result = migrateApi.submitMigration(data)
        except HTTPError as he:
            if "is already at destination" in he.msg:
                msg = "Block is already at destination."
                self.logger.info(wfnamemsg+msg)
                atDestination = True
            else:
                msg  = "Request to migrate %s failed." % (block)
                msg += "\nRequest detail: %s" % (data)
                msg += "\nDBS3 exception: %s" % (he.msg)
                self.logger.error(wfnamemsg+msg)
        if not atDestination:
            msg = "Result of migration request: %s" % (str(result))
            self.logger.debug(wfnamemsg+msg)
            reqid = result.get('migration_details', {}).get('migration_request_id')
            report = result.get('migration_report')
            if reqid == None:
                msg  = "Migration request failed to submit."
                msg += "\nMigration request results: %s" % (str(result))
                self.logger.error(wfnamemsg+msg)
            if "REQUEST ALREADY QUEUED" in report:
                ## Request could be queued in another thread, then there would be
                ## no id here, so look by block and use the id of the queued request.
                alreadyQueued = True
                try:
                    status = migrateApi.statusMigration(block_name = block)
                    reqid = status[0].get('migration_request_id')
                except Exception:
                    msg = "Could not get status for already queued migration of block %s." % (block)
                    self.logger.error(wfnamemsg+msg)
        return (reqid, atDestination, alreadyQueued)


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


    def publishInDBS3(self, workflow, sourceURL, inputDataset, toPublish, pnn):
        """
        Publish files into DBS3
        """
        wfnamemsg = "%s: " % (workflow)
        published = {}
        failed = {}
        publish_in_next_iteration = {}
        results = {}
        failure_reason = {}

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
        self.logger.debug(wfnamemsg+"Source API URL: %s" % sourceURL)
        sourceApi = dbsClient.DbsApi(url=sourceURL, proxy=proxy)
        self.logger.debug(wfnamemsg+"Global API URL: %s" % globalURL)
        globalApi = dbsClient.DbsApi(url=globalURL, proxy=proxy)
        self.logger.debug(wfnamemsg+"Destination API URL: %s" % self.publish_dbs_url)
        destApi = dbsClient.DbsApi(url=self.publish_dbs_url, proxy=proxy)
        self.logger.debug(wfnamemsg+"Destination read API URL: %s" % self.publish_read_url)
        destReadApi = dbsClient.DbsApi(url=self.publish_read_url, proxy=proxy)
        self.logger.debug(wfnamemsg+"Migration API URL: %s" % self.publish_migrate_url)
        migrateApi = dbsClient.DbsApi(url=self.publish_migrate_url, proxy=proxy)

        ## The 'inputdataset' field in the Couch documents may not be a real input of
        ## the file we want to publish, given that CRAB also puts in that field primary
        ## datasets when it runs PrivateMC workflows and Analysis workflows with users
        ## input files. So we try to distinguish here between these two situations: is
        ## 'inputdataset' a parent of the file we have to publish or not.
        isRealInput = False
        if len(inputDataset.split('/')) == 4:
            existing_datasets = sourceApi.listDatasets(dataset=inputDataset, detail=True, dataset_access_type='*')
            if existing_datasets:
                isRealInput = True

        if isRealInput:
            primary_ds_type = existing_datasets[0]['primary_ds_type']
            # There's little chance this is correct, but it's our best guess for now.
            # CRAB2 uses 'crab2_tag' for all cases
            existing_output = destReadApi.listOutputConfigs(dataset=inputDataset)
            if not existing_output:
                msg = "Unable to list output config for input dataset %s." % (inputDataset)
                self.logger.error(wfnamemsg+msg)
                global_tag = 'crab3_tag'
            else:
                global_tag = existing_output[0]['global_tag']
        else:
            msg = "This publication appears to be for private MC."
            self.logger.info(wfnamemsg+msg)
            primary_ds_type = 'mc'
            global_tag = 'crab3_tag'

        acquisition_era_name = "CRAB"
        processing_era_config = {'processing_version': 1, 'description': 'CRAB3_processing_era'}

        ## Loop over the datasets to publish.
        msg = "Starting iteration through datasets/files for publication."
        self.logger.debug(wfnamemsg+msg)
        for dataset, files in toPublish.iteritems():
            ## Make sure to add the dataset name as a key in all the dictionaries that will
            ## be returned.
            results[dataset] = {'files': 0, 'blocks': 0, 'existingFiles': 0}
            published[dataset] = []
            failed[dataset] = []
            publish_in_next_iteration[dataset] = []
            failure_reason[dataset] = ""
            ## If there are no files to publish for this dataset, continue with the next
            ## dataset.
            if not files:
                continue

            appName = 'cmsRun'
            appVer  = files[0]["swversion"]
            pset_hash = files[0]['publishname'].split("-")[-1]
            gtag = str(files[0]['globaltag'])
            if gtag == "None":
                gtag = global_tag
            acquisitionera = str(files[0]['acquisitionera'])
            if acquisitionera == "null":
                acquisitionera = acquisition_era_name
            empty, primName, procName, tier = dataset.split('/')

            primds_config = {'primary_ds_name': primName, 'primary_ds_type': primary_ds_type}
            msg = "About to insert primary dataset: %s" % (str(primds_config))
            self.logger.debug(wfnamemsg+msg)
            destApi.insertPrimaryDataset(primds_config)
            msg = "Successfully inserted primary dataset %s." % (primName)
            self.logger.debug(wfnamemsg+msg)

            ## Find all (valid) files already published in this dataset.
            try:
                existingDBSFiles = destReadApi.listFiles(dataset = dataset, detail = True)
                existingFiles = [f['logical_file_name'] for f in existingDBSFiles]
                existingFilesValid = [f['logical_file_name'] for f in existingDBSFiles if f['is_file_valid']]
                msg  = "Dataset %s already contains %d files" % (dataset, len(existingFiles))
                msg += " (%d valid, %d invalid)." % (len(existingFilesValid), len(existingFiles) - len(existingFilesValid))
                self.logger.info(wfnamemsg+msg)
                results[dataset]['existingFiles'] = len(existingFiles)
            except Exception as ex:
                msg  = "Error when listing files in DBS: %s" % (str(ex))
                msg += "\n%s" % (str(traceback.format_exc()))
                self.logger.error(wfnamemsg+msg)
                continue

            ## Is there anything to do?
            workToDo = False
            for file in files:
                if file['lfn'] not in existingFilesValid:
                    workToDo = True
                    break
            ## If there is no work to do (because all the files that were requested
            ## to be published are already published and in valid state), put the
            ## files in the list of published files and continue with the next dataset.
            if not workToDo:
                msg = "Nothing uploaded, %s has these files already or not enough files." % (dataset)
                self.logger.info(wfnamemsg+msg)
                published[dataset].extend([f['lfn'] for f in files])
                continue

            acquisition_era_config = {'acquisition_era_name': acquisitionera, 'start_date': 0}

            output_config = {'release_version': appVer,
                             'pset_hash': pset_hash,
                             'app_name': appName,
                             'output_module_label': 'o', #TODO
                             'global_tag': global_tag,
                            }
            msg = "Published output config."
            self.logger.debug(wfnamemsg+msg)

            dataset_config = {'dataset': dataset,
                              'processed_ds_name': procName,
                              'data_tier_name': tier,
                              'acquisition_era_name': acquisitionera,
                              'dataset_access_type': 'VALID', # TODO
                              'physics_group_name': 'CRAB3',
                              'last_modification_date': int(time.time()),
                             }
            msg = "About to insert dataset: %s" % (str(dataset_config))
            self.logger.info(wfnamemsg+msg)
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
                if file['lfn'] not in existingFilesValid:
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
                            blocksDict = destReadApi.listBlocks(logical_file_name=parentFile)
                            if not blocksDict:
                                ## No, this parent file is not in the destination DBS instance.
                                ## Maybe it is in the same DBS instance as the input dataset?
                                blocksDict = sourceApi.listBlocks(logical_file_name=parentFile)
                                if blocksDict:
                                    ## Yes, this parent file is in the same DBS instance as the input dataset.
                                    ## Add the corresponding block to the set of blocks from the source DBS
                                    ## instance that have to be migrated to the destination DBS.
                                    localParentBlocks.add(blocksDict[0]['block_name'])
                                else:
                                    ## No, this parent file is not in the same DBS instance as input dataset.
                                    ## Maybe it is in global DBS instance?
                                    blocksDict = globalApi.listBlocks(logical_file_name=parentFile)
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
                            self.logger.info(wfnamemsg+msg)
                            if parentFile in file['parents']:
                                file['parents'].remove(parentFile)
                    ## Add this file to the list of files to be published.
                    dbsFiles.append(self.format_file_3(file))
                published[dataset].append(file['lfn'])

            ## Print a message with the number of files to publish.
            msg = "Found %d files not already present in DBS which will be published." % (len(dbsFiles))
            self.logger.info(wfnamemsg+msg)

            ## If there are no files to publish, continue with the next dataset.
            if len(dbsFiles) == 0:
                msg = "Nothing to do for this dataset."
                self.logger.info(wfnamemsg+msg)
                continue

            ## Migrate parent blocks before publishing.
            ## First migrate the parent blocks that are in the same DBS instance
            ## as the input dataset.
            if localParentBlocks:
                msg = "List of parent blocks that need to be migrated from %s:\n%s" % (sourceApi.url, localParentBlocks)
                self.logger.info(wfnamemsg+msg)
                statusCode, failureMsg = self.migrateByBlockDBS3(workflow, migrateApi, destReadApi, sourceApi, inputDataset, localParentBlocks)
                if statusCode:
                    failureMsg += " Not publishing any files."
                    self.logger.info(wfnamemsg+failureMsg)
                    failed[dataset].extend([f['logical_file_name'] for f in dbsFiles])
                    failure_reason[dataset] = failureMsg
                    continue
            ## Then migrate the parent blocks that are in the global DBS instance.
            if globalParentBlocks:
                msg = "List of parent blocks that need to be migrated from %s:\n%s" % (globalApi.url, globalParentBlocks)
                self.logger.info(wfnamemsg+msg)
                statusCode, failureMsg = self.migrateByBlockDBS3(workflow, migrateApi, destReadApi, globalApi, inputDataset, globalParentBlocks)
                if statusCode:
                    failureMsg += " Not publishing any files."
                    self.logger.info(wfnamemsg+failureMsg)
                    failed[dataset].extend([f['logical_file_name'] for f in dbsFiles])
                    failure_reason[dataset] = failureMsg
                    continue
            ## Publish the files in blocks. The blocks must have exactly max_files_per_block
            ## files, unless there are less than max_files_per_block files to publish to
            ## begin with. If there are more than max_files_per_block files to publish,
            ## publish as many blocks as possible and leave the tail of files for the next
            ## PublisherWorker call, unless forced to published.
            block_count = 0
            count = 0
            while True:
                block_name = "%s#%s" % (dataset, str(uuid.uuid4()))
                files_to_publish = dbsFiles[count:count+self.max_files_per_block]
                try:
                    block_config = {'block_name': block_name, 'origin_site_name': pnn, 'open_for_writing': 0}
                    msg = "Inserting files %s into block %s." % ([f['logical_file_name'] for f in files_to_publish], block_name)
                    self.logger.debug(wfnamemsg+msg)
                    blockDump = self.createBulkBlock(output_config, processing_era_config, primds_config, dataset_config, \
                                                     acquisition_era_config, block_config, files_to_publish)
                    #self.logger.debug(wfnamemsg+"Block to insert: %s\n" % pprint.pformat(blockDump))
                    destApi.insertBulkBlock(blockDump)
                    block_count += 1
                except Exception as ex:
                    failed[dataset].extend([f['logical_file_name'] for f in files_to_publish])
                    msg  = "Error when publishing (%s) " % ", ".join(failed[dataset])
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(wfnamemsg+msg)
                    failure_reason[dataset] = str(ex)
                count += self.max_files_per_block
                files_to_publish_next = dbsFiles[count:count+self.max_files_per_block]
                if len(files_to_publish_next) < self.max_files_per_block:
                    publish_in_next_iteration[dataset].extend([f['logical_file_name'] for f in files_to_publish_next])
                    break
            published[dataset] = filter(lambda x: x not in failed[dataset] + publish_in_next_iteration[dataset], published[dataset])
            ## Fill number of files/blocks published for this dataset.
            results[dataset]['files'] = len(dbsFiles) - len(failed[dataset]) - len(publish_in_next_iteration[dataset])
            results[dataset]['blocks'] = block_count
            ## Print a publication status summary for this dataset.
            msg  = "End of publication status for dataset %s:" % (dataset)
            msg += " failed (%s) %s" % (len(failed[dataset]), failed[dataset])
            msg += ", published (%s) %s" % (len(published[dataset]), published[dataset])
            msg += ", publish_in_next_iteration (%s) %s" % (len(publish_in_next_iteration[dataset]), publish_in_next_iteration[dataset])
            msg += ", results %s" % (results[dataset])
            self.logger.info(wfnamemsg+msg)
        return failed, failure_reason, published, results

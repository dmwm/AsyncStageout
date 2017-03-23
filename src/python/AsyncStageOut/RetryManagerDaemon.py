#!/usr/bin/env python
#pylint: disable-msg=W0613,invalid-name,logging-not-lazy,broad-except
"""
__RetryManagerPoller__

This component does the actualy retry logic. It allows to have
different algorithms.
"""

import time
import urllib
import logging
import datetime
import traceback
import json
from datetime import timedelta
from Queue import Queue
from threading import Thread
import fts3.rest.client.easy as fts3
import re

from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException
from WMCore.Database.CMSCouch import CouchServer

from AsyncStageOut import getProxy
from AsyncStageOut import getDNFromUserName
from AsyncStageOut.BaseDaemon import BaseDaemon
from AsyncStageOut import getHashLfn
from RESTInteractions import HTTPRequests
from ServerUtilities import  generateTaskName,\
        PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping

__all__ = []


def convertdatetime(time_to_convert):
    """
    Convert dates into useable format.
    """
    return int(time.mktime(time_to_convert.timetuple()))


def timestamp():
    """
    generate a timestamp
    """
    time_now = datetime.datetime.now()
    return convertdatetime(time_now)


class RetryManagerException(WMException):
    """
    _RetryManagerException_

    It's totally awesome, except it's not.
    """


class RetryManagerDaemon(BaseDaemon):
    """
    _RetryManagerPoller_

    Polls for Files in CoolOff State and attempts to retry them
    based on the requirements in the selected plugin
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseDaemon.__init__(self, config, 'RetryManager')

        if self.config.isOracle:
            try:
                self.oracleDB = HTTPRequests(self.config.oracleDB,
                                             self.config.opsProxy,
                                             self.config.opsProxy)
            except:
                self.logger.exception('Failed to connect to Oracle')
        else:
            try:
                server = CouchServer(dburl=self.config.couch_instance,
                                     ckey=self.config.opsProxy,
                                     cert=self.config.opsProxy)
                self.db = server.connectDatabase(self.config.files_database)
            except Exception as e:
                self.logger.exception('A problem occured when connecting to couchDB: %s' % e)
                raise
            self.logger.debug('Connected to files DB')

            # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.retryAlgoDir, namespace=self.config.retryAlgoDir)
        try:
            self.plugin = self.factory.loadObject(self.config.algoName, self.config,
                                                  getFromCache=False, listFlag=True)
        except Exception as ex:
            msg = "Error loading plugin %s on path %s\n" % (self.config.algoName,
                                                            self.config.retryAlgoDir)
            msg += str(ex)
            self.logger.error(msg)
            raise RetryManagerException(msg)
        self.cooloffTime = self.config.cooloffTime

    def terminate(self, params):
        """
        Run one more time through, then terminate

        """
        logging.debug("Terminating. doing one more pass before we die")
        self.algorithm(params)

    def algorithm(self, parameters=None):
        """
        Performs the doRetries method, loading the appropriate
        plugin for each job and handling it.
        """
        logging.debug("Running retryManager algorithm")
        if self.config.isOracle:
            fileDoc = dict()
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'retryTransfers'
            fileDoc['time_to'] = self.cooloffTime
            self.logger.debug('fileDoc: %s' % fileDoc)
            try:
                results = self.oracleDB.post(self.config.oracleFileTrans,
                                             data=encodeRequest(fileDoc))
            except Exception:
                self.logger.exception("Failed to get retry transfers in oracleDB: %s")
                return
            logging.info("Retried files in cooloff: %s,\n now getting transfers to kill" % str(results))

            fileDoc = dict()
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'getTransfersToKill'
            fileDoc['grouping'] = 0
            try:
                results = self.oracleDB.get(self.config.oracleFileTrans,
                                            data=encodeRequest(fileDoc))
                result = oracleOutputMapping(results)
            except Exception as ex:
                self.logger.error("Failed to get killed transfers \
                                   from oracleDB: %s" % ex)
                return

            usersToKill = list(set([(x['username'], x['user_group'], x['user_role']) for x in result]))

            self.logger.debug("Users with transfers to kill: %s" % usersToKill)
            transfers = Queue()

            for i in range(self.config.kill_threads):
                worker = Thread(target=self.killThread, args=(i, transfers,))
                worker.setDaemon(True)
                worker.start()

            for user in usersToKill:
                user_trans = [x for x in result if (x['username'], x['user_group'], x['user_role']) == user]
                self.logger.info("Inserting %s transfers of user %s in the killing queue" % (len(user_trans), user))
                transfers.put(user_trans)

            transfers.join()
            self.logger.info("Transfers killed.")
        else:
            self.doRetries()

    def killThread(self, thread_id, transfers):
        """This is the worker thread function for kill command.
        """
        while True:
            transfer_list = transfers.get()
            self.logger.info("Starting thread %s" % (thread_id))
            user = transfer_list[0]['username']
            group = transfer_list[0]['user_group']
            role = transfer_list[0]['user_role']

            uiSetupScript = getattr(self.config, 'UISetupScript', None)

            self.logger.debug("Trying to get DN for %s %s %s %s" % (user, self.logger, self.config.opsProxy, self.config.opsProxy))
            try:
                userDN = getDNFromUserName(user, self.logger, ckey=self.config.opsProxy, cert=self.config.opsProxy)
            except Exception as ex:
                msg = "Error retrieving the user DN"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if not userDN:
                transfers.task_done()
                time.sleep(1)
                continue
            self.logger.debug("user DN: %s" % userDN)

            try:
                defaultDelegation = {'logger': self.logger,
                                     'credServerPath': self.config.credentialDir,
                                     'myProxySvr': 'myproxy.cern.ch',
                                     'min_time_left': getattr(self.config, 'minTimeLeft', 36000),
                                     'serverDN': self.config.serverDN,
                                     'uisource': uiSetupScript,
                                     'cleanEnvironment': getattr(self.config, 'cleanEnvironment', False)}
                if hasattr(self.config, "cache_area"):
                    cache_area = self.config.cache_area
                    defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(cache_area)[0]
            except IndexError:
                self.logger.error('MyproxyAccount parameter cannot be retrieved from %s . ' % self.config.cache_area)
                transfers.task_done()
                time.sleep(1)
                continue
            if getattr(self.config, 'serviceCert', None):
                defaultDelegation['server_cert'] = self.config.serviceCert
            if getattr(self.config, 'serviceKey', None):
                defaultDelegation['server_key'] = self.config.serviceKey
            try:
                defaultDelegation['userDN'] = userDN
                defaultDelegation['group'] = group if group else ''
                defaultDelegation['role'] = role if group else ''
                self.logger.debug('delegation: %s' % defaultDelegation)
                valid_proxy, user_proxy = getProxy(defaultDelegation, self.logger)
            except Exception as ex:
                msg = "Error getting the user proxy"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                transfers.task_done()
                time.sleep(1)
                continue

            # TODO: take server from db, right now, take only the first of the list and assuming it valid for all
            try:
                # TODO: debug u added during info upload. To be fixed soon! For now worked around
                fts_server = transfer_list[0]['fts_instance'].split('u')[1]
                self.logger.info("Delegating proxy to %s" % fts_server)
                context = fts3.Context(fts_server, user_proxy, user_proxy, verify=True)
                self.logger.debug(fts3.delegate(context, lifetime=timedelta(hours=48), force=False))

                self.logger.info("Proxy delegated. Grouping files by jobId")
                jobs = {}
                for fileToKill in transfer_list:
                    # TODO: debug u added during info upload. To be fixed soon! For now worked around
                    jid = str(fileToKill['fts_id']).split('u')[1]
                    if jid not in jobs:
                        jobs[jid] = []
                    jobs[jid].append(fileToKill)

                self.logger.info("Found %s jobIds", len(jobs.keys()))
                self.logger.debug("jobIds: %s", jobs.keys)

                # list for files killed or failed to
                killed = []
                too_late = []

                for ftsJobId, files in jobs.iteritems():
                    self.logger.info("Cancelling tranfers in %s" % ftsJobId)

                    ref_lfns = [str(x['destination_lfn'].split('/store/')[1]) for x in files]
                    source_lfns = [x['source_lfn'] for x in files]

                    job_list = fts3.get_job_status(context, ftsJobId, list_files=True)
                    tx = job_list['files']

                    # TODO: this workaround is needed to get FTS file id, we may want to add a column in the db?
                    idListToKill = [x['file_id'] for x in tx
                                    if x['dest_surl'].split('/cms/store/')[1] in ref_lfns]

                    # needed for the state update
                    lfnListToKill = [ref_lfns.index(str(x['dest_surl'].split('/cms/store/')[1])) for x in tx
                                       if x['dest_surl'].split('/cms/store/')[1] in ref_lfns]

                    self.logger.debug("List of ids to cancel for job %s: %s" % (ftsJobId, idListToKill))
                    res = fts3.cancel(context, ftsJobId, idListToKill)
                    self.logger.debug('Kill command result: %s' % json.dumps(res))

                    if not isinstance(res, list):
                        res = [res]

                    # Verify if the kill command succeeded
                    for k, kill_res in enumerate(res):
                        indexToUpdate = lfnListToKill[k]
                        if kill_res in ("FINISHEDDIRTY", "FINISHED", "FAILED"):
                            self.logger.debug(source_lfns[indexToUpdate])
                            too_late.append(getHashLfn(source_lfns[indexToUpdate]))
                        else:
                            killed.append(getHashLfn(source_lfns[indexToUpdate]))

                # TODO: decide how to update status for too_late files
                killed += too_late
                self.logger.debug('Updating status of killed files: %s' % killed)

                if len(killed) > 0:
                    data = dict()
                    data['asoworker'] = self.config.asoworker
                    data['subresource'] = 'updateTransfers'
                    data['list_of_ids'] = killed
                    data['list_of_transfer_state'] = ["KILLED" for _ in killed]
                    self.oracleDB.post(self.config.oracleFileTrans,
                                       data=encodeRequest(data))
                    self.logger.debug("Marked killed %s" % killed)
            except:
                # TODO: split and improve try/except
                self.logger.exception('Kill command failed')

            transfers.task_done()

    def processRetries(self, files):
        """
        _processRetries_

        Actually does the dirty work of figuring out what to do with jobs
        """
        if len(files) < 1:
            # We got no files?
            return

        propList = []
        fileList = self.loadFilesFromList(recList=files)
        logging.debug("Files in cooloff %s" % fileList)
        # Now we should have the files
        propList = self.selectFilesToRetry(fileList)
        logging.debug("Files to retry %s" % propList)
        now = str(datetime.datetime.now())
        for file in propList:
            # update couch
            self.logger.debug("Trying to resubmit %s" % file['id'])
            try:
                document = self.db.document(file['id'])
            except Exception as ex:
                msg = "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if document['state'] != 'killed':
                data = dict()
                data['state'] = 'new'
                data['last_update'] = time.time()
                data['retry'] = now
                updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + file['id']
                updateUri += "?" + urllib.urlencode(data)
                try:
                    self.db.makeRequest(uri=updateUri, type="PUT", decode=False)
                except Exception as ex:
                    msg = "Error updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                self.logger.debug("%s resubmitted" % file['id'])
            else:
                continue
        return

    def loadFilesFromList(self, recList):
        """
        _loadFilesFromList_

        Load jobs in bulk
        """
        all_files = []
        index = 0
        for record in recList:
            all_files.append({})
            all_files[index]['id'] = record['key']
            all_files[index]['state_time'] = record['value']
            index += 1
        return all_files

    def selectFilesToRetry(self, fileList):
        """
        _selectFilesToRetry_

       Select files to retry
       """
        result = []

        if len(fileList) == 0:
            return result
        for file in fileList:
            logging.debug("Current file %s" %file)
            try:
                if self.plugin.isReady(file=file, cooloffTime=self.cooloffTime):
                    result.append(file)
            except Exception as ex:
                msg = "Exception while checking for cooloff timeout for file %s\n" % file
                msg += str(ex)
                logging.error(msg)
                logging.debug("File: %s\n" % file)
                raise RetryManagerException(msg)

        return result

    def doRetries(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover files that are in cooloff
        query = {'stale': 'ok'}
        try:
            files = self.db.loadView('AsyncTransfer', 'getFilesToRetry', query)['rows']
        except Exception as e:
            self.logger.exception('A problem occured when contacting \
                                  couchDB to retrieve LFNs: %s' % e)
            return
        logging.info("Found %s files in cooloff" % len(files))
        self.processRetries(files)

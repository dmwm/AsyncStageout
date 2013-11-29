#!/usr/bin/env
#pylint: disable-msg=W0613,C0103
"""
It populates user_monitoring_db database
with the details of users transfers from
files_database.
"""
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from AsyncStageOut import getHashLfn
import time
import traceback
from time import strftime
import json
import socket
import stomp

def clean_states( states ):
    """
    _clean_states_
    Funtion to clean the dic. of states.
    """
    new_states = {}
    for state in states:
        if (states[state] != 0) :
            new_states[state] = states[state]
    return new_states

class AnalyticsDaemon(BaseWorkerThread):
    """
    _AnalyticsDaemon_
    Update user_monitoring_db database on couch
    Delete old documents from user_monitoring_db.
    """
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config.Analytics

        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)
        self.logger.debug('Configuration loaded')
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to local couchDB')
        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.amq_auth_file = self.config.amq_auth_file
        monitoring_server = CouchServer(dburl=self.config.couch_user_monitoring_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.monitoring_db = monitoring_server.connectDatabase(self.config.user_monitoring_db)
        self.logger.debug('Connected to user_monitoring_db in couchDB')

    def algorithm(self, parameters = None):
        """
        a. create/update summary_per_workflow documents
        b. create summary_per_file documents
        c. clean user_monitoring_db database by removing old docs
        """
        query = {'stale':'ok'}
        try:
            param = self.config_db.loadView('asynctransfer_config', 'GetAnalyticsConfig', query)
            self.config.summaries_expiration_days = param['rows'][0]['key']
            self.logger.debug('Got summaries_expiration_days %s from Couch' % self.config.summaries_expiration_days)
        except Exception, e:
            self.logger.exception('A problem occured when contacting config DB in couch: %s' % e)
        self.logger.debug('Analytics starts at: %s' %str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime())))
        if self.amq_auth_file:
            self.updateDatabaseSource()
        # TODO: Evaluate if we still need for crab -status
        #self.updateWorkflowSummaries()
        self.updateFilesSummaries()
        self.cleanOldDocs()
        self.logger.debug('Analytics ends at: %s' %str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime())))

    def updateWorkflowSummaries(self):
        """
        Get the list of new states and update documents in user_monitoring_db
        """
        active_jobs = 0
        updated = 0
        states = {}
        now = int(time.time())
        query = {'reduce':True, 'group': True, 'stale':'ok'}
        try:
            active_jobs = self.db.loadView('AsyncTransfer', 'JobsSatesByWorkflow', query)['rows']
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)
            return
        for job in active_jobs:
            workflow = job['key']
            jobs_states = job['value']
            all_states = {}
            pub_state = {}
            query = {'reduce':True, 'group': True, 'key':workflow, 'stale':'ok'}
            try:
                publication_state = self.db.loadView('AsyncTransfer', 'PublicationStateByWorkflow', query)['rows']
            except:
                return
            if publication_state:
                all_states = publication_state[0]['value'].copy()
            all_states.update(jobs_states)
            current_states = clean_states( all_states )
            query = {'key': workflow, 'stale':'ok'}
            try:
                mon_states = self.monitoring_db.loadView('UserMonitoring', 'StatesByWorkflow', query)['rows'][0]['value']
                mon_publication_states = self.monitoring_db.loadView('UserMonitoring',
                                                                     'PublicationStatesByWorkflow',
                                                                     query)['rows'][0]['value']
                all_mon_states = dict(mon_states.items() + mon_publication_states.items())
            except IndexError:
                # Prepare a new document and insert it in couch
                doc = {}
                doc['publication_state'] = {}
                doc['_id'] = workflow
                doc['state'] = clean_states( jobs_states )
                if publication_state:
                    doc['publication_state'] = clean_states( publication_state[0]['value'].copy() )
                if doc['state'].has_key("failed"):
                    failures_reasons = {}
                    query = {'reduce':True, 'group_level':2, 'startkey': [workflow], 'endkey':[workflow, {}], 'stale':'ok'}
                    failures = self.db.loadView('AsyncTransfer', 'JobsByFailuresReasons', query)['rows']
                    self.logger.error(failures)
                    for failure in failures:
                        if failure['key'][1]:
                            if  isinstance(failure['key'][1], list):
                                failures_reasons[failure['key'][1][0]] = failure['value']
                            else:
                                failures_reasons[failure['key'][1]] = failure['value']
                    doc['failures_reasons'] = failures_reasons
                doc['last_update'] = now
                doc['type'] = 'aso_workflow'
                try:
                    self.monitoring_db.queue(doc, True)
                    updated += 1
                except Exception, ex:
                    msg =  "Error queuing document in monitoring_db"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                continue
            except:
                return
            if all_mon_states != current_states:
                try:
                    doc = self.monitoring_db.document( workflow )
                    doc['state'] = clean_states( jobs_states )
                    if publication_state:
                        doc['publication_state'] = clean_states( publication_state[0]['value'].copy() )
                    if doc['state'].has_key("failed"):
                        failures_reasons = {}
                        query = {'reduce':True, 'group_level':2, 'startkey': [workflow], 'endkey':[workflow, {}], 'stale':'ok'}
                        failures = self.db.loadView('AsyncTransfer', 'JobsByFailuresReasons', query)['rows']
                        for failure in failures:
                            if failure['key'][1]:
                                if  isinstance(failure['key'][1], list):
                                    failures_reasons[failure['key'][1][0]] = failure['value']
                                else:
                                    failures_reasons[failure['key'][1]] = failure['value']
                        doc['failures_reasons'] = failures_reasons
                    doc['last_update'] = now
                except Exception, ex:
                    msg =  "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                try:
                    self.monitoring_db.queue(doc, True)
                    updated += 1
                except Exception, ex:
                    msg =  "Error queuing document in monitoring_db"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
        # Perform a bulk update of documents
        try:
            self.monitoring_db.commit()
        except Exception, ex:
            msg =  "Error commiting documents in monitoring_db"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

        self.logger.debug("%d summary_per_workflows documents updated..." % updated)
        return

    def updateFilesSummaries(self):
        """
        Create summaries docs from new files done/failed
        """
        end_time = 0
        all_files = 0
        try:
            query = {}
            since = self.config_db.loadView('asynctransfer_config', 'LastSummariesUpdate', query)['rows'][0]['key']
            # TODO: Evaluate if it helps to improve performance and does not break anything
            #query = {'limit' : 1, 'descending': True, 'stale':'ok'}
            query = {'limit' : 1, 'descending': True}
            end_time = self.db.loadView('AsyncTransfer', 'LastUpdatePerFile', query)['rows'][0]['key']
        except IndexError:
            self.logger.debug('No records to determine end time, waiting for next iteration')
            return
        except KeyError:
            self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
            return
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB to updateFilesSummaries: %s' % e)
            return
        self.logger.debug('start summaries update %f end time %f' %(since, end_time))
        updateUri = "/" + self.config_db.name + "/_design/asynctransfer_config/_update/lastDBUpdate/MONITORING_DB_UPDATE"
        updateUri += "?db_update=%d" % ( end_time + 0.000001)
        try:
            self.config_db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            query = { 'startkey': since, 'endkey': end_time + 1 }
            all_files = self.db.loadView('AsyncTransfer', 'LastUpdatePerFile', query)['rows']
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB for LastUpdatePerFile: %s' % e)
            return
        for file in all_files:
            if self.monitoring_db.documentExists(file['value']['id']):
                doc = self.monitoring_db.document(file['value']['id'])
            else:
                doc = {}
            doc['type'] = 'aso_file'
            doc['_id'] = file['value']['id']
            doc['workflow'] = file['value']['workflow']
            doc['lfn'] = file['value']['lfn']
            doc['location'] = file['value']['location']
            doc['checksum'] = file['value']['checksum']
            doc['jobid'] = file['value']['jobid']
            doc['retry_count'] = file['value']['retry_count']
            doc['size'] = file['value']['size']
            doc['state'] = file['value']['state']
            # TODO: For backward compatibility. Remove it later
            if file['value'].has_key('publication_state'):
                doc['publication_state'] = file['value']['publication_state']
            #doc['publication_state'] = file['value']['publication_state']
            doc['last_update'] = time.time()
            if file['value'].has_key('publish'):
                doc['publish'] = file['value']['publish']
            else:
                doc['publish'] = 0
            if file['value']['failure_reason']:
                doc['failure_reason'] = file['value']['failure_reason']
            if file['value'].has_key('type'):
                doc['file_type'] = file['value']['type']
            else:
                doc['file_type'] = 'unknown'
            try:
                self.logger.debug("updating %s" %doc)
                self.monitoring_db.queue(doc, True)
            except Exception, ex:
                msg =  "Error queuing document in monitoring_db"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
        try:
            self.monitoring_db.commit()
            self.logger.debug("%d summary_per_files documents updated..." % len(all_files))
        except Exception, ex:
            msg =  "Error commiting documents in monitoring_db"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        return

    def updateDatabaseSource(self):
        """
        Update database source
        """
        end_time = 0
        all_files = 0
        list_jobs = []
        try:
            query = {}
            since = self.config_db.loadView('asynctransfer_config', 'LastStatusCheck', query)['rows'][0]['key']
            ###query = {'limit':1, 'descending':True, 'stale':'ok'}
            query = {'limit':1, 'descending':True}
            end_time = self.monitoring_db.loadView('UserMonitoring', 'JobIdByEndTime', query)['rows'][0]['key']
            self.logger.debug('end time %s' %end_time)
        except IndexError:
            self.logger.debug('No records to determine end time, waiting for next iteration')
            return
        except KeyError:
            self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
            return
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB to updateDatabaseSource: %s' % e)
            return
        self.logger.debug('start time %f end time %f' %(since, end_time))
        updateUri = "/" + self.config_db.name + "/_design/asynctransfer_config/_update/lastCheckStatusTime/LAST_CHECKSTATUS_TIME"
        updateUri += "?last_checkstatus_time=%f" % ( end_time + 0.000001)
        try:
            query = {'startkey': since, 'endkey': end_time + 1}
            all_jobs = self.monitoring_db.loadView('UserMonitoring', 'JobIdByEndTime', query)['rows']
        except Exception, e:
            self.logger.exception('A problem occured when contacting UserMonitoring - JobIdByEndTime: %s' % e)
            return
        self.logger.debug("Processing record...")
        cache_list = []
        for job in all_jobs:
            done_files = []
            failed_files = []
            killed_files = []
            message = {}
            status = {}
            cache_list.append(job['value'])
            query = {'reduce':True, 'key':job['value']}
            try:
                ended_files = self.monitoring_db.loadView('UserMonitoring', 'EndedLFNByJobId', query)['rows']
            except Exception, e:
                self.logger.exception('A problem occured when contacting UserMonitoring - EndedLFNByJobId: %s' % e)
                return
            if ended_files:
                try:
                    job_doc = self.monitoring_db.document(str(job['value']))
                except:
                    self.logger.debug("%s doc not found" %str(job['value']))
                    continue
                number_ended_files = ended_files[0]['value']
                self.logger.info("Number of ended file is %s for %s" %(number_ended_files, job))
                # TODO: Implement this fix to make the log transfer optional
                ##if number_ended_files == job_doc['files_to_transfer']:
                if number_ended_files == job_doc['files']:
                    query = {'key':job['value'], 'reduce': False}
                    try:
                        done_files = self.monitoring_db.loadView('UserMonitoring', 'LFNDoneByJobId', query)['rows']
                    except Exception, e:
                        self.logger.exception('A problem occured when contacting UserMonitoring - LFNDoneByJobId: %s' % e)
                        return
                    self.logger.info("the jobid %s has to publish %s done files" %(job, len(done_files)))
                    message['PandaID'] = job['value']
                    self.logger.info("the job %s has %s done files %s" %(job, number_ended_files, done_files))
                    for file in done_files:
                        status[file['value'].replace('store', 'store/temp', 1)] = 'done'
                    if job_doc.has_key('log_file'):
                        status[job_doc['log_file']] = 'done'
                        ##if len(files_to_publish) < len(job_doc['files']):
                        ##    for out_lfn in job_doc['files']:
                        ##        if not status.has_key(out_lfn): status[out_lfn] = 'done'
                    message['transferStatus'] = status
                    if len(done_files) != number_ended_files:
                        try:
                            failed_files = self.monitoring_db.loadView('UserMonitoring', 'LFNFailedByJobId', query)['rows']
                        except Exception, e:
                            self.logger.exception('A problem occured when contacting UserMonitoring - LFNFailedByJobId: %s' % e)
                            return
                        self.logger.info("the job %s has %s failed files %s" %(job, len(failed_files), failed_files))
                        transferError = ""
                        for file in failed_files:
                            if file['value'].find('temp') > 1:
                                status[file['value']] = 'failed'
                                lfn = file['value']
                                docId = getHashLfn(lfn)
                                # Load document to get the failure reason from output file
                                try:
                                    document = self.monitoring_db.document( docId )
                                    #if (document['file_type'] == "output" and document.has_key('failure_reason')):
                                    if document.has_key('failure_reason'):
                                        if transferError:
                                            transferError = transferError + "," + document['failure_reason']
                                        else:
                                            transferError = document['failure_reason']
                                    if document.has_key('publication_state'):
                                        if document['publication_state'] == 'publication_failed':
                                            if transferError:
                                                transferError = transferError + "," + 'Publication Failure'
                                            else:
                                                transferError = 'Publication Failure'
                                except Exception, ex:
                                    msg =  "Error loading document from couch"
                                    msg += str(ex)
                                    msg += str(traceback.format_exc())
                                    self.logger.error(msg)
                                    continue
                            else:
                                lfn = file['value'].replace('store', 'store/temp', 1)
                                status[lfn] = 'failed'
                                docId = getHashLfn(lfn)
                                # Load document to get the failure reason from output file
                                try:
                                    document = self.monitoring_db.document( docId )
                                    #if (document['file_type'] == "output" and document.has_key('failure_reason')):
                                    if document.has_key('failure_reason'):
                                        if transferError:
                                            transferError = transferError + "," + document['failure_reason']
                                        else:
                                            transferError = document['failure_reason']
                                    if document.has_key('publication_state'):
                                        if document['publication_state'] == 'publication_failed':
                                            if transferError:
                                                transferError = transferError + "," + 'Publication Failure'
                                            else:
                                                transferError = 'Publication Failure'
                                except Exception, ex:
                                    msg =  "Error loading document from couch"
                                    msg += str(ex)
                                    msg += str(traceback.format_exc())
                                    self.logger.error(msg)
                                    continue
                        ##if len(files_to_publish) < len(job_doc['files']):
                        ##    for out_lfn in job_doc['files']:
                        ##        if not status.has_key(out_lfn): status[out_lfn] = 'failed'
                        if len(failed_files) and not transferError:
                            transferError = "Outputs management error"
                        message['transferStatus'] = status
                        message['transferError'] = transferError
                        message['transferErrorCode'] = 100
                    if (len(done_files)+len(failed_files)) != number_ended_files:
                        try:
                            cancelled_files = self.monitoring_db.loadView('UserMonitoring', 'LFNKilledByJobId', query)['rows']
                        except Exception, e:
                            self.logger.exception('A problem occured when contacting UserMonitoring - LFNKilledByJobId: %s' % e)
                            return
                        self.logger.info("the jobid %s has to publish %s killed files" %(job, len(cancelled_files)))
                        message['PandaID'] = job['value']
                        self.logger.info("the job %s has %s killed files %s" %(job, number_ended_files, cancelled_files))
                        for file in cancelled_files:
                            if file['value'].find('temp') > 1:
                                status[file['value']] = 'cancelled'
                            else:
                                status[file['value'].replace('store', 'store/temp', 1)] = 'cancelled'
                        message['transferStatus'] = status
                        message['transferError'] = "Output files killed"
            if message:
                self.logger.info("publish this %s" %message)
                try:
                    self.produce(message)
                except Exception, ex:
                    msg =  "Error producing message"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                #try:
                #    self.logger.info("remove this doc %s" %job_doc['_id'])
                #    self.monitoring_db.queueDelete(job_doc)
                #    self.monitoring_db.commit( )
                #except Exception, e:
                #    self.logger.exception('A problem occured when removing docs: %s %s' % (message['PandaID'], e))

        try:
            self.config_db.makeRequest(uri = updateUri, type = "PUT", decode = False)
        except Exception, e:
            self.logger.error('A problem occured when updating last check time!!!: %s' % e)
            return
        # TODO:bulk commit of docs but not sure if it works
        #try:
        #    self.monitoring_db.commit( )
        #except Exception, e:
        #    self.logger.exception('A problem occured when commiting docs: %s' %e)

        return

    def produce(self, message ):
        """
        Produce state messages: jobid:state
        """
        opened = False
        while not opened:
            try:
                f = open(self.amq_auth_file)
                authParams = json.loads(f.read())
                opened = True
            except Exception, ex:
                msg =  "Error loading auth params"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                pass
        connected = False
        while not connected:
            try:
                # connect to the stompserver
                host = [(authParams['MSG_HOST'], authParams['MSG_PORT'])]
                conn = stomp.Connection(host, authParams['MSG_USER'], authParams['MSG_PWD'])
                conn.start()
                conn.connect()
                messageDict = json.dumps(message)
                # send the message
                conn.send( messageDict, destination=authParams['MSG_QUEUE'] )
                # disconnect from the stomp server
                conn.disconnect()
                connected = True
            except Exception, ex:
                msg =  "Error contacting Message Broker"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                pass

    def cleanOldDocs(self):
        """
        Clean summaries created before self.config.summaries_expiration_days
        """
        files_to_remove = []
        expiration_time = int(time.time()) - (86400 * self.config.summaries_expiration_days)
        query = {'startkey': 1, 'endkey': expiration_time, 'stale': 'ok'}
        try:
            files_to_remove = self.monitoring_db.loadView('UserMonitoring', 'DocsByTimestamp', query)['rows']
        except Exception, e:
            self.logger.exception('A problem occured when contacting UserMonitoring to get old docs: %s' % e)
            return
        self.logger.debug('removing %s docs' %len(files_to_remove))
        for old_file in files_to_remove:
            try:
                document = self.monitoring_db.document( old_file['value'] )
            except:
                self.logger.debug( '%s does not exist and cannot be remove' %old_file['value'] )
                pass
            try:
                self.monitoring_db.queueDelete(document)
                self.logger.debug( '%s removed' %old_file['value'] )
            except:
                self.logger.debug( '%s cannot be remove from couch' %old_file['value'] )
                pass
        try:
            self.monitoring_db.commit()
            self.logger.debug("%d Old summaries cleaned..." % len(files_to_remove))
        except Exception, ex:
            msg =  "Error commiting documents in monitoring_db"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        return

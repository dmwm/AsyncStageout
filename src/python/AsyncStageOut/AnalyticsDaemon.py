#!/usr/bin/env
#pylint: disable-msg=W0613,C0103
"""
It populates user_monitoring_db database
with the details of users transfers from
files_database.
"""
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from AsyncStageOut import getFTServer

import hashlib
import time
import datetime
import traceback

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
        self.config = config.AsyncTransfer

        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)

        self.logger.debug('Configuration loaded')

        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to local couchDB')

        monitoring_server = CouchServer(self.config.couch_user_monitoring_instance)
        self.monitoring_db = monitoring_server.connectDatabase(self.config.user_monitoring_db)
        self.logger.debug('Connected to user_monitoring_db in couchDB')

    def algorithm(self, parameters = None):
        """
        a. create/update summary_per_workflow documents
        b. create summary_per_file documents
        c. clean user_monitoring_db database by removing old docs
        """
        self.updateWorkflowSummaries()
        self.updateJobSummaries()
        self.cleanOldDocs()

    def updateWorkflowSummaries(self):

        """
        Get the list of new states and update documents in user_monitoring_db
        """
        active_jobs = 0
        updated = 0
        states = {}
        now = int(time.time())

        query = {'reduce':True, 'group': True}
        active_jobs = self.db.loadView('AsyncTransfer', 'JobsSatesByWorkflow', query)['rows']

        for job in active_jobs:

            workflow = job['key']
            jobs_states = job['value']
            all_states = {}
            pub_state = {}
            query = {'reduce':True, 'group': True, 'key':workflow}
            publication_state = self.db.loadView('AsyncTransfer', 'PublicationStateByWorkflow', query)['rows']
            if publication_state:
                all_states = publication_state[0]['value'].copy()
            all_states.update(jobs_states)
            current_states = clean_states( all_states )
            query = {'key': workflow}
            try:
                mon_states = self.monitoring_db.loadView('UserMonitoring', 'StatesByWorkflow', query)['rows'][0]['value']
                mon_publication_states = self.monitoring_db.loadView('UserMonitoring', 'PublicationStatesByWorkflow', query)['rows'][0]['value']
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
                    query = {'reduce':True, 'group_level':2, 'startkey': [workflow], 'endkey':[workflow, {}]}
                    failures = self.db.loadView('AsyncTransfer', 'JobsByFailuresReasons', query)['rows']
                    self.logger.error(failures)
                    for failure in failures:
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

            if all_mon_states != current_states:
                try:
                    doc = self.monitoring_db.document( workflow )
                    doc['state'] = clean_states( jobs_states )
                    if publication_state:
   	                doc['publication_state'] = clean_states( publication_state[0]['value'].copy() )
                    if doc['state'].has_key("failed"):
                        failures_reasons = {}
                        query = {'reduce':True, 'group_level':2, 'startkey': [workflow], 'endkey':[workflow, {}]}
                        failures = self.db.loadView('AsyncTransfer', 'JobsByFailuresReasons', query)['rows']
                        for failure in failures:
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

    def updateJobSummaries(self):
        """
        Create summaries docs from new files done/failed
        """
        end_time = 0
        all_files = 0

        try:
            query = {}
            since = self.monitoring_db.loadView('UserMonitoring', 'LastSummariesUpdate', query)['rows'][0]['key']
            query = {'limit' : 1, 'descending': True}
            end_time = self.db.loadView('AsyncTransfer', 'LastUpdatePerFile', query)['rows'][0]['key']
        except IndexError:
            self.logger.debug('No records to determine end time, waiting for next iteration')
            return
        except KeyError:
            self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
            return
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)
            return

        updateUri = "/" + self.monitoring_db.name + "/_design/UserMonitoring/_update/lastDBUpdate/MONITORING_DB_UPDATE"
        updateUri += "?db_update=%d" % ( end_time + 1)
        self.monitoring_db.makeRequest(uri = updateUri, type = "PUT", decode = False)

        query = { 'startkey': since, 'endkey': end_time + 1 }
        all_files = self.db.loadView('AsyncTransfer', 'LastUpdatePerFile', query)['rows']

        for file in all_files:
            doc = {}
            doc['type'] = 'aso_file'
            doc['workflow'] = file['value']['workflow']
            doc['lfn'] = file['value']['lfn']
            doc['location'] = file['value']['location']
            doc['checksum'] = file['value']['checksum']
            doc['jobid'] = file['value']['jobid']
            doc['retry_count'] = file['value']['retry_count']
            doc['size'] = file['value']['size']
            try:
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

    def cleanOldDocs(self):
        """
        Clean summaries created before self.config.summaries_expiration_days
        """
        files_to_remove = []
        expiration_time = int(time.time()) - (86400 * self.config.summaries_expiration_days)
        query = {'startkey': 1, 'endkey': expiration_time}
        files_to_remove = self.monitoring_db.loadView('UserMonitoring', 'DocsByTimestamp', query)['rows']

        for old_file in files_to_remove:
            document = self.monitoring_db.document( old_file['value'] )
            self.monitoring_db.queueDelete(document)
        try:
            self.monitoring_db.commit()
            self.logger.debug("%d Old summaries cleaned..." % len(files_to_remove))
        except Exception, ex:
            msg =  "Error commiting documents in monitoring_db"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        return

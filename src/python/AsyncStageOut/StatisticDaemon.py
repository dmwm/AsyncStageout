#!/usr/bin/env
#pylint: disable-msg=W0613,C0103
"""
It populates Async. statistics database
with transfers details from old documents
in runtime database. It creates a document
per fts server per iteration and removes
old documents from runtime database.
"""
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from AsyncStageOut import getFTServer

import datetime
import traceback

class StatisticDaemon(BaseWorkerThread):
    """
    _StatisticDeamon_
    Update Async. statistics database on couch
    Delete older finished job from files_database
    """
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config.Statistics

        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)

        self.logger.debug('Configuration loaded')

        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)

        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.mon_db = server.connectDatabase(self.config.mon_database)
        self.logger.debug('Connected to CouchDB')

        statserver = CouchServer(self.config.couch_statinstance)
        self.statdb = statserver.connectDatabase(self.config.statitics_database)
        self.logger.debug('Connected to Stat CouchDB')

        self.iteration_docs = []
        self.exptime = None

    def algorithm(self, parameters = None):
        """
        1. Get the list of finished jobs older than N days (N is from config)
        2. For each old job:
            a. retrive job document
            b. update the fts server documents for this iteration
            c. delete the document from files_database
            d. update the stat_db by adding the fts servers documents for this iteration
        """
        query = {'stale':'ok'}
        try:
            param = self.config_db.loadView('asynctransfer_config', 'GetStatConfig', query)
            self.config.expiration_days = param['rows'][0]['key']
            self.logger.debug('Got expiration_days %s from Couch' % self.config.expiration_days)
        except IndexError:
            self.logger.exception('Config data could not be retrieved from the config database. Fallback to the config file')
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)

        expdays = datetime.timedelta(days = self.config.expiration_days)
        self.exptime = datetime.datetime.now() - expdays
        self.logger.debug('Deleting Jobs ended before %s' %str(self.exptime) )

        oldJobs = self.getOldJobs()
        self.logger.debug('%d jobs to delete' % len(oldJobs) )

        jobDoc = {}
        for doc in oldJobs:

            try:
                self.logger.debug('deleting %s' %doc)
                jobDoc = self.db.document(doc)
                self.updateStat(jobDoc)
            except Exception, ex:
                msg =  "Error retriving document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            if jobDoc:
                try:
                    self.db.queueDelete(jobDoc)

                except Exception, ex:

                    msg =  "Error queuing document for delete in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)

        for newDoc in self.iteration_docs:
            try:

                self.statdb.queue(newDoc)

            except Exception, ex:

                msg =  "Error queuing document in statDB"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

        try:
            self.statdb.commit()
        except Exception, ex:
            msg =  "Error commiting documents in statdb"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        try:
            self.db.commit()
        except Exception, ex:
            msg =  "Error commiting documents in files_db"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def getOldJobs(self):
        """
        Get the list of finished jobs older than the exptime.
        """
        query = {'reduce': False,
                 'endkey':[self.exptime.year, self.exptime.month, self.exptime.day, self.exptime.hour, self.exptime.minute],
                 'stale': 'ok'}
        try:
            oldJobs = self.mon_db.loadView('monitor', 'endedSizeByTime', query)['rows']
        except Exception, e:
            self.logger.exception('A problem occured when contacting couchDB: %s' % e)
            return []

        def docIdMap(row):
            """
            Map old jobs.
            """
            return row['id']

        oldJobs = map(docIdMap, oldJobs)

        return oldJobs

    def updateStat(self, document):

        """
        Update the stat documents list.
        """
        ftserver = getFTServer(document['destination'], 'getAllFTSserver', self.config_db, self.logger)

        for oldDoc in self.iteration_docs:
            if oldDoc['fts'] == ftserver:
                self.updateServerDocument(oldDoc, document)
                return
        self.createServerDocument(ftserver, document)
        return

    def createServerDocument(self, fts, document):
        """
        Create a new document for the FTServer statistics according to the job document data
        """
        startTime = datetime.datetime.strptime(document['start_time'], "%Y-%m-%d %H:%M:%S.%f")
        endTime = datetime.datetime.strptime(document['end_time'], "%Y-%m-%d %H:%M:%S.%f")
        jobDuration = (endTime - startTime).seconds / 60

        nretry = "%s_retry" % len(document['retry_count'])

        # Init the document
        serverDocument = {'fts': fts,
                          'day': startTime.date().isoformat(),
                          'sites_served': {document['destination']: {'done': 0,
                                                                     'failed': 0,
                                                                     'killed': 0}
                                            },
                          'users': { document['user']: [document['workflow']]},
                          'timing': {'min_transfer_duration': jobDuration,
                                     'max_transfer_duration': jobDuration,
                                     'avg_transfer_duration': jobDuration},
                          'done': {},
                          'failed' : {},
                          'killed': {},
                          'avg_size' : document['size']
                           }

        serverDocument['sites_served'][document['destination']][document['state']] = 1
        serverDocument[document['state']][nretry] = 1

        self.iteration_docs.append(serverDocument)

    def updateServerDocument(self, oldDoc, document):
        """
        Update the FTServer statistics document according to the job document data
        """
        startTime = datetime.datetime.strptime(document['start_time'], "%Y-%m-%d %H:%M:%S.%f")
        endTime = datetime.datetime.strptime(document['end_time'], "%Y-%m-%d %H:%M:%S.%f")
        jobDuration = (endTime - startTime).seconds / 60

        try:
            self.logger.debug('FTS = %s' % oldDoc['fts'])
            server_users = oldDoc['users']

            if server_users.has_key(document['user']):
                #user already in users list
                user_entry = server_users[document['user']]
                if not document['workflow'] in user_entry:
                #add workflow to user
                    user_entry.append(document['workflow'])
                    server_users[document['user']] = user_entry
                    oldDoc['users'] = server_users
            else:
                #add user to users list with workflow
                user_entry = [document['workflow']]
                server_users[document['user']] = user_entry
                oldDoc['users'] = server_users

            #add destination site to sites_served dict if not already there
            if document['destination'] in oldDoc['sites_served']:
                if (document['state'] == 'done'):
                    oldDoc['sites_served'][document['destination']]['done'] += 1
                if (document['state'] == 'killed'):
                    oldDoc['sites_served'][document['destination']]['killed'] += 1
                if (document['state'] == 'failed'):
                    oldDoc['sites_served'][document['destination']]['failed'] += 1
            else:
                if(document['state'] == 'done'):
                    oldDoc['sites_served'][document['destination']] = {'done': 1,
                                                                       'failed': 0,
                                                                       'killed': 0}
                if(document['state'] == 'failed'):
                    oldDoc['sites_served'][document['destination']] = {'done': 0,
                                                                       'failed': 1,
								       'killed': 0}
                if(document['state'] == 'killed'):
                    oldDoc['sites_served'][document['destination']] = {'done': 0,
                                                                       'failed': 0,
                                                                       'killed': 1}
            ntransfer = sum(oldDoc['done'].values()) + sum(oldDoc['failed'].values()) + sum(oldDoc['killed'].values())

            #update max, min duration time
            if(jobDuration > int(oldDoc['timing']['max_transfer_duration']) ):
                oldDoc['timing']['max_transfer_duration'] = jobDuration
            if(jobDuration < int(oldDoc['timing']['min_transfer_duration']) ):
                oldDoc['timing']['min_transfer_duration'] = jobDuration

            avgTime = oldDoc['timing']['avg_transfer_duration']
            avgTime = avgTime * (ntransfer / float(ntransfer+1)) + jobDuration / float(ntransfer+1)
            oldDoc['timing']['avg_transfer_duration'] = int(avgTime)

            nretry = "%s_retry" % len(document['retry_count'])

            if(oldDoc[document['state']].has_key(nretry)):
                oldDoc[document['state']][nretry] += 1
            else:
                oldDoc[document['state']][nretry] = 1

            oldDoc['avg_size'] = int(oldDoc['avg_size']*(ntransfer / float(ntransfer+1)) + document['size'] / float(ntransfer+1))

        except Exception, ex:

            msg =  "Error when retriving document from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)


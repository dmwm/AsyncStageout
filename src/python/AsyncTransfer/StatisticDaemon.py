#!/usr/bin/env
#pylint: disable-msg=W0613,C0103
"""
StatisticDeamon
Update Async. statistics database on couch
Delete older finished job from Async. database
"""
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

import datetime
import traceback

class StatisticDaemon(BaseWorkerThread):

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

        self.map_fts_servers = self.config.map_FTSserver

        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to CouchDB')

        statserver = CouchServer(self.config.couch_statinstance)
        self.statdb = statserver.connectDatabase(self.config.statitics_database)
        self.logger.debug('Connected to Stat CouchDB')

        expdays = datetime.timedelta(days = self.config.expiration_days)
        self.exptime = datetime.datetime.now() - expdays
        self.logger.debug('Deleting Jobs ended before %s' %str(self.exptime) )

    def algorithm(self, parameters = None):
        """
        1. Get the list of finished jobs older than N days (N is from config)
        2. For each old job:
            a. retrives job document
            b. update the document 
            c. delete documents
            d. update the stat db
        """
        self.iteration_docs = []
        oldJob = self.getOldJob()
        self.logger.debug('%d jobs to delete' % len(oldJob) )

        for doc in oldJob:

            try:
                jobDoc = self.db.document(doc)
                self.updateStat(jobDoc)

            except Exception, ex:

                msg =  "Error retriving document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)


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

        self.statdb.commit()
        self.db.commit()


    def getFTServer(self, site):
        """
        Parse site string to know the fts server to use
        """
        country = site.split('_')[1]
        if country in self.map_fts_servers :
            return self.map_fts_servers[country]
        else:
            return self.map_fts_servers['defaultServer']

    def getOldJob(self):

        """
        Get the list of finished jobs older than the exptime.
        """

        query = {'reduce': False,
                 'endkey':[self.exptime.year, self.exptime.month, self.exptime.day, self.exptime.hour, self.exptime.minute]}
        oldJob = self.db.loadView('monitor', 'endedByTime', query)['rows']

        def docIdMap(row):
            return row['id']

        oldJob = map(docIdMap, oldJob)

        return oldJob

    def updateStat(self, document):

        """
        Update the stat documents list.
        """
        ftserver = self.getFTServer(document['destination'])

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

        if document['state'] == 'done':
            nretry = "%s_retry" % len(document['retry_count'])
            self.iteration_docs.append({'fts': fts,
                          'day': startTime.date().isoformat(),
                          'sites_served': {document['destination']: {'success': 1,
                                                                     'failed': 0}
                                            },
                          'users': { document['user']: [document['task']]},
                          'timing': {'min_transfer_duration': jobDuration,
                                     'max_transfer_duration': jobDuration,
                                     'avg_transfer_duration': jobDuration},
                          'success': {nretry : 1},
                          'failed' : 0,
                          'avg_size' : document['size']
                          })
        elif document['state'] == 'failed':
            self.iteration_docs.append({'fts': fts,
                          'day': startTime.date().isoformat(),
                          'sites_served': {document['destination']: {'success': 0,
                                                                     'failed': 1}
                                            },
                          'users': { document['user']: [document['task']]},
                          'timing': {'min_transfer_duration': jobDuration,
                                     'max_transfer_duration': jobDuration,
                                     'avg_transfer_duration': jobDuration},
                          'success': {},
                          'failed' : 1,
                          'avg_size' : document['size']
                          })


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
                if not document['task'] in user_entry:
                #add workflow to user
                    user_entry.append(document['task'])
                    server_users[document['user']] = user_entry
                    oldDoc['users'] = server_users
            else:
                #add user to users list with workflow
                user_entry = [document['task']]
                server_users[document['user']] = user_entry
                oldDoc['users'] = server_users

            #add destination site to sites_served dict if not already there
            if document['destination'] in oldDoc['sites_served']:
                if (document['state'] == 'done'):
                    oldDoc['sites_served'][document['destination']]['success'] += 1
                else:
                    oldDoc['sites_served'][document['destination']]['failed'] += 1
            else:
                if(document['state'] == 'done'):
                    oldDoc['sites_served'][document['destination']] = {'success': 1,
                                                                          'failed': 0}
                else:
                    oldDoc['sites_served'][document['destination']] = {'success': 0,
                                                                          'failed': 1}


            ntransfer = sum(oldDoc['success'].values())+oldDoc['failed']

            #update max, min duration time
            if(jobDuration > int(oldDoc['timing']['max_transfer_duration']) ):
                oldDoc['timing']['max_transfer_duration'] = jobDuration
            if(jobDuration < int(oldDoc['timing']['min_transfer_duration']) ):
                oldDoc['timing']['min_transfer_duration'] = jobDuration

            avgTime = oldDoc['timing']['avg_transfer_duration']
            avgTime = avgTime * (ntransfer / float(ntransfer+1)) + jobDuration / float(ntransfer+1)
            oldDoc['timing']['avg_transfer_duration'] = int(avgTime)

            if(document['state'] == 'done'):
                nretry = "%s_retry" % len(document['retry_count'])
                if(oldDoc['success'].has_key(nretry)):
                    oldDoc['success'][nretry] += 1
                else:
                    oldDoc['success'][nretry] = 1
            else:
                oldDoc['failed'] += 1

            oldDoc['avg_size'] = int(oldDoc['avg_size']*(ntransfer / float(ntransfer+1)) + document['size'] / float(ntransfer+1))

        except Exception, ex:

            msg =  "Error when retriving document from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)


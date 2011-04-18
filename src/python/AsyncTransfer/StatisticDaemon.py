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
            b. update the stat db
            c. delete documente
        """
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
        Update the statistics db on couch according to the job document
        1. Look for an existing stat document for the destination FTServer
        2. Create a new stat document if it doesn't allready exists
        3. Retrive the stat document and update it if exists
        """

        ftserver = self.getFTServer(document['destination'])
        startTime = datetime.datetime.strptime(document['start_time'], "%Y-%m-%d %H:%M:%S.%f")
        query = {'key': [ftserver, startTime.date().isoformat()]}
        serverRows = self.statdb.loadView('stat', 'ftservers', query)['rows']

        if(len(serverRows) == 0):
            self.createServerDocument(ftserver, document)
        else:
            self.updateServerDocument(ftserver, document)

        self.statdb.commit()

    def createServerDocument(self, fts, document):
        """
        Create a new document for te FTServer statistics according to the job document data
        """
        startTime = datetime.datetime.strptime(document['start_time'], "%Y-%m-%d %H:%M:%S.%f")
        endTime = datetime.datetime.strptime(document['end_time'], "%Y-%m-%d %H:%M:%S.%f")
        jobDuration = (endTime - startTime).seconds / 60

        if document['state'] == 'done':
            nretry = "%s_retry" % len(document['retry_count'])
            serverDoc = {'_id': "%s_%s" % (fts, startTime.date().isoformat()),
                         'fts': fts,
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
                          }
        elif document['state'] == 'false':
            serverDoc = {'_id': "%s_%s" % (fts, startTime.date().isoformat()),
                         'fts': fts,
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
                          }

        try:

            self.statdb.queue(serverDoc)

        except Exception, ex:

            msg =  "Error when queuing document in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def updateServerDocument(self, fts, document):
        """
        Update the FTServer statistics document according to the job document data
        """
        startTime = datetime.datetime.strptime(document['start_time'], "%Y-%m-%d %H:%M:%S.%f")
        endTime = datetime.datetime.strptime(document['end_time'], "%Y-%m-%d %H:%M:%S.%f")
        jobDuration = (endTime - startTime).seconds / 60

        try:
            serverDoc = self.statdb.document("%s_%s" % (fts, startTime.date().isoformat()))
            self.logger.debug('FTS = %s' % serverDoc['fts'])
            server_users = serverDoc['users']

            if server_users.has_key(document['user']):
                #user already in users list
                user_entry = server_users[document['user']]
                if not document['task'] in user_entry:
                #add workflow to user
                    user_entry.append(document['task'])
                    server_users[document['user']] = user_entry
                    serverDoc['users'] = server_users
            else:
                #add user to users list with workflow
                user_entry = [document['task']]
                server_users[document['user']] = user_entry
                serverDoc['users'] = server_users

            #add destination site to sites_served dict if not already there
            if document['destination'] in serverDoc['sites_served']:
                if (document['state'] == 'done'):
                    serverDoc['sites_served'][document['destination']]['success'] += 1
                else:
                    serverDoc['sites_served'][document['destination']]['failed'] += 1
            else:
                if(document['state'] == 'done'):
                    serverDoc['sites_served'][document['destination']] = {'success': 1,
                                                                          'failed': 0}
                else:
                    serverDoc['sites_served'][document['destination']] = {'success': 0,
                                                                          'failed': 1}


            ntransfer = sum(serverDoc['success'].values())+serverDoc['failed']

            #update max, min duration time
            if(jobDuration > int(serverDoc['timing']['max_transfer_duration']) ):
                serverDoc['timing']['max_transfer_duration'] = jobDuration
            if(jobDuration < int(serverDoc['timing']['min_transfer_duration']) ):
                serverDoc['timing']['min_transfer_duration'] = jobDuration

            avgTime = serverDoc['timing']['avg_transfer_duration']
            avgTime = avgTime * (ntransfer / float(ntransfer+1)) + jobDuration / float(ntransfer+1)
            serverDoc['timing']['avg_transfer_duration'] = int(avgTime)

            if(document['state'] == 'done'):
                nretry = "%s_retry" % len(document['retry_count'])
                if(serverDoc['success'].has_key(nretry)):
                    serverDoc['success'][nretry] += 1
                else:
                    serverDoc['success'][nretry] = 1
            else:
                serverDoc['failed'] += 1

            serverDoc['avg_size'] = int(serverDoc['avg_size']*(ntransfer / float(ntransfer+1)) + document['size'] / float(ntransfer+1))

        except Exception, ex:

            msg =  "Error when retriving document from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

        try:

            self.statdb.queue(serverDoc)

        except Exception, ex:

            msg =  "Error when queuing document in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)


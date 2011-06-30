#!/usr/bin/env python
#pylint: disable=C0103
"""
_JSM_t_
Duplicate view from JSM database
"""
from WMCore.Database.CMSCouch import CouchServer
from Source import Source
import datetime

class JSM(Source):
    """
    _JSM_
    JSM plugins to query JSM DBs.
    """
    def __init__(self, config, logger):
        """
        Initialise class members
        """
        Source.__init__(self, config, logger)

        # input db

        self.sourceServer = CouchServer(self.config.data_source)
        self.dbSource = self.sourceServer.connectDatabase(self.config.jsm_db)
        self.viewSource = 'inputAsycStageOut'
        self.fwjrsID = 'fwjrByJobIDTimestamp'
        self.designSource = 'FWJRDump' 

        self.logger.debug('Connected to CouchDB source')

    def __call__(self):
        """
        _call_
        Get the result of viewSource from JSM db.
        """

        result = []

        try:
            # Get the time of the last record we're going to pull in
            query = {'limit' : 1, 'descending': True}
            endtime = dbSource.loadView(designSource, viewSource, query)['rows'][0]['key']

            # If the above throws an exception there's no files to process, so just move on

            # Get the files we want to process
            self.logger.debug('Querying JSM for files added between %s and %s' % (self.since, endtime))

            query = { 'startkey': self.since, 'endkey': endtime }
            result = dbSource.loadView(designSource, viewSource, query)['rows']

            # Now record where we got up to so next iteration we'll continue from there
            if result:
                # TODO: persist the value of self.since somewhere, so that the agent will work over restarts
                self.since = endtime + 1
        except IndexError:
            self.logger.debug('No records to determine end time, waiting for next iteration')
        except KeyError:
            self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
        except Exception, e:
            self.logger.exception('A problem occured in the JSM Source __call__: %s' % e)

        # Little map function to pull out the data we need
        def pull_value(row):
            now = str(datetime.datetime.now())

            value = row['value']
            value['source'] = self.phedexApi.getNodeNames( value['source'] )[0]
            value['user'] = value["_id"].split('/')[3]
            value['size'] = value['size']
            value['retry_count'] = []
            value['state'] = 'new'
            value['start_time'] = now
            value['dbSource_update'] = row['key']
            return value

        return map(pull_value, result)

    def updateSource(self, inputDict):
        """
        Update FWJR DB by adding an AsyncStageOut step. 
        """
        query = { 'reduce':False, 'key':[ inputDict['jobid'] , inputDict['timestamp'] ] }

        couchDocID = self.dbSource.loadView(self.designSource, self.fwjrsID, query)['rows'][0]['id']

        updateUri = "/" + self.dbSource.name + "/_design/" + self.designSource + "/_update/addAsyncStageOutStep/" + couchDocID
        updateUri += "?location=%s&lfn=%s" % ( inputDict['location'], inputDict['lfn'] )

        self.dbSource.makeRequest(uri = updateUri, type = "PUT", decode = False)

        self.dbSource.commit()
        return []


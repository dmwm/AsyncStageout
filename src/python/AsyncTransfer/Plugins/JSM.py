#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
_JSM_t_
Duplicate view from JSM database
"""
from WMCore.Database.CMSCouch import CouchServer
from Source import Source
import logging

class JSM(Source):
    """
    _JSM_
    JSM plugins to query JSM DBs.     
    """
    def __call__(self):
        """
        _call_
        Get the result of viewSource from JSM db. 
        """
        # input db
        sourceServer = CouchServer(self.config.data_source)
        dbSource = sourceServer.connectDatabase(self.config.jsm_db)
        viewSource = 'inputAsycStageOut' 
        designSource = 'JobDump' 

        logging.debug('Connected to CouchDB source ')

        # Get the time of the last record we're going to pull in
        query = {'limit' : 1, 'descending': True}
        endtime = dbSource.loadView(designSource, viewSource, query)['rows']['key']
        
        # Get the files we want to process
        self.logger.debug('Querying JSM for files added between %s and %s' % (self.since, endtime))
        
        query = { 'startkey': self.since 'endkey': endtime}
        result = dbSource.loadView(designSource, viewSource, query)['rows']

        # Now record where we got up to so next iteration we'll continue from there
        self.since = end_key
        # TODO: persist the value of self.since somewhere, so that the agent will work over restarts
        
        def pull_value(row):
            value = row['value']
            value['source'] = self.phedexApi.getNodeNames( res["value"]['source'] )[0]
            value['user'] = res["value"]["_id"].split('/')[3]
            return value
            
        return map(pull_value, result) 

#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
_JSM_t_
Duplicate view from JSM database
"""
from WMCore.Database.CMSCouch import CouchServer
from Source import Source

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

        self.logger.debug('Connected to CouchDB source ')
        
        result = []
        
        try:
            # Get the time of the last record we're going to pull in
            query = {'limit' : 1, 'descending': True}
            endtime = dbSource.loadView(designSource, viewSource, query)['rows'][0]['key']
            
            # If the above throws an exception there's no files to process, so just move on
            
            # Get the files we want to process
            self.logger.debug('Querying JSM for files added between %s and %s' % (self.since, endtime))

            query = { 'startkey': self.since, 'endkey': endtime}
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
            value = row['value']
            value['source'] = self.phedexApi.getNodeNames( value['source'] )[0]
            value['user'] = value["_id"].split('/')[3]
            value['retry_count'] = []
            value['state'] = 'new'
            return value
            
        return map(pull_value, result)

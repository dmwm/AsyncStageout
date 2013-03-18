#!/usr/bin/env python
#pylint: disable=C0103
"""
_CentralMonitoring_
Duplicate docs from the centralMonitoring database
"""
from WMCore.Database.CMSCouch import CouchServer
from AsyncStageOut.Plugins.Source import Source
import datetime
import time
from AsyncStageOut import getHashLfn

class CentralMonitoring(Source):
    """
    _CentralMonitoring_
    CentralMonitoring plugins to query central_monitoring DB.
    """
    def __init__(self, config, logger):
        """
        Initialise class members
        """
        Source.__init__(self, config, logger)

        # input db
        self.sourceServer = CouchServer(self.config.data_source)
        self.dbSource = self.sourceServer.connectDatabase(self.config.db_source)
        self.viewSource = 'inputAsyncStageOut'
        self.designSource = 'WMStats'

        self.logger.debug('Connected to CouchDB source')

    def __call__(self):
        """
        _call_
        Get the result of viewSource from central_monitoring db.
        """
        result = []
        jobs = []

        try:
            # Get the time of the last record we're going to pull in
            query = {'limit' : 1, 'descending': True}
            endtime = self.dbSource.loadView(self.designSource, self.viewSource, query)['rows'][0]['key']

            # If the above throws an exception there's no files to process, so just move on

            # Get the files we want to process
            self.logger.debug('Querying the central_monitoring for files added between %s and %s' % (self.since, endtime + 1))

            query = { 'startkey': self.since, 'endkey': endtime + 1 }
            jobs = self.dbSource.loadView(self.designSource, self.viewSource, query)['rows']

            # Now record where we got up to so next iteration we'll continue from there
            if jobs:
                # TODO: persist the value of self.since somewhere, so that the agent will work over restarts
                self.since = endtime + 1
        except IndexError:
            self.logger.debug('No records to determine end time, waiting for next iteration')
        except KeyError:
            self.logger.debug('Could not get results from CouchDB, waiting for next iteration')
        except Exception, e:
            self.logger.exception('A problem occured in the central_monitoring Source __call__: %s' % e)

        # Prepare the input to ASO
        if jobs:
            cache = {}
            for job in jobs:
                temp = {}
                temp = job
                workflow = job['value']['workflow']
                if not cache.has_key(workflow):
                    user_details = self.dbSource.document( workflow )
                    cache[workflow] = {'user_dn': user_details['user_dn'], 'vo_role': user_details['vo_role'], 'vo_group': user_details['vo_group'], 'async_dest': user_details['async_dest'], 'inputdataset': user_details['inputdataset'], 'dbs_url': user_details['dbs_url'], 'publish_dbs_url': user_details['publish_dbs_url']}
                temp['value']['dn'] = cache[workflow]['user_dn']
                temp['value']['role'] = cache[workflow]['vo_role']
                temp['value']['group'] = cache[workflow]['vo_group']
                temp['value']['destination'] = cache[workflow]['async_dest']
                temp['value']['inputdataset'] = cache[workflow]['inputdataset']
                temp['value']['dbs_url'] = cache[workflow]['dbs_url']
                temp['value']['publish_dbs_url'] = cache[workflow]['publish_dbs_url']
                result.append(temp)

        # Little map function to pull out the data we need
        def pull_value(row):
            now = str(datetime.datetime.now())
            last_update = int(time.time())

            # Prepare file documents
            value = row['value']
            value['lfn'] = value["_id"]
            value['user'] = value["_id"].split('/')[4]
            value['_id'] = getHashLfn( value["_id"] )
            value['size'] = value['size']
            value['retry_count'] = []
            value['state'] = 'new'
            value['start_time'] = now
            value['last_update'] = last_update

            # Attributes required for publication
            value['job_end_time'] = row['key']
	    value['publication_state'] = 'not_published'
            value['publication_retry_count'] = []
            try:
                value['dbSource_url'] = self.config.data_source.replace(((self.config.data_source).split("@")[0]).split("//")[1]+"@", "")
            except:
                value['dbSource_url'] = self.config.data_source

            return value

        return map(pull_value, result)

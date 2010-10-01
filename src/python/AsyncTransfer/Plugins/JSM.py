#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
_JSM_t_
Duplicate view from JSM database
"""
from WMCore.Database.CMSCouch                 import CouchServer
import logging
class JSM:
    """
    _JSM_
    JSM plugins to query JSM DBs.     
    """
    def __init__( self ):

        # input db
        sourceServer = CouchServer('http://riahi:password@crab.pg.infn.it:5984')
        self.dbSource = sourceServer.connectDatabase(\
              'crabserver/asynctransfer')
        self.viewSource = 'ftscp' 
        self.designSource = 'AsyncTransfer' 
        logging.debug('Connected to CouchDB source')

    def getViewResult(self):
        """
        _getViewResults_
        Get the result of the view. 
        """

        query = {'reduce':False}
        view_results = self.dbSource.loadView(\
self.designSource, self.viewSource, query)['rows']
        results = [] 

        for res in view_results:

            results.append( {'_id': res["value"],
                         'source': res["key"][2],
                         'destination': res["key"][1],
                         'user': res["key"][0]
                        } )

        logging.debug("docs to duplicate %s" %results)
        return results


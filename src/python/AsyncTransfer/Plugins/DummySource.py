#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
_DummySource_t_
Duplicate view from Dummy database
"""
import logging
import random
from WMCore.Database.CMSCouch                 import CouchServer

class DummySource:
    """
    _DummySource_
    Create dummy data to be stored in couch by the LFNDuplicatorPoller.     
    """
    def __init__( self ):

        logging.debug('Dummy connection to CouchDB source')
         
    def getViewResult(self):
        """
        _getViewResults_
        Get the result of the view. 
        """
        sites = ['T2_IT_Rome', 'T2_CH_CAF', 'T2_DE_DESY']
        numberUsers = 5
        j = 1

        users = []
        while j <= numberUsers:

            users.append( 'user'+ str( random.randint(1, 1000) ) )
            j += 1

        size = 3

        i = 1

        lfn_base = '/store/temp/riahi/user/%s/store/temp/file-duplic-%s-%s.root'
        results = []

        while i <= size:

            user = random.choice(users)
            results.append( {'_id': lfn_base % (user,
                                           random.randint(1000, 9999),
                                           i),
                        'source': random.choice(sites),
                        'destination': 'T2_IT_Pisa',
                        'user': user
            } )

            i += 1

        logging.debug("Dummy docs queued %s" %results)
        return results


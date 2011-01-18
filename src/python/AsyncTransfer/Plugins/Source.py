from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Database.CMSCouch import CouchServer

"""
A base class for Source's
"""

class Source:
    def __init__(self, config, logger):

        self.config = config
        self.logger = logger

        self.asyncServer = CouchServer(self.config.couch_instance)
        self.db = self.asyncServer.connectDatabase(self.config.couch_database)

        try:

            query = {'limit' : 1, 'descending': True} 
            last_pollTime = self.db.loadView('AsyncTransfer', 'lastPollTime', query)['rows'][0]['key']
        
            self.since = last_pollTime + 1
 
        except:

            self.since = 0 

        self.phedexApi = PhEDEx( secure = True, dict = {} )

    def __call__(self):
        """
        __call__ should be over written by subclasses such that useful results are returned
        """
        return []

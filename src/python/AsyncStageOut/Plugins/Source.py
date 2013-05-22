#pylint: disable=C0103

"""
A base class for Source's
"""

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Database.CMSCouch import CouchServer

class Source:
    """
    Plugins parent class.
    """
    def __init__(self, config, logger):
        """
        Initialise class members
        """
        self.config = config
        self.logger = logger
        self.asyncServer = CouchServer(self.config.couch_instance)
        self.db = self.asyncServer.connectDatabase(self.config.files_database)
        try:
            query = {'limit' : 1, 'descending': True}
            last_pollTime = self.db.loadView('AsyncTransfer', 'lastPollTime', query)['rows'][0]['key']
            self.since = last_pollTime + 1
        except:
            self.since = 0
        try:
            self.phedexApi = PhEDEx( secure = True, dict = {} )
        except Exception, e:
            self.logger.exception('PhEDEx object exception: %s' % e)

    def __call__(self):
        """
        __call__ should be over written by subclasses such that useful results are returned
        """
        return []

    def updateSource(self, inputDict):
        """
        UpdateSource should be over written by subclasses to make a specific update in the source
        """
        return []

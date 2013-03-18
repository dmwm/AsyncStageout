#pylint: disable=C0103

"""
A base class for Source's
"""

from WMCore.Database.CMSCouch import CouchServer

class Algo:
    """
    Plugins parent class.
    """
    def __init__(self, config, logger, users, pool_size):
        """
        Initialise class members
        """

        self.config = config
        self.logger = logger

        self.asyncServer = CouchServer(self.config.couch_instance)
        self.db = self.asyncServer.connectDatabase(self.config.files_database)

        self.config_db = self.asyncServer.connectDatabase(self.config.config_database)

        self.users = users
        self.pool_size = pool_size

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

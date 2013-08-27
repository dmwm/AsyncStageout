#!/usr/bin/env python
#pylint: disable-msg=C0103,W0613
"""
_LFNSourceDuplicator_
Duplicate view in Async. database
"""
from WMCore.Database.CMSCouch                 import CouchServer
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.WMFactory import WMFactory

class LFNSourceDuplicator(BaseWorkerThread):
    """
    _LFNSourceDuplicator_
    Load plugin to get the result of the source database query.
    Duplicates the result got into the local database.
    """

    def __init__(self, config):

        BaseWorkerThread.__init__(self)

        self.config = config.AsyncTransfer

        # self.logger is set up by the BaseWorkerThread, we just set it's level
        try:
            self.logger.setLevel(self.config.log_level)
        except:
            import logging
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)

        self.logger.debug('Configuration loaded')

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)

        # Asynch db
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger.debug('Connected to CouchDB')

        return

    def algorithm(self, parameters = None):
        """
        _algorithm_
        Load the plugin of a db source which load its view.
        Duplicates the results got from the plugin in Async database.
        """
        self.logger.debug('Duplication algorithm begins')

        try:
            duplicator = self.factory.loadObject(self.config.pluginName, args = [self.config, self.logger], getFromCache = True, listFlag = True)

        except ImportError,e :
            msg = "plugin \'%s\' unknown" % self.config.pluginName
            self.logger.info(msg)
            self.logger.info(e)

        for doc in duplicator():
            self.db.queue(doc, True, ['AsyncTransfer/ftscp'] )
            self.logger.debug("doc queued %s" %doc)

        self.db.commit(viewlist=['AsyncTransfer/ftscp'])
        self.logger.debug('Duplication done')

        return


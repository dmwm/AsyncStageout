#!/usr/bin/env python
#pylint: disable-msg=C0103,W0613
"""
_LFNSourceDuplicator_
Duplicate view in Async. database
"""
from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer

from AsyncStageOut.BaseDaemon import BaseDaemon

class LFNSourceDuplicator(BaseDaemon):
    """
    _LFNSourceDuplicator_
    Load plugin to get the result of the source database query.
    Duplicates the result got into the local database.
    """

    def __init__(self, config):

        BaseDaemon.__init__(self, config, 'AsyncTransfer')

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


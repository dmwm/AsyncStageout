import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from AsyncStageOut import getCommonLogFormatter

class BaseDaemon(BaseWorkerThread):
    """
    _DaemonBase_
    Base class for AsyncStageOut daemons.
    """
    def __init__(self, config, componentName):
        BaseWorkerThread.__init__(self)
        self.config = getattr(config, componentName)
        self.editLogger()
        self.logger.debug("Configuration loaded")

    def editLogger(self):
        #logging.basicConfig(format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt = '%m-%d %H:%M')
        #self.logger = logging.getLogger()
        # self.logger is set up by the BaseWorkerThread, we just set it's level
        try:
            self.logger.setLevel(self.config.log_level)
        except:
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)
        formatter = getCommonLogFormatter(self.config)
        for handler in self.logger.handlers:
            handler.setFormatter(formatter)

import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from AsyncStageOut import getCommonLogFormatter
from logging.handlers import TimedRotatingFileHandler

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
  
     
        try:
            self.logger.setLevel(self.config.log_level)
        except:
            self.logger = logging.getLogger()
            self.logger.setLevel(self.config.log_level)
        formatter = getCommonLogFormatter(self.config)
    
        LOG_PATH = self.config.componentDir + "/CompLog"
        hndlr = TimedRotatingFileHandler(LOG_PATH, when = "d", interval= 4,  backupCount=7) 
        self.logger.addHandler(hndlr)
        
        for handler in self.logger.handlers:
            handler.setFormatter(formatter)

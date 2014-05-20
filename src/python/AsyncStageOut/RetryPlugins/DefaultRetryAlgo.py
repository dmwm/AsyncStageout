#!/bin/env python
"""
_DefaultRetryAlgo_
This is the default.  It's alarmingly simple.
Stolen from WMCore. Align this code within the existing one in WMCore.
"""
import logging
from AsyncStageOut.RetryPlugins.RetryAlgoBase import RetryAlgoBase

class DefaultRetryAlgo(RetryAlgoBase):
    """
    _DefaultRetryAlgo_
    This is the simple 'wait a bit' cooloff algo
    """
    def isReady(self, file, cooloffTime):
        """
        Actual function that does the work

        """
        if not cooloffTime:
            logging.error('Unknown cooloffTime for %s: passing' %(file))
            return False

        currentTime = self.timestamp()
        if currentTime - file['state_time'] > cooloffTime:
            return True
        else:
            return False

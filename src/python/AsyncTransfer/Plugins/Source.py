from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

"""
A base class for Source's
"""

class Source:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.since = 0
        self.phedexApi = PhEDEx( secure = True, dict = {} )

    def __call__(self):
        """
        __call__ should be over written by subclasses such that useful results are returned
        """
        return []

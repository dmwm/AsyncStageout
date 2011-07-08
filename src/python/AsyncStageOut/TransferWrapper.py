import datetime

class TransferWrapper:

    def __init__(self, logger, db):
        """
        Super-class constructor
        """
        self.logger = logger
        self.db = db

    def __call__(self, files=[], userProxy = None, destSite = []):
        """
        This is where the work is done. A list of files are passed into the
        __call__ method and code is executed here to process each one. To transfer
        data userProxy is needed. for some protocole destSite is needed.
        """
        start_time = str(datetime.datetime.now())
        transferred, failed, allFiles = self.command(files, userProxy, destSite)
        msg = "from %s files, %s files are transferred, %s files failed to transfer"
        self.logger.info(msg % (len(allFiles), len(transferred), len(failed)))
        end_time = str(datetime.datetime.now())
        self.mark_good(transferred)
        self.mark_failed(failed)
        return

    def command(self, files, userProxy, destSite):
        """
        A null TransferWrapper - This should be
        overritten by subclasses. Return allFiles, transferred and failed transfers.
          transferred: files has been transferred
          failed: the transfer has failed
        """
        return files, []

    def mark_good(self, transferred):
        """
        Mark the list as transferred in database.
        """
        pass 

    def mark_failed(self, failed, force_fail):
        """
        mark the list as failed transfers in database.
        """
        pass

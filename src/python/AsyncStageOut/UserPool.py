'''
Track active users and sort on number of active files.

'''
from WMCore.Database.CMSCouch import CouchServer
import random

class UserPool(Pool):
    def __init__(self, config, logger):
        self.config = config.CRABAsyncTransfer
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        self.logger = logger
        self.size = 0
        self.result = []

    def get_users(self):
        """
        returm users up to pool size
        """
        users = active_users(self.db)
        self.logger.info('%s active users' % len(users))
        self.logger.debug('Active users are: %s' % users)

        if len(users) <= self.size:
            # If the number of active users is less than the pool return them all
            self.result = users
        else:
            # Do some sorting
            self.result = self.algorithm(users)
        return self.result

    def algorithm(self, users):
        """
        Do a no-op, take the first N users from the list
        """
        return users[:self.size]

class RandomUserPool(UserPool):
    def algorithm(self, users):
        """
        Choose pool size users at random
        """
        return random.sample(users, self.size)



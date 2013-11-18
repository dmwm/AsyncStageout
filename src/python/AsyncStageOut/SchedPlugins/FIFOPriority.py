#!/usr/bin/env python
#pylint: disable=C0103
"""
_FIFOPriority_
Scheduling algo.
"""
from AsyncStageOut.SchedPlugins.Algo import Algo
import time
import datetime
import logging

def fifo_algo(user_by_start):
    """
    FIFO algo.
    """
    sorted_users = []
    for i in range(1, len(user_by_start)+1):
        min = user_by_start.keys()[0]
        for u in user_by_start:
            if u < min:
                min = u
        sorted_users.append(user_by_start[min])
        del user_by_start[min]
    return sorted_users

def priority_algo(fifo_sort_users, priority_users):
    """
    Priority applied to the FIFO list.
    """
    sorted_list = []
    for u in fifo_sort_users:
        if u in priority_users:
            sorted_list.append(u)
    priority_sort_list = sorted_list
    for user in fifo_sort_users:
        if user not in sorted_list:
            priority_sort_list.append(user)
    return priority_sort_list

class FIFOPriority(Algo):
    """
    _FIFOPriority_
    FIFOPriority plugins to schedule transfers.
    """
    def __init__(self, config, logger, users, pool_size):
        """
        Initialise class members
        """
        Algo.__init__(self, config, logger, users, pool_size)
        self.logger.debug('Connected to files database')

    def __call__(self):
        """
        _call_
        Get the result of viewSource from central_monitoring db.
        """
        start_by_user = {}
        user_time = 0
        for u in self.users:
            user = u['key']
            end = user.append({})
            query = {'limit' : 1, 'descending': True, 'startkey':u['key'], 'endkey':end}
            try:
                UserByStartTime = self.db.loadView('AsyncTransfer', 'UserByStartTime', query)['rows'][0]['key']
            except:
                return []
            self.logger.debug( 'User %s and start time %s' % (u, UserByStartTime[3:]) )
            user_time = int(time.mktime(time.strptime(\
                                        str(UserByStartTime[3]), '%Y-%m-%d %H:%M:%S'))) \
                                        - time.timezone
            start_by_user[user_time] = u['key'][:3]
        self.logger.debug('Start by user %s' %start_by_user)
        fifo_sort_users = fifo_algo(start_by_user)
        query = {}
        try:
            users = self.config_db.loadView('asynctransfer_config', 'GetHighPriorityUsers', query)
        except:
            return []
        def keys_map(inputDict):
            """
            Map function.
            """
            return inputDict['key']
        priority_users = map(keys_map, users['rows'])
        priority_sort_users = priority_algo(fifo_sort_users, priority_users)
        self.logger.debug('Final list %s' %priority_sort_users)
        return priority_sort_users

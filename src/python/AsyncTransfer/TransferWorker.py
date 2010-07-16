'''
The TransferWorker does the following:

    a. make the ftscp copyjob
    b. submit ftscp and watch
    c. delete successfully transferred files from the database
    
There should be one worker per user transfer.


'''
from WMCore.Database.CMSCouch import CouchServer

import time
import logging

class TransferWorker:
    def __init__(self, user, tfc_map, config):
        """
        store the user and tfc the worker 
        """
        self.user = user
        self.tfc_map = tfc_map 
        self.config = config.AsyncTransfer
        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.couch_database)
        logging.basicConfig(level=config.AsyncTransfer.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker')
        self.logger.info('Worker loaded for %s' % user)
        
        
    def run(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files
        """
        jobs, pfn_lfn_map = self.files_for_transfer()
        
        for job in jobs:
            for task in job:
                pfn = task.split(' ')[0].strip()
                assert pfn in pfn_lfn_map.keys() 
        
            transferred, failed = self.command(job, self.user)
            self.mark_failed(failed)
            self.mark_good(transferred)
                
        self.logger.info('%s is sleeping for %s seconds' % (self.user, len(jobs))) 
        time.sleep(len(jobs))
        return 'Transfers completed for %s' % self.user
        
    
    def destinations_by_user(self):
        """
        Get all the destinations for a user
        """
        query = {'group': True, 'group_level':2,
                 'startkey':[self.user], 'endkey':[self.user, {}]}
        sites = self.db.loadView('AsyncTransfer', 'ftscp', query)
        
        def keys_map(dict):
            return dict['key'][1]
        
        return map(keys_map, sites['rows'])
    
    
    def files_for_transfer(self):
        """
        Process a queue of work per transfer destination for a user. Return one 
        ftscp copyjob per destination. 
        """         
        dests = self.destinations_by_user()
        jobs = []
        self.logger.debug('%s is transferring to %s' % (self.user, dests))
        for site in dests:
            # We could push applying the TFC into the list function, not sure if
            # this would be faster, but might use up less memory. Probably more
            # complicated, though. 
            query = {'reduce':False,
                 'limit': self.config.max_files_per_transfer,
                 'startkey':[self.user, site], 
                 'endkey':[self.user, site, {}]}
            
            active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']
            
            self.logger.info('%s has %s files' % (self.user, len(active_files)))
            
            # take these active files and make a pfn:id/lfn dictionary
            def tfc_map(item):
                pfn = self.apply_tfc('%s:%s' % (item['key'][2], item['value']))
                return (pfn, item['id'])
            
            pfn_lfn_map = dict(map(tfc_map, active_files))
            
            files = self.db.loadList('AsyncTransfer', 'ftscp', 'ftscp', query)
            # files is a list of 2 lfn's - temp and permanent
            jobs.append(self.create_ftscp_input(files))
            
        self.logger.debug('ftscp input created for %s' % self.user)
        return jobs, pfn_lfn_map
        
        
    def apply_tfc(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn
        """
        site, lfn = tuple(file.split(':'))
        
        return self.tfc_map[site].matchLFN('srmv2', lfn)
    
    
    def create_ftscp_input(self, files):
        """
        Apply the TFC to the ftscp input
        """
        def tfc_map(item):
            #print item, item.split(' ')
            source, dest = tuple(item.split(' '))
            return "%s %s" % (self.apply_tfc(source), self.apply_tfc(dest))
            
        files = files.strip().split('\n')
        return map(tfc_map, files) 
    
    
    def command(self, job, userProxy):
        """
        A null TransferWrapper - This should be
        overritten by subclasses. Return allFiles, transferred and failed transfers.
          transferred: files has been transferred 
          failed: the transfer has failed 
        """
        return job, []
    
    
    def mark_good(self, transferred):
        """
        Mark the list as transferred in database.
        """
        pass
       
       
    def mark_failed(self, failed):
        """
        mark the list as failed transfers in database.  
        """
        pass
    
    
    def record_stats(self, transferred, failed):
        """
        Record some statistics
        """
        pass
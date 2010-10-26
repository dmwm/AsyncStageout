no'''
The TransferWorker does the following:

    a. make the ftscp copyjob
    b. submit ftscp and watch
    c. delete successfully transferred files from the database
    
There should be one worker per user transfer.


'''
from WMCore.Database.CMSCouch import CouchServer

import time
import logging
import subprocess, os, errno
import tempfile
import datetime
import traceback

class TransferWorker:

    def __init__(self, user, tfc_map, config):
        """
        store the user and tfc the worker 
        """
        self.user = user
        self.tfc_map = tfc_map
        self.config = config
        self.log_dir = '%s/logs/%s' % (self.config.componentDir, self.user)
        
        try:
            os.makedirs(self.log_dir)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else: raise
            
        
        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.couch_database)
        self.map_fts_servers = config.map_FTSserver
        # TODO: improve how the worker gets a log
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker-%s' % user)

        # TODO: use proxy management to pick this up - ticket #202 
        self.userProxy = config.serviceCert     
  
    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """
        
        jobs = self.files_for_transfer()   
 
        transferred, failed = self.command()
        self.mark_failed(failed)
        self.mark_good(transferred)
                
        self.logger.info('Transfers completed')
        return

    def cleanSpace(self, copyjob):
        """
        Remove all __destination__ PFNs got in input. The delete can fail because of a file not found
        so issue the command and hope for the best.
        
        TODO: better parsing of output
        """
        for task in copyjob:
            destination_pfn = task.split()[1]
            command = 'export X509_USER_PROXY=%s ; srmrm %s'  %( self.userProxy, destination_pfn )
            self.logger.debug("Running remove command %s" % command)
            proc = subprocess.Popen(
                    ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE,
            )
            proc.stdin.write(command)
            stdout, stderr = proc.communicate()
            rc = proc.returncode


    def getFTServer(self, site):
        """
        Parse site string to know the fts server to use 
        """
        country = site.split('_')[1]
        if country in self.map_fts_servers :
            return self.map_fts_servers[country]
        #TODO: decide what to do if site.split('_')[0] == 'T1'
        else:
            return self.map_fts_servers['defaultServer']

    def source_destinations_by_user(self):
        """
        Get all the destinations for a user
        """
        query = {'group': True, 
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}
        sites = self.db.loadView('AsyncTransfer', 'ftscp', query)
        
        def keys_map(dict):
            return dict['key'][2], dict['key'][1]
        
        return map(keys_map, sites['rows'])
    
    
    def files_for_transfer(self):
        """
        Process a queue of work per transfer source:destination for a user. Return one 
        ftscp copyjob per source:destination. 
        """         
        source_dests = self.source_destinations_by_user()
        jobs = {}
        self.logger.info('%s has %s links to transfer on' % (self.user, len(source_dests)))
        try:        
            for (source, destination) in source_dests:
                # We could push applying the TFC into the list function, not sure if
                # this would be faster, but might use up less memory. Probably more
                # complicated, though. 
                query = {'reduce':False,
                     'limit': self.config.max_files_per_transfer,
                     'key':[self.user, destination, source]}
            
                active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']
                self.logger.debug('%s has %s files to transfer from %s to %s' % (self.user, len(active_files), source, destination))
                new_job = []
                # take these active files and make a copyjob entry
                def tfc_map(item):
                    source_pfn = self.apply_tfc_to_lfn('%s:%s' % (source, item['value']))
                    destination_pfn = self.apply_tfc_to_lfn('%s:%s' % (destination, item['value'].replace('store/temp', 'store', 1)))
                    new_job.append('%s %s' % (source_pfn, destination_pfn))
                    
                map(tfc_map, active_files)

                jobs[(source, destination)] = new_job
            self.logger.debug('ftscp input created for %s (%s jobs)' % (self.user, len(jobs.keys())))
            return jobs
        except:
            self.logger.exception("fail")
        
        
    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn
        """
        site, lfn = tuple(file.split(':'))
        
        return self.tfc_map[site].matchLFN('srmv2', lfn)

    def apply_tfc_to_pfn(self, site, pfn):
        """
        Take a site and pfn and make an lfn
        """
        lfn = self.tfc_map[site].matchPFN('srmv2', pfn)

        #TODO: improve fix for wrong tfc on sites
        try:
            if lfn.split('store')[0] != '/':
                self.logger.error('Broken tfc for file %s at site %s' % (pfn, site))
                return None
        except IndexError:
            pass

        return lfn
    
    def command(self):
        """
        For each job the worker has to complete:
           Delete files that have failed previously 
           Create a temporary copyjob file
           Submit the copyjob to the appropriate FTS server
           Parse the output of the FTS transfer and return complete and failed files for recording
        """
        jobs = self.files_for_transfer()   
        transferred_files = []
        failed_files = []
        
        #Loop through all the jobs for the links we have
        for link, copyjob in jobs.items():
            # Clean cruft files from previous transfer attempts
            # TODO: check that the job has a retry count > 0 and only delete files if that is the case
            self.cleanSpace( copyjob )
            
            tmp_copyjob_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_copyjob_file.write('\n'.join(copyjob))
            tmp_copyjob_file.close()
            
            fts_server_for_transfer = self.getFTServer(link[1])
            
            logfile = open('%s/%s_%s-%s.ftslog' % (self.log_dir, self.user, link[0], link[1]), 'w')
            
            self.logger.debug("Running FTSCP command")
            self.logger.debug("FTS server: %s" % fts_server_for_transfer)
            self.logger.debug("link: %s -> %s" % link)
            self.logger.debug("copyjob file: %s" % tmp_copyjob_file.name)
            self.logger.debug("log file: %s" % logfile.name)

            #TODO: Sending stdin/stdout/stderr can cause locks - should write to logfiles instead
            proc = subprocess.Popen(
                            ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                            stdout=logfile,
                            stderr=logfile,
                            stdin=subprocess.PIPE,
                        )

            command = 'export X509_USER_PROXY=%s ; ftscp -copyjobfile=%s -server=%s -mode=single' % (
                             self.userProxy, 
                             tmp_copyjob_file.name,
                             fts_server_for_transfer )
            proc.stdin.write(command)
            
            stdout, stderr = proc.communicate()
            rc = proc.returncode
            logfile.close()
            
            # now populate results by parsing the copy job's log file.
            # results is a tuple of lists, 1st list is transferred files, second is failed
            results = self.parse_ftscp_results(logfile.name, link[0])
            
            transferred_files.extend(results[0])
            failed_files.extend(results[1])

            # Clean up the temp copy job file
            os.unlink( tmp_copyjob_file.name )
            
            # The ftscp log file is left for operators to clean up, as per PhEDEx
            # TODO: recover from restarts by examining the log files, this would mean moving
            # the ftscp log files to a done folder once parsed, and parsing all files in some
            # work directory.
                
        return transferred_files, failed_files

    def parse_ftscp_results(self, ftscp_logfile, siteSource):
        """
        parse_ftscp_results parses the output of ftscp to get a list of all files that the job tried to transfer
        and adds the file to the appropriate list. This means that the lfn needs to be recreated from the __source__
        pfn.
        """

        transferred_files = []
        failed_files = []
        logfile = open(ftscp_logfile)

        for line in logfile.readlines():

            try:
                if line.split(':')[0].strip() == 'Source':
                   lfn = self.apply_tfc_to_pfn( siteSource, line.split('Source:')[1:][0].strip() )
                   # Now we have the lfn, skip to the next line
                   continue

                if line.split(':')[0].strip() == 'State' and lfn:
                   if line.split(':')[1].strip() == 'Finished':
                       transferred_files.append(lfn)
                   else:
                       failed_files.append(lfn)

            except IndexError, ex:

                self.logger.debug("wrong log file! %s" %ex)
                pass

        logfile.close()

        return (transferred_files, failed_files)

    def mark_good(self, files=[]):
        """
        Mark the list of files as tranferred 
        """
        results = []

        for i in files:

           # TODO: Delete without loading first
           try:

               document = self.db.document(i)
               self.db.queueDelete(document, viewlist=['AsyncTransfer/ftscp'])
               self.db.commit()

           except Exception, ex:

               msg =  "Error in deleting document from couch"
               msg += str(ex)
               msg += str(traceback.format_exc())
               self.logger.error(msg)

    def mark_failed(self, files=[]):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())

        for i in files:

            # TODO: modify without loading first
            try:
                document = self.db.document(i)
                document['state'] = 'acquired'
                document['retry_count'].append(now)
                self.db.queue(document, viewlist=['AsyncTransfer/ftscp'])

            except Exception, ex:

                msg =  "Error in updating document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

        self.db.commit()
                
    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

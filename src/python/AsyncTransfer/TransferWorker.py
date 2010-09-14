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
import subprocess, os
import tempfile
class TransferWorker:

    def __init__(self, user, tfc_map, config):
        """
        store the user and tfc the worker 
        """
        self.user = user
        self.tfc_map = tfc_map 
        self.config = config
        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.couch_database)
        self.map_fts_servers = config.map_FTSserver
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker')
        self.logger.info('Worker loaded for %s' % user)

        # TODO: use proxy management to pick this up - ticket #202 
        self.userProxy = config.serviceCert     
  
    def run(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files
        """
        jobs, pfn_lfn_map = self.files_for_transfer()

        for job in jobs:
            for task in job:
                pfn = (task.split(' ')[0].strip()).split(':',1)[1]
                assert pfn in pfn_lfn_map.keys() 

        filePathList, sortedLists, destList = self.buildTransferFilesAndLists(jobs)        
 
        count = 0

        # Loop on dest keys
        # To move to command method

        for filePath in filePathList:

            ret = self.cleanSpace( destList[filePath] )

            if not ret:


                for copyJob in filePathList[filePath]:

                    self.logger.info( "Running FTSCP command: %s %s %s from %s " %( filePathList[filePath][copyJob], self.getFTServer(filePath), jobs, sortedLists[filePath] ) )

                    proc = subprocess.Popen(
 
                                ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdin=subprocess.PIPE,

                            )

                    command = 'export X509_USER_PROXY=%s ; ftscp -copyjobfile=%s -server=%s -mode=single' %( self.userProxy, filePathList[filePath][copyJob], self.getFTServer(filePath) )
                    proc.stdin.write(command)
                    stdout, stderr = proc.communicate()
                    rc = proc.returncode

                    self.logger.info("Output %s" %stdout.split('\n'))
                    self.logger.info("ret code %s" %rc)

                    os.unlink( filePathList[filePath][copyJob] )

                    # Call mark_good and mark_failed methods 


        for job in jobs:
 
            transferred, failed = self.command(job, self.user)
            self.mark_failed(failed)
            self.mark_good(transferred)
                
        self.logger.info('%s is sleeping for %s seconds' % (self.user, len(jobs))) 
        time.sleep(len(jobs))
        return 'Transfers completed for %s' % self.user

    def cleanSpace(self, allPfn):
        """
        Remove all PFNs got in input.
        """
        for listPfn in allPfn:

            for pfn in listPfn:

                command = 'export X509_USER_PROXY=%s ; srmrm %s'  %( self.userProxy, pfn )
                self.logger.info("Running remove command %s" %command)
                proc = subprocess.Popen(

                                ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdin=subprocess.PIPE,
                            )
                proc.stdin.write(command)
                stdout, stderr = proc.communicate()
                rc = proc.returncode

                # return 1 if the remove fails
                #if rc: return 1

        return 0


    def getFTServer(self, site):
        """
        Parse site string to know the fts server to use 
        """
        indexSite = site.split('_')
        if indexSite[1] in self.map_fts_servers : return self.map_fts_servers[ indexSite[1] ]
        #TODO: decide what to do if indexSite[0] == 'T1'
        else: return self.map_fts_servers['defaultServer']
        
    def buildTransferFilesAndLists(self, files):
        """
        Order source/dest to create fts copy job file 
        create the temporary copy job file
        """
        allPathLists = {}
        sortedFile, listDest = self.orderFileAlgo(files)

        for files in sortedFile:

            filePathList = {}

            for sites in sortedFile[files]:

                f = tempfile.NamedTemporaryFile(delete=False)
                for jobs in sites:
                    f.write(jobs + "\n")

                f.close()
                filePathList[f] = f.name

            allPathLists[files] = filePathList 

        return allPathLists, sortedFile, listDest


    def orderFileAlgo(self, listTuple):
        """
        Order file by source/destination to create copy job file. Return also a list of destination pfn
        to clean pfn dest before the transfer and if the transfer will faild. 
        If T2_CH_CAF is the destination then we will have:
        ordredList = { 'T2_CH_CAF':[ [ job1FromSite1, job2FromSite1  ] , [ job3FromSite2, job4FromSite2 ] ] ... } 
        """

        ordredList = {}
        listDest = {}
        siteSource = {}
        pfnDist = {}

        for transfer in listTuple[0]:

            destSite = transfer.split(' ')[1].split(':')[0]

            if destSite not in ordredList:

                ordredList[destSite] = []
                listDest[destSite] = []

        for sortByDest in ordredList:  

            for sortBySource in listTuple[0]:
               
                destSite = sortBySource.split(' ')[1].split(':')[0]
 
                if ( destSite == sortByDest ):

                    sourceSite = sortBySource.split(' ')[0].split(':')[0]

                    if sourceSite not in siteSource:  
                        siteSource[sourceSite] = []
                        pfnDist[sourceSite] = []

                    siteSource[sourceSite].append( sortBySource.replace(sortByDest+':','').replace(sourceSite+':','') )
                    pfnDist[sourceSite].append( sortBySource.split(' ')[1].replace(sortByDest+':','') )

            for lists in siteSource:

                ordredList[sortByDest].append( siteSource[lists] )
                listDest[sortByDest].append( pfnDist[lists] )

        return ordredList, listDest
    
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

            #return "%s %s" % (self.apply_tfc(source), self.apply_tfc(dest))

            # give siteSource:PFN siteDestPFN to split different sources in different jobs later  
            return "%s %s" % (source.split(':')[0]+':'+self.apply_tfc(source), dest.split(':')[0]+':'+self.apply_tfc(dest))
 
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

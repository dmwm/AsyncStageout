#!/usr/bin/env
#pylint: disable-msg=C0103
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
import subprocess, os, errno
import tempfile
import datetime
import traceback
from WMCore.WMFactory import WMFactory
import urllib
from WMCore.Credential.Proxy import Proxy
from AsyncStageOut import getHashLfn

def getProxy(userdn, group, role, defaultDelegation, logger):
    """
    _getProxy_
    """

    logger.debug("Retrieving proxy for %s" % userdn)
    config = defaultDelegation
    config['userDN'] = userdn
    config['group'] = group
    config['role'] = role
    proxy = Proxy(defaultDelegation)
    proxyPath = proxy.getProxyFilename( True )
    timeleft = proxy.getTimeLeft( proxyPath )
    if timeleft is not None and timeleft > 3600:
        return (True, proxyPath)
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft( proxyPath )
    if timeleft is not None and timeleft > 0:
        return (True, proxyPath)
    return (False, None)


class TransferWorker:

    def __init__(self, user, tfc_map, config):
        """
        store the user and tfc the worker
        """
        self.user = user
        self.tfc_map = tfc_map
        self.config = config
        self.log_dir = '%s/logs/%s/%s/%s' % ( self.config.componentDir, \
 str(datetime.datetime.now().month), str(datetime.datetime.now().year), self.user)
        self.pfn_to_lfn_mapping = {}

        try:
            os.makedirs(self.log_dir)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else: raise


        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.map_fts_servers = config.map_FTSserver
        self.max_retry = config.max_retry
        # TODO: improve how the worker gets a log
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker-%s' % user)

        query = {'group': True,
                 'startkey':[user], 'endkey':[user, {}, {}]}
        self.listCred = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows'][0]['key'][2:]

        self.userDN = self.listCred[0]
        self.group = self.listCred[1]
        self.role = self.listCred[2]

        defaultDelegation = {
                                  'logger': self.logger,
                                  'credServerPath' : \
                                      self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN' : self.config.serverDN,
                                  'setupScript' : getattr(self.config, 'UISetupScript', None)
                            }
        valid = False
        try:

            valid, proxy = getProxy(self.userDN, self.group, self.role, defaultDelegation, self.logger)

        except Exception, ex:

            msg =  "Error getting the user proxy"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

        if valid:

            self.userProxy = proxy

        else:

            # Use the operator's proxy when the user proxy in invalid.
            # This will be moved soon
            self.userProxy = config.serviceCert

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)


    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """

        jobs = self.files_for_transfer()

        transferred, failed = self.command()

        self.mark_failed( failed )
        self.mark_good( transferred )

        self.logger.info('Transfers completed')
        return

    def cleanSpace(self, copyjob, site_to_clean, force_delete = False ):
        """
        Remove all __destination__ PFNs got in input. The delete can fail because of a file not found
        so issue the command and hope for the best.

        TODO: better parsing of output
        """
        if copyjob:

            for task in copyjob:

                # Decomment this if we want to clean before
                # the beginning of the transfer in the future
                #    destination_path = task.split()[1]
                destination_path = task

                destination_pfn = self.apply_tfc_to_lfn( '%s:%s' % ( site_to_clean, destination_path ) )
                logfile = open('%s/%s_%s.lcg-del.log' % ( self.log_dir, site_to_clean, str(time.time()) ), 'w')

                command = 'export X509_USER_PROXY=%s ; lcg-del -l %s'  % ( self.userProxy, destination_pfn )
                self.logger.debug("Running remove command %s" % command)
                self.logger.debug("log file: %s" % logfile.name)

                proc = subprocess.Popen(
                        ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                        stdout=logfile,
                        stderr=logfile,
                        stdin=subprocess.PIPE,
                )
                proc.stdin.write(command)
                stdout, stderr = proc.communicate()

                rc = proc.returncode
                logfile.close()


                if force_delete:

                    ls_logfile = open('%s/%s_%s.lcg-ls.log' % ( self.log_dir, site_to_clean, str(time.time()) ), 'w')

                    # Running Ls command to be sure that the file is not there anymore. It is better to do so rather opening
                    # the srmrm log and parse it
                    commandLs = 'export X509_USER_PROXY=%s ; lcg-ls %s'  % ( self.userProxy, destination_pfn )
                    self.logger.debug("Running list command %s" % commandLs)
                    self.logger.debug("log file: %s" % ls_logfile.name)

                    procLs = subprocess.Popen(
                            ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                            stdout=ls_logfile,
                            stderr=ls_logfile,
                            stdin=subprocess.PIPE,
                    )
                    procLs.stdin.write(commandLs)
                    stdout, stderr = procLs.communicate()
                    rcLs = procLs.returncode
                    ls_logfile.close()

                    # rcLs = 0 file exists while rcLs = 1 it doesn't
                    # Fallback to srmrm if the file still exists
                    if not rcLs:

                        rm_logfile = open('%s/%s_%s.srmrm.log' % ( self.log_dir, site_to_clean, str(time.time()) ), 'w')
                        commandRm = 'export X509_USER_PROXY=%s ; srmrm %s'  % ( self.userProxy, destination_pfn )
                        self.logger.debug("Running rm command %s" % commandRm)
                        self.logger.debug("log file: %s" % rm_logfile.name)


                        procRm = subprocess.Popen(
                                ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                                stdout=rm_logfile,
                                stderr=rm_logfile,
                                stdin=subprocess.PIPE,
                        )
                        procRm.stdin.write(commandRm)
                        stdout, stderr = procRm.communicate()
                        rcRm = procRm.returncode
                        rm_logfile.close()

                        # rcRm = 0 the remove was succeeding.
                        if rcRm:

                            copyjob.remove(task)
                            # Force file failure
                            self.mark_failed( [task], True )


        return copyjob

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
                     'key':[self.user, destination, source, self.userDN]}

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
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        site, lfn = tuple(file.split(':'))
        pfn = self.tfc_map[site].matchLFN('srmv2', lfn)

        #TODO: improve fix for wrong tfc on sites
        try:
            if pfn.split(':')[0] != 'srm':
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
        except IndexError:
            self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None
        except AttributeError:
            self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None

        # Add the pfn key into pfn-to-lfn mapping
        if not self.pfn_to_lfn_mapping.has_key(pfn):
            self.pfn_to_lfn_mapping[pfn] = lfn

        return pfn

    def get_lfn_from_pfn(self, site, pfn):
        """
        Take a site and pfn and get the lfn from self.pfn_to_lfn_mapping
        """
        lfn = self.pfn_to_lfn_mapping[pfn]

        # Clean pfn-to-lfn map
        del self.pfn_to_lfn_mapping[pfn]

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

            self.logger.debug("Valid %s" %self.validate_copyjob(copyjob) )
            # Validate copyjob file before doing anything
            if not self.validate_copyjob(copyjob): continue

            # Clean cruft files from previous transfer attempts
            # TODO: check that the job has a retry count > 0 and only delete files if that is the case
            #self.cleanSpace( copyjob, link[1] )

            tmp_copyjob_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_copyjob_file.write('\n'.join(copyjob))
            tmp_copyjob_file.close()

            fts_server_for_transfer = self.getFTServer(link[1])

            logfile = open('%s/%s-%s_%s.ftslog' % ( self.log_dir, link[0], link[1], str(time.time()) ), 'w')

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

            # Removing lfn from the source if the transfer succeeded else from the destination
            transferred_files.extend( self.cleanSpace( results[0], link[0] ) )
            failed_files.extend( self.cleanSpace( results[1], link[1], force_delete = True ) )

            # Clean up the temp copy job file
            os.unlink( tmp_copyjob_file.name )

            # The ftscp log file is left for operators to clean up, as per PhEDEx
            # TODO: recover from restarts by examining the log files, this would mean moving
            # the ftscp log files to a done folder once parsed, and parsing all files in some
            # work directory.

        return transferred_files, failed_files

    def validate_copyjob(self, copyjob):
        """
        the copyjob file is valid when source pfn and destination pfn are not None.
        """
        for task in copyjob:
            if task.split()[0] == 'None' or task.split()[1] == 'None': return False
        return True

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

                if line.strip() == 'Too many errors from status update, cancelling transfer':
                    self.logger.debug("Problem to contact the FTS server!")
                    break

                if line.split(':')[0].strip() == 'Source':
                    lfn = self.get_lfn_from_pfn( siteSource, line.split('Source:')[1:][0].strip() )
                    # Now we have the lfn, skip to the next line
                    continue

                if line.split(':')[0].strip() == 'State' and lfn:
                    if line.split(':')[1].strip() == 'Finished' or line.split(':')[1].strip() == 'Done':
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
        for lfn in files:

            try:

                updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + getHashLfn(lfn)
                data = {}
                data['end_time'] = str(datetime.datetime.now())
                data['state'] = 'done'
                updateUri += "?" + urllib.urlencode(data)
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)

            except Exception, ex:

                msg =  "Error in updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            try:

                document = self.db.document( getHashLfn(lfn) )

            except Exception, ex:

                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            outputPfn = self.apply_tfc_to_lfn( '%s:%s' % ( document['destination'], document['lfn'].replace('store/temp', 'store', 1) ) )
            pluginSource = self.factory.loadObject(self.config.pluginName, args = [self.config, self.logger], listFlag = True)
            pluginSource.updateSource({ 'jobid':document['jobid'], 'timestamp':document['dbSource_update'], \
                                        'pfn': outputPfn, 'checksums': document['checksums'] })

        try:
            self.db.commit()
        except Exception, ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def mark_failed(self, files=[], force_fail = False ):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())

        for lfn in files:

            # TODO: modify without loading first
            try:


                document = self.db.document( getHashLfn(lfn) )

            except Exception, ex:

                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            document['retry_count'].append(now)

            if force_fail or len(document['retry_count']) > self.max_retry:

                document['state'] = 'failed'
                document['end_time'] = now

            else:

                document['state'] = 'acquired'

            try:

                self.db.queue(document, viewlist=['AsyncTransfer/ftscp'])

            except Exception, ex:

                msg =  "Error when queuing document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
        try:
            self.db.commit()
        except Exception, ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

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
from AsyncStageOut import getFTServer
from AsyncStageOut import getDNFromUserName
#import json
#import socket
#import stomp
from time import strftime

def execute_command( command, logger, timeout ):
    """
    _execute_command_
    Funtion to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
    )

    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))

    return stdout, rc

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
        self.user = user[0]
        self.group = user[1]
        self.role = user[2]
        self.tfc_map = tfc_map
        self.config = config
        self.log_dir = '%s/logs/%s/%s/%s' % ( self.config.componentDir, \
 str(datetime.datetime.now().month), str(datetime.datetime.now().year), self.user)
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker-%s' % self.user)
        self.pfn_to_lfn_mapping = {}
        try:
            os.makedirs(self.log_dir)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                self.logger.error('Unknown error in mkdir' % e.errno)
                raise
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.max_retry = config.max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.transfer_script = getattr(self.config, 'transfer_script', 'ftscp')
        self.cleanEnvironment = ''
        self.userDN = ''
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        # TODO: improve how the worker gets a log

        query = {'group': True,
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}#,
        #         'stale': 'ok'}
        self.logger.debug("Trying to get DN")
        try:
            self.userDN = getDNFromUserName(self.user, self.logger)
        except Exception, ex:
            msg =  "Error retrieving the user DN"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            self.init = False
            return
        if not self.userDN:
            self.init = False
            return
        defaultDelegation = {
                                  'logger': self.logger,
                                  'credServerPath' : \
                                      self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN' : self.config.serverDN,
                                  'uisource' : self.uiSetupScript,
                                  'cleanEnvironment' : getattr(self.config, 'cleanEnvironment', False),
                            }

        if getattr(self.config, 'serviceCert', None):
            defaultDelegation['server_cert'] = self.config.serviceCert
        if getattr(self.config, 'serviceKey', None):
            defaultDelegation['server_key'] = self.config.serviceKey

        self.valid = False
        try:

            self.valid, proxy = getProxy(self.userDN, self.group, self.role, defaultDelegation, self.logger)

        except Exception, ex:

            msg =  "Error getting the user proxy"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

        if self.valid:
            self.userProxy = proxy
        else:
            # Use the operator's proxy when the user proxy in invalid.
            # This will be moved soon
            self.logger.error('Did not get valid proxy. Setting proxy to ops proxy')
            self.userProxy = config.opsProxy

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)
        self.failures_reasons =  {}
        self.commandTimeout = 1200

    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """
        jobs = self.files_for_transfer()
        self.logger.debug("FIRST RETRY for %s " %self.userProxy )
        if jobs:
            self.command(jobs)
        jobs_failed = self.files_for_transfer(True)
        self.logger.debug("RETRY > 1 for %s " %self.userProxy )
        if jobs_failed:
            self.command(jobs_failed, True)
        self.logger.info('Transfers completed')
        return

    def cleanSpace(self, to_clean_dict, force_delete = False ):
        """
        Remove all __destination__ PFNs got in input. The delete can fail because of a file not found
        so issue the command and hope for the best.

        TODO: better parsing of output
        """
        if to_clean_dict:

            for task in to_clean_dict:

                for destination_paths in task:
                    # Decomment this if we want to clean before
                    # the beginning of the transfer in the future
                    #    destination_path = task.split()[1]

                    try:
                        destination_path = destination_paths.split()[1]
                    except:
                        destination_path = destination_paths

                    # Test if it is an lfn
                    if destination_path.find(":") < 0:
                        destination_pfn = self.apply_tfc_to_lfn( '%s:%s' %( to_clean_dict[ task ], destination_path ) )
                    else:
                        destination_pfn = destination_path

                    lcgdel_file = open('%s/%s_%s.lcg-del.log' % ( self.log_dir, to_clean_dict[ task ], str(time.time()) ), 'w')

                    command = '%s export X509_USER_PROXY=%s ; source %s ; lcg-del -lv --connect-timeout 20 --sendreceive-timeout 240 %s'  % \
                              (self.cleanEnvironment, self.userProxy, self.uiSetupScript, destination_pfn)
                    self.logger.debug("Running remove command %s" % command)
                    self.logger.debug("log file: %s" % lcgdel_file.name)

                    proc = subprocess.Popen(
                            ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                            stdout=lcgdel_file,
                            stderr=lcgdel_file,
                            stdin=subprocess.PIPE,
                    )
                    proc.stdin.write(command)
                    stdout, stderr = proc.communicate()

                    rc = proc.returncode
                    lcgdel_file.close()


                    if force_delete:

                        ls_logfile = open('%s/%s_%s.lcg-ls.log' % ( self.log_dir, to_clean_dict[ task ], str(time.time()) ), 'w')

                        # Running Ls command to be sure that the file is not there anymore. It is better to do so rather opening
                        # the srmrm log and parse it
                        commandLs = '%s export X509_USER_PROXY=%s ; source %s ; lcg-ls %s'  % \
                                    (self.cleanEnvironment, self.userProxy, self.uiSetupScript, destination_pfn)
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

                            rm_logfile = open('%s/%s_%s.srmrm.log' % (self.log_dir,
                                                                      to_clean_dict[ task ],
                                                                      str(time.time())),
                                                                      'w')
                            commandRm = '%s export X509_USER_PROXY=%s ; source %s ; srmrm %s'  % \
                                        (self.cleanEnvironment, self.userProxy, self.uiSetupScript, destination_pfn)
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
                            #if rcRm:

                            #    del to_clean_dict[task]
                            #    # Force file failure
                            #    self.mark_failed( [task], True )

        return

    def source_destinations_by_user(self):
        """
        Get all the destinations for a user
        """
        query = {'group': True,
                 'startkey':[self.user, self.group, self.role], 'endkey':[self.user, self.group, self.role, {}, {}]}
                 #'stale': 'ok'}
        try:
            sites = self.db.loadView('AsyncTransfer', 'ftscp_all', query)
        except:
            return []

        def keys_map(dict):
            return dict['key'][4], dict['key'][3]

        return map(keys_map, sites['rows'])


    def files_for_transfer(self, retry=False):
        """
        Process a queue of work per transfer source:destination for a user. Return one
        ftscp copyjob per source:destination.
        """
        source_dests = self.source_destinations_by_user()
        jobs = {}
        failed_files = []
        self.logger.info('%s has %s links to transfer on: %s' % (self.user, len(source_dests), str(source_dests)))
        try:
            for (source, destination) in source_dests:
                # We could push applying the TFC into the list function, not sure if
                # this would be faster, but might use up less memory. Probably more
                # complicated, though.
                query = {'reduce':False,
                     'limit': self.config.max_files_per_transfer,
                     'key':[self.user, self.group, self.role, destination, source]}
                     #'stale': 'ok'}
                try:
                    if not retry:
                        active_files = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows']
                    else:
                        active_files = self.db.loadView('AsyncTransfer', 'ftscp_retry', query)['rows']
                except:
                    return {}
                # Prepare the list of active files updating the status to in transfer if the proxy is valid.
                acquired_files = self.mark_acquired(active_files)
                if not acquired_files:
                    continue
                self.logger.debug('%s has %s files to transfer from %s to %s' % (self.user,
                                                                                 len(acquired_files),
                                                                                 source,
                                                                                 destination))
                new_job = []
                # take these active files and make a copyjob entry
                def tfc_map(item):
                    source_pfn = self.apply_tfc_to_lfn('%s:%s' % (source, item['value']))
                    destination_pfn = self.apply_tfc_to_lfn('%s:%s' % (destination,
                                                                       item['value'].replace('store/temp', 'store', 1).replace(\
                                                                       '.' + item['value'].split('.', 1)[1].split('/', 1)[0], '', 1)))
                    new_job.append('%s %s' % (source_pfn, destination_pfn))

                map(tfc_map, acquired_files)

                jobs[(source, destination)] = new_job
            self.logger.debug('ftscp input created for %s (%s jobs)' % (self.user, len(jobs.keys())))

            if failed_files:
                failed_files = jobs
            return jobs
        except:
            self.logger.exception("fail")
            return {}


    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        try:
            site, lfn = tuple(file.split(':'))
        except Exception, e:
            self.logger.error('it does not seem to be an lfn %s' %file.split(':'))
            return None
        if site == 'T1_US_FNAL':
            site = 'T1_US_FNAL_Buffer'
        if site == 'T1_ES_PIC':
            site = 'T1_ES_PIC_Buffer'
        if site == 'T1_DE_KIT':
            site = 'T1_DE_KIT_Buffer'
        if site == 'T1_FR_CCIN2P3':
            site = 'T1_FR_CCIN2P3_Buffer'
        if site == 'T1_IT_CNAF':
            site = 'T1_IT_CNAF_Buffer'
        if site == 'T1_RU_JINR':
            site = 'T1_RU_JINR_Buffer'
        if site == 'T1_TW_ASGC':
            site = 'T1_TW_ASGC_Buffer'
        if site == 'T1_UK_RAL':
            site = 'T1_UK_RAL_Buffer'
        if site == 'T1_CH_CERN':
            site = 'T1_CH_CERN_Buffer'
        if self.tfc_map.has_key(site):
            pfn = self.tfc_map[site].matchLFN('srmv2', lfn)
            if pfn.find("\\") != -1: pfn = pfn.replace("\\","")
            #TODO: improve fix for wrong tfc on sites
            try:
                if pfn.split(':')[0] != 'srm' and pfn.split(':')[0] != 'gsiftp' :
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
        else:
            self.logger.error('Wrong site %s!' % site)
            return None

    def get_lfn_from_pfn(self, site, pfn):
        """
        Take a site and pfn and get the lfn from self.pfn_to_lfn_mapping
        """
        lfn = self.pfn_to_lfn_mapping[pfn]

        # Clean pfn-to-lfn map
        del self.pfn_to_lfn_mapping[pfn]

        return lfn

    def command(self, jobs, retry = False):
        """
        For each job the worker has to complete:
           Delete files that have failed previously
           Create a temporary copyjob file
           Submit the copyjob to the appropriate FTS server
           Parse the output of the FTS transfer and return complete and failed files for recording
        """
        transferred_files = []
        failed_files = []
        fts_logs = []
        r = []

        logs_pool = {}
        mapping_link_process = {}
        tmp_file_pool = []

        failed_to_clean = {}
        ftslog_file = None

        processes = set()
        #self.logger.debug( "COMMAND FOR %s with jobs %s" %(self.userProxy, jobs) )

        os.environ['X509_USER_PROXY'] = self.userProxy
        #Loop through all the jobs for the links we have
        for link, copyjob in jobs.items():

            self.logger.debug("Valid %s" %self.validate_copyjob(copyjob) )
            # Validate copyjob file before doing anything
            if not self.validate_copyjob(copyjob): continue

            # Clean cruft files from previous transfer attempts
            # TODO: check that the job has a retry count > 0 and only delete files if that is the case
            to_clean = {}
            to_clean[ tuple( copyjob ) ] = link[1]

            if retry:
                self.cleanSpace( to_clean )

            tmp_copyjob_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_copyjob_file.write('\n'.join(copyjob))
            tmp_copyjob_file.close()

            tmp_file_pool.append(tmp_copyjob_file.name)

            fts_server_for_transfer = getFTServer(link[1], 'getRunningFTSserver', self.config_db, self.logger)

            ftslog_file = open('%s/%s-%s_%s.ftslog' % ( self.log_dir, link[0], link[1], str(time.time()) ), 'w')

            logs_pool[link] = ftslog_file

            self.logger.debug("Running FTSCP command")
            self.logger.debug("FTS server: %s" % fts_server_for_transfer)
            self.logger.debug("link: %s -> %s" % link)
            self.logger.debug("copyjob file: %s" % tmp_copyjob_file.name)
            self.logger.debug("log file created: %s" % ftslog_file.name)

            command = '%s -copyjobfile=%s -server=%s -mode=single' % (self.transfer_script,
                                                                      tmp_copyjob_file.name,
                                                                      fts_server_for_transfer)

            init_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
            self.logger.debug("executing command: %s in log: %s at: %s for: %s" % (command,
                                                                                   ftslog_file.name,
                                                                                   init_time,
                                                                                   self.userDN))
            self.logger.debug(command.split())
            proc = subprocess.Popen(
                            command.split(),
                            stdout=ftslog_file,
                            stderr=ftslog_file,
                        )

            processes.add(proc)
            mapping_link_process[proc] = link

            # now populate results by parsing the copy job's log file.
            # results is a tuple of lists, 1st list is transferred files, second is failed

            self.logger.debug("link %s" % link[0])

            ###results = self.parse_ftscp_results(ftslog_file.name, link[0])
            # Removing lfn from the source if the transfer succeeded else from the destination
            ###transferred_files.extend( results[0] )
            ###failed_files.extend( results[1] )
            ###self.logger.debug("transferred : %s" % transferred_files)
            ###self.logger.info("failed : %s" % failed_files)

            # Clean up the temp copy job file
            ###os.unlink( tmp_copyjob_file.name )

            # The ftscp log file is left for operators to clean up, as per PhEDEx
            # TODO: recover from restarts by examining the log files, this would mean moving
            # the ftscp log files to a done folder once parsed, and parsing all files in some
            # work directory.

        self.logger.debug("WORK DONE WAITING and CLEANING")

        for p in processes:
            self.logger.debug("Process list %s" %processes)
            self.logger.debug("Link to Process %s" %mapping_link_process)
            self.logger.debug("Link to Log %s" %logs_pool)
            self.logger.debug("Current process %s" %p)
            if p.poll() is None:
                p.wait();
                link = mapping_link_process[p]
                log_file = logs_pool[link]
                log_file.close()
                end_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
                self.logger.debug("UPDATING %s %s for %s at %s" %(link, log_file, self.userDN, end_time))
                results = self.parse_ftscp_results(log_file.name, link)
                self.logger.debug("RESULTS %s %s" %( str(results[0]), str(results[1]) ))
                self.mark_good( results[0], log_file.name)
                self.mark_failed( results[1], log_file.name)

            else:
                link = mapping_link_process[p]
                log_file = logs_pool[link]
                log_file.close()
                end_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
                self.logger.debug("UPDATING %s %s for %s at %s" %(link, log_file, self.userDN, end_time))
                results = self.parse_ftscp_results(log_file.name, link)
                self.logger.debug("RESULTS %s %s" %( str(results[0]), str(results[1]) ))
                self.mark_good( results[0], log_file.name)
                self.mark_failed( results[1], log_file.name)


        self.logger.debug("PROCESS WORK DONE")

#        for link, log_file in logs_pool.iteritems():

#            log_file.close()
#            end_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
#            self.logger.debug("UPDATING %s %s for %s at %s" %(link, log_file, self.userDN, end_time))
#            results = self.parse_ftscp_results(log_file.name, link)
#            self.logger.debug("RESULTS %s %s" %( str(results[0]), str(results[1]) ))
#            self.mark_good( results[0], log_file.name)
#            self.mark_failed( results[1], log_file.name)
            ##transferred_files.extend( results[0] )
            ##failed_files.extend( results[1] )
            #transferred_files[log_file] = results[0]
            #log_file.close()

#        self.logger.debug("transferred : %s" % transferred_files)
#        self.logger.info("failed : %s" % failed_files)

        for tmp in tmp_file_pool:
            os.unlink( tmp )

        #if ftslog_file:
        #    return transferred_files, failed_files, ftslog_file.name
        #else:
        #    return transferred_files, failed_files, ""
        return

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
        ftscp_file = open(ftscp_logfile)

        for line in ftscp_file.readlines():
            try:
                if line.split(':')[0].strip() == 'Source':
                    lfn = self.get_lfn_from_pfn( siteSource, line.split('Source:')[1:][0].strip() )
                    # Now we have the lfn, skip to the next line
                    continue
                if line.split(':')[0].strip() == 'State' and lfn:
                    if line.split(':')[1].strip() == 'Finished' or line.split(':')[1].strip() == 'Done' or line.split(':')[1].strip() == 'Finishing':
                        transferred_files.append(lfn)
                    else:
                        failed_files.append(lfn)
                if line.split(':')[0].strip() == 'Reason':
                    self.failures_reasons[lfn] = line.split('Reason:')[1:][0].strip()
            except IndexError, ex:
                self.logger.debug("wrong log file! %s" %ex)

        ftscp_file.close()

        return (transferred_files, failed_files)

    def mark_acquired(self, files=[], good_logfile=None):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        for lfn in files:
            if lfn['value'].find('temp') > 1:
                docId = getHashLfn(lfn['value'])
                # Load document to get the retry_count
                try:
                    document = self.db.document( docId )
                except Exception, ex:
                    msg =  "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                if document['state'] == 'new':
                    document['state'] = 'acquired'
                    try:
                        self.db.queue( document )
                    except Exception, ex:
                        msg =  "Error updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue

                    try:
                        self.db.commit()
                    except Exception, ex:
                        msg =  "Error commiting documents in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                if (document['state'] == 'new' or document['state'] == 'acquired'):
                    lfn_in_transfer.append(lfn)
                else:
                    continue

            else:
                good_lfn = lfn['value'].replace('store', 'store/temp', 1)
                self.mark_good([good_lfn])
        return lfn_in_transfer

    def mark_good(self, files=[], good_logfile=None):
        """
        Mark the list of files as tranferred
        """
        for lfn in files:
            try:
                document = self.db.document( getHashLfn(lfn) )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            if document['state'] != 'killed':
                if good_logfile:
                    to_attach = file(good_logfile)
                    content = to_attach.read(-1)
                    retval = self.db.addAttachment(document["_id"],
                                                   document["_rev"],
                                                   content,
                                                   to_attach.name.split('/')[len(to_attach.name.split('/')) - 1],
                                                   "text/plain")
                    if retval.get('ok', False) != True:
                        # Then we have a problem
                        msg = "Adding an attachment to document failed\n"
                        msg += str(retval)
                        msg += "ID: %s, Rev: %s" % (document["_id"], document["_rev"])
                        self.logger.error(msg)
                outputLfn = document['lfn'].replace('store/temp', 'store', 1)
                try:
                    now = str(datetime.datetime.now())
                    last_update = time.time()
                    data = {}
                    data['end_time'] = now
                    data['state'] = 'done'
                    data['lfn'] = outputLfn
                    data['last_update'] = last_update
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + getHashLfn(lfn)
                    updateUri += "?" + urllib.urlencode(data)
                    self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                except Exception, ex:
                    msg =  "Error updating document in couch"
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
#            outputPfn = self.apply_tfc_to_lfn( '%s:%s' % ( document['destination'], outputLfn ) )
#            pluginSource = self.factory.loadObject(self.config.pluginName, args = [self.config, self.logger], listFlag = True)
#            pluginSource.updateSource({ 'jobid':document['jobid'], 'timestamp':document['job_end_time'], \
#                                        'lfn': outputLfn, 'location': document['destination'], 'pfn': outputPfn, 'checksums': document['checksums'] })
#            if document["type"] == "output":
#                try:
#                    self.logger.debug("Worker producing %s" %(str(document["jobid"]) + ":" + "done"))
#                    message = { 'PandaID':document["jobid"], 'transferStatus': { document["lfn"] : "done" } }
#                    self.produce(message)
#                except Exception, ex:
#                    msg =  "Error producing message"
#                    msg += str(ex)
#                    msg += str(traceback.format_exc())
#                    self.logger.error(msg)

        self.logger.debug("transferred file updated")
#        try:
#            self.db.commit()
#        except Exception, ex:
#            msg =  "Error commiting documents in couch"
#            msg += str(ex)
#            msg += str(traceback.format_exc())
#            self.logger.error(msg)

    def mark_failed(self, files=[], bad_logfile=None, force_fail = False ):
        """
        Something failed for these files so increment the retry count
        """
        for lfn in files:

            data = {}
            if 'temp' not in lfn:
                temp_lfn = lfn.replace('store', 'store/temp', 1)
            else:
                temp_lfn = lfn

            docId = getHashLfn(temp_lfn)

            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
            if document['state'] != 'killed':
                if bad_logfile:
                    to_attach = file(bad_logfile)
                    content = to_attach.read(-1)
                    retval = self.db.addAttachment( document["_id"], document["_rev"], content, to_attach.name.split('/')[ len(to_attach.name.split('/')) - 1 ], "text/plain" )
                    if retval.get('ok', False) != True:
                        # Then we have a problem
                        msg = "Adding an attachment to document failed\n"
                        msg += str(retval)
                        msg += "ID: %s, Rev: %s" % (document["_id"], document["_rev"])
                        self.logger.error(msg)
                now = str(datetime.datetime.now())
                last_update = time.time()
                # Prepare data to update the document in couch
                if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                    data['state'] = 'failed'
                    if self.failures_reasons[lfn]:
                        data['failure_reason'] = self.failures_reasons[lfn]
                    else:
                        data['failure_reason'] = "User Proxy has expired."
                    data['end_time'] = now
                else:
                    data['state'] = 'acquired'
                data['last_update'] = last_update
                data['retry'] = now
                # Update the document in couch
                try:
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                    updateUri += "?" + urllib.urlencode(data)
                    self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                except Exception, ex:
                    msg =  "Error in updating document in couch"
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
             # TODO: Evaluate message per file
#            try:
#                self.logger.debug("Worker producing %s" %(str(document["jobid"]) + ":" + document["state"]))
#                message = { 'PandaID':document["jobid"], 'transferStatus': { document["lfn"] : document["state"] } }
#                self.produce(str(message))
#            except Exception, ex:
#                msg =  "Error producing message"
#                msg += str(ex)
#                msg += str(traceback.format_exc())
#                self.logger.error(msg)

        self.logger.debug("failed file updated")
         # TODO: check if the bulk commit works
#        try:
#            self.db.commit()
#        except Exception, ex:
#            msg =  "Error commiting documents in couch"
#            msg += str(ex)
#            msg += str(traceback.format_exc())
#            self.logger.error(msg)

#    def produce(self, message ):
#        """
#        Produce state messages: jobid:state
#        """
#        f = open('/home/vosusr01/example_AMQ_dash/auth_xrootd_producer.txt')
#        authParams = json.loads(f.read())

#        connected = False
#        while not connected:
#            try:
                # connect to the stompserver
                #host=[(self.config.msg_host, self.config.msg_port)]
                #conn = stomp.Connection(host, self.config.msg_user, self.config.msg_pwd)
#                host=[(authParams['MSG_HOST'], authParams['MSG_PORT'])]
#                conn = stomp.Connection(host, authParams['MSG_USER'], authParams['MSG_PWD'])
#                conn.start()
#                conn.connect()
#                messageDict = json.dumps(message)
                # send the message
#                conn.send( messageDict, destination=authParams['MSG_QUEUE'] )
                #conn.send(message,destination=self.config.msg_queue)
                # disconnect from the stomp server
#                conn.disconnect()
#                connected = True
#            except socket.error:
#                pass

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

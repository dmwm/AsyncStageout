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
import re
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

    def __init__(self, user, tfc_map, config, list_to_process, link_to_process, pfn_to_lfn_mapping, lfn_to_pfn_mapping):
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
        try:
            os.makedirs(self.log_dir)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                self.logger.error('Unknown error in mkdir' % e.errno)
                raise
        self.max_retry = config.max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.transfer_script = getattr(self.config, 'transfer_script', 'ftscp')
        self.cleanEnvironment = ''
        self.userDN = ''
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        # TODO: improve how the worker gets a log
        self.logger.debug("Trying to get the DN of %s" % self.user)
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
                                  'cleanEnvironment' : getattr(self.config, 'cleanEnvironment', False)
                            }
        if hasattr(self.config, "cache_area"):
            try:
                defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(self.config.cache_area)[0]
            except IndexError:
                self.logger.error('MyproxyAccount parameter cannot be retrieved from %s' % self.config.cache_area)
                pass
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
        # Make the polling cycle configurable
        self.polling_cycle = 600
        # Proxy management in Couch
        os.environ['X509_USER_PROXY'] = self.userProxy
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.list_process = list_to_process
        self.link_process = link_to_process
        self.pfn_to_lfn_mapping = pfn_to_lfn_mapping
        self.lfn_to_pfn_mapping = lfn_to_pfn_mapping


    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """
        self.logger.info("Starting retrieving jobs")
        jobs = self.files_for_transfer()
        self.logger.info("Starting submission")
        self.command(jobs)
        self.logger.info('Transfers completed')

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
                     'key':[self.user, self.group, self.role, destination, source], 'stale': 'ok'}
                try:
                    active_files = self.db.loadView('AsyncTransfer', 'ftscp_all', query)['rows']
                except:
                    continue

                # Prepare the list of active files updating the status to in transfer.
                self.logger.debug('%s has %s files to transfer from %s to %s' % (self.user,
                                                                                 len(active_files),
                                                                                 source,
                                                                                 destination))
                new_job = []
                # take these active files and make a copyjob entry
                def tfc_map(item):
                    source_pfn = self.apply_tfc_to_lfn('%s:%s' % (source, item['value']), True)
                    destination_pfn = self.apply_tfc_to_lfn('%s:%s' % (destination,
                                                                       item['value'].replace('store/temp', 'store', 1).replace(\
                                                                       '.' + item['value'].split('.', 1)[1].split('/', 1)[0], '', 1)), False)
                    if source_pfn and destination_pfn:
                        acquired_file = self.mark_acquired([item])
                        if acquired_file:
                            new_job.append('%s %s' % (source_pfn, destination_pfn))
                    else:
                        self.mark_failed([item])
                map(tfc_map, active_files)
                if new_job:
                    jobs[(source, destination)] = new_job
            self.logger.debug('ftscp input created for %s (%s jobs)' % (self.user, len(jobs.keys())))
            return jobs
        except:
            self.logger.exception("fail")
            return {}


    def apply_tfc_to_lfn(self, file, storePFN):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        try:
            site, lfn = tuple(file.split(':'))
        except Exception, e:
            self.logger.error('it does not seem to be an lfn %s' %file.split(':'))
            return None
        if self.tfc_map.has_key(site):
            pfn = self.tfc_map[site].matchLFN('srmv2', lfn)
            #TODO: improve fix for wrong tfc on sites
            try:
                if pfn.find("\\") != -1: pfn = pfn.replace("\\","")
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
            # Add the lfn key into lfn-to-pfn mapping
            if storePFN:
                if not self.pfn_to_lfn_mapping.has_key(pfn):
                    self.pfn_to_lfn_mapping[pfn] = lfn
                if not self.lfn_to_pfn_mapping.has_key(lfn):
                    self.lfn_to_pfn_mapping[lfn] = pfn
            return pfn
        else:
            self.logger.error('Wrong site %s!' % site)
            return None

    def get_lfn_from_pfn(self, site, pfn):
        """
        Take a site and pfn and get the lfn from self.pfn_to_lfn_mapping
        """
        if self.pfn_to_lfn_mapping.has_key(pfn):
            lfn = self.pfn_to_lfn_mapping[pfn]
            # Clean pfn-to-lfn map
            #del self.pfn_to_lfn_mapping[pfn]
        else:
            lfn = None
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

        processes = []
        self.logger.debug( "COMMAND FOR %s with jobs %s" %(self.userProxy, jobs) )
        os.environ['X509_USER_PROXY'] = self.userProxy

        #Loop through all the jobs for the links we have
        if jobs:
            for link, copyjob in jobs.items():

                self.logger.debug("Valid %s" %self.validate_copyjob(copyjob) )
                # Validate copyjob file before doing anything
                if not self.validate_copyjob(copyjob): continue

                # Clean cruft files from previous transfer attempts
                # TODO: check that the job has a retry count > 0 and only delete files if that is the case
                to_clean = {}
                to_clean[ tuple( copyjob ) ] = link[1]

                tmp_copyjob_file = tempfile.NamedTemporaryFile(delete=False)
                tmp_copyjob_file.write('\n'.join(copyjob))
                tmp_copyjob_file.close()
                tmp_file_pool.append(tmp_copyjob_file.name)

                fts_server_for_transfer = getFTServer(link[1], 'getRunningFTSserver', self.config_db, self.logger)
                ftslog_file = open('%s/%s-%s_%s.ftslog' % ( self.log_dir, link[0], link[1], str(time.time()) ), 'w')

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
                # TODO: Put the FTS command timeout here
                proc = subprocess.Popen(
                                command.split(),
                                stdout=ftslog_file,
                                stderr=ftslog_file,
                            )

                self.logger.debug("Process started: %s" % proc)
                processes.append(proc)
                mapping_link_process[proc] = []
                mapping_link_process[proc].append(ftslog_file)
                mapping_link_process[proc].append(link)
                self.logger.debug("Link added: %s --> %s" % link)
                self.logger.debug("Mapping updated: %s" % mapping_link_process[proc])

            self.logger.debug("WORK DONE WAITING and CLEANING: mapping %s process %s" %( mapping_link_process, processes ))

        # Update the list of process/links to track
        processes.extend(self.list_process)
        self.list_process = processes
        self.link_process.update(mapping_link_process)

        self.logger.debug("list/link updated %s %s" % (self.list_process, self.link_process))
        process_done = []
        start_time = int(time.time())
        for p in self.list_process:
            self.logger.debug("Process list %s" % self.list_process)
            self.logger.debug("Link to Process %s" % self.link_process)
            self.logger.debug("Current process %s" % p)
            # results is a tuple of lists, 1st list is transferred files, second is failed
            if p.poll() is None:
                actual_time = int(time.time())
                elapsed_time = actual_time - start_time
                if elapsed_time < self.polling_cycle:
                    self.logger.debug("Waiting %s..." % (self.polling_cycle - elapsed_time))
                    time.sleep(self.polling_cycle - elapsed_time)
                    self.logger.debug("Checking and updating after waiting")
                if p.poll() is None:
                    pass
                else:
                    link = self.link_process[p][1]
                    log_file = self.link_process[p][0]
                    log_file.close()
                    end_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
                    self.logger.debug("UPDATING %s %s for %s at %s" %(link[0], log_file, self.userDN, end_time))
                    results = self.parse_ftscp_results(log_file.name, link[0])
                    self.logger.debug("RESULTS %s %s" %( str(results[0]), str(results[1]) ))
                    goodlfn_to_update = self.mark_good( results[0], log_file.name)
                    failedlfn_to_update = self.mark_failed( results[1], log_file.name)
                    if not goodlfn_to_update and not failedlfn_to_update:
                        process_done.append(p)
            else:
                link = self.link_process[p][1]
                log_file = self.link_process[p][0]
                log_file.close()
                end_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
                self.logger.debug("UPDATING %s %s for %s at %s" %(link[0], log_file, self.userDN, end_time))
                results = self.parse_ftscp_results(log_file.name, link[0])
                self.logger.debug("RESULTS %s %s" %( str(results[0]), str(results[1]) ))
                goodlfn_to_update = self.mark_good( results[0], log_file.name)
                failedlfn_to_update = self.mark_failed( results[1], log_file.name)
                if not goodlfn_to_update and not failedlfn_to_update:
                    process_done.append(p)
        self.logger.debug("PROCESS WORK DONE")

        # Unlink copy job files
        #for tmp in tmp_file_pool:
        #    os.unlink( tmp )

        # Clean the list of process and links
        for p_done in process_done:
            self.list_process.remove(p_done)
            del self.link_process[p_done]

        self.logger.debug("Checking remaining work...")
        self.logger.debug("Remaining process %s..." % self.list_process)
        self.logger.debug("Remaining links %s..." % self.link_process)

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
        self.logger.debug("Parsing ftslog")
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
                if line.split(':')[0].strip() == 'Reason' and lfn:
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
                self.logger.debug("Marking acquired %s" % docId)
                # Load document to get the retry_count
                try:
                    document = self.db.document( docId )
                except Exception, ex:
                    msg =  "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                if (document['state'] == 'new' or document['state'] == 'retry'):
                    data = {}
                    data['state'] = 'acquired'
                    data['last_update'] = time.time()
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                    updateUri += "?" + urllib.urlencode(data)
                    try:
                        self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                    except Exception, ex:
                        msg = "Error updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                    self.logger.debug("Marked acquired %s of %s" % (docId,lfn))
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
        lfn_to_update = []
        for lfn in files:
            self.logger.info("Marking good %s" % getHashLfn(lfn))
            self.logger.debug("Marking good %s" % lfn)
            try:
                document = self.db.document( getHashLfn(lfn) )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                lfn_to_update.append(lfn)
                continue
            if document['state'] != 'killed' and document['state'] != 'done':
                if good_logfile:
                    to_attach = file(good_logfile)
                    content = to_attach.read(-1)
                    try:
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
                            lfn_to_update.append(lfn)
                            continue
                    except Exception, ex:
                        msg =  "Error updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        lfn_to_update.append(lfn)
                        continue
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
                    lfn_to_update.append(lfn)
                    continue
                try:
                    self.db.commit()
                except Exception, ex:
                    msg =  "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    lfn_to_update.append(lfn)
                    continue
            if self.lfn_to_pfn_mapping.has_key(lfn):
                pfn = self.lfn_to_pfn_mapping[lfn]
                del self.lfn_to_pfn_mapping[lfn]
                if self.pfn_to_lfn_mapping.has_key(pfn):
                    del self.pfn_to_lfn_mapping[pfn]
        self.logger.debug("transferred file updated")
        return lfn_to_update

    def mark_failed(self, files=[], bad_logfile=None, force_fail = False ):
        """
        Something failed for these files so increment the retry count
        """
        lfn_to_update = []
        for lfn in files:
            data = {}
            if not isinstance(lfn, dict):
                if 'temp' not in lfn:
                    temp_lfn = lfn.replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn
                perm_lfn = lfn
            else:
                if 'temp' not in lfn['value']:
                    temp_lfn = lfn['value'].replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn['value']
                perm_lfn = lfn['value']
            docId = getHashLfn(temp_lfn)
            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                lfn_to_update.append(temp_lfn)
                continue
            if document['state'] != 'killed' and document['state'] != 'failed':
                if bad_logfile:
                    to_attach = file(bad_logfile)
                    content = to_attach.read(-1)
                    try:
                        retval = self.db.addAttachment( document["_id"], document["_rev"], content, to_attach.name.split('/')[ len(to_attach.name.split('/')) - 1 ], "text/plain" )
                        if retval.get('ok', False) != True:
                            # Then we have a problem
                            msg = "Adding an attachment to document failed\n"
                            msg += str(retval)
                            msg += "ID: %s, Rev: %s" % (document["_id"], document["_rev"])
                            self.logger.error(msg)
                            lfn_to_update.append(temp_lfn)
                            continue
                    except Exception, ex:
                        msg =  "Error updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        lfn_to_update.append(temp_lfn)
                        continue
                now = str(datetime.datetime.now())
                last_update = time.time()
                # Prepare data to update the document in couch
                if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                    data['state'] = 'failed'
                    data['end_time'] = now
                else:
		    data['state'] = 'retry'
                if self.failures_reasons.has_key(perm_lfn):
                    if self.failures_reasons[perm_lfn]:
                        data['failure_reason'] = self.failures_reasons[perm_lfn]
                    else:
                        data['failure_reason'] = "User Proxy has expired."
                else:
                    data['failure_reason'] = "Site config problem."

                data['last_update'] = last_update
                data['retry'] = now
                # Update the document in couch
                self.logger.debug("Marking failed %s" % docId)
                try:
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                    updateUri += "?" + urllib.urlencode(data)
                    self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
                except Exception, ex:
                    msg =  "Error in updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    lfn_to_update.append(temp_lfn)
                    continue
                try:
                    self.db.commit()
                except Exception, ex:
                    msg =  "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    lfn_to_update.append(temp_lfn)
                    continue
            if self.lfn_to_pfn_mapping.has_key(temp_lfn):
                pfn = self.lfn_to_pfn_mapping[temp_lfn]
                del self.lfn_to_pfn_mapping[temp_lfn]
                if self.pfn_to_lfn_mapping.has_key(pfn):
                    del self.pfn_to_lfn_mapping[pfn]
        self.logger.debug("failed file updated")
        return lfn_to_update

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

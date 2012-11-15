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


        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        self.max_retry = config.max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.transfer_script = getattr(self.config, 'transfer_script', 'ftscp')
        self.cleanEnvironment = ''
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        # TODO: improve how the worker gets a log

        query = {'group': True,
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}
        self.userDN = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows'][0]['key'][5]

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
            self.logger.error('Did not get valid proxy. Setting proxy to host cert')
            self.userProxy = config.serviceCert

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)
        self.failures_reasons =  {}

    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """

        jobs = self.files_for_transfer()

        transferred, failed, logfile1 = self.command()

        self.mark_failed( failed, logfile1 )
        self.mark_good( transferred, logfile1 )

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

                    command = '%s export X509_USER_PROXY=%s ; source %s ; lcg-del -lv %s'  % \
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

                            rm_logfile = open('%s/%s_%s.srmrm.log' % ( self.log_dir, to_clean_dict[ task ], str(time.time()) ), 'w')
                            commandRm = '%s export X509_USER_PROXY=%s ; source %s ; srmrm %s'  %\
                                        (self.cleanEnvironment, self.userProxy, self.uiSetupScript, destination_pfn )
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
        sites = self.db.loadView('AsyncTransfer', 'ftscp', query)

        def keys_map(dict):
            return dict['key'][4], dict['key'][3]

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
                     'key':[self.user, self.group, self.role, destination, source, self.userDN]}

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
        self.logger.debug('file to split %s' %file)
        try:
            site, lfn = tuple(file.split(':'))
        except Exception, e:
            self.logger.error('it does not seem to be an lfn %s' %file.split(':'))
            return None

        pfn = self.tfc_map[site].matchLFN('srmv2', lfn)

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

        failed_to_clean = {}
        ftslog_file = None

        #Loop through all the jobs for the links we have
        for link, copyjob in jobs.items():

            self.logger.debug("Valid %s" %self.validate_copyjob(copyjob) )
            # Validate copyjob file before doing anything
            if not self.validate_copyjob(copyjob): continue

            # Clean cruft files from previous transfer attempts
            # TODO: check that the job has a retry count > 0 and only delete files if that is the case
            to_clean = {}
            to_clean[ tuple( copyjob ) ] = link[1]
            self.cleanSpace( to_clean )

            tmp_copyjob_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_copyjob_file.write('\n'.join(copyjob))
            tmp_copyjob_file.close()

            fts_server_for_transfer = getFTServer(link[1], 'getRunningFTSserver', self.db, self.logger)

            ftslog_file = open('%s/%s-%s_%s.ftslog' % ( self.log_dir, link[0], link[1], str(time.time()) ), 'w')

            self.logger.debug("Running FTSCP command")
            self.logger.debug("FTS server: %s" % fts_server_for_transfer)
            self.logger.debug("link: %s -> %s" % link)
            self.logger.debug("copyjob file: %s" % tmp_copyjob_file.name)
            self.logger.debug("log file: %s" % ftslog_file.name)

            #TODO: Sending stdin/stdout/stderr can cause locks - should write to logfiles instead
            proc = subprocess.Popen(
                            ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                            stdout=ftslog_file,
                            stderr=ftslog_file,
                            stdin=subprocess.PIPE,
                        )

            command = '%s export X509_USER_PROXY=%s ; source %s ; %s -copyjobfile=%s -server=%s -mode=single' % (
                             self.cleanEnvironment,
                             self.userProxy,
                             self.uiSetupScript,
                             self.transfer_script,
                             tmp_copyjob_file.name,
                             fts_server_for_transfer )
            self.logger.debug("executing command: %s" % command)
            proc.stdin.write(command)

            stdout, stderr = proc.communicate()
            rc = proc.returncode
            ftslog_file.close()

            # now populate results by parsing the copy job's log file.
            # results is a tuple of lists, 1st list is transferred files, second is failed
            results = self.parse_ftscp_results(ftslog_file.name, link[0])
            # Removing lfn from the source if the transfer succeeded else from the destination
            transferred_files.extend( results[0] )
            failed_files.extend( results[1] )
            self.logger.debug("transferred : %s" % transferred_files)
            self.logger.info("failed : %s" % failed_files)


            # Clean up the temp copy job file
            os.unlink( tmp_copyjob_file.name )

            # The ftscp log file is left for operators to clean up, as per PhEDEx
            # TODO: recover from restarts by examining the log files, this would mean moving
            # the ftscp log files to a done folder once parsed, and parsing all files in some
            # work directory.

        if ftslog_file:
            return transferred_files, failed_files, ftslog_file.name
        else:
            return transferred_files, failed_files, ""
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

                if line.split(':')[0].strip() == 'Reason':
                    self.failures_reasons[lfn] = line.split('Reason:')[1:][0].strip()

            except IndexError, ex:

                self.logger.debug("wrong log file! %s" %ex)
                pass

        ftscp_file.close()

        return (transferred_files, failed_files)

    def mark_good(self, files=[], good_logfile=None):
        """
        Mark the list of files as tranferred
        """
        now = str(datetime.datetime.now())
        last_update = int(time.time())

        for lfn in files:

            try:
                document = self.db.document( getHashLfn(lfn) )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            to_attach = file(good_logfile)
            content = to_attach.read(-1)
            retval = self.db.addAttachment( document["_id"], document["_rev"], content, to_attach.name.split('/')[ len(to_attach.name.split('/')) - 1 ], "text/plain" )

            if retval.get('ok', False) != True:
                # Then we have a problem
                msg = "Adding an attachment to document failed\n"
                msg += str(retval)
                msg += "ID: %s, Rev: %s" % (document["_id"], document["_rev"])
                self.logger.error(msg)

            outputLfn = document['lfn'].replace('store/temp', 'store', 1)

            try:
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

            outputPfn = self.apply_tfc_to_lfn( '%s:%s' % ( document['destination'], outputLfn ) )
            pluginSource = self.factory.loadObject(self.config.pluginName, args = [self.config, self.logger], listFlag = True)
            pluginSource.updateSource({ 'jobid':document['jobid'], 'timestamp':document['job_end_time'], \
                                        'lfn': outputLfn, 'location': document['destination'], 'pfn': outputPfn, 'checksums': document['checksums'] })

        try:
            self.db.commit()
        except Exception, ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

    def mark_failed(self, files=[], bad_logfile=None, force_fail = False ):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        last_update = int(time.time())

        for lfn in files:

            data = {}
            docId = getHashLfn(lfn)

            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            to_attach = file(bad_logfile)
            content = to_attach.read(-1)
            retval = self.db.addAttachment( document["_id"], document["_rev"], content, to_attach.name.split('/')[ len(to_attach.name.split('/')) - 1 ], "text/plain" )

            if retval.get('ok', False) != True:
                # Then we have a problem
                msg = "Adding an attachment to document failed\n"
                msg += str(retval)
                msg += "ID: %s, Rev: %s" % (document["_id"], document["_rev"])
                self.logger.error(msg)

            # Prepare data to update the document in couch
            if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                data['state'] = 'failed'
                data['failure_reason'] = self.failures_reasons[lfn]
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

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')


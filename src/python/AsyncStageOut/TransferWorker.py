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
import subprocess, os
import datetime
import traceback
from WMCore.WMFactory import WMFactory
import urllib
import re
from AsyncStageOut import getProxy
from AsyncStageOut import getHashLfn
from AsyncStageOut import getFTServer
from AsyncStageOut import getDNFromUserName
import json
from time import strftime
import StringIO
from WMCore.Services.pycurl_manager import RequestHandler

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
        self.dropbox_dir = '%s/dropbox/outputs' % self.config.componentDir
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker-%s' % self.user)
        self.pfn_to_lfn_mapping = {}
        self.max_retry = config.max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.submission_command = getattr(self.config, 'submission_command', 'glite-transfer-submit')
        self.cleanEnvironment = ''
        self.userDN = ''
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        self.logger.debug("Trying to get DN for %s" % self.user)
        try:
            self.userDN = getDNFromUserName(self.user, self.logger, ckey = self.config.opsProxy, cert = self.config.opsProxy)
        except Exception, ex:
            msg = "Error retrieving the user DN"
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
        self.valid_proxy = False
        try:
            self.valid_proxy, proxy = getProxy(self.userDN, self.group, self.role, defaultDelegation, self.logger)
            self.Uproxy=proxy;
        except Exception, ex:
            msg = "Error getting the user proxy"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
        if self.valid_proxy:
            self.userProxy = proxy
        else:
            self.logger.error('Did not get valid proxy. Setting proxy to ops proxy to contact the DB')
            self.userProxy = config.opsProxy
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)
        self.commandTimeout = 1200
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.fts_server_for_transfer = getFTServer("T1_UK_RAL", 'getRunningFTSserver', self.config_db, self.logger)

    def __call__(self):
        """
        a. makes the ftscp copyjob
        b. submits ftscp
        c. deletes successfully transferred files from the DB
        """
        fts_url_delegation = self.fts_server_for_transfer.replace('8446','8443')
        command = 'export X509_USER_PROXY=%s ; source %s ; %s -s %s' % (self.Uproxy, self.uiSetupScript,
                                                                        'glite-delegation-init', fts_url_delegation)
        init_time = str(strftime("%a, %d %b %Y %H:%M:%S", time.localtime()))
        self.logger.debug("executing command: %s at: %s for: %s" % (command, init_time, self.userDN))
        stdout, rc = execute_command(command, self.logger, self.commandTimeout)
        if not rc or not self.valid_proxy:
          jobs, jobs_lfn, jobs_pfn, jobs_report = self.files_for_transfer()
          self.logger.debug("Processing files for %s " %self.userProxy )
          if jobs:
             self.command(jobs, jobs_lfn, jobs_pfn, jobs_report)
        else:
          self.logger.debug("User proxy of %s could not be delagated! Trying next time." % self.user)
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

    def files_for_transfer(self):
        """
        Process a queue of work per transfer source:destination for a user. Return one
        ftscp copyjob per source:destination.
        """
        source_dests = self.source_destinations_by_user()
        jobs = {}
        jobs_lfn = {}
        jobs_pfn = {}
        jobs_report = {}
        failed_files = []
        self.logger.info('%s has %s links to transfer on: %s' % (self.user, len(source_dests), str(source_dests)))
        try:
            for (source, destination) in source_dests:
                # We could push applying the TFC into the list function, not sure if
                # this would be faster, but might use up less memory. Probably more
                # complicated, though.
                query = {'reduce':False,
                     'limit': self.config.max_files_per_transfer,
                     'key':[self.user, self.group, self.role, destination, source],
                     'stale': 'ok'}
                     # TODO: Set stale to ok since the results has been got in the last call.
                try:
                    active_files = self.db.loadView('AsyncTransfer', 'ftscp_all', query)['rows']
                except:
                    continue
                self.logger.debug('%s has %s files to transfer from %s to %s' % (self.user,
                                                                                 len(active_files),
                                                                                 source,
                                                                                  destination))
                new_job = []
                lfn_list = []
                pfn_list = []
                dash_report = []

                # take these active files and make a copyjob entry
                def tfc_map(item):
                    self.logger.debug('Preparing PFNs...')
                    source_pfn = self.apply_tfc_to_lfn('%s:%s' % (source, item['value']))
                    destination_pfn = ""
                    if item['value'].startswith("/store/temp/user"):
                        destination_pfn = self.apply_tfc_to_lfn('%s:%s' % (destination,
                                                                           item['value'].replace('store/temp', 'store', 1).replace(\
                                                                           '.' + item['value'].split('.', 1)[1].split('/', 1)[0], '', 1)))
                    else:
                        destination_pfn = self.apply_tfc_to_lfn('%s:%s' % (destination,
                                                                           item['value'].replace('store/temp', 'store', 1)) )
                    self.logger.debug('PFNs prepared...')
                    if source_pfn and destination_pfn and self.valid_proxy:
                        acquired_file, dashboard_report = self.mark_acquired([item])
                        self.logger.debug('Files have been marked acquired')
                        if acquired_file:
                            self.logger.debug('Starting FTS Job creation...')
                            # Prepare Monitor metadata
                            lfn_list.append(item['value'])
                            pfn_list.append(source_pfn)
                            # Prepare FTS Dashboard metadata
                            dash_report.append(dashboard_report)
                            new_job.append('%s %s' % (source_pfn, destination_pfn))
                            self.logger.debug('FTS job created...')
                        else:
                            pass
                    else:
                        self.mark_failed([item])
                self.logger.debug('Preparing job...')
                map(tfc_map, active_files)
                self.logger.debug('Job prepared...')
                if new_job:
                    jobs[(source, destination)] = new_job
                    jobs_lfn[(source, destination)] = lfn_list
                    jobs_pfn[(source, destination)] = pfn_list
                    jobs_report[(source, destination)] = dash_report
                    self.logger.debug('FTS job ready for submission over  %s ---> %s ...going to next job' % (source, destination) )

            self.logger.debug('ftscp input created for %s (%s jobs)' % (self.user, len(jobs.keys())))
            return jobs, jobs_lfn, jobs_pfn, jobs_report
        except:
            self.logger.exception("fail")
            return jobs, jobs_lfn, jobs_pfn, jobs_report

    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        try:
            site, lfn = tuple(file.split(':'))
        except:
            self.logger.error('it does not seem to be an lfn %s' %file.split(':'))
            return None
        if self.tfc_map.has_key(site):
            pfn = self.tfc_map[site].matchLFN('srmv2', lfn)
            # TODO: improve fix for wrong tfc on sites
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
            if not self.pfn_to_lfn_mapping.has_key(pfn):
                self.pfn_to_lfn_mapping[pfn] = lfn
            return pfn
        else:
            self.logger.error('Wrong site %s!' % site)
            return None

    def command(self, jobs, jobs_lfn, jobs_pfn, jobs_report):
        """
        For each job the worker has to complete:
        Delete files that have failed previously
        Create a temporary copyjob file
        Submit the copyjob to the appropriate FTS server
        Parse the output of the FTS transfer and return complete and failed files for recording
        """
        # Output: {"userProxyPath":"/path/to/proxy","LFNs":["lfn1","lfn2","lfn3"],"PFNs":["pfn1","pfn2","pfn3"],"FTSJobid":'id-of-fts-job', "username": 'username'}
        #Loop through all the jobs for the links we have
        for link, copyjob in jobs.items():
            submission_error = False
            status_error = False
            fts_job = {}
            # Validate copyjob file before doing anything
            self.logger.debug("Valid %s" % self.validate_copyjob(copyjob) )
            if not self.validate_copyjob(copyjob): continue
            rest_copyjob = '{"params":{"bring_online":null,"verify_checksum":false,"reuse":false,"copy_pin_lifetime":-1,"job_metadata":{"issuer": "ASO"},"spacetoken":null,"source_spacetoken":null,"fail_nearline":false,"overwrite":true,"gridftp":null},"files":['
            pairs = []
            for SrcDest in copyjob:
                pairs.append('{"sources":["' + SrcDest.split(" ")[0] + '"],"metadata":null,"destinations":["' + SrcDest.split(" ")[1] + '"]}')
            rest_copyjob += (','.join(pairs)) +']}'
            self.logger.debug("Subbmitting this REST copyjob %s" % rest_copyjob)
            post = urllib.quote(rest_copyjob)
            url = self.fts_server_for_transfer + '/jobs'
            self.logger.debug("Running FTS submission command")
            self.logger.debug("FTS server: %s" % self.fts_server_for_transfer)
            self.logger.debug("link: %s -> %s" % link)
            heade = {"Content-Type ":"application/json"}
            buf = StringIO.StringIO()
            try:
                connection = RequestHandler(config={'timeout': 300, 'connecttimeout' : 300})
            except Exception, ex:
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.debug(msg)
            try:
                response, datares = connection.request(url, post, heade, verb='POST', doseq=True, ckey=self.userProxy, \
                                                       cert=self.userProxy, capath='/etc/grid-security/certificates', \
                                                       cainfo=self.userProxy, verbose=True)
                self.logger.debug("Submission done")
                self.logger.debug('Submission header status: %s' % response.status)
                self.logger.debug('Submission header reason: %s' % response.reason)
            except Exception, ex:
                msg = "Error submitting to FTS3 REST interface: %s " % url
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.debug(msg)
                submission_error = True
            buf.close()
            if not submission_error:
                res = {}
                try:
                    self.logger.debug('Submission result %s' %  datares)
                    res = json.loads(datares)
                    res.has_key('job_id')
                except Exception, ex:
                    msg = "Couldn't load json"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.debug(msg)
                    status_error = True
                if res.has_key('job_id'):
                    fileId_list = []
                    job_id = res['job_id']
                    file_url = self.fts_server_for_transfer + '/jobs/' + job_id +'/files'
                    self.logger.debug("Submitting to %s" % file_url)
                    file_buf = StringIO.StringIO()
                    try:
                        response, files_ = connection.request(file_url, {}, heade, doseq=True, ckey=self.userProxy, \
                                                              cert=self.userProxy, capath='/etc/grid-security/certificates', \
                                                              cainfo=self.userProxy, verbose=True)
                    except Exception, ex:
                        msg = "Error retrieveing files from FTS3 REST interface: %s " % file_url
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.debug(msg)
                        status_error = True
                    files_res = []
                    fileId_list = []
                    self.logger.debug("List files in job %s" % files_)
                    file_buf.close()
                    try:
                        files_res = json.loads(files_)
                    except Exception, ex:
                        msg = "Couldn't load files in job json "
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.debug(msg)
                        status_error = True
                    for file_in_job in files_res:
                        if file_in_job.has_key('file_id'):
                            fileId_list.append(file_in_job['file_id'])
                        else:
                            self.logger.debug("Job id could not be retrieved")
                            status_error = True
                    self.logger.debug("File id list %s" % fileId_list)
            if status_error or submission_error:
                self.logger.debug("Submission failed")
                self.logger.info("Mark failed %s files" % len(jobs_lfn[link]))
                self.logger.debug("Mark failed %s files" % jobs_lfn[link])
                failed_files = self.mark_failed(jobs_lfn[link], force_fail = False, submission_error=True)
                self.logger.info("Marked failed %s" % len(failed_files))
                continue
            fts_job['userProxyPath'] = self.userProxy
            fts_job['LFNs'] = jobs_lfn[link]
            fts_job['PFNs'] = jobs_pfn[link]
            fts_job['FTSJobid'] = job_id
            fts_job['files_id'] = fileId_list
            fts_job['username'] = self.user
            self.logger.debug("Creating json file %s in %s" % (fts_job, self.dropbox_dir))
            ftsjob_file = open('%s/Monitor.%s.json' % (self.dropbox_dir, fts_job['FTSJobid'] ), 'w')
            jsondata = json.dumps(fts_job)
            ftsjob_file.write(jsondata)
            ftsjob_file.close()
            self.logger.debug("%s ready." % fts_job)
            # Prepare Dashboard report
            for lfn in fts_job['LFNs']:
                lfn_report = {}
                lfn_report['FTSJobid'] = fts_job['FTSJobid']
                index = fts_job['LFNs'].index(lfn)
                lfn_report['PFN'] = fts_job['PFNs'][index]
                lfn_report['FTSFileid'] = fts_job['files_id'][index]
                lfn_report['Workflow'] = jobs_report[link][index][2]
                lfn_report['JobVersion'] = jobs_report[link][index][1]
                job_id = '%d_https://glidein.cern.ch/%d/%s_%s' % (int(jobs_report[link][index][0]), int(jobs_report[link][index][0]), lfn_report['Workflow'].replace("_", ":"), lfn_report['JobVersion'])
                lfn_report['JobId'] = job_id
                lfn_report['URL'] = self.fts_server_for_transfer
                self.logger.debug("Creating json file %s in %s for FTS3 Dashboard" % (lfn_report, self.dropbox_dir))
                dash_job_file = open('/tmp/Dashboard.%s.json' % getHashLfn(lfn_report['PFN']) , 'w')
                jsondata = json.dumps(lfn_report)
                dash_job_file.write(jsondata)
                dash_job_file.close()
                self.logger.debug("%s ready for FTS Dashboard report." % lfn_report)
        return

    def validate_copyjob(self, copyjob):
        """
        the copyjob file is valid when source pfn and destination pfn are not None.
        """
        for task in copyjob:
            if task.split()[0] == 'None' or task.split()[1] == 'None': return False
        return True

    def mark_acquired(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        dash_rep = ()
        for lfn in files:
            if lfn['value'].find('temp') == 7:
                docId = getHashLfn(lfn['value'])
                self.logger.debug("Marking acquired %s" % docId)
                # Load document to get the retry_count
                try:
                    document = self.db.document( docId )
                except Exception, ex:
                    msg = "Error loading document from couch"
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
                    self.logger.debug("Marked acquired %s of %s" % (docId, lfn))
                    lfn_in_transfer.append(lfn)
                    dash_rep = (document['jobid'], document['job_retry_count'], document['workflow'])
                else:
                    continue
            else:
                good_lfn = lfn['value'].replace('store', 'store/temp', 1)
                self.mark_good([good_lfn])
        return lfn_in_transfer, dash_rep

    def mark_good(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        for lfn in files:
            try:
                document = self.db.document( getHashLfn(lfn) )
            except Exception, ex:
                msg = "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if document['state'] != 'killed' and document['state'] != 'done' and document['state'] != 'failed':
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
                    msg = "Error updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                try:
                    self.db.commit()
                except Exception, ex:
                    msg = "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
        self.logger.debug("transferred file updated")

    def mark_failed(self, files=[], force_fail = False, submission_error = False):
        """
        Something failed for these files so increment the retry count
        """
        updated_lfn = []
        for lfn in files:
            data = {}
            if not isinstance(lfn, dict):
                if 'temp' not in lfn:
                    temp_lfn = lfn.replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn
            else:
                if 'temp' not in lfn['value']:
                    temp_lfn = lfn['value'].replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn['value']

            docId = getHashLfn(temp_lfn)

            # Load document to get the retry_count
            try:
                document = self.db.document( docId )
            except Exception, ex:
                msg = "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if document['state'] != 'killed' and document['state'] != 'done' and document['state'] != 'failed':
                now = str(datetime.datetime.now())
                last_update = time.time()
                # Prepare data to update the document in couch
                if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                    data['state'] = 'failed'
                else:
                    data['state'] = 'retry'
                if submission_error:
                    data['failure_reason'] = "Job could not be submitted to FTS: temporary problem of FTS"
                elif not self.valid_proxy:
                    data['failure_reason'] = "Job could not be submitted to FTS: user's proxy expired"
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
                    updated_lfn.append(docId)
                    self.logger.debug("Marked failed %s" % docId)
                except Exception, ex:
                    msg = "Error in updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                try:
                    self.db.commit()
                except Exception, ex:
                    msg = "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
        self.logger.debug("failed file updated")
        return updated_lfn

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')

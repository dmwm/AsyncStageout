# pylint: disable-msg=C0103
"""
The CMS Component in Panda does the following:
        a. parse job and output files metadata
        b. upload files metadata in cache for publication report
        c. inject files into ASO.
This should be called once the analysis job is done/failed.
"""
try:
    from AdderPluginBase import AdderPluginBase
except:
    # The below fake class is meant for allowing reuse in non-PanDA
    # environments
    class AdderPluginBaseFake:
        def __init__(self, a, b):
            pass

    AdderPluginBase = AdderPluginBaseFake
try:
    # This seems to be what is present on the PanDA server
    from CouchAPI.CMSCouch import CouchServer
except ImportError:
    # This seems to be the proper module from WMCore
    from WMCore.Database.CMSCouch import CouchServer
import time, datetime, traceback, logging
import hashlib
import simplejson as json
import subprocess, os

def getHashLfn(lfn):
    """
    Provide a hashed lfn from an lfn.
    """
    return hashlib.sha224(lfn).hexdigest()

config = None

class AdderCmsPlugin(AdderPluginBase):
    """
    _AdderCmsPlugin_
    Parse jobs and files metadata for the later transfer and publication.
    """
    def __init__(self, job, **params):
        """
        Get the config params and define the logger attribute.
	"""
        AdderPluginBase.__init__(self, job, params)
        # Load and parse auth file
        aso_auth_file = '/data/atlpan/srv/etc/panda/adder_secret.config'
        if config:
            aso_auth_file = getattr(config.Adder, "authfile", "adder_secret.config")
        f = open(aso_auth_file)
        authParams = json.loads(f.read())
        self.proxy = authParams['PROXY']
        self.aso_db_url = authParams['ASO_DB_URL']
        self.aso_cache = authParams['FWJR_CACHE']
        server = CouchServer(dburl = self.aso_db_url, ckey = self.proxy, cert = self.proxy)
        self.db = server.connectDatabase("asynctransfer")
        self.mon_db = server.connectDatabase("user_monitoring_asynctransfer")
        self.initlog()
        self.logger.info("ASO plugin starts")

    def initlog(self):
        """
        Setup the logger according to the config
        """
        adder_plugin_log = '/data/atlpan/srv/var/log/panda/ASOPlugin.log'
        if config:
            adder_plugin_log = getattr(config.Adder, "logfile", "./ASOPlugin.log")
        # Define the logger attribute
        self.logger = logging.getLogger('ASOPlugin')
        hdlr = logging.FileHandler(adder_plugin_log)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        # TODO: Get the log verbosity from a config file
        self.logger.setLevel(logging.DEBUG)

    def execute(self):
        """
        Execute the Adder algo.
        """
        # do nothing in plugin for transferring jobs
        if self.job.jobStatus == 'transferring':
            self.result.setSucceeded()
            return
        elif self.job.jobStatus == 'failed':
            self.logger.debug("Skipping failed jobs")
            # TODO:Evaluate what we want to do with failed jobs: inject only the log file?
        elif self.job.transExitCode != '0':
            self.logger.debug("Skipping finished jobs with EXIT CODE %s" %self.job.transExitCode)
        else:
            # inject the metada for data report and publication
            report = None
            if self.job.metadata != 'NULL':
                report = json.loads(self.job.metadata)
            # init execution variables
            last_update = int(time.time())
            now = str(datetime.datetime.now())
            doc = {}
            workflow = ''
            state = 'new'
            end_time = ""
            save_logs = False
            job_doc = {}
            # *** file info
            for tmpFile in self.job.Files:
                out_type = ''
                destination = tmpFile.destinationSE
                lfn = tmpFile.lfn
                # Get the saveLogs option
                try:
                    saveLogs = self.job.jobParameters.split("saveLogs")[1].split("=")[1].split(" ")[0]
                    if tmpFile.type == 'log':
                        if saveLogs == 'True':
                            job_doc = { "_id": str(self.job.PandaID),
                                        "files": len(self.job.Files)
                                      }
                            save_logs = True
                        else:
                            job_doc = { "_id": str(self.job.PandaID),
                                        "files": len(self.job.Files) - 1,
                                        "log_file": lfn
                                      }
                except:
                    if tmpFile.type == 'log':
                        self.logger.debug("Cannot retrieve saveLogs option. Set to False!")
                        job_doc = { "_id": str(self.job.PandaID),
                                    "files": len(self.job.Files) - 1,
                                    "log_file": lfn
                                  }
                if len(lfn.split('/')) < 2:
                    self.logger.debug("Fallback to destinationDBlock to get username!")
                    user_dataset = str(self.job.destinationDBlock)
                    user = user_dataset.split('/')[2].split('-')[0]
                    self.logger.debug("Username %s got from destinationDBlock!" %user)
                else:
                    if tmpFile.lfn.split('/')[2] == 'temp':
                        user = tmpFile.lfn.split('/')[4]
                    else:
                        user = tmpFile.lfn.split('/')[3]
                        destination = self.job.computingSite.split("ANALY_")[-1]
                        state = "done"
                        end_time = now
                workflow = self.job.jobName
                # Build correctly the user dn
                dn = self.job.prodUserID
                if dn:
                    try:
                        if dn.split("/")[-1].split("=")[1] == 'proxy': dn = dn.replace("/CN=proxy","")
                    except:
                        pass
                # Get role and group
                try:
                    voGroup = self.job.jobParameters.split("group")[1].split("=")[1].split(" ")[0]
                except:
                    self.logger.debug("Cannot retrieve user vo group")
                try:
                    voRole = self.job.jobParameters.split("role")[1].split("=")[1].split(" ")[0]
                except:
                    self.logger.debug("Cannot retrieve user vo role")

                # Get the dbs_url params
                try:
                    publish_dbs_url = self.job.jobParameters.split("publish_dbs_url")[1].split("=")[1].split(" ")[0]
                except:
                    self.logger.debug("Cannot retrieve publish_dbs_url. Set to default!")
                    publish_dbs_url = "https://cmsdbsprod.cern.ch:8443/cms_dbs_caf_analysis_02_writer/servlet/DBSServlet"
                try:
                    dbs_url = self.job.jobParameters.split("dbs_url")[1].split("=")[1].split(" ")[0]
                except:
                    self.logger.debug("Cannot retrieve publish_dbs_url. Set to default!")
                    dbs_url = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global/servlet/DBSServlet"
                # Get the publication option
                try:
                    publishFiles = self.job.jobParameters.split("publishFiles")[1].split("=")[1].split(" ")[0]
                    if publishFiles == 'True':
                        publish = 1
                    else:
                        publish = 0
                except:
                    self.logger.debug("Cannot retrieve publishFiles option. Set to False!")
                    publish = 0
                # Prepare the file document and queue it
                doc = { "_id": getHashLfn( lfn ),
                        "inputdataset": self.job.prodDBlock,
                        "group": voGroup,
                        "lfn": lfn,
                        "checksums": {'adler32': tmpFile.checksum},
                        "size": tmpFile.fsize,
                        "user": user,
                        "source": self.job.computingSite.split("ANALY_")[-1],
                        "destination": destination,
                        "last_update": last_update,
                        "state": state,
                        "role": voRole,
                        "dbSource_url": "Panda",
                        "publish_dbs_url": publish_dbs_url,
                        "dbs_url": dbs_url,
                        "dn": dn,
                        "workflow": workflow,
                        "start_time": now,
                        "end_time": end_time,
                        "job_end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                        "jobid": self.job.PandaID,
                        "retry_count": [],
                        "failure_reason": [],
                        "publication_state": 'not_published',
                        "publication_retry_count": [],
                        "type" : tmpFile.type,
                        "publish" : publish
                     }
                try:
                    user_dataset = str(self.job.destinationDBlock)
                    hash_config = user_dataset.split('/')[2].split('-', 1)[1]
                except:
                    hash_config = 'publishname-psethash'
                params = self.job.jobParameters.split(" ")
                cmssw_version = ""
                for elt in params:
                    if elt.find('cmssw') > 1:
                        cmssw_version = elt.split("=")[1]
                if report:
                    # TODO: prepare report in a dedicated function
                    if tmpFile.type == 'output':
                        for OutModule in report['steps']['cmsRun']['output']:
                            temp_out = {}
                            for out in report['steps']['cmsRun']['output'][OutModule]:
                                str_report = ""
                                if out.has_key('ouput_module_class'):
                                    # Retrieve info. only for the actual EDM file
                                    if lfn.find(out['pfn'].split('.root')[0]) == -1:
                                        continue
                                    if out['ouput_module_class'] == 'PoolOutputModule':
                                        out_type = 'EDM'
                                        temp_out['runs'] = out['runs']
                                        first = True
                                        runs_report = ''
                                        lumis_report = ''
                                        for run, value in temp_out['runs'].iteritems() :
                                            for lumi in temp_out['runs'][run]:
                                                runs_report = runs_report + "&outfileruns=" + str(run)
                                                if first:
                                                    lumis_report = "outfilelumis="  + str(lumi)
                                                    first = False
                                                else:
                                                    lumis_report = lumis_report + "&outfilelumis=" + str(lumi)
                                        str_report = lumis_report + runs_report
                                        if out.has_key('input'):
                                            temp_out['in_parentlfns'] = out['input']
                                            for parent_lfn in temp_out['in_parentlfns']:
                                                str_report = str_report + "&inparentlfns=" + str(parent_lfn)
                                        temp_out['events'] = out['events']
                                        str_report = str_report + "&globalTag=None&events=" + str(temp_out['events'])
                                if out.has_key('Source'):
                                    # Retrieve info. only for the actual TFILE
                                    tfile_out_name = out['fileName'].split('/')[(len(out['fileName'].split('/'))-1)].split('.')[0]
                                    out_name = lfn.split('/')[(len(lfn.split('/'))-1)].split('_')[0]
                                    if tfile_out_name != out_name:
                                        continue
                                    out_type = 'TFILE'
                                    doc['publish'] = 0
                                if str_report:
                                    # TODO: Fix checksummd5 and checksumcksum retrieval
                                    str_report = str_report + "&pandajobid=" + str(doc['jobid']) + "&outsize=" + \
                                                 str(doc['size']) + "&publishdataname=" + hash_config + \
                                                 "&appver=" + cmssw_version + "&outtype=" + out_type + \
                                                 "&checksummd5=asda&checksumcksum=3701783610&outlocation=" + \
                                                 str(destination) + "&taskname=" + str(workflow) + "&outdatasetname=" + \
                                                 str(self.job.destinationDBlock) + "&outlfn=" + \
                                                 str(doc['lfn']) + "&checksumadler32=" + \
                                                 str(doc['checksums']['adler32']).split(":")[-1] + "&acquisitionera=null" + \
                                                 "&outtmplocation=" + str(self.job.computingSite.split("ANALY_")[-1])
                                else:
                                    # TODO: Fix checksummd5 and checksumcksum retrieval
                                    str_report = "pandajobid=" + str(doc['jobid']) + "&outsize=" + str(doc['size']) + \
                                                 "&events=0" + "&publishdataname=" + hash_config + "&appver=" + cmssw_version + \
                                                 "&outtype=" + out_type + \
                                                 "&checksummd5=asda&checksumcksum=3701783610&outlocation=" + \
                                                 str(destination) + "&taskname=" + str(workflow) + "&outdatasetname=" + \
                                                 str(self.job.destinationDBlock) + "&outlfn=" + str(doc['lfn']) + \
                                                 "&checksumadler32=" + str(doc['checksums']['adler32']).split(":")[1] + \
                                                 "&acquisitionera=null" + "&outtmplocation=" + \
                                                 str(self.job.computingSite.split("ANALY_")[-1])
                                # Uploading here
                                if config and getattr(config.Adder, "skipcache", False):
                                    # Allow us to skip CS integration for the sake of testing the ASO server only.
                                    # This option is not meant to be used in production.
                                    rc = 0
                                else:
                                    command = "curl7280 -X PUT " + self.aso_cache + \
                                              " --key " + self.proxy + " --cert " + \
                                              self.proxy + " -k -d " + "\"" + str_report + "\"" + " -v"
                                    proc = subprocess.Popen(command, shell=True, cwd=os.environ['PWD'],
                                                            stdout=subprocess.PIPE,
                                                            stderr=subprocess.PIPE,
                                                            stdin=subprocess.PIPE,)
                                    for line in iter(proc.stdout.readline,''):
                                        if "Service Unavailable" in line:
                                            self.logger.debug("Cache Service %s is temporary unavailable" %self.aso_cache)
                                            self.result.statusCode = 1
                                            return self.result
                                    stdout, stderr = proc.communicate()
                                    rc = proc.returncode
                                    if rc:
                                        self.logger.debug("Cache Service %s cannot be contacted trying next time" %self.aso_cache)
                                        self.result.statusCode = 1
                                        return self.result
                                    self.logger.debug("Upload Result %s, %s, %s" %(stdout, stderr, rc))
                    else:
                        self.logger.debug("LOG file...preparing the report")
                        out_type = 'LOG'
                        # TODO: Fix checksummd5 and checksumcksum retrieval
                        str_report = "pandajobid=" + str(doc['jobid']) + "&outsize=" + \
                                     str(doc['size']) + "&events=0" + "&publishdataname=" + \
                                     hash_config + "&appver=" + cmssw_version + "&outtype=" + \
                                     out_type + "&checksummd5=asda&checksumcksum=3701783610&outlocation=" + \
                                     str(destination) + "&taskname=" + str(workflow) + \
                                     "&outdatasetname=" + str(self.job.destinationDBlock) + \
                                     "&outlfn=" + str(doc['lfn']) + "&checksumadler32=" + \
                                     str(doc['checksums']['adler32']).split(":")[1] + "&acquisitionera=null" + \
                                     "&outtmplocation=" + str(self.job.computingSite.split("ANALY_")[-1])
                        # Uploading here
                        command = "curl7280 -X PUT " + self.aso_cache + " --key " + \
                                  self.proxy + " --cert " + self.proxy + " -k -d " + \
                                  "\"" + str_report + "\"" + " -v"
                        proc = subprocess.Popen(command, shell=True, cwd=os.environ['PWD'],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                stdin=subprocess.PIPE,)
                        for line in iter(proc.stdout.readline,''):
                            if "Service Unavailable" in line:
                                self.logger.debug("Cache Service %s is temporary unavailable" %self.aso_cache)
                                self.result.statusCode = 1
                                return self.result
                        stdout, stderr = proc.communicate()
                        rc = proc.returncode
                        if rc:
                            self.logger.debug("Cache Service %s cannot be contacted trying next time" %self.aso_cache)
                            self.result.statusCode = 1
                            return self.result
                        self.logger.debug("Upload Result %s, %s, %s" %(stdout, stderr, rc))
                self.logger.debug("Trying to commit %s" % doc)
                try:
                    if doc['type'] == 'log' and not save_logs:
                        continue
                    self.db.queue(doc, True)
                    self.db.commit()
                    # append LFN to the list of transferring files,
                    # which gets the job status to change to transferring
                    self.result.transferringFiles.append(lfn)
                except Exception, ex:
                    msg =  "Error queuing document in asyncdb"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    self.result.statusCode = 1
                    self.logger.info("ASOPlugin ends with error.")
                    return self.result
            if job_doc:
                try:
                    self.mon_db.queue(job_doc, True)
                    self.mon_db.commit()
                except Exception, ex:
                    msg =  "Error queuing doc in monitoring db"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    self.result.statusCode = 1
                    self.logger.info("ASOPlugin ends with error.")
                    return self.result

            # TODO: Fix bulk commit of documents
#            if doc:
#                try:
#                    self.db.commit()
#                except Exception, ex:
#                    msg =  "Error commiting documents in asyncdb"
#                    msg += str(ex)
#                    msg += str(traceback.format_exc())
#                    self.logger.error(msg)
#                    self.result.statusCode = 1
#                    return self.result

        # set 0 to self.result.statusCode when interactions succeeded
        # set 1 to self.result.statusCode when interactions failed due to a temporary error
        # set 2 to self.result.statusCode when interactions failed due to a fatal error
        self.result.statusCode = 0
        self.logger.info("ASOPlugin ends.")
        return self.result

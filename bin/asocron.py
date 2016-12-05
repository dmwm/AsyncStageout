import os
import sys
import socket
import json
import time
import pycurl
import urllib
import urllib2
import httplib
import logging
import datetime
import WMCore.Database.CMSCouch as CMSCouch

logging.basicConfig(filename='/home/crab3/asocron.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s: %(message)s')

 

def get_aso_Url():
    if socket.gethostname()=="vocms0108.cern.ch":
        logging.info("Updating views for vocms021 polls https://cmsweb.cern.ch/couchdb2/asodb2")
        return 'https://cmsweb.cern.ch/couchdb2','asodb2'
    elif socket.gethostname()=="vocms031.cern.ch":
        logging.info("Updating views for vocms031 polls https://cmsweb.cern.ch/couchdb2/asodb1")
        return 'https://cmsweb.cern.ch/couchdb2','asodb1'
    elif socket.gethostname()=="vocms030.cern.ch":
        logging.info("Updating views for vocms030 polls https://cmsweb-testbed.cern.ch/couchdb/asodb3")
        return 'https://cmsweb-testbed.cern.ch/couchdb','asodb3'
    else:
        logging.exception("Not a registered hostname")
        return -1


serverKey = "/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy"
serverCert = serverKey

asourl, asodb = get_aso_Url()

server = CMSCouch.CouchServer(dburl=asourl, ckey=serverKey, cert=serverCert)

try:
    db = server.connectDatabase(asodb)
except Exception:
    msg = "Error while connecting to asynctransfer CouchDB for workflow"
    msg += "\n asourl=%s" % (asourl)
    logging.exception(msg)


query = {'stale': 'update_after'}
#'AsyncTransfer', 'PublicationStateByWorkflow'
try:
    publicationList = db.loadView('AsyncTransfer', 'PublicationStateByWorkflow', query)['rows']
    logging.info("PublicationStateByWorkflow updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'Others', 'sites'
try:
    publicationList = db.loadView('Others', 'sites', query)['rows']
    logging.info("sites updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'AsyncTransfer', 'JobsIdsStatesByWorkflow'
try:
    publicationList = db.loadView('AsyncTransfer', 'JobsIdsStatesByWorkflow', query)['rows']
    logging.info("JobsIdsStatesByWorkflow updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'Acquired', 'get_acquired'
try:
    publicationList = db.loadView('Acquired', 'get_acquired', query)['rows']
    logging.info("get_acquired updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'Others', 'LFNSiteByLastUpdate'
try:
    publicationList = db.loadView('Others', 'LFNSiteByLastUpdate', query)['rows']
    logging.info("LFNSiteByLastUpdate updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'MonitorStartedEnded', 'startedSizeByTime'
try:
    publicationList = db.loadView('MonitorStartedEnded', 'startedSizeByTime', query)['rows']
    logging.info("startedSizeByTime updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'MonitorStartedEnded', 'endedSizeByTime'
try:
    publicationList = db.loadView('MonitorStartedEnded', 'endedSizeByTime', query)['rows']
    logging.info("endedSizeByTime updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)

#'monitor', 'publicationStateSizeByTime'
try:
    publicationList = db.loadView('monitor', 'publicationStateSizeByTime', query)['rows']
    logging.info("publicationStateSizeByTime updated")
except Exception:
    msg = "Error while querying CouchDB for publication status information for workflow "
    logging.exception(msg)
#'ftscp', 'ftscp_all'
try:
    publicationList = db.loadView('ftscp', 'ftscp_all', query)['rows']
    logging.info("ftscp_all updated")
except Exception:
    msg = "Error while querying CouchDB for ftscp_all"
    logging.exception(msg)

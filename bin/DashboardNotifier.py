#!/usr/bin/env python
from __future__ import print_function
from __future__ import division
import stomp
import json
import os
import sys
import datetime
import logging 
from multiprocessing import Process


def reportToAmq(filePath, log, connect):
    """
    """
    jsonData=open(filePath)
    message = json.load(jsonData)
    # point to new (as of 2017) FTS monitor
    message[u'URL']=u'https://fts3.cern.ch:8449/fts3/ftsmon/#/job/'
    log.debug("Producing...%s" % message)
    try:
        messageDict = json.dumps(message)
        connect.send(messageDict, destination=authParams['MSG_QUEUE'] )
    except Exception:
        log.exception("Error contacting Message Broker or reading json")
        sys.exit(1)

logging.basicConfig(filename='/data/srv/asyncstageout/current/config/log.config', level=logging.DEBUG)
amqAuthFile = "/data/srv/asyncstageout/current/config/asyncstageout/amq_auth_file.json"

try:
    f = open(amqAuthFile)
    authParams = json.loads(f.read())
    f.close()
except Exception:
    logging.exception("Error loading auth params")
    sys.exit(1)

try:
    host = [(authParams['MSG_HOST'], authParams['MSG_PORT'])]
    conn = stomp.Connection(host, authParams['MSG_USER'], authParams['MSG_PWD'])
    conn.start()
    conn.connect()
except Exception:
    logging.exception("Error contacting Message Broker")
    conn.disconnect()
    sys.exit(1)

for dashboardFile in os.listdir("/tmp/DashboardReport"):
    logging.debug(dashboardFile)
    filePath = '/tmp/DashboardReport/' + dashboardFile
    logging.debug(filePath)
    p = Process(target=reportToAmq, args=(filePath, logging, conn))
    p.start()
    p.join()
    logging.debug("Removing file at %s" % datetime.datetime.now())
    os.unlink( '/tmp/DashboardReport/' + dashboardFile )

conn.disconnect()

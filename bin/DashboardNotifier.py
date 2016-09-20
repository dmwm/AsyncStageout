#!/usr/bin/env python
from __future__ import print_function
from __future__ import division
import stomp
import json
import traceback
import os
import sys
import datetime
import logging 
from multiprocessing import Process

def reportToAmq(filePath, logging, conn):
    """
    """
    jsonData=open(filePath)
    message = json.load(jsonData)
    logging.debug("Producing...%s" % message)
    try:
        messageDict = json.dumps(message)
        conn.send(messageDict, destination=authParams['MSG_QUEUE'] )
    except Exception as ex:
        logging.exception("Error contacting Message Broker or reading json")
        sys.exit(1)

logging.basicConfig(filename='/data/srv/asyncstageout/current/config/log.config', level=logging.DEBUG)
amqAuthFile = "/data/srv/asyncstageout/current/config/asyncstageout/amq_auth_file.json"

try:
    f = open(amqAuthFile)
    authParams = json.loads(f.read())
    f.close()
except Exception as ex:
    logging.exception("Error loading auth params")
    sys.exit(1)

try:
    host = [(authParams['MSG_HOST'], authParams['MSG_PORT'])]
    conn = stomp.Connection(host, authParams['MSG_USER'], authParams['MSG_PWD'])
    conn.start()
    conn.connect()
except Exception as ex:
    logging.exception("Error contacting Message Broker")
    sys.exit(1)

for dashboardFile in os.listdir("/tmp"):
    logging.debug(dashboardFile)
    if os.path.basename(dashboardFile).endswith('json'):
        filePath = '/tmp/' + dashboardFile
        logging.debug(filePath)
        p = Process(target=reportToAmq, args=(filePath, logging, conn))
        p.start()
        p.join()
        logging.debug("Removing file at %s" % datetime.datetime.now())
        os.unlink( '/tmp/' + dashboardFile )

conn.disconnect()

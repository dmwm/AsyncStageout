#!/usr/bin/env python
from __future__ import print_function
from __future__ import division
import stomp
import json
import traceback
import os
import datetime
import logging 
from multiprocessing import Process

def produce(file_path, logging, conn):
    """
    """
    json_data=open(file_path)
    message = json.load(json_data)
    logging.debug("Producing...%s" % message)
    try:
        messageDict = json.dumps(message)
        conn.send(messageDict, destination=authParams['MSG_QUEUE'] )
    except Exception as ex:
        msg = "Error contacting Message Broker"
        msg += str(ex)
        msg += str(traceback.format_exc())
        logging.debug(msg)

logging.basicConfig(filename='/data/srv/asyncstageout/current/config/log', level=logging.DEBUG)
amq_auth_file = "/data/srv/asyncstageout/current/config/asyncstageout/amq_auth_file.json"
opened = False

try:
    f = open(amq_auth_file)
    authParams = json.loads(f.read())
    opened = True
    f.close()
except Exception as ex:
    msg = "Error loading auth params"
    msg += str(ex)
    msg += str(traceback.format_exc())
    logging.debug(msg)

try:
    host = [(authParams['MSG_HOST'], authParams['MSG_PORT'])]
    conn = stomp.Connection(host, authParams['MSG_USER'], authParams['MSG_PWD'])
    conn.start()
    conn.connect()
except Exception as ex:
    msg = "Error contacting Message Broker"
    msg += str(ex)
    msg += str(traceback.format_exc())
    logging.debug(msg)

for dashboard_file in os.listdir("/tmp"):
    logging.debug(dashboard_file)
    if os.path.basename(dashboard_file).endswith('json'):
        file_path = '/tmp/' + dashboard_file
        logging.debug(file_path)
        p = Process(target=produce, args=(file_path, logging, conn))
        p.start()
        p.join()
        logging.debug("Removing file at %s" % datetime.datetime.now())
        os.unlink( '/tmp/' + dashboard_file )

conn.disconnect()

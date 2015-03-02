import hashlib
import subprocess
import os
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Credential.Proxy import Proxy

__version__ = '1.0.3'

def getHashLfn(lfn):
    """
    Provide a hashed lfn from an lfn.
    """
    return hashlib.sha224(lfn).hexdigest()

def getFTServer(site, view, db, log):
    """
    Parse site string to know the fts server to use
    """
    country = site.split('_')[1]
    query = {'key':country}
    try:
        fts_server = db.loadView('asynctransfer_config', view, query)['rows'][0]['value']
    except IndexError:
        log.info("FTS server for %s is down" % country)
        fts_server = ''
    return fts_server

def execute_command(command):
    """
    _execute_command_
    Function to manage commands.
    """
    proc = subprocess.Popen(
           ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
           stdout=subprocess.PIPE,
           stderr=subprocess.PIPE,
           stdin=subprocess.PIPE,
    )
    proc.stdin.write(command)
    stdout, stderr = proc.communicate()
    rc = proc.returncode

    return stdout, stderr, rc

def getDNFromUserName(username, log, ckey = None, cert = None):
    """
    Parse site string to know the fts server to use
    """
    dn = ''
    site_db = SiteDBJSON(config={'key': ckey, 'cert': cert})
    try:
       dn = site_db.userNameDn(username)
    except IndexError:
       log.error("user does not exist")
       return dn
    except RuntimeError:
       log.error("SiteDB URL cannot be accessed")
       return dn
    return dn

def getProxy(defaultDelegation, log):
    """
    _getProxy_
    """
    log.debug("Retrieving proxy for %s" % defaultDelegation['userDN'])
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

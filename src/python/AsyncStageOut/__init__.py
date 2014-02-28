import hashlib
import subprocess
import os
import datetime
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

__version__ = '1.0.1'

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

def getDNFromUserName(username, log):
    """
    Parse site string to know the fts server to use
    """
    dn = ''
    site_db = SiteDBJSON()
    try:
       dn = site_db.userNameDn(username)
    except IndexError:
       log.error("user does not exist")
       return dn
    except RuntimeError:
       log.error("SiteDB URL cannot be accessed")
       return dn
    return dn

se_site_map = {}
se_site_map_last_update = None

def getSiteNamesFromSENames(sites, log, force=False):
    """
    When injecting from the WN, the CMS site name for the temporary stageout location is not known.
    Only the SE name is know.
    """

    def make_se_site_map():
        log.debug("Will generate SE-Site map")
        try:
            phedex = PhEDEx()
        except Exception, e:
            log.debug("Could not initialize PhEDEx!:" %e)
            return False
        global se_site_map
        se_site_map = {}
        nodes = phedex.getNodeMap()['phedex']['node']
        for node in nodes:
            se_site_map[str(node[u'se'])] = str(node[u'name'])
        if not se_site_map:
            return False
        global se_site_map_last_update
        se_site_map_last_update = datetime.datetime.now()
        return True

    make_map = force
    if not force:
        if not se_site_map:
            make_map = True
        else:
            # In any case, update the se_site_map every 1 day.
            interval_for_update = datetime.timedelta(days=1)
            now = datetime.datetime.now()
            if (type(se_site_map_last_update) != type(now)) or \
               ((now - se_site_map_last_update) >= interval_for_update):
                make_map = True
    if make_map and not make_se_site_map():
        log.debug("Could not generate SE-Site map!:")
    if not se_site_map:
        return []
    all_site_names = se_site_map.values()

    def keys_map(site):
        if (site in all_site_names) or (site[:3] in ['T1_','T2_','T3_']):
            return site
        if not se_site_map.has_key(site):
            log.debug("Name %s not found in the PhEDEx node map" % site)
            return ''
        return se_site_map[site]

    return map(keys_map,sites)


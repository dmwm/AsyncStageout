import hashlib

__version__ = '0.1.3'

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
        fts_server = db.loadView('AsyncTransfer', view, query)['rows'][0]['value']
    except IndexError:
        log.info("FTS server for %s is down" % country)
        fts_server = ''
    return fts_server

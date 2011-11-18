"""
Insert dummy data into statitics_database in CouchDB
python LoadStatDocuments.py --couchUrl=http://user:passwd@hostname:5184 --database=asynctransfer_stat
"""
import sys
import random
from WMCore.Configuration import loadConfigurationFile
import datetime
from WMQuality.TestInitCouchApp import CouchAppTestHarness
import logging
from WMCore.Database.CMSCouch import CouchServer, Database, Document
from optparse import OptionParser, TitledHelpFormatter

def _getDbConnection(couchUrl, dbName):
    """
    Check if the database exists, create if not.

    """
    couchServer = CouchServer(couchUrl)
    if not dbName in couchServer.listDatabases():
        logging.info("Database '%s' does not exits, creating it." % dbName)
        db = couchServer.createDatabase(dbName)
    else:
        logging.debug("Database '%s' exists." % dbName)
        db = Database(dbName, couchUrl)

    couchapps = "../../../src/couchapp"
    stat_couchapp = "%s/stat" % couchapps

    harness = CouchAppTestHarness(dbName, couchUrl)
    harness.create()
    harness.pushCouchapps(stat_couchapp)

    return couchServer, db

def _processAndStoreFile(couchServer, dbName, number):
    """
    Check if the database exists, create if not.
    """
    db = couchServer.connectDatabase(dbName)

    users = ['fred', 'barney', 'wilma', 'betty']
    sites = ['T2_DE_RWTH', 'T2_DE_DESY',
             'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR',
             'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome', 'T2_KR_KNU', 'T2_PK_NCP',
             'T2_UA_KIPT', 'T2_UK_London_Brunel', 'T2_UK_London_IC',
             'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech',
             'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue',
             'T2_US_UCSD', 'T2_US_Wisconsin']

    FTSserver = ['https://fts.cr.cnaf.infn.it:8443/glite-data-transfer-fts/services/FileTransfer',
                 'https://cmsfts1.fnal.gov:8443/glite-data-transfer-fts/services/FileTransfer',
                 'https://fts-fzk.gridka.de:8443/glite-data-transfer-fts/services/FileTransfer',
                 'https://cclcgftsprod.in2p3.fr:8443/glite-data-transfer-fts/services/FileTransfer',
                 'https://lcgfts.gridpp.rl.ac.uk:8443/glite-data-transfer-fts/services/FileTransfer']

    worfklow_base = 'Analysis_%s'
    docs_done_per_server = 10
    docs_failed_per_server = 15
    now = str(datetime.datetime.now())

    for server in FTSserver:
        for i in xrange(number):
            user = random.choice(users)
            file_doc = { "users": { user: [ worfklow_base % random.randint(1000, 9999) ] },
             	     "done": { "0_retry":  docs_done_per_server },
    	      	     "timing": { "avg_transfer_duration": random.randint(100, 200),
          		                 "max_transfer_duration": random.randint(200, 300),
    	  	                 "min_transfer_duration": random.randint(1, 100)},
    		     "sites_served": { random.choice(sites): { "failed": docs_failed_per_server,
               	    					       "done": docs_done_per_server },
    		    	               random.choice(sites): { "failed": docs_failed_per_server,
               						       "done": docs_done_per_server}

    				     },
    	             "day": "201%s-%s-%s" % (random.randint(0, 5), random.randint(1, 12), random.randint(1, 31)),
                         "fts": server,
                         "failed": { "0_retry":  docs_failed_per_server },
                         "avg_size": random.randint(1000000, 9999999)

                       }
            db.queue(file_doc, True, ['stat/transfersByFtsByDay'])

    db.commit()

def _processCmdLineArgs(args):
    usage = \
"""usage: %prog options"""
    form = TitledHelpFormatter(width = 78)
    parser = OptionParser(usage = usage, formatter = form)
    _defineCmdLineOptions(parser)

    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args = args)

    return opts.couchUrl, opts.database, opts.number

def _defineCmdLineOptions(parser):
    help = "CouchDB server URL (default: http://localhost:5984)"
    parser.add_option("-c", "--couchUrl", help=help, default="http://localhost:5984")
    help = "CouchDB database name (default: asyncstageout)"
    parser.add_option("-d", "--database", help=help, default="asyncstageout")
    help = "Number of transfers to simulate (default: 10)"
    parser.add_option("-n", "--number", help=help, type="int", default=10)

def main():
    couchUrl, dbName, number = _processCmdLineArgs(sys.argv)
    couchServer, couchDb = _getDbConnection(couchUrl, dbName)

    _processAndStoreFile(couchServer, dbName, number)
    logging.info("Finished.")

    #couchServer.deleteDatabase(dbName)

if __name__ == "__main__":
    main()

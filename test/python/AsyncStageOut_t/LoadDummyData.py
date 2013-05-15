"""
Insert dummy data into the AsyncTransfer CouchDB instance
"""
from AsyncStageOut import getHashLfn
import random
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile
import datetime

#config = loadConfigurationFile('../../../src/python/DefaultConfig.py')

config = loadConfigurationFile('config/asyncstageout/config.py')

server = CouchServer(config.AsyncTransfer.couch_instance)

db = server.connectDatabase(config.AsyncTransfer.files_database)

users = ['fred', 'barney', 'wilma', 'betty']
sites = ['T2_AT_Vienna', 'T2_BE_IIHE', 'T2_BE_UCL', 'T2_BR_SPRACE',
         'T2_BR_UERJ', 'T2_CH_CAF', 'T2_CH_CSCS', 'T2_CN_Beijing', 'T2_DE_DESY',
         'T2_DE_RWTH', 'T2_EE_Estonia', 'T2_ES_CIEMAT', 'T2_ES_IFCA',
         'T2_FI_HIP', 'T2_FR_CCIN2P3', 'T2_FR_GRIF_IRFU', 'T2_FR_GRIF_LLR',
         'T2_FR_IPHC', 'T2_HU_Budapest', 'T2_IN_TIFR', 'T2_IT_Bari',
         'T2_IT_Legnaro', 'T2_IT_Pisa', 'T2_IT_Rome', 'T2_KR_KNU', 'T2_PK_NCP',
         'T2_PL_Cracow', 'T2_PL_Warsaw', 'T2_PT_LIP_Lisbon', 'T2_PT_NCG_Lisbon',
         'T2_RU_IHEP', 'T2_RU_INR', 'T2_RU_ITEP', 'T2_RU_JINR', 'T2_RU_PNPI',
         'T2_RU_RRC_KI', 'T2_RU_SINP', 'T2_TR_METU', 'T2_TW_Taiwan',
         'T2_UA_KIPT', 'T2_UK_London_Brunel', 'T2_UK_London_IC',
         'T2_UK_SGrid_Bristol', 'T2_UK_SGrid_RALPP', 'T2_US_Caltech',
         'T2_US_Florida', 'T2_US_MIT', 'T2_US_Nebraska', 'T2_US_Purdue',
         'T2_US_UCSD', 'T2_US_Wisconsin']

state = [ 'new', 'acquired', 'done', 'failed' ]

size = 200 #TODO: read from script input
i = 1

# lfn_base has store/temp in it twice to make sure that
# the temp->permananet lfn change is correct.
lfn_base = '/store/temp/user/%s/my_cool_dataset/store/temp/file-%s-%s.root'
now = str(datetime.datetime.now())

while i <= size:
    id = random.randint(1000, 9999)
    user = random.choice(users)
    _id = getHashLfn(lfn_base % (user, id, i))
    states = random.choice(state)
    if states == 'done' or states == 'failed':
        file_doc = {'_id': '%s' % (_id) ,
	       	    'lfn': lfn_base % (user,
                                       id,
                                       i),
   		    'dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=spiga/CN=606831/CN=Daniele Spiga',
		    'checksums': {'adler32': 'ad:b2378cab'},
         	    'failure_reason': [],
	            'source': random.choice(sites),
                    'role': '',
		    'type': 'output',
		    'dbSource_url': 'Panda',
		    'inputdataset': 'NULL',
		    'publication_state': 'not_published',
		    'publication_retry_count': [],
		    'jobid': '%s' %(random.randint(1,1000)),
		    'destination': random.choice(sites),
                    'start_time' : now,
                    'end_time' : now,
                    'state' : states,
                    'dbSource_update' : now,
                    'workflow': 'CmsRunAnalysis-%s' %(random.randint(11,13)),
                    'retry_count': [],
                    'user': user,
                    'size': random.randint(1, 9999)
   		   }
    if states == 'new' or states == 'acquired':
        file_doc = {'_id': '%s' % (_id) ,
                    'lfn': lfn_base % (user,
                                       id,
                                       i),
                    'dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=spiga/CN=606831/CN=Daniele Spiga',
                    'checksums': {
                             "adler32": "ad:b2378cab"
                    },
                    "failure_reason": [
                    ],
                    'source': random.choice(sites),
                    'role': '',
                    'type': 'output',
                    'dbSource_url': 'Panda',
                    'inputdataset': 'NULL',
                    'publication_state': 'not_published',
                    'publication_retry_count': [
                    ],
                    'jobid': '%s' %(random.randint(1,1000)),
                    'destination': random.choice(sites),
                    'start_time' : now,
                    'state' : states,
                    'dbSource_update' : now,
                    'workflow': 'CmsRunAnalysis-%s' %(random.randint(11,13)),
                    'retry_count': [],
                    'user': user,
                    'size': random.randint(1, 9999)
                   }
    db.queue(file_doc, True, ['AsyncTransfer/ftscp'])
    i += 1

db.commit(viewlist=['AsyncTransfer/ftscp'])

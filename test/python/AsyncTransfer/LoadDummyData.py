import random
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile

config = loadConfigurationFile('../../../src/python/DefaultConfig.py')

server = CouchServer(config.AsyncTransfer.couch_instance)

db = server.connectDatabase(config.AsyncTransfer.couch_database)

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
size = 2000 #TODO: read from script input
i = 1

# lfn_base has store/temp in it twice to make sure that
# the temp->permananet lfn change is correct.
lfn_base = '/store/temp/user/%s/my_cool_dataset/store/temp/file-%s-%s.root' 

while i <= size:
    user = random.choice(users)
    file_doc = {'_id': lfn_base % (user, 
                                   random.randint(1000, 9999),
                                   i),
                'source': random.choice(sites),
                'destination': 'T2_UK_SGrid_Bristol',
                'user': user    
    }
    db.queue(file_doc, True, ['AsyncTransfer/ftscp'])
    i += 1
    
db.commit(viewlist=['AsyncTransfer/ftscp'])

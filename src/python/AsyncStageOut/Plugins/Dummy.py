#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
_DummySource_t_
Duplicate view from Dummy database
"""
import logging
import random
from AsyncStageOut.Plugins.Source import Source
from AsyncStageOut import getHashLfn
import time

class Dummy(Source):
    """
    _DummySource_
    Create dummy data to be stored in couch by the LFNDuplicatorPoller.
    """
    def __call__(self):
        """
        _getViewResults_
        Get the result of the view.
        """
        sites = ['T2_IT_Rome', 'T2_CH_CAF', 'T2_DE_DESY',
                 'T2_BR_UERJ', 'T2_CH_CSCS', 'T2_CN_Beijing', 'T2_DE_DESY',
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

        numberUsers = 5
        j = 1

        users = []
        while j <= numberUsers:

            users.append( 'user'+ str( random.randint(1, 1000) ) )
            j += 1

        size = 3

        i = 1

        lfn_base = '/store/temp/riahi/user/%s/store/temp/file-duplic-%s-%s.root'
        results = []

        while i <= size:

            last_update = int(time.time())
            user = random.choice(users)
            lfn = lfn_base % (user, random.randint(1000, 9999), i)
            id = getHashLfn( lfn )
            workflow = 'workflow-%s-%s' % (user, random.randint(1, 100))
            results.append( {'_id': id,
                        'source': random.choice(sites),
                        'destination': random.choice(sites),
                        'task': workflow,
                        'workflow': workflow,
                        'lfn': lfn,
                        'jobid': random.randint(1000, 9999),
                        'state': 'new',
                        'last_update':last_update,
                        'dbSource_update': last_update,
                        'retry_count': [],
                        'checksums': 'checksum',
                        'size': random.randint(1000, 9999),
                        'dn': '/UserDN',
                        'group': '',
                        'role': '',
                        'user': user}
            )

            i += 1

        logging.debug("Dummy docs queued %s" %results)
        return results


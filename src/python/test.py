"""
asoOracle test API
"""
#pylint: disable=C0103,W0105
#!/usr/bin/env python

# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division


from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping

server = HTTPRequests('cmsweb-testbed.cern.ch',
                      '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy',
                      '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy')

"""
fileDoc = {}
fileDoc['asoworker'] = 'asodciangot1'
fileDoc['subresource'] = 'acquireTransfers'

result = server.post('/crabserver/dev/filetransfers',
                     data=encodeRequest(fileDoc))


print(result)

fileDoc = {}
fileDoc['asoworker'] = 'asodciangot1'
fileDoc['subresource'] = 'acquiredTransfers'
fileDoc['grouping'] = 0

result = server.get('/crabserver/dev/filetransfers',
                    data=encodeRequest(fileDoc))

#print(oracleOutputMapping(result))

ids = [str(x['id']) for x in oracleOutputMapping(result)]

fileDoc = {}
fileDoc['subresource'] = 'groupedTransferStatistics'
fileDoc['grouping'] = 0

result = server.get('/crabserver/dev/filetransfers',
                    data=encodeRequest(fileDoc))
"""
#print (oracleOutputMapping(result))
fileDoc = {}
fileDoc['asoworker'] = 'asodciangot1'
fileDoc['subresource'] = 'updateTransfers'
fileDoc['list_of_ids'] = '64856469f4602d45c26a23bc6d3b94c3d5f47ba5143ddf84f8b3c1e4'
fileDoc['list_of_transfer_state'] = 4
fileDoc['retry_value'] = 1
fileDoc['fail_reason'] = 'fail_reason'
#print(encodeRequest(fileDoc))

fileDoc =  {'list_of_publication_state': 'FAILED', 'subresource': 'updatePublication', 'list_of_ids': '9327a427210deb30d5407500e8380ad6f8950999bc0facb51c00f343', 'asoworker': 'asodciangot1', 'list_of_failure_reason': 'Publication description files not found! Will force publication failure. Workflow 160804_141528:jmsilva_crab_HG1608a-rc1-MinBias_PrivateMC_EventBased-L-T_O-T_P-T_IL-F expired since 9h:40m:4s!', 'list_of_retry_value': 1}

result = server.post('/crabserver/preprod/filetransfers',
                     data=encodeRequest(fileDoc))
print(result)



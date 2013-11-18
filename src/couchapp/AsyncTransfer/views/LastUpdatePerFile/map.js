function(doc) {
        if( doc.lfn ){
                if ( doc['state'] == 'done') {
                        location = doc.destination;
                }
                else {
                        location = doc.source;
                }
                publish = 0
                if (doc['publish'] == 1){
      			publish = 1;
            	}
		else {
			publish = 0;
		}
                emit(doc.last_update, {"lfn": doc.lfn, "workflow": doc.workflow, "location": location, "checksum": doc.checksums, "jobid": doc.jobid, "retry_count": doc.retry_count.length+1, "size": doc.size, "type": doc.type, "id": doc._id, "state": doc.state, "failure_reason": doc.failure_reason, "publication_state": doc.publication_state, "publish": publish});
        }
}

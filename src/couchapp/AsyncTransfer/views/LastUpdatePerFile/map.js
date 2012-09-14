function complete_job(doc, req) {
        if ( doc['state'] != 'done' && doc['state'] != 'failed' ) {
                return false;
        }
        return true;
}

function(doc) {
	if(doc.lfn && complete_job(doc)){
		emit(doc.last_update, {"lfn": doc.lfn, "state": doc.state, "errors": doc.failure_reason, "workflow": doc.workflow, "location": doc.destination, "checksum": doc.checksums, "jobid": doc.jobid, "retry_count": doc.retry_count.length+1, "size": doc.size});
	}
}

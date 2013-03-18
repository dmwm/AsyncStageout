function(doc) {
        if( doc.lfn ){
                if ( doc['state'] == 'done') {
                        location = doc.destination;
                }
                else {
                        location = doc.source;
                }
                emit(doc.last_update, {"lfn": doc.lfn, "workflow": doc.workflow, "location": location, "checksum": doc.checksums, "jobid": doc.jobid, "retry_count": doc.retry_count.length+1, "size": doc.size, "type": doc.type, "id": doc._id, "state": doc.state});
        }
}

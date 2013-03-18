function(doc) {
if (doc.workflow) {
       if (doc.end_time && doc.state == 'done') { 
        emit([doc.workflow, doc.timestamp], {'lfn': doc.lfn, 'location': doc.destination, 'checksum': doc.checksum, 'size': doc.size, 'jobid': doc.jobid});
        }
        else {
        emit([doc.workflow, doc.timestamp], {'lfn': doc.lfn, 'location': doc.source, 'checksum': doc.checksum, 'size': doc.size, 'jobid': doc.jobid});
        } 
              
}
}

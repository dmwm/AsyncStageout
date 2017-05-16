function(doc) {
        if (doc.state != 'failed' && doc.state != 'done' && doc.lfn) {
                emit([doc.user, doc.group, doc.role, doc.start_time.split('.')[0]], 1);
        }
}

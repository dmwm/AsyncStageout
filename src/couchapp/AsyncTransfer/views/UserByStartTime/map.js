function(doc) {
        if (((doc.state != 'failed' && doc.state != 'done') || (doc.publication_state != 'publication_done' && doc.publication_state != 'publication_failed')) && doc.lfn) {
                emit([doc.user, doc.group, doc.role, doc.job_end_time], 1);
        }
}

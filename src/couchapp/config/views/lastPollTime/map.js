function(doc) {
        if (doc.lfn) {
                emit(doc.job_end_time, doc._id) ;
        }
}

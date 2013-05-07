function(doc) {
        if (doc.type == "aso_file") {
                emit(doc.jobid, doc.lfn);
        }
}

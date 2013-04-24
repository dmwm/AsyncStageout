function(doc) {
        if (doc.type == "aso_file" && doc.state=='failed') {
                emit(doc.jobid, doc.lfn);
        }
}

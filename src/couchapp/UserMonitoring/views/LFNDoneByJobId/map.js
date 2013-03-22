function(doc) {
        if (doc.type == "aso_file" && doc.state=='done') {
                emit(doc.jobid, doc.lfn);
        }
}

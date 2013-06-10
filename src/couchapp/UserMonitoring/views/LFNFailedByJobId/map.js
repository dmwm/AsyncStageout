function(doc) {
        if (doc.type == "aso_file"){
                if (doc.file_type == "output" && (doc.state=='failed' || doc.publication_state=='publication_failed')){
                        emit(doc.jobid, doc.lfn);
                }
                if (doc.file_type == "log" && doc.state == "failed") {
                        emit(doc.jobid, doc.lfn);
                }

        }
}

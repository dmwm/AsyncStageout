function(doc) {
        if (doc.type == "aso_file" && doc.file_type == "output" && (doc.publication_state=='published'|| (doc.state=='done' && doc.publish == 0))) {
                emit(doc.jobid, doc.lfn);
        }
   if (doc.type == "aso_file" && doc.file_type == "log" && doc.state == "done") {
        emit(doc.jobid, doc.lfn);
   }
}

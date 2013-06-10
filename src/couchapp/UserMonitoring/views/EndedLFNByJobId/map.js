function(doc) {
   if (doc.type == "aso_file") {
        if ( (doc.file_type == "output" && (doc.publication_state == "published" || doc.state == "failed" || doc.publication_state == "publication_failed" || (doc.state == "done" && doc.publish == 0))) || (doc.file_type == "log" && (doc.state == "done" || doc.state == "failed"))){
                emit(doc.jobid, 1);
        }
   }
}

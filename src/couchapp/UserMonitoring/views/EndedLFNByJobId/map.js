function(doc) {
   if (doc.type == "aso_file" && (doc.state == "done" || doc.state == "failed")) { 
        emit(doc.jobid, 1);
   }
}

function(doc) {
        if (doc.type == "aso_file" && (doc.state == 'done' || doc.state == 'failed' || doc.state == 'killed') ) {
                emit(doc.last_update, doc.jobid);
        }
}

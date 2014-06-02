function(doc) {
        if (doc.lfn) {
                emit([doc.state, doc.user, doc.workflow, doc.source, doc.destination], 1);
        }
}

function(doc) {
        if (doc.lfn) {
                emit([doc.state, doc.user, doc.task, doc.source, doc.destination], 1);
        }
}

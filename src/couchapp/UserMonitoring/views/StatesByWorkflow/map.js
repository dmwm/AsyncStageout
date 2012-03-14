function(doc) {
        if (doc.state) {
                emit(doc._id, doc.state);
        }
}


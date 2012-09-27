function(doc) {
        if (doc.state) {
                emit(doc._id, doc.publication_state);
        }
}


function maybe_first_in_object(ob) {
    for (var props in ob) {
        return props;
    }
}

function(doc) {
        if (doc._attachments && doc.state == 'done') {
                first = maybe_first_in_object(doc._attachments);
                emit( doc.workflow, {docid: doc._id, attachments: first});
        }
}

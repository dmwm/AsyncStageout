function(doc) {
        if (doc.priority == 'high') {
        	emit([doc._id, doc.group, doc.role],1);
        }
}

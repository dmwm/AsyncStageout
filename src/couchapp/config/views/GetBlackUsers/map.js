function(doc) {
        if (doc.experiment) {
        	emit([doc._id, doc.group, doc.role], 1);
        }
}

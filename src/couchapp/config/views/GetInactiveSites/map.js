function(doc) {
        if (doc.state == 'inactive') {
        	emit(doc._id, 1);
        }
}

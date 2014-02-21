function(doc) {
        if ((doc.state == 'new' || doc.state == 'retry') && doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.destination, doc.source], doc.lfn);
	}
}

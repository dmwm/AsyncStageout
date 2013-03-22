function(doc) {
        if (doc.state != 'failed' && doc.state != 'done' && doc.state != 'in transfer' && doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.source], 1);
	}
}

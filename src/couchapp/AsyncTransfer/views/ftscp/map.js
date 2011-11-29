function(doc) {
        if (doc.state != 'failed' && doc.state != 'done' && doc.lfn) {
		emit([doc.user, doc.destination, doc.source, doc.dn, doc.group, doc.role], doc.lfn);
	}
}

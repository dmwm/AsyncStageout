function(doc) {
        if (doc.state != 'failed' && doc.state != 'done' && doc.state != 'killed' && doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.destination, doc.source, doc.dn], doc.lfn);
	}
}

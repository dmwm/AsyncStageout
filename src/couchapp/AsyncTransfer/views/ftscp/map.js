function(doc) {
        if (doc.state != 'failed' && doc.state != 'done') {
		emit([doc.user, doc.destination, doc.source, doc.dn], doc.lfn);
	}
}

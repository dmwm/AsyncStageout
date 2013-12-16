function(doc) {
        if (doc.state != 'failed' && doc.state != 'done' && doc.state != 'killed' && doc.lfn && doc.retry_count.length < 1) {
		emit([doc.user, doc.group, doc.role, doc.destination, doc.source], doc.lfn);
	}
}

function(doc) {
        if (doc.state == 'acquired'&& doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.destination, doc.source], doc.lfn);
	}
}

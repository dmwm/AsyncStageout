function(doc) {
	if (doc.state != 'failed') {
		emit([doc.user, doc.destination, doc.source, doc.dn], doc._id);
	}
}

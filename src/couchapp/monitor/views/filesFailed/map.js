function(doc) {
	if (doc.state == 'failed') {
		emit([doc.user, doc.task, doc.source, doc.destination], 1);
	}
}

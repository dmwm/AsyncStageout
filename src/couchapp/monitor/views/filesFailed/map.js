function(doc) {
	if (doc.state == 'failed') {
		emit([doc.user, doc.workflow, doc.source, doc.destination], 1);
	}
}

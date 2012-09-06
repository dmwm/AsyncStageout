function(doc) {
	if (doc.state == 'new') {
		emit([doc.user, doc.workflow, doc.source, doc.destination], 1);
	}
}

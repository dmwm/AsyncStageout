function(doc) {
	if (doc.state == 'done') {
		emit([doc.user, doc.workflow, doc.source, doc.destination], 1);
	}
}

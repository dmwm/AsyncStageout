function(doc) {
	if (doc.state == 'done') {
		emit([doc.user, doc.task, doc.source, doc.destination], 1);
	}
}

function(doc) {
	if (doc.state == 'acquired') {
		emit([doc.user, doc.workflow, doc.source, doc.destination], 1);
	}
}

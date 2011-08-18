function(doc) {
	if (doc.state) {
		emit([doc.lfn], {"state": doc.state, "destination": doc.destination, "source": doc.source, "user": doc.user, "task": doc.task});
	}
}

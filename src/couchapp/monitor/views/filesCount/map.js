function(doc) {
	if (doc.lfn) {
		emit([doc.lfn], {"state": doc.state, "destination": doc.destination, "source": doc.source, "user": doc.user, "task": doc.workflow});
	}
}

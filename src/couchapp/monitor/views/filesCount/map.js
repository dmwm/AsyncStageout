function(doc) {
	if (doc.state) {
		emit([doc._id], {"state": doc.state, "destination": doc.destination, "source": doc.source, "user": doc.user, "task": doc.task});
	}
}

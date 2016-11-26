function(doc) {
	if (doc.state == 'retry' && doc.lfn) {
		emit(doc._id, doc.last_update);
	}
}

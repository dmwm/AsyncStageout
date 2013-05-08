function(doc) {
	if (doc.workflow && doc.state=='failed'){
		emit(doc.workflow, doc._id);
	}
}

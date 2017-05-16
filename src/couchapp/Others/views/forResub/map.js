function(doc) {
	if (doc.workflow && (doc.state=='failed'||doc.state=='killed')){
		emit(doc.workflow, doc._id);
	}
}

function(doc) {
	if (doc.workflow && (doc.state=='new' || doc.state=='acquired')){
		emit(doc.workflow,doc._id);
	}
}

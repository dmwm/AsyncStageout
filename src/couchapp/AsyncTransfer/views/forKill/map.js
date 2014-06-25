function(doc) {
	if (doc.workflow && (doc.state=='new' || doc.state=='acquired' || doc.state=='retry')){
		emit(doc.workflow,doc._id);
	}
}

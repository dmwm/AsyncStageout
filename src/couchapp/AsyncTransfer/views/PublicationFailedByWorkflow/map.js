function(doc) {
	if(doc.workflow){
		if(doc.publication_state=='publication_failed'){
			emit(doc.workflow, doc._id);
		}
	}
}

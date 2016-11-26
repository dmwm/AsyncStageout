function(doc) {
	if(doc.publish == 1 && doc.type == 'output'){
		emit(doc.workflow, doc.publication_state);
	}
}

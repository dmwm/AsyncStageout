function(doc) {
	if(doc.publication_state && doc.publication_state != 'not_published'){
		emit(doc.workflow, doc.publication_state);
	}
}

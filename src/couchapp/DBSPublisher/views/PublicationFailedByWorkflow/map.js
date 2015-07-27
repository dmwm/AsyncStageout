function(doc) {
	if(doc.workflow){
		if(doc.publication_state=='publication_failed' && doc.publication_failure_reason){
			emit([doc.workflow,doc.publication_failure_reason], 1);
		}
	}
}

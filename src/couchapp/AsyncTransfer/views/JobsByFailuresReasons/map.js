function(doc) {
	if(doc.failure_reason){
		emit([doc.workflow, doc.failure_reason], 1);
	}
}

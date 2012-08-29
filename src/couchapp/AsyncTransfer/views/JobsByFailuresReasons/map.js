function(doc) {
	if(doc.workflow){
		emit([doc.workflow, doc.failure_reason], 1);
	}
}
